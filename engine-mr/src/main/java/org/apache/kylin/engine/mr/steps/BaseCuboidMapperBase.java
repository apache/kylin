/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 */
abstract public class BaseCuboidMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(BaseCuboidMapperBase.class);
    public static final String HIVE_NULL = "\\N";
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String cubeName;
    protected String segmentID;
    protected Cuboid baseCuboid;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected Set<String> nullStrs;
    protected CubeJoinedFlatTableEnrich intermediateTableDesc;
    protected String intermediateTableRowDelimiter;
    protected byte byteRowDelimiter;
    protected int counter;
    protected MeasureIngester<?>[] aggrIngesters;
    protected Map<TblColRef, Dictionary<String>> dictionaryMap;
    protected Object[] measures;
    protected AbstractRowKeyEncoder rowKeyEncoder;
    protected BufferedMeasureCodec measureCodec;
    private int errorRecordCounter;
    protected Text outputKey = new Text();
    protected Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        intermediateTableRowDelimiter = context.getConfiguration().get(BatchConstants.CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER, Character.toString(BatchConstants.INTERMEDIATE_TABLE_ROW_DELIMITER));
        if (Bytes.toBytes(intermediateTableRowDelimiter).length > 1) {
            throw new RuntimeException("Expected delimiter byte length is 1, but got " + Bytes.toBytes(intermediateTableRowDelimiter).length);
        }

        byteRowDelimiter = Bytes.toBytes(intermediateTableRowDelimiter)[0];

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegmentById(segmentID);

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        intermediateTableDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(cubeSegment), cubeDesc);

        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        measureCodec = new BufferedMeasureCodec(cubeDesc.getMeasures());
        measures = new Object[cubeDesc.getMeasures().size()];

        aggrIngesters = MeasureIngester.create(cubeDesc.getMeasures());
        dictionaryMap = cubeSegment.buildDictionaryMap();

        initNullBytes();
    }

    private void initNullBytes() {
        nullStrs = Sets.newHashSet();
        nullStrs.add(HIVE_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullStrs.add(s);
            }
        }
    }

    protected boolean isNull(String v) {
        return nullStrs.contains(v);
    }

    protected byte[] buildKey(String[] flatRow) {
        int[] rowKeyColumnIndexes = intermediateTableDesc.getRowKeyColumnIndexes();
        List<TblColRef> columns = baseCuboid.getColumns();
        String[] colValues = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            colValues[i] = getCell(rowKeyColumnIndexes[i], flatRow);
        }
        return rowKeyEncoder.encode(colValues);
    }

    private ByteBuffer buildValue(String[] flatRow) {

        for (int i = 0; i < measures.length; i++) {
            measures[i] = buildValueOf(i, flatRow);
        }

        return measureCodec.encode(measures);
    }

    private Object buildValueOf(int idxOfMeasure, String[] flatRow) {
        MeasureDesc measure = cubeDesc.getMeasures().get(idxOfMeasure);
        FunctionDesc function = measure.getFunction();
        int[] colIdxOnFlatTable = intermediateTableDesc.getMeasureColumnIndexes()[idxOfMeasure];

        int paramCount = function.getParameterCount();
        String[] inputToMeasure = new String[paramCount];

        // pick up parameter values
        ParameterDesc param = function.getParameter();
        int colParamIdx = 0; // index among parameters of column type
        for (int i = 0; i < paramCount; i++, param = param.getNextParameter()) {
            String value;
            if (function.isCount()) {
                value = "1";
            } else if (param.isColumnType()) {
                value = getCell(colIdxOnFlatTable[colParamIdx++], flatRow);
            } else {
                value = param.getValue();
            }
            inputToMeasure[i] = value;
        }

        return aggrIngesters[idxOfMeasure].valueOf(inputToMeasure, measure, dictionaryMap);
    }

    private String getCell(int i, String[] flatRow) {
        if (isNull(flatRow[i]))
            return null;
        else
            return flatRow[i];
    }

    protected void outputKV(String[] flatRow, Context context) throws IOException, InterruptedException {
        byte[] rowKey = buildKey(flatRow);
        outputKey.set(rowKey, 0, rowKey.length);

        ByteBuffer valueBuf = buildValue(flatRow);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(outputKey, outputValue);
    }

    protected void handleErrorRecord(String[] flatRow, Exception ex) throws IOException {

        logger.error("Insane record: " + Arrays.toString(flatRow), ex);

        // TODO expose errorRecordCounter as hadoop counter
        errorRecordCounter++;
        if (errorRecordCounter > BatchConstants.ERROR_RECORD_LOG_THRESHOLD) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
