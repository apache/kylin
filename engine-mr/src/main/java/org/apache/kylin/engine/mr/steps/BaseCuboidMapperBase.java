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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.SplittedBytes;
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
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class BaseCuboidMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(BaseCuboidMapperBase.class);
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String cubeName;
    protected String segmentID;
    protected Cuboid baseCuboid;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected List<byte[]> nullBytes;
    protected CubeJoinedFlatTableEnrich intermediateTableDesc;
    protected String intermediateTableRowDelimiter;
    protected byte byteRowDelimiter;
    protected int counter;
    protected MeasureIngester<?>[] aggrIngesters;
    protected Map<TblColRef, Dictionary<String>> dictionaryMap;
    protected Object[] measures;
    protected byte[][] keyBytesBuf;
    protected BytesSplitter bytesSplitter;
    protected AbstractRowKeyEncoder rowKeyEncoder;
    protected BufferedMeasureEncoder measureCodec;
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

        bytesSplitter = new BytesSplitter(200, 16384);
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        measureCodec = new BufferedMeasureEncoder(cubeDesc.getMeasures());
        measures = new Object[cubeDesc.getMeasures().size()];

        int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        keyBytesBuf = new byte[colCount][];

        aggrIngesters = MeasureIngester.create(cubeDesc.getMeasures());
        dictionaryMap = cubeSegment.buildDictionaryMap();

        initNullBytes();
    }

    private void initNullBytes() {
        nullBytes = Lists.newArrayList();
        nullBytes.add(HIVE_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullBytes.add(Bytes.toBytes(s));
            }
        }
    }

    protected boolean isNull(byte[] v) {
        for (byte[] nullByte : nullBytes) {
            if (Bytes.equals(v, nullByte))
                return true;
        }
        return false;
    }

    protected byte[] buildKey(SplittedBytes[] splitBuffers) {
        int[] rowKeyColumnIndexes = intermediateTableDesc.getRowKeyColumnIndexes();
        for (int i = 0; i < baseCuboid.getColumns().size(); i++) {
            int index = rowKeyColumnIndexes[i];
            keyBytesBuf[i] = Arrays.copyOf(splitBuffers[index].value, splitBuffers[index].length);
            if (isNull(keyBytesBuf[i])) {
                keyBytesBuf[i] = null;
            }
        }
        return rowKeyEncoder.encode(keyBytesBuf);
    }

    private ByteBuffer buildValue(SplittedBytes[] splitBuffers) {

        for (int i = 0; i < measures.length; i++) {
            measures[i] = buildValueOf(i, splitBuffers);
        }

        return measureCodec.encode(measures);
    }

    private Object buildValueOf(int idxOfMeasure, SplittedBytes[] splitBuffers) {
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
                value = getCell(colIdxOnFlatTable[colParamIdx++], splitBuffers);
            } else {
                value = param.getValue();
            }
            inputToMeasure[i] = value;
        }

        return aggrIngesters[idxOfMeasure].valueOf(inputToMeasure, measure, dictionaryMap);
    }

    private String getCell(int i, SplittedBytes[] splitBuffers) {
        byte[] bytes = Arrays.copyOf(splitBuffers[i].value, splitBuffers[i].length);
        if (isNull(bytes))
            return null;
        else
            return Bytes.toString(bytes);
    }

    protected void outputKV(Context context) throws IOException, InterruptedException {
        intermediateTableDesc.sanityCheck(bytesSplitter);

        byte[] rowKey = buildKey(bytesSplitter.getSplitBuffers());
        outputKey.set(rowKey, 0, rowKey.length);

        ByteBuffer valueBuf = buildValue(bytesSplitter.getSplitBuffers());
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(outputKey, outputValue);
    }

    protected byte[][] convertUTF8Bytes(String[] row) throws UnsupportedEncodingException {
        byte[][] result = new byte[row.length][];
        for (int i = 0; i < row.length; i++) {
            result[i] = row[i] == null ? HIVE_NULL : row[i].getBytes("UTF-8");
        }
        return result;
    }

    protected void handleErrorRecord(BytesSplitter bytesSplitter, Exception ex) throws IOException {

        logger.error("Insane record: " + bytesSplitter, ex);

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
