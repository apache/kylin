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

import com.google.common.collect.Maps;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 */
public class BaseCuboidMapperBase<KEYIN, VALUEIN> extends KylinMapper<KEYIN, VALUEIN, Text, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(HiveToBaseCuboidMapper.class);
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final byte[] ONE = Bytes.toBytes("1");
    protected String cubeName;
    protected String segmentName;
    protected Cuboid baseCuboid;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected CubeSegment cubeSegment;
    protected List<byte[]> nullBytes;
    protected CubeJoinedFlatTableDesc intermediateTableDesc;
    protected String intermediateTableRowDelimiter;
    protected byte byteRowDelimiter;
    protected int counter;
    protected Object[] measures;
    protected byte[][] keyBytesBuf;
    protected BytesSplitter bytesSplitter;
    protected AbstractRowKeyEncoder rowKeyEncoder;
    protected MeasureCodec measureCodec;
    private int errorRecordCounter;
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private Map<Integer, Dictionary<String>> topNLiteralColDictMap;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        intermediateTableRowDelimiter = context.getConfiguration().get(BatchConstants.CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER, Character.toString(BatchConstants.INTERMEDIATE_TABLE_ROW_DELIMITER));
        if (Bytes.toBytes(intermediateTableRowDelimiter).length > 1) {
            throw new RuntimeException("Expected delimiter byte length is 1, but got " + Bytes.toBytes(intermediateTableRowDelimiter).length);
        }

        byteRowDelimiter = Bytes.toBytes(intermediateTableRowDelimiter)[0];

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        intermediateTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), cubeSegment);

        bytesSplitter = new BytesSplitter(200, 4096);
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        measureCodec = new MeasureCodec(cubeDesc.getMeasures());
        measures = new Object[cubeDesc.getMeasures().size()];

        int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        keyBytesBuf = new byte[colCount][];

        initTopNLiteralColDictionaryMap();
        initNullBytes();
    }
    
    private void initTopNLiteralColDictionaryMap() {
        topNLiteralColDictMap = Maps.newHashMap();
        for (int measureIdx = 0; measureIdx < measures.length; measureIdx++) {
            MeasureDesc measureDesc = cubeDesc.getMeasures().get(measureIdx);
            FunctionDesc func = measureDesc.getFunction();
            if (func.isTopN()) {
                int[] flatTableIdx = intermediateTableDesc.getMeasureColumnIndexes()[measureIdx];
                int literalColIdx = flatTableIdx[flatTableIdx.length - 1];
                TblColRef literalCol = func.getTopNLiteralColumn();
                Dictionary<String> dictionary = (Dictionary<String>)cubeSegment.getDictionary(literalCol);
                topNLiteralColDictMap.put(literalColIdx, dictionary);
            }
        }
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

    private boolean isNull(byte[] v) {
        for (byte[] nullByte : nullBytes) {
            if (Bytes.equals(v, nullByte))
                return true;
        }
        return false;
    }

    private byte[] buildKey(SplittedBytes[] splitBuffers) {
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

    private void buildValue(SplittedBytes[] splitBuffers) {

        for (int i = 0; i < measures.length; i++) {
            byte[] valueBytes = getValueBytes(splitBuffers, i);
            measures[i] = measureCodec.getSerializer(i).valueOf(valueBytes);
        }

        valueBuf.clear();
        measureCodec.encode(measures, valueBuf);
    }

    private byte[] getValueBytes(SplittedBytes[] splitBuffers, int measureIdx) {
        MeasureDesc desc = cubeDesc.getMeasures().get(measureIdx);
        FunctionDesc func = desc.getFunction();
        ParameterDesc paramDesc = func.getParameter();
        int[] flatTableIdx = intermediateTableDesc.getMeasureColumnIndexes()[measureIdx];

        byte[] result = null;

        // constant
        if (flatTableIdx == null) {
            result = Bytes.toBytes(paramDesc.getValue());
        } 
        // count and count distinct
        else if (func.isCount() || func.isHolisticCountDistinct()) {
            // note for holistic count distinct, this value will be ignored
            result = ONE;
        }
        // topN, need encode the key column
        else if(func.isTopN()) {
            // encode the key column with dict, and get the counter column;
            int keyColIndex = flatTableIdx[flatTableIdx.length - 1];
            Dictionary<String> literalColDict = topNLiteralColDictMap.get(keyColIndex);
            int keyColEncoded = literalColDict.getIdFromValue(Bytes.toString(splitBuffers[keyColIndex].value));
            valueBuf.clear();
            valueBuf.putInt(literalColDict.getSizeOfId());
            valueBuf.putInt(keyColEncoded);
            if (flatTableIdx.length == 1) {
                // only literalCol, use 1.0 as counter
                valueBuf.putDouble(1.0);
            } else {
                // get the counter column value
                valueBuf.putDouble(Double.valueOf(Bytes.toString(splitBuffers[flatTableIdx[0]].value)));
            }
            
            result = valueBuf.array();
            
        } 
        // normal case, concat column values
        else {
                // for multiple columns, their values are joined
                for (int i = 0; i < flatTableIdx.length; i++) {
                    SplittedBytes split = splitBuffers[flatTableIdx[i]];
                    if (result == null) {
                        result = Arrays.copyOf(split.value, split.length);
                    } else {
                        byte[] newResult = new byte[result.length + split.length];
                        System.arraycopy(result, 0, newResult, 0, result.length);
                        System.arraycopy(split.value, 0, newResult, result.length, split.length);
                        result = newResult;
                    }
                }
            }

        

        if (isNull(result)) {
            result = null;
        }

        return result;
    }

    protected void outputKV(Context context) throws IOException, InterruptedException {
        intermediateTableDesc.sanityCheck(bytesSplitter);

        byte[] rowKey = buildKey(bytesSplitter.getSplitBuffers());
        outputKey.set(rowKey, 0, rowKey.length);

        buildValue(bytesSplitter.getSplitBuffers());
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(outputKey, outputValue);
    }

    protected void handleErrorRecord(BytesSplitter bytesSplitter, Exception ex) throws IOException {

        logger.error("Insane record: " + bytesSplitter, ex);

        // TODO expose errorRecordCounter as hadoop counter
        errorRecordCounter++;
        if (errorRecordCounter > BatchConstants.ERROR_RECORD_THRESHOLD) {
            if (ex instanceof IOException)
                throw (IOException) ex;
            else if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
