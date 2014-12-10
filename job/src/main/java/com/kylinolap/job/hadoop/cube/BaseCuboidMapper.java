/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.hadoop.cube;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.common.BytesSplitter;
import com.kylinolap.cube.common.SplittedBytes;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.AbstractRowKeyEncoder;
import com.kylinolap.cube.kv.RowConstants;
import com.kylinolap.cube.measure.MeasureCodec;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.MeasureDesc;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.hadoop.hive.JoinedFlatTableDesc;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.ParameterDesc;

/**
 * @author George Song (ysong1)
 */
public class BaseCuboidMapper<KEYIN> extends Mapper<KEYIN, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(BaseCuboidMapper.class);

    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final byte[] ONE = Bytes.toBytes("1");

    private String cubeName;
    private String segmentName;
    private Cuboid baseCuboid;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;
    private List<byte[]> nullBytes;

    private JoinedFlatTableDesc intermediateTableDesc;
    private String intermediateTableRowDelimiter;
    private byte byteRowDelimiter;

    private int counter;
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private Object[] measures;
    private byte[][] keyBytesBuf;
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

    private BytesSplitter bytesSplitter;
    private AbstractRowKeyEncoder rowKeyEncoder;
    private MeasureCodec measureCodec;

    @Override
    protected void setup(Context context) throws IOException {
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        intermediateTableRowDelimiter = context.getConfiguration().get(BatchConstants.CFG_CUBE_INTERMEDIATE_TABLE_ROW_DELIMITER, Character.toString(BatchConstants.INTERMEDIATE_TABLE_ROW_DELIMITER));
        if (Bytes.toBytes(intermediateTableRowDelimiter).length > 1) {
            throw new RuntimeException("Expected delimiter byte length is 1, but got " + Bytes.toBytes(intermediateTableRowDelimiter).length);
        }

        byteRowDelimiter = Bytes.toBytes(intermediateTableRowDelimiter)[0];

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(context.getConfiguration());

        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegment(segmentName, CubeSegmentStatusEnum.NEW);

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        intermediateTableDesc = new JoinedFlatTableDesc(cube.getDescriptor(), cubeSegment);

        bytesSplitter = new BytesSplitter(200, 4096);
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        measureCodec = new MeasureCodec(cubeDesc.getMeasures());
        measures = new Object[cubeDesc.getMeasures().size()];

        int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        keyBytesBuf = new byte[colCount][];

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
        // column values
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

        if (func.isCount() || func.isHolisticCountDistinct()) {
            // note for holistic count distinct, this value will be ignored
            result = ONE;
        }

        if (isNull(result)) {
            result = null;
        }

        return result;
    }

    @Override
    public void map(KEYIN key, Text value, Context context) throws IOException, InterruptedException {
        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }

        bytesSplitter.split(value.getBytes(), value.getLength(), byteRowDelimiter);

        byte[] rowKey = buildKey(bytesSplitter.getSplitBuffers());
        outputKey.set(rowKey, 0, rowKey.length);

        buildValue(bytesSplitter.getSplitBuffers());
        outputValue.set(valueBuf.array(), 0, valueBuf.position());

        context.write(outputKey, outputValue);
    }
}
