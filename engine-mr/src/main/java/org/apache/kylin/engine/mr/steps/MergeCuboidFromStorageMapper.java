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
import java.util.HashMap;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.topn.Counter;
import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMROutput2.IMRStorageInputFormat;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * @author shaoshi
 */
public class MergeCuboidFromStorageMapper extends KylinMapper<Object, Object, ByteArrayWritable, ByteArrayWritable> {

    private static final Logger logger = LoggerFactory.getLogger(MergeCuboidFromStorageMapper.class);

    private KylinConfig config;
    private String cubeName;
    private String segmentName;
    private CubeManager cubeManager;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment mergedCubeSegment;
    private CubeSegment sourceCubeSegment; // Must be unique during a mapper's life cycle
    private IMRStorageInputFormat storageInputFormat;

    private ByteArrayWritable outputKey = new ByteArrayWritable();
    private byte[] newKeyBodyBuf;
    private ByteArray newKeyBuf;
    private RowKeySplitter rowKeySplitter;
    private RowKeyEncoderProvider rowKeyEncoderProvider;

    private HashMap<TblColRef, Boolean> dimensionsNeedDict = new HashMap<TblColRef, Boolean>();

    private List<MeasureDesc> measureDescs;
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private MeasureCodec codec;
    private ByteArrayWritable outputValue = new ByteArrayWritable();

    private List<Integer> topNMeasureIdx;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.bindCurrentConfiguration(context.getConfiguration());
        config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME).toUpperCase();

        cubeManager = CubeManager.getInstance(config);
        cube = cubeManager.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        mergedCubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        storageInputFormat = MRUtil.getBatchMergeInputSide2(mergedCubeSegment).getStorageInputFormat();

        newKeyBodyBuf = new byte[RowConstants.ROWKEY_BUFFER_SIZE]; // size will auto-grow
        newKeyBuf = ByteArray.allocate(RowConstants.ROWKEY_BUFFER_SIZE);

        sourceCubeSegment = storageInputFormat.findSourceSegment(context);
        logger.info("Source cube segment: " + sourceCubeSegment);

        rowKeySplitter = new RowKeySplitter(sourceCubeSegment, 65, 255);
        rowKeyEncoderProvider = new RowKeyEncoderProvider(mergedCubeSegment);

        measureDescs = cubeDesc.getMeasures();
        codec = new MeasureCodec(measureDescs);

        topNMeasureIdx = Lists.newArrayList();
        for (int i = 0; i < measureDescs.size(); i++) {
            if (measureDescs.get(i).getFunction().isTopN()) {
                topNMeasureIdx.add(i);
            }
        }
    }

    @Override
    public void map(Object inKey, Object inValue, Context context) throws IOException, InterruptedException {
        Pair<ByteArrayWritable, Object[]> pair = storageInputFormat.parseMapperInput(inKey, inValue);
        ByteArrayWritable key = pair.getFirst();
        Object[] value = pair.getSecond();

        Preconditions.checkState(key.offset() == 0);

        long cuboidID = rowKeySplitter.split(key.array());
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidID);
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(cuboid);

        SplittedBytes[] splittedByteses = rowKeySplitter.getSplitBuffers();
        int bufOffset = 0;
        int bodySplitOffset = rowKeySplitter.getBodySplitOffset();

        for (int i = 0; i < cuboid.getColumns().size(); ++i) {
            int useSplit = i + bodySplitOffset;
            TblColRef col = cuboid.getColumns().get(i);

            if (this.checkNeedMerging(col)) {
                // if dictionary on fact table column, needs rewrite
                DictionaryManager dictMgr = DictionaryManager.getInstance(config);
                Dictionary<?> sourceDict = dictMgr.getDictionary(sourceCubeSegment.getDictResPath(col));
                Dictionary<?> mergedDict = dictMgr.getDictionary(mergedCubeSegment.getDictResPath(col));

                while (sourceDict.getSizeOfValue() > newKeyBodyBuf.length - bufOffset || //
                        mergedDict.getSizeOfValue() > newKeyBodyBuf.length - bufOffset || //
                        mergedDict.getSizeOfId() > newKeyBodyBuf.length - bufOffset) {
                    //also use this buf to hold value before translating
                    byte[] oldBuf = newKeyBodyBuf;
                    newKeyBodyBuf = new byte[2 * newKeyBodyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBodyBuf, 0, oldBuf.length);
                }

                int idInSourceDict = BytesUtil.readUnsigned(splittedByteses[useSplit].value, 0, splittedByteses[useSplit].length);
                int size = sourceDict.getValueBytesFromId(idInSourceDict, newKeyBodyBuf, bufOffset);

                int idInMergedDict;
                if (size < 0) {
                    idInMergedDict = mergedDict.nullId();
                } else {
                    idInMergedDict = mergedDict.getIdFromValueBytes(newKeyBodyBuf, bufOffset, size);
                }
                BytesUtil.writeUnsigned(idInMergedDict, newKeyBodyBuf, bufOffset, mergedDict.getSizeOfId());

                bufOffset += mergedDict.getSizeOfId();
            } else {
                // keep as it is
                while (splittedByteses[useSplit].length > newKeyBodyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBodyBuf;
                    newKeyBodyBuf = new byte[2 * newKeyBodyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBodyBuf, 0, oldBuf.length);
                }

                System.arraycopy(splittedByteses[useSplit].value, 0, newKeyBodyBuf, bufOffset, splittedByteses[useSplit].length);
                bufOffset += splittedByteses[useSplit].length;
            }
        }

        int fullKeySize = rowkeyEncoder.getBytesLength();
        while (newKeyBuf.array().length < fullKeySize) {
            newKeyBuf.set(new byte[newKeyBuf.length() * 2]);
        }
        newKeyBuf.set(0, fullKeySize);

        rowkeyEncoder.encode(new ByteArray(newKeyBodyBuf, 0, bufOffset), newKeyBuf);
        outputKey.set(newKeyBuf.array(), 0, fullKeySize);

        // re-encode measures if dictionary is used
        reEncodeMeasure(value);

        valueBuf.clear();
        codec.encode(value, valueBuf);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());

        context.write(outputKey, outputValue);
    }

    private Boolean checkNeedMerging(TblColRef col) throws IOException {
        Boolean ret = dimensionsNeedDict.get(col);
        if (ret != null)
            return ret;
        else {
            ret = cubeDesc.getRowkey().isUseDictionary(col);
            if (ret) {
                String dictTable = DictionaryManager.getInstance(config).decideSourceData(cubeDesc.getModel(), cubeDesc.getRowkey().isUseDictionary(col), col).getTable();
                ret = cubeDesc.getFactTable().equalsIgnoreCase(dictTable);
            }
            dimensionsNeedDict.put(col, ret);
            return ret;
        }
    }

    @SuppressWarnings("unchecked")
    private void reEncodeMeasure(Object[] measureObjs) throws IOException, InterruptedException {
        // currently only topN uses dictionary in measure obj
        if (topNMeasureIdx.isEmpty())
            return;

        int bufOffset = 0;
        for (int idx : topNMeasureIdx) {
            TopNCounter<ByteArray> topNCounters = (TopNCounter<ByteArray>) measureObjs[idx];

            MeasureDesc measureDesc = measureDescs.get(idx);
            TblColRef colRef = measureDesc.getFunction().getTopNLiteralColumn();
            DictionaryManager dictMgr = DictionaryManager.getInstance(config);
            Dictionary<?> sourceDict = dictMgr.getDictionary(sourceCubeSegment.getDictResPath(colRef));
            Dictionary<?> mergedDict = dictMgr.getDictionary(mergedCubeSegment.getDictResPath(colRef));

            int topNSize = topNCounters.size();
            while (sourceDict.getSizeOfValue() * topNSize > newKeyBodyBuf.length - bufOffset || //
                    mergedDict.getSizeOfValue() * topNSize > newKeyBodyBuf.length - bufOffset || //
                    mergedDict.getSizeOfId() * topNSize > newKeyBodyBuf.length - bufOffset) {
                byte[] oldBuf = newKeyBodyBuf;
                newKeyBodyBuf = new byte[2 * newKeyBodyBuf.length];
                System.arraycopy(oldBuf, 0, newKeyBodyBuf, 0, oldBuf.length);
            }

            for (Counter<ByteArray> c : topNCounters) {
                int idInSourceDict = BytesUtil.readUnsigned(c.getItem().array(), c.getItem().offset(), c.getItem().length());
                int idInMergedDict;
                int size = sourceDict.getValueBytesFromId(idInSourceDict, newKeyBodyBuf, bufOffset);
                if (size < 0) {
                    idInMergedDict = mergedDict.nullId();
                } else {
                    idInMergedDict = mergedDict.getIdFromValueBytes(newKeyBodyBuf, bufOffset, size);
                }

                BytesUtil.writeUnsigned(idInMergedDict, newKeyBodyBuf, bufOffset, mergedDict.getSizeOfId());
                c.getItem().set(newKeyBodyBuf, bufOffset, mergedDict.getSizeOfId());
                bufOffset += mergedDict.getSizeOfId();
            }
        }
    }
}
