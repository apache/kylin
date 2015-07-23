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
import java.util.HashMap;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMROutput2.IMRStorageInputFormat;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.measure.MeasureCodec;
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
    private byte[] newKeyBuf;
    private RowKeySplitter rowKeySplitter;

    private HashMap<TblColRef, Boolean> dictsNeedMerging = new HashMap<TblColRef, Boolean>();

    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private MeasureCodec codec;
    private ByteArrayWritable outputValue = new ByteArrayWritable();

    private Boolean checkNeedMerging(TblColRef col) throws IOException {
        Boolean ret = dictsNeedMerging.get(col);
        if (ret != null)
            return ret;
        else {
            ret = cubeDesc.getRowkey().isUseDictionary(col);
            if (ret) {
                String dictTable = (String) DictionaryManager.getInstance(config).decideSourceData(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null)[0];
                ret = cubeDesc.getFactTable().equalsIgnoreCase(dictTable);
            }
            dictsNeedMerging.put(col, ret);
            return ret;
        }
    }

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

        newKeyBuf = new byte[256];// size will auto-grow

        sourceCubeSegment = storageInputFormat.findSourceSegment(context, cube);
        logger.info(sourceCubeSegment.toString());

        this.rowKeySplitter = new RowKeySplitter(sourceCubeSegment, 65, 255);

        List<MeasureDesc> measuresDescs = Lists.newArrayList();
        for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc colDesc : cfDesc.getColumns()) {
                for (MeasureDesc measure : colDesc.getMeasures()) {
                    measuresDescs.add(measure);
                }
            }
        }
        codec = new MeasureCodec(measuresDescs);
    }

    @Override
    public void map(Object inKey, Object inValue, Context context) throws IOException, InterruptedException {
        Pair<ByteArrayWritable, Object[]> pair = storageInputFormat.parseMapperInput(inKey, inValue);
        ByteArrayWritable key = pair.getFirst();
        Object[] value = pair.getSecond();
        
        Preconditions.checkState(key.offset() == 0);
        
        long cuboidID = rowKeySplitter.split(key.array(), key.length());
        Cuboid cuboid = Cuboid.findById(cubeDesc, cuboidID);

        SplittedBytes[] splittedByteses = rowKeySplitter.getSplitBuffers();
        int bufOffset = 0;
        BytesUtil.writeLong(cuboidID, newKeyBuf, bufOffset, RowConstants.ROWKEY_CUBOIDID_LEN);
        bufOffset += RowConstants.ROWKEY_CUBOIDID_LEN;

        for (int i = 0; i < cuboid.getColumns().size(); ++i) {
            TblColRef col = cuboid.getColumns().get(i);

            if (this.checkNeedMerging(col)) {
                // if dictionary on fact table column, needs rewrite
                DictionaryManager dictMgr = DictionaryManager.getInstance(config);
                Dictionary<?> sourceDict = dictMgr.getDictionary(sourceCubeSegment.getDictResPath(col));
                Dictionary<?> mergedDict = dictMgr.getDictionary(mergedCubeSegment.getDictResPath(col));

                while (sourceDict.getSizeOfValue() > newKeyBuf.length - bufOffset || mergedDict.getSizeOfValue() > newKeyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBuf;
                    newKeyBuf = new byte[2 * newKeyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBuf, 0, oldBuf.length);
                }

                int idInSourceDict = BytesUtil.readUnsigned(splittedByteses[i + 1].value, 0, splittedByteses[i + 1].length);

                int size = sourceDict.getValueBytesFromId(idInSourceDict, newKeyBuf, bufOffset);
                int idInMergedDict;
                if (size < 0) {
                    idInMergedDict = mergedDict.nullId();
                } else {
                    idInMergedDict = mergedDict.getIdFromValueBytes(newKeyBuf, bufOffset, size);
                }
                BytesUtil.writeUnsigned(idInMergedDict, newKeyBuf, bufOffset, mergedDict.getSizeOfId());

                bufOffset += mergedDict.getSizeOfId();
            } else {
                // keep as it is
                while (splittedByteses[i + 1].length > newKeyBuf.length - bufOffset) {
                    byte[] oldBuf = newKeyBuf;
                    newKeyBuf = new byte[2 * newKeyBuf.length];
                    System.arraycopy(oldBuf, 0, newKeyBuf, 0, oldBuf.length);
                }

                System.arraycopy(splittedByteses[i + 1].value, 0, newKeyBuf, bufOffset, splittedByteses[i + 1].length);
                bufOffset += splittedByteses[i + 1].length;
            }
        }
        byte[] newKey = Arrays.copyOf(newKeyBuf, bufOffset);
        outputKey.set(newKey, 0, newKey.length);

        valueBuf.clear();
        codec.encode(value, valueBuf);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        
        context.write(outputKey, outputValue);
    }

}
