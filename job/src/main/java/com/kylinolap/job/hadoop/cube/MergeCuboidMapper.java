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
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.common.RowKeySplitter;
import com.kylinolap.cube.common.SplittedBytes;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.RowConstants;
import com.kylinolap.dict.Dictionary;
import com.kylinolap.dict.DictionaryManager;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.TblColRef;

/**
 * @author ysong1, honma
 */
public class MergeCuboidMapper extends Mapper<Text, Text, Text, Text> {

    private KylinConfig config;
    private String cubeName;
    private String segmentName;
    private CubeManager cubeManager;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment mergedCubeSegment;
    private CubeSegment sourceCubeSegment;// Must be unique during a mapper's
                                          // life cycle

    private Text outputKey = new Text();

    private byte[] newKeyBuf;
    private RowKeySplitter rowKeySplitter;

    private HashMap<TblColRef, Boolean> dictsNeedMerging = new HashMap<TblColRef, Boolean>();

    private static final Pattern JOB_NAME_PATTERN = Pattern.compile("kylin-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    private Boolean checkNeedMerging(TblColRef col) throws IOException {
        Boolean ret = dictsNeedMerging.get(col);
        if (ret != null)
            return ret;
        else {
            ret = cubeDesc.getRowkey().isUseDictionary(col) && cubeDesc.getFactTable().equalsIgnoreCase((String) DictionaryManager.getInstance(config).decideSourceData(cubeDesc, col, null)[0]);
            dictsNeedMerging.put(col, ret);
            return ret;
        }
    }

    private String extractJobIDFromPath(String path) {
        Matcher matcher = JOB_NAME_PATTERN.matcher(path);
        // check the first occurance
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            throw new IllegalStateException("Can not extract job ID from file path : " + path);
        }
    }

    private CubeSegment findSegmentWithUuid(String jobID, CubeInstance cubeInstance) {
        for (CubeSegment segment : cubeInstance.getSegments()) {
            if (segment.getUuid().equalsIgnoreCase(jobID)) {
                return segment;
            }
        }

        throw new IllegalStateException("No merging segment's last build job ID equals " + jobID);

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME).toUpperCase();

        config = AbstractHadoopJob.loadKylinPropsAndMetadata(context.getConfiguration());

        cubeManager = CubeManager.getInstance(config);
        cube = cubeManager.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        mergedCubeSegment = cube.getSegment(segmentName, CubeSegmentStatusEnum.NEW);

        // int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        newKeyBuf = new byte[256];// size will auto-grow

        // decide which source segment
        org.apache.hadoop.mapreduce.InputSplit inputSplit = context.getInputSplit();
        String filePath = ((FileSplit) inputSplit).getPath().toString();
        String jobID = extractJobIDFromPath(filePath);
        sourceCubeSegment = findSegmentWithUuid(jobID, cube);

        this.rowKeySplitter = new RowKeySplitter(sourceCubeSegment, 65, 255);
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidID = rowKeySplitter.split(key.getBytes(), key.getBytes().length);
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
                int idInMergedDict = mergedDict.getIdFromValueBytes(newKeyBuf, bufOffset, size);
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

        context.write(outputKey, value);
    }
}
