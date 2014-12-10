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
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.common.RowKeySplitter;
import com.kylinolap.cube.common.SplittedBytes;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.cuboid.CuboidScheduler;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author George Song (ysong1)
 * 
 */
public class NDCuboidMapper extends Mapper<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(NDCuboidMapper.class);

    private Text outputKey = new Text();
    private String cubeName;
    private String segmentName;
    private CubeDesc cubeDesc;
    private CuboidScheduler cuboidScheduler;

    private int handleCounter;
    private int skipCounter;

    private byte[] keyBuf = new byte[4096];
    private RowKeySplitter rowKeySplitter;

    @Override
    protected void setup(Context context) throws IOException {

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME).toUpperCase();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(context.getConfiguration());

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeSegment cubeSegment = cube.getSegment(segmentName, CubeSegmentStatusEnum.NEW);
        cubeDesc = cube.getDescriptor();

        // initialize CubiodScheduler
        cuboidScheduler = new CuboidScheduler(cubeDesc);

        rowKeySplitter = new RowKeySplitter(cubeSegment, 65, 256);
    }

    private int buildKey(Cuboid parentCuboid, Cuboid childCuboid, SplittedBytes[] splitBuffers) {
        int offset = 0;

        // cuboid id
        System.arraycopy(childCuboid.getBytes(), 0, keyBuf, offset, childCuboid.getBytes().length);
        offset += childCuboid.getBytes().length;

        // rowkey columns
        long mask = Long.highestOneBit(parentCuboid.getId());
        long parentCuboidId = parentCuboid.getId();
        long childCuboidId = childCuboid.getId();
        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(parentCuboid.getId());
        int index = 1; // skip cuboidId
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & parentCuboidId) > 0) {// if the this bit position equals
                                              // 1
                if ((mask & childCuboidId) > 0) {// if the child cuboid has this
                                                 // column
                    System.arraycopy(splitBuffers[index].value, 0, keyBuf, offset, splitBuffers[index].length);
                    offset += splitBuffers[index].length;
                }
                index++;
            }
            mask = mask >> 1;
        }

        return offset;
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidId = rowKeySplitter.split(key.getBytes(), key.getLength());
        Cuboid parentCuboid = Cuboid.findById(cubeDesc, cuboidId);

        Collection<Long> myChildren = cuboidScheduler.getSpanningCuboid(cuboidId);

        // if still empty or null
        if (myChildren == null || myChildren.size() == 0) {
            context.getCounter(BatchConstants.MAPREDUCE_COUTNER_GROUP_NAME, "Skipped records").increment(1L);
            skipCounter++;
            if (skipCounter % BatchConstants.COUNTER_MAX == 0) {
                logger.info("Skipped " + skipCounter + " records!");
            }
            return;
        }

        context.getCounter(BatchConstants.MAPREDUCE_COUTNER_GROUP_NAME, "Processed records").increment(1L);

        handleCounter++;
        if (handleCounter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + handleCounter + " records!");
        }

        for (Long child : myChildren) {
            Cuboid childCuboid = Cuboid.findById(cubeDesc, child);
            int keyLength = buildKey(parentCuboid, childCuboid, rowKeySplitter.getSplitBuffers());
            outputKey.set(keyBuf, 0, keyLength);
            context.write(outputKey, value);
        }

    }
}
