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

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoder;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateOldCuboidShardMapper extends KylinMapper<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(UpdateOldCuboidShardMapper.class);

    private CubeDesc cubeDesc;
    private RowKeySplitter rowKeySplitter;
    private RowKeyEncoderProvider rowKeyEncoderProvider;

    private Text outputKey = new Text();
    private byte[] newKeyBodyBuf = new byte[RowConstants.ROWKEY_BUFFER_SIZE];
    private ByteArray newKeyBuf = ByteArray.allocate(RowConstants.ROWKEY_BUFFER_SIZE);

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeSegment cubeSegment = cube.getSegmentById(segmentID);
        CubeSegment oldSegment = cube.getOriginalSegmentToOptimize(cubeSegment);

        cubeDesc = cube.getDescriptor();

        rowKeySplitter = new RowKeySplitter(oldSegment);
        rowKeyEncoderProvider = new RowKeyEncoderProvider(cubeSegment);
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidID = rowKeySplitter.split(key.getBytes());

        Cuboid cuboid = Cuboid.findForMandatory(cubeDesc, cuboidID);
        int fullKeySize = buildKey(cuboid, rowKeySplitter.getSplitBuffers());
        outputKey.set(newKeyBuf.array(), 0, fullKeySize);

        context.write(outputKey, value);
    }

    private int buildKey(Cuboid cuboid, ByteArray[] splitBuffers) {
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(cuboid);

        int startIdx = rowKeySplitter.getBodySplitOffset(); // skip shard and cuboidId
        int endIdx = startIdx + Long.bitCount(cuboid.getId());
        int offset = 0;
        for (int i = startIdx; i < endIdx; i++) {
            System.arraycopy(splitBuffers[i].array(), splitBuffers[i].offset(), newKeyBodyBuf, offset,
                    splitBuffers[i].length());
            offset += splitBuffers[i].length();
        }

        int fullKeySize = rowkeyEncoder.getBytesLength();
        while (newKeyBuf.array().length < fullKeySize) {
            newKeyBuf = new ByteArray(newKeyBuf.length() * 2);
        }
        newKeyBuf.setLength(fullKeySize);

        rowkeyEncoder.encode(new ByteArray(newKeyBodyBuf, 0, offset), newKeyBuf);

        return fullKeySize;
    }
}
