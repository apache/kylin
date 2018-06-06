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

import static org.apache.kylin.engine.mr.JobBuilderSupport.PathNameCuboidBase;
import static org.apache.kylin.engine.mr.JobBuilderSupport.PathNameCuboidOld;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
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
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateOldCuboidShardMapper extends KylinMapper<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(UpdateOldCuboidShardMapper.class);

    private MultipleOutputs mos;
    private long baseCuboid;

    private CubeDesc cubeDesc;
    private RowKeySplitter rowKeySplitter;
    private RowKeyEncoderProvider rowKeyEncoderProvider;

    private Text outputKey = new Text();
    private byte[] newKeyBodyBuf = new byte[RowConstants.ROWKEY_BUFFER_SIZE];
    private ByteArray newKeyBuf = ByteArray.allocate(RowConstants.ROWKEY_BUFFER_SIZE);

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        mos = new MultipleOutputs(context);

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeSegment cubeSegment = cube.getSegmentById(segmentID);
        CubeSegment oldSegment = cube.getOriginalSegmentToOptimize(cubeSegment);

        cubeDesc = cube.getDescriptor();
        baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();

        rowKeySplitter = new RowKeySplitter(oldSegment);
        rowKeyEncoderProvider = new RowKeyEncoderProvider(cubeSegment);
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidID = rowKeySplitter.split(key.getBytes());

        Cuboid cuboid = Cuboid.findForMandatory(cubeDesc, cuboidID);
        int fullKeySize = buildKey(cuboid, rowKeySplitter.getSplitBuffers());
        outputKey.set(newKeyBuf.array(), 0, fullKeySize);

        String baseOutputPath = PathNameCuboidOld;
        if (cuboidID == baseCuboid) {
            baseOutputPath = PathNameCuboidBase;
        }
        mos.write(outputKey, value, generateFileName(baseOutputPath));
    }

    private int buildKey(Cuboid cuboid, SplittedBytes[] splitBuffers) {
        RowKeyEncoder rowkeyEncoder = rowKeyEncoderProvider.getRowkeyEncoder(cuboid);

        int startIdx = rowKeySplitter.getBodySplitOffset(); // skip shard and cuboidId
        int endIdx = startIdx + Long.bitCount(cuboid.getId());
        int offset = 0;
        for (int i = startIdx; i < endIdx; i++) {
            System.arraycopy(splitBuffers[i].value, 0, newKeyBodyBuf, offset, splitBuffers[i].length);
            offset += splitBuffers[i].length;
        }

        int fullKeySize = rowkeyEncoder.getBytesLength();
        while (newKeyBuf.array().length < fullKeySize) {
            newKeyBuf = new ByteArray(newKeyBuf.length() * 2);
        }
        newKeyBuf.setLength(fullKeySize);

        rowkeyEncoder.encode(new ByteArray(newKeyBodyBuf, 0, offset), newKeyBuf);

        return fullKeySize;
    }

    @Override
    public void doCleanup(Context context) throws IOException, InterruptedException {
        mos.close();

        Path outputDirBase = new Path(context.getConfiguration().get(FileOutputFormat.OUTDIR), PathNameCuboidBase);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (!fs.exists(outputDirBase)) {
            fs.mkdirs(outputDirBase);
            SequenceFile
                    .createWriter(context.getConfiguration(),
                            SequenceFile.Writer.file(new Path(outputDirBase, "part-m-00000")),
                            SequenceFile.Writer.keyClass(Text.class), SequenceFile.Writer.valueClass(Text.class))
                    .close();
        }
    }

    private String generateFileName(String subDir) {
        return subDir + "/part";
    }
}
