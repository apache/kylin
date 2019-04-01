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
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;

public class ConvergeCuboidDataReducer extends KylinReducer<Text, Text, Text, Text> {

    private MultipleOutputs mos;

    private boolean enableSharding;
    private long baseCuboid;

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

        this.enableSharding = oldSegment.isEnableSharding();
        this.baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();
    }

    @Override
    public void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long cuboidID = RowKeySplitter.getCuboidId(key.getBytes(), enableSharding);

        String baseOutputPath = cuboidID == baseCuboid ? PathNameCuboidBase : PathNameCuboidOld;
        int n = 0;
        for (Text value : values) {
            mos.write(key, value, generateFileName(baseOutputPath));
            n++;
        }
        if (n > 1) {
            throw new RuntimeException(
                    "multiple records share the same key in aggregated cuboid data for cuboid " + cuboidID);
        }
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
