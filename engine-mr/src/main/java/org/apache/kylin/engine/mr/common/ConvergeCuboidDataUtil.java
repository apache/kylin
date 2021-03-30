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

package org.apache.kylin.engine.mr.common;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.ConvergeCuboidDataPartitioner;
import org.apache.kylin.engine.mr.steps.ConvergeCuboidDataReducer;

public class ConvergeCuboidDataUtil {

    public static void setupReducer(Job job, CubeSegment cubeSegment, Path output) throws IOException {
        // Output
        //// prevent to create zero-sized default output
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        // Reducer
        job.setReducerClass(ConvergeCuboidDataReducer.class);
        job.setPartitionerClass(ConvergeCuboidDataPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Pair<Integer, Integer> numReduceTasks = MapReduceUtil.getConvergeCuboidDataReduceTaskNums(cubeSegment);
        job.setNumReduceTasks(numReduceTasks.getFirst());

        int nBaseReduceTasks = numReduceTasks.getSecond();
        boolean enableSharding = cubeSegment.isEnableSharding();
        long baseCuboidId = cubeSegment.getCuboidScheduler().getBaseCuboidId();
        String partiParams = enableSharding + "," + baseCuboidId + "," + nBaseReduceTasks;
        job.getConfiguration().set(BatchConstants.CFG_CONVERGE_CUBOID_PARTITION_PARAM, partiParams);
    }
}
