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

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateStatsFromBaseCuboidJob extends AbstractHadoopJob {

    private static final Logger logger = LoggerFactory.getLogger(CalculateStatsFromBaseCuboidJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_STATISTICS_SAMPLING_PERCENT);
            options.addOption(OPTION_CUBOID_MODE);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);
            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String statistics_sampling_percent = getOptionValue(OPTION_STATISTICS_SAMPLING_PERCENT);
            String cuboidMode = getOptionValue(OPTION_CUBOID_MODE);

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment cubeSegment = cube.getSegmentById(segmentID);

            job.getConfiguration().set(BatchConstants.CFG_CUBOID_MODE, cuboidMode);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, statistics_sampling_percent);
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            setupMapper(input);
            setupReducer(output, cubeSegment);

            attachSegmentMetadataWithDict(cubeSegment, job.getConfiguration());

            return waitForCompletion(job);

        } catch (Exception e) {
            logger.error("error in CalculateStatsFromBaseCuboidJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private void setupMapper(Path input) throws IOException {
        FileInputFormat.setInputPaths(job, input);
        job.setMapperClass(CalculateStatsFromBaseCuboidMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
    }

    private void setupReducer(Path output, CubeSegment cubeSeg) throws IOException {
        int hllShardBase = MapReduceUtil.getCuboidHLLCounterReducerNum(cubeSeg.getCubeInstance());

        job.setReducerClass(CalculateStatsFromBaseCuboidReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(hllShardBase);

        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.CFG_OUTPUT_PATH, output.toString());

        deletePath(job.getConfiguration(), output);
    }
}
