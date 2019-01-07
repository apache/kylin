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
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class FactDistinctColumnsJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(FactDistinctColumnsJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_CUBING_JOB_ID);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_STATISTICS_OUTPUT);
            options.addOption(OPTION_STATISTICS_SAMPLING_PERCENT);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String job_id = getOptionValue(OPTION_CUBING_JOB_ID);
            job.getConfiguration().set(BatchConstants.ARG_CUBING_JOB_ID, job_id);
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));

            String segmentID = getOptionValue(OPTION_SEGMENT_ID);
            String statistics_output = getOptionValue(OPTION_STATISTICS_OUTPUT);
            String statistics_sampling_percent = getOptionValue(OPTION_STATISTICS_SAMPLING_PERCENT);

            // ----------------------------------------------------------------------------
            // add metadata to distributed cache
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment segment = cube.getSegmentById(segmentID);
            if (segment == null) {
                logger.warn("Failed to find segment {} in cube {}", segmentID, cube);
                cube = cubeMgr.reloadCubeQuietly(cubeName);
                segment = cube.getSegmentById(segmentID);
            }
            if (segment == null) {
                logger.error("Failed to find {} in cube {}", segmentID, cube);
                for (CubeSegment s : cube.getSegments()) {
                    logger.error(s.getName() + " with status " + s.getStatus());
                }
                throw new IllegalStateException();
            }

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_OUTPUT, statistics_output);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, statistics_sampling_percent);

            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            setupMapper(segment);
            setupReducer(output, segment);

            attachCubeMetadata(cube, job.getConfiguration());

            /**
             * don't compress the reducer output so that {@link CreateDictionaryJob} and {@link UpdateCubeInfoAfterBuildStep}
             * could read the reducer file directly
             */
            job.getConfiguration().set(BatchConstants.CFG_MAPRED_OUTPUT_COMPRESS, "false");

            return waitForCompletion(job);

        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }

    }

    private void setupMapper(CubeSegment cubeSeg) throws IOException {
        IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();
        flatTableInputFormat.configureJob(job);

        job.setMapperClass(FactDistinctColumnsMapper.class);
        job.setCombinerClass(FactDistinctColumnsCombiner.class);
        job.setMapOutputKeyClass(SelfDefineSortableKey.class);
        job.setMapOutputValueClass(Text.class);
    }

    private void setupReducer(Path output, CubeSegment cubeSeg)
            throws IOException {
        FactDistinctColumnsReducerMapping reducerMapping = new FactDistinctColumnsReducerMapping(cubeSeg.getCubeInstance());
        int numberOfReducers = reducerMapping.getTotalReducerNum();
        logger.info("{} has reducers {}.", this.getClass().getName(), numberOfReducers);
        if (numberOfReducers > 250) {
            throw new IllegalArgumentException(
                    "The max reducer number for FactDistinctColumnsJob is 250, but now it is "
                            + numberOfReducers
                            + ", decrease 'kylin.engine.mr.uhc-reducer-count'");
        }

        job.setReducerClass(FactDistinctColumnsReducer.class);
        job.setPartitionerClass(FactDistinctColumnPartitioner.class);
        job.setNumReduceTasks(numberOfReducers);

        // make each reducer output to respective dir
        MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_COLUMN, SequenceFileOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_DICT, SequenceFileOutputFormat.class, NullWritable.class, ArrayPrimitiveWritable.class);
        MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_STATISTICS, SequenceFileOutputFormat.class, LongWritable.class, BytesWritable.class);
        MultipleOutputs.addNamedOutput(job, BatchConstants.CFG_OUTPUT_PARTITION, TextOutputFormat.class, NullWritable.class, LongWritable.class);

        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.CFG_OUTPUT_PATH, output.toString());

        // prevent to create zero-sized default output
        LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

        deletePath(job.getConfiguration(), output);
    }

    public static void main(String[] args) throws Exception {
        FactDistinctColumnsJob job = new FactDistinctColumnsJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }

}
