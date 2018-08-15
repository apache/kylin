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

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directly using global dictionary to encode values will bring lots of memory swapping of the slices, which will make
 * the encoding process very slow. This job will change the encoding process for the raw column values to
 * 1. For each data block, a mapper will generating distinct values, sort them, extract shrunken dictionary from global
 * 2. For each data block, scan again to encode the raw values by the shrunken dictionary rather than the global one
 */
public class ExtractDictionaryFromGlobalJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(ExtractDictionaryFromGlobalJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBING_JOB_ID);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String job_id = getOptionValue(OPTION_CUBING_JOB_ID);
            job.getConfiguration().set(BatchConstants.ARG_CUBING_JOB_ID, job_id);

            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);

            // ----------------------------------------------------------------------------
            // add metadata to distributed cache
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment segment = cube.getSegmentById(segmentID);

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            logger.info("Starting: " + job.getJobName());

            job.getConfiguration().set("mapreduce.map.speculative", "false");
            setJobClasspath(job, cube.getConfig());

            // Mapper
            job.setMapperClass(ExtractDictionaryFromGlobalMapper.class);

            // Reducer
            job.setNumReduceTasks(0);

            // Input
            IMRInput.IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(segment)
                    .getFlatTableInputFormat();
            flatTableInputFormat.configureJob(job);
            // Output
            //// prevent to create zero-sized default output
            LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileOutputFormat.setOutputPath(job, output);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            deletePath(job.getConfiguration(), output);

            attachSegmentMetadataWithDict(segment, job.getConfiguration());
            return waitForCompletion(job);
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }
}
