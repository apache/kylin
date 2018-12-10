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

package org.apache.kylin.engine.mr.streaming;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class MergeDictJob extends AbstractHadoopJob {
    private static final Logger logger = LoggerFactory.getLogger(MergeDictJob.class);

    public static void main(String[] args) throws Exception {
        MergeDictJob mergeDictJob = new MergeDictJob();
        ToolRunner.run(mergeDictJob, args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String jobName = getOptionValue(OPTION_JOB_NAME);
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase(Locale.ROOT);
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME);

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment segment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            setJobClasspath(job, cube.getConfig());
            job.setJobName(jobName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);
            FileInputFormat.setInputPaths(job, input);
            FileOutputFormat.setOutputPath(job, output);

            logger.info("MergeDictReducer output path: {}", output);

            // Mapper
            job.setMapperClass(MergeDictMapper.class);
            job.setInputFormatClass(ColumnarSplitDictInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //Reducer
            job.setReducerClass(MergeDictReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            attachCubeMetadata(cube, job.getConfiguration());

            deletePath(job.getConfiguration(), output);
            return waitForCompletion(job);
        } catch (Exception e) {
            printUsage(options);
            logger.error("job {} failed. ", job.getJobName(), e);
            throw e;
        }
    }

}
