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

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
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

public class ColumnToRowJob extends AbstractHadoopJob {
    private static final Logger logger = LoggerFactory.getLogger(ColumnToRowJob.class);
    private static final long DEFAULT_SIZE_PER_REDUCER = 16 * 1024 * 1024L;
    private static final int MAX_REDUCERS = 1000;

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
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase(Locale.ROOT);
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME);

            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(kylinConfig);
            CubeInstance cube = cubeMgr.getCube(cubeName);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            setJobClasspath(job, cube.getConfig());
            FileInputFormat.setInputPaths(job, input);
            FileOutputFormat.setOutputPath(job, output);

            job.setMapperClass(ColumnToRowMapper.class);
            job.setInputFormatClass(ColumnarSplitDataInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(ColumnToRowReducer.class);
            job.setNumReduceTasks(calReducerNum(input));
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.getConfiguration().set("dfs.block.size", cube.getConfig().getStreamingBasicCuboidJobDFSBlockSize());
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);

            CubeSegment segment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
            attachSegmentMetadataWithDict(segment, job.getConfiguration());
            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in CuboidJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private int calReducerNum(Path input) {
        try {
            long bytesPerReducer = DEFAULT_SIZE_PER_REDUCER;
            FileSystem fs = FileSystem.get(job.getConfiguration());
            ContentSummary cs = fs.getContentSummary(input);
            long totalInputFileSize = cs.getLength();

            int reducers = (int) ((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer);
            reducers = Math.max(1, reducers);
            reducers = Math.min(MAX_REDUCERS, reducers);
            logger.info("BytesPerReducer={}, maxReducers={}, totalInputFileSize={}, setReducers={}", bytesPerReducer,
                    MAX_REDUCERS, totalInputFileSize, reducers);
            return reducers;
        } catch (IOException e) {
            logger.error("error when calculate reducer number", e);
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        ColumnToRowJob job = new ColumnToRowJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }

}
