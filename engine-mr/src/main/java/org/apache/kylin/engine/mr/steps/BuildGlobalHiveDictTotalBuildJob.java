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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildGlobalHiveDictTotalBuildJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(BuildGlobalHiveDictTotalBuildJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        String[] dicColsArr = null;
        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_GLOBAL_DIC_MAX_DISTINCT_COUNT);
            options.addOption(OPTION_GLOBAL_DIC_PART_REDUCE_STATS);
            parseOptions(options, args);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            dicColsArr = config.getMrHiveDictColumnsExcludeRefColumns();
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            logger.info("Starting: " + job.getJobName());

            // ----------------------------------------------------------------------------
            // add metadata to distributed cache
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment segment = cube.getSegmentById(segmentID);

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
            job.getConfiguration().set("partition.statistics.path", getOptionValue(OPTION_GLOBAL_DIC_PART_REDUCE_STATS));
            job.getConfiguration().set("last.max.dic.value.path", getOptionValue(OPTION_GLOBAL_DIC_MAX_DISTINCT_COUNT));
            job.getConfiguration().setBoolean("mapreduce.output.fileoutputformat.compress", false);

            job.setJarByClass(BuildGlobalHiveDictTotalBuildJob.class);

            setJobClasspath(job, cube.getConfig());

            // Mapper
            job.setMapperClass(BuildGlobalHiveDictTotalBuildMapper.class);

            // Input Output
            setInput(job, getOptionValue(OPTION_INPUT_PATH));
            setOutput(job, dicColsArr, getOptionValue(OPTION_OUTPUT_PATH));

            job.setNumReduceTasks(0);//no reduce

            job.setInputFormatClass(KeyValueTextInputFormat.class);

            // prevent to create zero-sized default output
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

            // delete output
            Path baseOutputPath = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            deletePath(job.getConfiguration(), baseOutputPath);

            attachSegmentMetadataWithDict(segment, job.getConfiguration());
            return waitForCompletion(job);
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private void setOutput(Job job, String[] dicColsArr, String outputBase) {
        // make each reducer output to respective dir
        ///user/prod_kylin/tmp/kylin2/globaldic_test/kylin-188c9f9d_dabb_944e_9f20_99dc95be66e6/bs_order_scene_day_new_cube_clone/dict_column=DM_ES_REPORT_ORDER_VIEW0420_DRIVER_ID/part_sort
        for (int i = 0; i < dicColsArr.length; i++) {
            MultipleOutputs.addNamedOutput(job, i + "", TextOutputFormat.class, Text.class, LongWritable.class);
        }
        Path outputPath = new Path(outputBase);
        FileOutputFormat.setOutputPath(job, outputPath);
    }

    private void setInput(Job job, String input) throws IOException {
        Path path = new Path(input);
        FileSystem fs = path.getFileSystem(job.getConfiguration());
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        FileInputFormat.setInputPaths(job, getOptionValue(OPTION_INPUT_PATH));
    }

}
