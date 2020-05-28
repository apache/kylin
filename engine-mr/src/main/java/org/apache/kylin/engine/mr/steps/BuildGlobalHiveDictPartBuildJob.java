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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildGlobalHiveDictPartBuildJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(BuildGlobalHiveDictPartBuildJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        String[] dicColsArr = null;

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            parseOptions(options, args);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            dicColsArr = config.getMrHiveDictColumnsExcludeRefColumns();

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));

            // add metadata to distributed cache
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment segment = cube.getSegmentById(segmentID);

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            logger.info("Starting: " + job.getJobName());

            job.setJarByClass(BuildGlobalHiveDictPartBuildJob.class);

            setJobClasspath(job, cube.getConfig());

            //FileInputFormat.setInputPaths(job, input);
            setInput(job, dicColsArr, getInputPath(config, segment));

            // make each reducer output to respective dir
            setOutput(job, dicColsArr, getOptionValue(OPTION_OUTPUT_PATH));
            job.getConfiguration().setBoolean("mapreduce.output.fileoutputformat.compress", false);

            //set reduce num
            setReduceNum(job, config);

            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(BuildGlobalHiveDictPartBuildMapper.class);
            job.setPartitionerClass(BuildGlobalHiveDictPartPartitioner.class);
            job.setReducerClass(BuildGlobalHiveDictPartBuildReducer.class);

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
        // eg: /user/kylin/tmp/kylin/globaldic_test/kylin-188c9f9d_dabb_944e_9f20_99dc95be66e6/kylin_sales_cube_mr/dict_column=KYLIN_SALES_SELLER_ID/part_sort
        for (int i = 0; i < dicColsArr.length; i++) {
            MultipleOutputs.addNamedOutput(job, i + "", TextOutputFormat.class, LongWritable.class, Text.class);
        }
        Path outputPath = new Path(outputBase);
        FileOutputFormat.setOutputPath(job, outputPath);
    }

    private void setInput(Job job, String[] dicColsArray, String inputBase) throws IOException {
        StringBuffer paths = new StringBuffer();
        // make each reducer output to respective dir
        for (String col : dicColsArray) {
            paths.append(inputBase).append("/dict_column=").append(col).append(",");
        }
        paths.delete(paths.length() - 1, paths.length());
        FileInputFormat.setInputPaths(job, paths.toString());
    }

    private void setReduceNum(Job job, KylinConfig config) {
        Integer[] reduceNumArr = config.getMrHiveDictColumnsReduceNumExcludeRefCols();
        int totalReduceNum = 0;
        for (Integer num : reduceNumArr) {
            totalReduceNum += num;
        }
        logger.info("BuildGlobalHiveDictPartBuildJob total reduce num is {}", totalReduceNum);
        job.setNumReduceTasks(totalReduceNum);
    }

    private String getInputPath(KylinConfig config, CubeSegment segment) {
        String dbDir = config.getHiveDatabaseDir();
        String tableName = EngineFactory.getJoinedFlatTableDesc(segment).getTableName() + config.getMrHiveDistinctValueTableSuffix();
        String input = dbDir + "/" + tableName;
        logger.info("part build base input path:" + input);
        return input;
    }
}
