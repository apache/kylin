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

package org.apache.kylin.job.hadoop.cube;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author yangli9
 */
public class FactDistinctColumnsJob extends AbstractHadoopJob {
    protected static final Logger log = LoggerFactory.getLogger(FactDistinctColumnsJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_TABLE_NAME);
            options.addOption(OPTION_SEGMENT_NAME);
            options.addOption(OPTION_STATISTICS_ENABLED);
            options.addOption(OPTION_STATISTICS_OUTPUT);
            options.addOption(OPTION_STATISTICS_SAMPLING_PERCENT);
            parseOptions(options, args);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String intermediateTable = getOptionValue(OPTION_TABLE_NAME);
            ;
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME);
            String statistics_enabled = getOptionValue(OPTION_STATISTICS_ENABLED);
            String statistics_output = getOptionValue(OPTION_STATISTICS_OUTPUT);
            String statistics_sampling_percent = getOptionValue(OPTION_STATISTICS_SAMPLING_PERCENT);

            // ----------------------------------------------------------------------------
            // add metadata to distributed cache
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cubeInstance = cubeMgr.getCube(cubeName);
            CubeSegment newSegment = cubeInstance.getSegment(segmentName, SegmentStatusEnum.NEW);

            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_ENABLED, statistics_enabled);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_OUTPUT, statistics_output);
            job.getConfiguration().set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, statistics_sampling_percent);
            log.info("Starting: " + job.getJobName());

            setJobClasspath(job);

            setupMapper(intermediateTable);
            setupReducer(output);

            // CubeSegment seg = cubeMgr.getCube(cubeName).getTheOnlySegment();
            attachKylinPropsAndMetadata(cubeInstance, job.getConfiguration());

            int result = waitForCompletion(job);

            if(Boolean.parseBoolean(statistics_enabled)) {
                putStatisticsToResourceStore(statistics_output, newSegment);
            }

            return result;


        } catch (Exception e) {
            logger.error("error in FactDistinctColumnsJob", e);
            printUsage(options);
            throw e;
        }

    }

    private void setupMapper(String intermediateTable) throws IOException {
//        FileInputFormat.setInputPaths(job, input);

        String[] dbTableNames = HadoopUtil.parseHiveTableName(intermediateTable);
        HCatInputFormat.setInput(job, dbTableNames[0],
                dbTableNames[1]);

        job.setInputFormatClass(HCatInputFormat.class);
        job.setMapperClass(FactDistinctHiveColumnsMapper.class);
        job.setCombinerClass(FactDistinctColumnsCombiner.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
    }

    private void setupReducer(Path output) throws IOException {
        job.setReducerClass(FactDistinctColumnsReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, output);
        job.getConfiguration().set(BatchConstants.OUTPUT_PATH, output.toString());

        job.setNumReduceTasks(1);

        deletePath(job.getConfiguration(), output);
    }

    private void putStatisticsToResourceStore(String statisticsFolder, CubeSegment cubeSegment) throws IOException {
        Path statisticsFilePath = new Path(statisticsFolder, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION);
        FileSystem fs = HadoopUtil.getFileSystem("hdfs:///" + statisticsFilePath);
        if (!fs.exists(statisticsFilePath))
            throw new IOException("File " + statisticsFilePath + " does not exists;");

        FSDataInputStream is = fs.open(statisticsFilePath);
        try {
            // put the statistics to metadata store
            String statisticsFileName = ResourceStore.CUBE_STATISTICS_ROOT + "/" + cubeSegment.getCubeInstance().getName() + "/" + cubeSegment.getUuid() + ".seq";
            ResourceStore rs = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
            rs.putResource(statisticsFileName, is, System.currentTimeMillis());
        } finally {
            IOUtils.closeStream(is);
        }
    }

    public static void main(String[] args) throws Exception {
        FactDistinctColumnsJob job = new FactDistinctColumnsJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
