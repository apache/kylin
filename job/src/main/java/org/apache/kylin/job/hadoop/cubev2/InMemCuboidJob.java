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

package org.apache.kylin.job.hadoop.cubev2;

import com.google.common.collect.Lists;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author honma
 */

public class InMemCuboidJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(InMemCuboidJob.class);
    private static final String MAPRED_REDUCE_TASKS = "mapred.reduce.tasks";
    public static final String REGION_SPLITS = "kylin.region.splits";
    private String statisticsOutput;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_NCUBOID_LEVEL);
            options.addOption(OPTION_INPUT_FORMAT);
            options.addOption(OPTION_TABLE_NAME);
            options.addOption(OPTION_HTABLE_NAME);
            options.addOption(OPTION_STATISTICS_OUTPUT);
            parseOptions(options, args);

            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME);
            String intermediateTable = getOptionValue(OPTION_TABLE_NAME);
            statisticsOutput = getOptionValue(OPTION_STATISTICS_OUTPUT);
            String htableName = getOptionValue(OPTION_HTABLE_NAME).toUpperCase();

            System.out.println("statisticsOutput is " + statisticsOutput);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);


            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            logger.info("Starting: " + job.getJobName());
            FileInputFormat.setInputPaths(job, input);

            setJobClasspath(job);

            DataModelDesc.RealizationCapacity realizationCapacity = cube.getDescriptor().getModel().getCapacity();
            job.getConfiguration().set(BatchConstants.CUBE_CAPACITY, realizationCapacity.toString());

            String[] dbTableNames = HadoopUtil.parseHiveTableName(intermediateTable);
            HCatInputFormat.setInput(job, dbTableNames[0],
                    dbTableNames[1]);

            job.setInputFormatClass(HCatInputFormat.class);

            // set Mapper
            job.setMapperClass(InMemCuboidMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Text.class);
            FileOutputFormat.setOutputPath(job, output);

            /*
            List<Long> regionSplits = parseCuboidStatistics();
            String regionSplitStr = StringUtils.join(regionSplits.toArray(new Long[regionSplits.size()]), ",");
            System.out.println("The region splits is : " + regionSplitStr);

            job.getConfiguration().setInt(MAPRED_REDUCE_TASKS, regionSplits.size() + 1);
            job.getConfiguration().set(REGION_SPLITS, regionSplitStr);

            //job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

            */

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);
            Configuration conf = HBaseConfiguration.create(getConf());

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            HTable htable = new HTable(conf, htableName);
            HFileOutputFormat2.configureIncrementalLoad(job,
                    htable);


            // set Reducer; This need be after configureIncrementalLoad, to overwrite the default reducer class
            job.setReducerClass(InMemCuboidReducer.class);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(KeyValue.class);

            this.deletePath(job.getConfiguration(), output);
            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in CuboidJob", e);
            printUsage(options);
            throw e;
        }
    }

    @SuppressWarnings("deprecation")
    private List<Long> parseCuboidStatistics() throws IOException {

        List<Long> regionSplit = Lists.newArrayList();

        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path seqFilePath = new Path(statisticsOutput, "part-r-00000");

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, seqFilePath, conf);

        try {
            LongWritable key = (LongWritable) reader.getKeyClass().newInstance();
            LongWritable value = (LongWritable) reader.getValueClass().newInstance();
            while (reader.next(key, value)) {
                regionSplit.add(key.get());
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            reader.close();
        }

        return regionSplit;
    }

    public static void main(String[] args) throws Exception {
        InMemCuboidJob job = new InMemCuboidJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
