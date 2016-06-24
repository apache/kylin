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

package org.apache.kylin.storage.hbase.steps;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xjiang, ysong1
 * 
 */

public class RangeKeyDistributionJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(RangeKeyDistributionJob.class);

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);

            parseOptions(options, args);

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            job = Job.getInstance(getConf(), jobName);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            KylinConfig kylinConfig = cube.getConfig();

            setJobClasspath(job, kylinConfig);

            addInputDirs(getOptionValue(OPTION_INPUT_PATH), job);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileOutputFormat.setOutputPath(job, output);
            // job.getConfiguration().set("dfs.block.size", "67108864");

            // Mapper
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(RangeKeyDistributionMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            // Reducer - only one
            job.setReducerClass(RangeKeyDistributionReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setNumReduceTasks(1);

            this.deletePath(job.getConfiguration(), output);

            float hfileSizeGB = kylinConfig.getHBaseHFileSizeGB();
            float regionSplitSize = kylinConfig.getKylinHBaseRegionCut();

            int compactionThreshold = Integer.valueOf(HBaseConnection.getCurrentHBaseConfiguration().get("hbase.hstore.compactionThreshold", "3"));
            if (hfileSizeGB > 0 && hfileSizeGB * compactionThreshold < regionSplitSize) {
                hfileSizeGB = regionSplitSize / compactionThreshold;
                logger.info("Adjust hfile size' to " + hfileSizeGB);
            }
            int maxRegionCount = kylinConfig.getHBaseRegionCountMax();
            int minRegionCount = kylinConfig.getHBaseRegionCountMin();
            job.getConfiguration().set(BatchConstants.CFG_OUTPUT_PATH, output.toString());
            job.getConfiguration().set(BatchConstants.CFG_HFILE_SIZE_GB, String.valueOf(hfileSizeGB));
            job.getConfiguration().set(BatchConstants.CFG_REGION_SPLIT_SIZE, String.valueOf(regionSplitSize));
            job.getConfiguration().set(BatchConstants.CFG_REGION_NUMBER_MAX, String.valueOf(maxRegionCount));
            job.getConfiguration().set(BatchConstants.CFG_REGION_NUMBER_MIN, String.valueOf(minRegionCount));
            // The partition file for hfile is sequenece file consists of ImmutableBytesWritable and NullWritable
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), ImmutableBytesWritable.class, NullWritable.class);

            return waitForCompletion(job);
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RangeKeyDistributionJob(), args);
        System.exit(exitCode);
    }

}
