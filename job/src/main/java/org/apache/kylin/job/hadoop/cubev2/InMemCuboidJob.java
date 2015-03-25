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

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.CuboidCLI;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.job.hadoop.cube.BaseCuboidMapper;
import org.apache.kylin.job.hadoop.cube.CuboidJob;
import org.apache.kylin.job.hadoop.cube.CuboidReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author honma
 * 
 */

public class InMemCuboidJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(InMemCuboidJob.class);
    private static final String MAPRED_REDUCE_TASKS = "mapred.reduce.tasks";

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
            parseOptions(options, args);

            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            int nCuboidLevel = Integer.parseInt(getOptionValue(OPTION_NCUBOID_LEVEL));
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME);
            String intermediateTable = getOptionValue(OPTION_TABLE_NAME);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            logger.info("Starting: " + job.getJobName());
            FileInputFormat.setInputPaths(job, input);

            setJobClasspath(job);

            String[] dbTableNames = HadoopUtil.parseHiveTableName(intermediateTable);
            HCatInputFormat.setInput(job, dbTableNames[0],
                    dbTableNames[1]);

            job.setInputFormatClass(HCatInputFormat.class);

            job.setMapperClass(InMemCuboidMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setCombinerClass(CuboidReducer.class); // for base cuboid shuffle skew, some rowkey aggregates far more records than others

            // Reducer
            job.setReducerClass(CuboidReducer.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, output);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);
            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            setReduceTaskNum(job, config, cubeName, nCuboidLevel);

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in CuboidJob", e);
            printUsage(options);
            throw e;
        }
    }

    protected void setReduceTaskNum(Job job, KylinConfig config, String cubeName, int level) throws ClassNotFoundException, IOException, InterruptedException, JobException {
        Configuration jobConf = job.getConfiguration();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        CubeDesc cubeDesc = CubeManager.getInstance(config).getCube(cubeName).getDescriptor();

        double perReduceInputMB = kylinConfig.getDefaultHadoopJobReducerInputMB();
        double reduceCountRatio = kylinConfig.getDefaultHadoopJobReducerCountRatio();

        // total map input MB
        double totalMapInputMB = this.getTotalMapInputMB();

        // output / input ratio
        int preLevelCuboids, thisLevelCuboids;
        if (level == 0) { // base cuboid
            preLevelCuboids = thisLevelCuboids = 1;
        } else { // n-cuboid
            int[] allLevelCount = CuboidCLI.calculateAllLevelCount(cubeDesc);
            preLevelCuboids = allLevelCount[level - 1];
            thisLevelCuboids = allLevelCount[level];
        }

        // total reduce input MB
        double totalReduceInputMB = totalMapInputMB * thisLevelCuboids / preLevelCuboids;

        // number of reduce tasks
        int numReduceTasks = (int) Math.round(totalReduceInputMB / perReduceInputMB * reduceCountRatio);

        // adjust reducer number for cube which has DISTINCT_COUNT measures for
        // better performance
        if (cubeDesc.hasHolisticCountDistinctMeasures()) {
            numReduceTasks = numReduceTasks * 4;
        }

        // at least 1 reducer
        numReduceTasks = Math.max(1, numReduceTasks);
        // no more than 5000 reducer by default
        numReduceTasks = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), numReduceTasks);

        jobConf.setInt(MAPRED_REDUCE_TASKS, numReduceTasks);

        logger.info("Having total map input MB " + Math.round(totalMapInputMB));
        logger.info("Having level " + level + ", pre-level cuboids " + preLevelCuboids + ", this level cuboids " + thisLevelCuboids);
        logger.info("Having per reduce MB " + perReduceInputMB + ", reduce count ratio " + reduceCountRatio);
        logger.info("Setting " + MAPRED_REDUCE_TASKS + "=" + numReduceTasks);
    }

    public static void main(String[] args) throws Exception {
        InMemCuboidJob job = new InMemCuboidJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
    
}
