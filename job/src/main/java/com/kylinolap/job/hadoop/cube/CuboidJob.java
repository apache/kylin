/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.hadoop.cube;

import java.io.File;
import java.io.IOException;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.cuboid.CuboidCLI;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.metadata.model.cube.CubeDesc;

/**
 * @author ysong1
 */
public class CuboidJob extends AbstractHadoopJob {

    protected static final Logger log = LoggerFactory.getLogger(CuboidJob.class);
    private static final String MAPRED_REDUCE_TASKS = "mapred.reduce.tasks";

    @SuppressWarnings("rawtypes")
    private Class<? extends Mapper> mapperClass;

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
            parseOptions(options, args);

            Path input = new Path(getOptionValue(OPTION_INPUT_PATH));
            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            int nCuboidLevel = Integer.parseInt(getOptionValue(OPTION_NCUBOID_LEVEL));
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            System.out.println("Starting: " + job.getJobName());
            FileInputFormat.setInputPaths(job, input);

            File jarFile = new File(config.getKylinJobJarPath());
            if (jarFile.exists()) {
                job.setJar(config.getKylinJobJarPath());
            } else {
                job.setJarByClass(this.getClass());
            }

            // Mapper
            if (this.mapperClass == null) {
                throw new Exception("Mapper class is not set!");
            }

            boolean isInputTextFormat = false;
            if (hasOption(OPTION_INPUT_FORMAT) && ("textinputformat".equalsIgnoreCase(getOptionValue(OPTION_INPUT_FORMAT)))) {
                isInputTextFormat = true;
            }

            if (isInputTextFormat) {
                job.setInputFormatClass(TextInputFormat.class);

            } else {
                job.setInputFormatClass(SequenceFileInputFormat.class);
            }
            job.setMapperClass(this.mapperClass);
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
            printUsage(options);
            log.error(e.getLocalizedMessage(), e);
            return 2;
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

        System.out.println("Having total map input MB " + Math.round(totalMapInputMB));
        System.out.println("Having level " + level + ", pre-level cuboids " + preLevelCuboids + ", this level cuboids " + thisLevelCuboids);
        System.out.println("Having per reduce MB " + perReduceInputMB + ", reduce count ratio " + reduceCountRatio);
        System.out.println("Setting " + MAPRED_REDUCE_TASKS + "=" + numReduceTasks);
    }

    /**
     * @param mapperClass
     *            the mapperClass to set
     */
    @SuppressWarnings("rawtypes")
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
    }

}
