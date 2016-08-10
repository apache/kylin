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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidCLI;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.manager.ExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysong1
 */
public class CuboidJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(CuboidJob.class);
    private static final String MAPRED_REDUCE_TASKS = "mapred.reduce.tasks";

    @SuppressWarnings("rawtypes")
    private Class<? extends Mapper> mapperClass;

    private boolean skipped = false;

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    private boolean checkSkip(String cubingJobId) {
        if (cubingJobId == null)
            return false;

        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubingJob cubingJob = (CubingJob) execMgr.getJob(cubingJobId);
        skipped = cubingJob.isLayerCubing() == false;
        return skipped;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (this.mapperClass == null)
            throw new Exception("Mapper class is not set!");

        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_NCUBOID_LEVEL);
            options.addOption(OPTION_INPUT_FORMAT);
            options.addOption(OPTION_CUBING_JOB_ID);
            parseOptions(options, args);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            int nCuboidLevel = Integer.parseInt(getOptionValue(OPTION_NCUBOID_LEVEL));
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);
            String cubingJobId = getOptionValue(OPTION_CUBING_JOB_ID);

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);

            if (checkSkip(cubingJobId)) {
                logger.info("Skip job " + getOptionValue(OPTION_JOB_NAME) + " for " + segmentID + "[" + segmentID + "]");
                return 0;
            }

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            job.getConfiguration().set(BatchConstants.ARG_CUBING_JOB_ID, cubingJobId);
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            // Mapper
            configureMapperInputFormat(cube.getSegmentById(segmentID));
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
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
            job.getConfiguration().setInt(BatchConstants.CFG_CUBE_CUBOID_LEVEL, nCuboidLevel);
            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            setReduceTaskNum(job, cube.getDescriptor(), nCuboidLevel);

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

    private void configureMapperInputFormat(CubeSegment cubeSeg) throws IOException {
        String input = getOptionValue(OPTION_INPUT_PATH);

        if ("FLAT_TABLE".equals(input)) {
            // base cuboid case
            IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();
            flatTableInputFormat.configureJob(job);
        } else {
            // n-dimension cuboid case
            FileInputFormat.setInputPaths(job, new Path(input));
            if (hasOption(OPTION_INPUT_FORMAT) && ("textinputformat".equalsIgnoreCase(getOptionValue(OPTION_INPUT_FORMAT)))) {
                job.setInputFormatClass(TextInputFormat.class);
            } else {
                job.setInputFormatClass(SequenceFileInputFormat.class);
            }
        }
    }

    protected void setReduceTaskNum(Job job, CubeDesc cubeDesc, int level) throws ClassNotFoundException, IOException, InterruptedException, JobException {
        Configuration jobConf = job.getConfiguration();
        KylinConfig kylinConfig = cubeDesc.getConfig();

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

        // adjust reducer number for cube which has DISTINCT_COUNT measures for better performance
        if (cubeDesc.hasMemoryHungryMeasures()) {
            numReduceTasks = numReduceTasks * 4;
        }

        // at least 1 reducer by default
        numReduceTasks = Math.max(kylinConfig.getHadoopJobMinReducerNumber(), numReduceTasks);
        // no more than 500 reducer by default
        numReduceTasks = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), numReduceTasks);

        jobConf.setInt(MAPRED_REDUCE_TASKS, numReduceTasks);

        logger.info("Having total map input MB " + Math.round(totalMapInputMB));
        logger.info("Having level " + level + ", pre-level cuboids " + preLevelCuboids + ", this level cuboids " + thisLevelCuboids);
        logger.info("Having per reduce MB " + perReduceInputMB + ", reduce count ratio " + reduceCountRatio);
        logger.info("Setting " + MAPRED_REDUCE_TASKS + "=" + numReduceTasks);
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
