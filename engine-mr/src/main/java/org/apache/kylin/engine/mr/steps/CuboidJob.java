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

import java.util.Locale;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CuboidSchedulerUtil;
import org.apache.kylin.job.execution.ExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysong1
 */
public class CuboidJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(CuboidJob.class);

    @SuppressWarnings("rawtypes")
    private Class<? extends Mapper> mapperClass;

    private boolean skipped = false;

    private CuboidScheduler cuboidScheduler;

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    private boolean checkSkip(String cubingJobId, int level) {
        if (cubingJobId == null)
            return false;

        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubingJob cubingJob = (CubingJob) execMgr.getJob(cubingJobId);
        skipped = cubingJob.isLayerCubing() == false;
        if (!skipped) {
            skipped = (level > cuboidScheduler.getBuildLevel());
            if (skipped) {
                logger.info("Job level: " + level + " for " + cubingJobId + "[" + cubingJobId
                        + "] exceeds real cuboid tree levels : " + cuboidScheduler.getBuildLevel());
            }
        }
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
            options.addOption(OPTION_CUBING_JOB_ID);
            options.addOption(OPTION_CUBOID_MODE);
            options.addOption(OPTION_DICTIONARY_SHRUNKEN_PATH);
            parseOptions(options, args);

            String output = getOptionValue(OPTION_OUTPUT_PATH);
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase(Locale.ROOT);
            int nCuboidLevel = Integer.parseInt(getOptionValue(OPTION_NCUBOID_LEVEL));
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);
            String cubingJobId = getOptionValue(OPTION_CUBING_JOB_ID);
            String cuboidModeName = getOptionValue(OPTION_CUBOID_MODE);
            if (cuboidModeName == null) {
                cuboidModeName = CuboidModeEnum.CURRENT.toString();
            }

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment segment = cube.getSegmentById(segmentID);

            cuboidScheduler = CuboidSchedulerUtil.getCuboidSchedulerByMode(segment, cuboidModeName);

            if (checkSkip(cubingJobId, nCuboidLevel)) {
                logger.info(
                        "Skip job " + getOptionValue(OPTION_JOB_NAME) + " for " + segmentID + "[" + segmentID + "]");
                return 0;
            }

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            job.getConfiguration().set(BatchConstants.ARG_CUBING_JOB_ID, cubingJobId);
            String shrunkenDictPath = getOptionValue(OPTION_DICTIONARY_SHRUNKEN_PATH);
            if (shrunkenDictPath != null) {
                job.getConfiguration().set(BatchConstants.ARG_SHRUNKEN_DICT_PATH, shrunkenDictPath);
            }
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            // add metadata to distributed cache
            attachSegmentMetadataWithAll(segment, job.getConfiguration());

            // Mapper
            job.setMapperClass(this.mapperClass);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setCombinerClass(CuboidReducer.class); // for base cuboid shuffle skew, some rowkey aggregates far more records than others

            // Reducer
            job.setReducerClass(CuboidReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // set input
            configureMapperInputFormat(segment);

            // set output
            IMROutput2.IMROutputFormat outputFormat = MRUtil.getBatchCubingOutputSide2(segment).getOutputFormat();
            outputFormat.configureJobOutput(job, output, segment, cuboidScheduler, nCuboidLevel);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
            job.getConfiguration().setInt(BatchConstants.CFG_CUBE_CUBOID_LEVEL, nCuboidLevel);
            job.getConfiguration().set(BatchConstants.CFG_CUBOID_MODE, cuboidModeName);

            return waitForCompletion(job);
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private void configureMapperInputFormat(CubeSegment cubeSeg) throws Exception {
        String input = getOptionValue(OPTION_INPUT_PATH);

        if ("FLAT_TABLE".equals(input)) {
            // base cuboid case
            IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg)
                    .getFlatTableInputFormat();
            flatTableInputFormat.configureJob(job);
        } else {
            // n-dimension cuboid case
            IMROutput2.IMROutputFormat outputFormat = MRUtil.getBatchCubingOutputSide2(cubeSeg).getOutputFormat();
            outputFormat.configureJobInput(job, input);
            FileInputFormat.setInputPaths(job, new Path(input));
        }
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
