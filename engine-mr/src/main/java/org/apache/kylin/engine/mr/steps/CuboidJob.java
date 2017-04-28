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
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
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
            CubeSegment segment = cube.getSegmentById(segmentID);

            if (checkSkip(cubingJobId)) {
                logger.info("Skip job " + getOptionValue(OPTION_JOB_NAME) + " for " + segmentID + "[" + segmentID + "]");
                return 0;
            }

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            job.getConfiguration().set(BatchConstants.ARG_CUBING_JOB_ID, cubingJobId);
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            // Mapper
            configureMapperInputFormat(segment);
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
            attachSegmentMetadataWithDict(segment, job.getConfiguration());

            job.setNumReduceTasks(LayerReducerNumSizing.getReduceTaskNum(segment, getTotalMapInputMB(), nCuboidLevel));

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
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

    /**
     * @param mapperClass
     *            the mapperClass to set
     */
    @SuppressWarnings("rawtypes")
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
    }

}
