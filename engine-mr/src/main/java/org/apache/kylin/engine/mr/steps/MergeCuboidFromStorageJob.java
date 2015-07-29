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

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMROutput2.IMRStorageInputFormat;
import org.apache.kylin.engine.mr.IMROutput2.IMRStorageOutputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

/**
 */
public class MergeCuboidFromStorageJob extends CuboidJob {

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_JOB_FLOW_ID);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_NAME);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME).toUpperCase();
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment cubeSeg = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

            Configuration conf = this.getConf();

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            System.out.println("Starting: " + jobName);
            job = Job.getInstance(conf, jobName);

            setJobClasspath(job);
            
            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);
            job.getConfiguration().set(BatchConstants.CFG_IS_MERGE, "true");

            // configure mapper input
            IMRStorageInputFormat storageInputFormat = MRUtil.getBatchMergeInputSide2(cubeSeg).getStorageInputFormat();
            storageInputFormat.configureInput(MergeCuboidFromStorageMapper.class, ByteArrayWritable.class, ByteArrayWritable.class, job);

            // configure reducer output
            IMRStorageOutputFormat storageOutputFormat = MRUtil.getBatchMergeOutputSide2(cubeSeg).getStorageOutputFormat();
            storageOutputFormat.configureOutput(InMemCuboidReducer.class, getOptionValue(OPTION_JOB_FLOW_ID), job);
            
            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in MergeCuboidFromHBaseJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }

    }

}
