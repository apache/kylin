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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.steps.HiveToBaseCuboidMapper;
import org.apache.kylin.engine.mr.steps.InMemCuboidMapper;
import org.apache.kylin.engine.mr.steps.MergeCuboidJob;
import org.apache.kylin.engine.mr.steps.NDCuboidMapper;
import org.apache.kylin.engine.mr.steps.ReducerNumSizing;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This "Transition" impl generates cuboid files and then convert to HFile.
 * The additional step slows down build process, but the gains is merge
 * can read from HDFS instead of over HBase region server. See KYLIN-1007.
 * 
 * This is transitional because finally we want to merge from HTable snapshot.
 * However multiple snapshots as MR input is only supported by HBase 1.x.
 * Before most users upgrade to latest HBase, they can only use this transitional
 * cuboid file solution.
 */
public class HBaseMROutput2Transition implements IMROutput2 {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HBaseMROutput2Transition.class);

    @Override
    public IMRBatchCubingOutputSide2 getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMRBatchCubingOutputSide2() {
            HBaseMRSteps steps = new HBaseMRSteps(seg);

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCreateHTableStepWithStats(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(jobFlow.getId()));
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                // nothing to do
            }

            @Override
            public IMROutputFormat getOuputFormat() {
                return new HBaseMROutputFormat();
            }
        };
    }

    public static class HBaseMROutputFormat implements IMROutputFormat {

        @Override
        public void configureJobInput(Job job, String input) throws Exception {
            job.setInputFormatClass(SequenceFileInputFormat.class);
        }

        @Override
        public void configureJobOutput(Job job, String output, CubeSegment segment, int level) throws Exception {
            int reducerNum = 1;
            Class mapperClass = job.getMapperClass();
            if (mapperClass == HiveToBaseCuboidMapper.class || mapperClass == NDCuboidMapper.class) {
                reducerNum = ReducerNumSizing.getLayeredCubingReduceTaskNum(segment, AbstractHadoopJob.getTotalMapInputMB(job), level);
            } else if (mapperClass == InMemCuboidMapper.class) {
                reducerNum = ReducerNumSizing.getInmemCubingReduceTaskNum(segment);
            }
            Path outputPath = new Path(output);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setNumReduceTasks(reducerNum);
            HadoopUtil.deletePath(job.getConfiguration(), outputPath);
        }
    }

    @Override
    public IMRBatchMergeOutputSide2 getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMRBatchMergeOutputSide2() {
            HBaseMRSteps steps = new HBaseMRSteps(seg);

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCreateHTableStepWithStats(jobFlow.getId()));
            }

            @Override
            public void addStepPhase2_BuildCube(CubeSegment seg, List<CubeSegment> mergingSegments, DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createMergeCuboidDataStep(seg, mergingSegments, jobFlow.getId(), MergeCuboidJob.class));
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(jobFlow.getId()));
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addMergingGarbageCollectionSteps(jobFlow);
            }

            @Override
            public IMRMergeOutputFormat getOuputFormat() {
                return new HBaseMergeMROutputFormat();
            }
        };
    }

    public static class HBaseMergeMROutputFormat implements IMRMergeOutputFormat{

        private static final Pattern JOB_NAME_PATTERN = Pattern.compile("kylin-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

        @Override
        public void configureJobInput(Job job, String input) throws Exception {
            job.setInputFormatClass(SequenceFileInputFormat.class);
        }

        @Override
        public void configureJobOutput(Job job, String output, CubeSegment segment) throws Exception {
            int reducerNum = ReducerNumSizing.getLayeredCubingReduceTaskNum(segment, AbstractHadoopJob.getTotalMapInputMB(job), -1);
            job.setNumReduceTasks(reducerNum);

            Path outputPath = new Path(output);
            HadoopUtil.deletePath(job.getConfiguration(), outputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        }

        @Override
        public CubeSegment findSourceSegment(FileSplit fileSplit, CubeInstance cube) {
            String filePath = fileSplit.getPath().toString();
            String jobID = extractJobIDFromPath(filePath);
            return findSegmentWithUuid(jobID, cube);
        }

        private static String extractJobIDFromPath(String path) {
            Matcher matcher = JOB_NAME_PATTERN.matcher(path);
            // check the first occurrence
            if (matcher.find()) {
                return matcher.group(1);
            } else {
                throw new IllegalStateException("Can not extract job ID from file path : " + path);
            }
        }

        private static CubeSegment findSegmentWithUuid(String jobID, CubeInstance cubeInstance) {
            for (CubeSegment segment : cubeInstance.getSegments()) {
                String lastBuildJobID = segment.getLastBuildJobID();
                if (lastBuildJobID != null && lastBuildJobID.equalsIgnoreCase(jobID)) {
                    return segment;
                }
            }
            throw new IllegalStateException("No merging segment's last build job ID equals " + jobID);
        }
    }
}