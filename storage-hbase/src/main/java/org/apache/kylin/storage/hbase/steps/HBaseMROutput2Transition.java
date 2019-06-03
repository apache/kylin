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
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.MapReduceUtil;
import org.apache.kylin.engine.mr.steps.HiveToBaseCuboidMapper;
import org.apache.kylin.engine.mr.steps.InMemCuboidMapper;
import org.apache.kylin.engine.mr.steps.MergeCuboidJob;
import org.apache.kylin.engine.mr.steps.NDCuboidMapper;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IEngineAware;
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

        boolean useSpark = seg.getCubeDesc().getEngineType() == IEngineAware.ID_SPARK;

        // TODO need refactor
        final HBaseJobSteps steps = useSpark ? new HBaseSparkSteps(seg) : new HBaseMRSteps(seg);

        return new IMRBatchCubingOutputSide2() {

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCreateHTableStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(jobFlow.getId()));
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubingGarbageCollectionSteps(jobFlow);
            }

            @Override
            public IMROutputFormat getOutputFormat() {
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
        public void configureJobOutput(Job job, String output, CubeSegment segment, CuboidScheduler cuboidScheduler,
                int level) throws Exception {
            int reducerNum = 1;
            Class mapperClass = job.getMapperClass();

            //allow user specially set config for base cuboid step
            if (mapperClass == HiveToBaseCuboidMapper.class) {
                for (Map.Entry<String, String> entry : segment.getConfig().getBaseCuboidMRConfigOverride().entrySet()) {
                    job.getConfiguration().set(entry.getKey(), entry.getValue());
                }
            }

            if (mapperClass == HiveToBaseCuboidMapper.class || mapperClass == NDCuboidMapper.class) {
                reducerNum = MapReduceUtil.getLayeredCubingReduceTaskNum(segment, cuboidScheduler,
                        AbstractHadoopJob.getTotalMapInputMB(job), level);
            } else if (mapperClass == InMemCuboidMapper.class) {
                reducerNum = MapReduceUtil.getInmemCubingReduceTaskNum(segment, cuboidScheduler);
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
                jobFlow.addTask(steps.createCreateHTableStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase2_BuildCube(CubeSegment seg, List<CubeSegment> mergingSegments,
                    DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(
                        steps.createMergeCuboidDataStep(seg, mergingSegments, jobFlow.getId(), MergeCuboidJob.class));
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(jobFlow.getId()));
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addMergingGarbageCollectionSteps(jobFlow);
            }

            @Override
            public IMRMergeOutputFormat getOutputFormat() {
                return new HBaseMergeMROutputFormat();
            }
        };
    }

    public static class HBaseMergeMROutputFormat implements IMRMergeOutputFormat {

        @Override
        public void configureJobInput(Job job, String input) throws Exception {
            job.setInputFormatClass(SequenceFileInputFormat.class);
        }

        @Override
        public void configureJobOutput(Job job, String output, CubeSegment segment) throws Exception {
            int reducerNum = MapReduceUtil.getLayeredCubingReduceTaskNum(segment, segment.getCuboidScheduler(),
                    AbstractHadoopJob.getTotalMapInputMB(job), -1);
            job.setNumReduceTasks(reducerNum);

            Path outputPath = new Path(output);
            HadoopUtil.deletePath(job.getConfiguration(), outputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        }

        @Override
        public CubeSegment findSourceSegment(FileSplit fileSplit, CubeInstance cube) {
            String filePath = fileSplit.getPath().toString();
            String jobID = JobBuilderSupport.extractJobIDFromPath(filePath);
            return CubeInstance.findSegmentWithJobId(jobID, cube);
        }

    }

    public IMRBatchOptimizeOutputSide2 getBatchOptimizeOutputSide(final CubeSegment seg) {
        return new IMRBatchOptimizeOutputSide2() {
            HBaseMRSteps steps = new HBaseMRSteps(seg);

            @Override
            public void addStepPhase2_CreateHTable(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCreateHTableStep(jobFlow.getId(), CuboidModeEnum.RECOMMEND));
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createConvertCuboidToHfileStep(jobFlow.getId()));
                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addOptimizeGarbageCollectionSteps(jobFlow);
            }

            @Override
            public void addStepPhase5_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCheckpointGarbageCollectionSteps(jobFlow);
            }
        };
    }
}