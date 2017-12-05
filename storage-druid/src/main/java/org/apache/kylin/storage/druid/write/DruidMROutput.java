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

package org.apache.kylin.storage.druid.write;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.common.MapReduceUtil;
import org.apache.kylin.engine.mr.steps.HiveToBaseCuboidMapper;
import org.apache.kylin.engine.mr.steps.InMemCuboidMapper;
import org.apache.kylin.engine.mr.steps.MergeCuboidJob;
import org.apache.kylin.engine.mr.steps.NDCuboidMapper;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.storage.druid.DruidSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kylin.engine.mr.JobBuilderSupport.appendExecCmdParameters;

public class DruidMROutput implements IMROutput2 {

    @Override
    public IMRBatchCubingOutputSide2 getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMRBatchCubingOutputSide2() {
            private JobBuilderSupport support = new JobBuilderSupport(seg, "");

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                createCalculateShardsStep(seg, jobFlow);
                createUpdateDruidTierStep(seg, jobFlow);
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                createConvertCuboidToDruidStep(support, seg, jobFlow);
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {

            }

            @Override
            public IMROutputFormat getOuputFormat() {
                return new DruidMROutputFormat();
            }

        };
    }

    @Override
    public IMRBatchMergeOutputSide2 getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMRBatchMergeOutputSide2() {
            private JobBuilderSupport support = new JobBuilderSupport(seg, "");

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                createCalculateShardsStep(seg, jobFlow);
                createUpdateDruidTierStep(seg, jobFlow);
            }

            @Override
            public void addStepPhase2_BuildCube(CubeSegment seg, List<CubeSegment> mergingSegments, DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(createMergeCuboidDataStep(support, seg, mergingSegments, jobFlow.getId(), MergeCuboidJob.class));

                createConvertCuboidToDruidStep(support, seg, jobFlow);
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                addMergingGarbageCollectionSteps(support, seg, jobFlow);
            }

            @Override
            public IMRMergeOutputFormat getOuputFormat() {
                return new DruidMergeMROutputFormat();
            }
        };
    }

    //Todo KOD support IMRBatchOptimizeOutputSide2
    public IMRBatchOptimizeOutputSide2 getBatchOptimizeOutputSide(final CubeSegment seg) {
        return new IMRBatchOptimizeOutputSide2() {

            @Override
            public void addStepPhase2_CreateHTable(DefaultChainedExecutable jobFlow) {
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            }

            @Override
            public void addStepPhase5_Cleanup(DefaultChainedExecutable jobFlow) {
            }
        };
    }

    private void createCalculateShardsStep(CubeSegment seg, DefaultChainedExecutable jobFlow) {
        CalculateShardsStep step = new CalculateShardsStep();
        step.setName("Calculate Shards Info");
        step.setCubeName(seg.getRealization().getName());
        step.setSegmentID(seg.getUuid());
        jobFlow.addTask(step);
    }

    private void createUpdateDruidTierStep(CubeSegment seg, DefaultChainedExecutable jobFlow) {
        UpdateDruidTierStep step = new UpdateDruidTierStep();
        step.setName("Update Druid Tier");
        step.setCubeName(seg.getRealization().getName());
        jobFlow.addTask(step);
    }

    private void createConvertCuboidToDruidStep(JobBuilderSupport support, CubeSegment seg, DefaultChainedExecutable jobFlow) {
        final String cubeName = seg.getRealization().getName();
        final String dataSource = DruidSchema.getDataSource(seg.getCubeDesc());

        final String inputPath = support.getCuboidRootPath(jobFlow.getId()) + "*";
        final String outputPath = seg.getConfig().getDruidHdfsLocation() + "/" + dataSource + "/" + seg.getUuid();

        StringBuilder cmd = new StringBuilder();
        support.appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Druid_Generator_" + seg + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, cubeName);
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, inputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);

        MapReduceExecutable step = new MapReduceExecutable();
        step.setName("Convert Cuboid to Druid");
        step.setMapReduceJobClass(ConvertToDruidJob.class);
        step.setMapReduceParams(cmd.toString());

        jobFlow.addTask(step);

        LoadDruidSegmentStep step2 = new LoadDruidSegmentStep();
        step2.setName("Load Segment to Druid");
        step2.setCubeName(seg.getRealization().getName());
        step2.setSegmentID(seg.getUuid());
        jobFlow.addTask(step2);
    }

    private MapReduceExecutable createMergeCuboidDataStep(JobBuilderSupport support, CubeSegment seg, List<CubeSegment> mergingSegments, String jobID, Class<? extends AbstractHadoopJob> clazz) {
        final List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingCuboidPaths.add(support.getCuboidRootPath(merging) + "*");
        }
        String formattedPath = StringUtil.join(mergingCuboidPaths, ",");
        String outputPath = support.getCuboidRootPath(jobID);

        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        support.appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, formattedPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Merge_Cuboid_" + seg.getCubeInstance().getName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(clazz);
        return mergeCuboidDataStep;
    }

    private void addMergingGarbageCollectionSteps(JobBuilderSupport support, CubeSegment seg, DefaultChainedExecutable jobFlow) {
        String jobId = jobFlow.getId();

        List<String> toDeletePaths = new ArrayList<>();
        toDeletePaths.addAll(getMergingHDFSPaths(support, seg));

        HDFSPathGCStep step = new HDFSPathGCStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION_HDFS);
        step.setDeletePaths(toDeletePaths);
        step.setJobId(jobId);

        jobFlow.addTask(step);
    }

    private List<String> getMergingHDFSPaths(JobBuilderSupport support, CubeSegment seg) {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization()).getMergingSegments((CubeSegment) seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge, target segment " + seg);
        final List<String> mergingHDFSPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            //kylin cuboid data
            mergingHDFSPaths.add(support.getJobWorkingDir(merging.getLastBuildJobID()));
        }
        return mergingHDFSPaths;
    }



    public static class DruidMROutputFormat implements IMROutputFormat {

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


    public static class DruidMergeMROutputFormat implements IMRMergeOutputFormat {

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
}
