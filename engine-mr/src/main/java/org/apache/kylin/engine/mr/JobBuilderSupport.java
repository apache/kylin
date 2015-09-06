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

package org.apache.kylin.engine.mr;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CreateDictionaryJob;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsJob;
import org.apache.kylin.engine.mr.steps.MergeDictionaryStep;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterBuildStep;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterMergeStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;

import com.google.common.base.Preconditions;

/**
 * Hold reusable steps for builders.
 */
public class JobBuilderSupport {

    final protected JobEngineConfig config;
    final protected CubeSegment seg;
    final protected String submitter;

    public JobBuilderSupport(CubeSegment seg, String submitter) {
        Preconditions.checkNotNull(seg, "segment cannot be null");
        this.config = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        this.seg = seg;
        this.submitter = submitter;
    }

    public MapReduceExecutable createFactDistinctColumnsStep(String jobId) {
        return createFactDistinctColumnsStep(jobId, false);
    }

    public MapReduceExecutable createFactDistinctColumnsStepWithStats(String jobId) {
        return createFactDistinctColumnsStep(jobId, true);
    }

    private MapReduceExecutable createFactDistinctColumnsStep(String jobId, boolean withStats) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(FactDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "output", getFactDistinctColumnsPath(jobId));
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "statisticsenabled", String.valueOf(withStats));
        appendExecCmdParameters(cmd, "statisticsoutput", getStatisticsPath(jobId));
        appendExecCmdParameters(cmd, "statisticssamplingpercent", String.valueOf(config.getConfig().getCubingInMemSamplingPercent()));
        appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + seg.getCubeInstance().getName() + "_Step");

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public HadoopShellExecutable createBuildDictionaryStep(String jobId) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", getFactDistinctColumnsPath(jobId));

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateDictionaryJob.class);
        return buildDictionaryStep;
    }

    public UpdateCubeInfoAfterBuildStep createUpdateCubeInfoAfterBuildStep(String jobId) {
        final UpdateCubeInfoAfterBuildStep updateCubeInfoStep = new UpdateCubeInfoAfterBuildStep();
        updateCubeInfoStep.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        updateCubeInfoStep.setCubeName(seg.getCubeInstance().getName());
        updateCubeInfoStep.setSegmentId(seg.getUuid());
        updateCubeInfoStep.setCubingJobId(jobId);
        return updateCubeInfoStep;
    }

    public MergeDictionaryStep createMergeDictionaryStep(List<String> mergingSegmentIds) {
        MergeDictionaryStep result = new MergeDictionaryStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_DICTIONARY);
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setMergingSegmentIds(mergingSegmentIds);
        return result;
    }

    public UpdateCubeInfoAfterMergeStep createUpdateCubeInfoAfterMergeStep(List<String> mergingSegmentIds, String jobId) {
        UpdateCubeInfoAfterMergeStep result = new UpdateCubeInfoAfterMergeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setMergingSegmentIds(mergingSegmentIds);
        result.setCubingJobId(jobId);
        return result;
    }

    // ============================================================================

    public String getJobWorkingDir(String jobId) {
        return getJobWorkingDir(config, jobId);
    }

    public String getCuboidRootPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/cuboid/";
    }

    public String getCuboidRootPath(CubeSegment seg) {
        return getCuboidRootPath(seg.getLastBuildJobID());
    }

    public void appendMapReduceParameters(StringBuilder buf, CubeSegment seg) {
        try {
            String jobConf = config.getHadoopJobConfFilePath(seg.getCubeDesc().getModel().getCapacity());
            if (jobConf != null && jobConf.length() > 0) {
                buf.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getFactDistinctColumnsPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/fact_distinct_columns";
    }

    public String getStatisticsPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/statistics";
    }

    // ============================================================================
    // static methods also shared by other job flow participant
    // ----------------------------------------------------------------------------

    public static String getJobWorkingDir(JobEngineConfig conf, String jobId) {
        return conf.getHdfsWorkingDirectory() + "kylin-" + jobId;
    }

    public static StringBuilder appendExecCmdParameters(StringBuilder buf, String paraName, String paraValue) {
        return buf.append(" -").append(paraName).append(" ").append(paraValue);
    }

}
