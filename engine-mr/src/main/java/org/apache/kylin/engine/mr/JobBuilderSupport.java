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

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CreateDictionaryJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
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
        this.config = new JobEngineConfig(seg.getConfig());
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
        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getFactDistinctColumnsPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_ENABLED, String.valueOf(withStats));
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_OUTPUT, getStatisticsPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_STATS_SAMPLING_PERCENT, String.valueOf(config.getConfig().getCubingInMemSamplingPercent()));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Fact_Distinct_Columns_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public HadoopShellExecutable createBuildDictionaryStep(String jobId) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getFactDistinctColumnsPath(jobId));

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateDictionaryJob.class);
        return buildDictionaryStep;
    }

    public UpdateCubeInfoAfterBuildStep createUpdateCubeInfoAfterBuildStep(String jobId) {
        final UpdateCubeInfoAfterBuildStep result = new UpdateCubeInfoAfterBuildStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        CubingExecutableUtil.setIndexPath(this.getSecondaryIndexPath(jobId), result.getParams());

        return result;
    }

    public MergeDictionaryStep createMergeDictionaryStep(List<String> mergingSegmentIds) {
        MergeDictionaryStep result = new MergeDictionaryStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_DICTIONARY);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());

        return result;
    }

    public UpdateCubeInfoAfterMergeStep createUpdateCubeInfoAfterMergeStep(List<String> mergingSegmentIds, String jobId) {
        UpdateCubeInfoAfterMergeStep result = new UpdateCubeInfoAfterMergeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());
        CubingExecutableUtil.setIndexPath(this.getSecondaryIndexPath(jobId), result.getParams());

        return result;
    }

    // ============================================================================

    public String getJobWorkingDir(String jobId) {
        return getJobWorkingDir(config, jobId);
    }

    public String getRealizationRootPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getRealization().getName();
    }

    public String getCuboidRootPath(String jobId) {
        return getRealizationRootPath(jobId) + "/cuboid/";
    }

    public String getCuboidRootPath(CubeSegment seg) {
        return getCuboidRootPath(seg.getLastBuildJobID());
    }

    public String getSecondaryIndexPath(String jobId) {
        return getRealizationRootPath(jobId) + "/secondary_index/";
    }

    public void appendMapReduceParameters(StringBuilder buf) {
        appendMapReduceParameters(buf, JobEngineConfig.DEFAUL_JOB_CONF_SUFFIX);
    }

    public void appendMapReduceParameters(StringBuilder buf, String jobType) {
        try {
            String jobConf = config.getHadoopJobConfFilePath(jobType);
            if (jobConf != null && jobConf.length() > 0) {
                buf.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getFactDistinctColumnsPath(String jobId) {
        return getRealizationRootPath(jobId) + "/fact_distinct_columns";
    }

    public String getStatisticsPath(String jobId) {
        return getRealizationRootPath(jobId) + "/statistics";
    }

    // ============================================================================
    // static methods also shared by other job flow participant
    // ----------------------------------------------------------------------------

    public static String getJobWorkingDir(JobEngineConfig conf, String jobId) {
        return getJobWorkingDir(conf.getHdfsWorkingDirectory(), jobId);
    }

    public static String getJobWorkingDir(String hdfsDir, String jobId) {
        if (!hdfsDir.endsWith("/")) {
            hdfsDir = hdfsDir + "/";
        }
        return hdfsDir + "kylin-" + jobId;
    }

    public static StringBuilder appendExecCmdParameters(StringBuilder buf, String paraName, String paraValue) {
        return buf.append(" -").append(paraName).append(" ").append(paraValue);
    }

    public String[] getCuboidOutputPaths(String cuboidRootPath, int totalRowkeyColumnCount, int groupRowkeyColumnsCount) {
        String[] paths = new String[groupRowkeyColumnsCount + 1];
        for (int i = 0; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnCount - i;
            if (dimNum == totalRowkeyColumnCount) {
                paths[i] = cuboidRootPath + "base_cuboid";
            } else {
                paths[i] = cuboidRootPath + dimNum + "d_cuboid";
            }
        }
        return paths;
    }

}
