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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.metrics.JobMetricsFacade;
import org.apache.kylin.job.util.MailNotificationUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class CubingJob extends DefaultChainedExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CubingJob.class);

    public enum AlgorithmEnum {
        LAYER, INMEM
    }
    public enum CubingJobTypeEnum {
        BUILD("BUILD", 20), OPTIMIZE("OPTIMIZE", 5), MERGE("MERGE", 25), STREAM("STREAM", 30);

        private final String name;
        private final int defaultPriority;

        CubingJobTypeEnum(String name, int priority) {
            this.name = name;
            this.defaultPriority = priority;
        }

        public int getDefaultPriority() {
            return defaultPriority;
        }

        public String toString() {
            return name;
        }

        public static CubingJobTypeEnum getByName(String name) {
            if (Strings.isNullOrEmpty(name)) {
                return null;
            }
            for (CubingJobTypeEnum jobTypeEnum : CubingJobTypeEnum.values()) {
                if (jobTypeEnum.name.equals(name.toUpperCase(Locale.ROOT))) {
                    return jobTypeEnum;
                }
            }
            return null;
        }
    }

    //32MB per block created by the first step
    private static final long MIN_SOURCE_SIZE = 33554432L;

    // KEYS of Output.extraInfo map, info passed across job steps
    public static final String SOURCE_RECORD_COUNT = "sourceRecordCount";
    public static final String SOURCE_SIZE_BYTES = "sourceSizeBytes";
    public static final String CUBE_SIZE_BYTES = "byteSizeBytes";
    public static final String MAP_REDUCE_WAIT_TIME = "mapReduceWaitTime";
    private static final String DEPLOY_ENV_NAME = "envName";
    private static final String JOB_TYPE = "jobType";
    private static final String SEGMENT_NAME = "segmentName";

    public static CubingJob createBuildJob(CubeSegment seg, String submitter, JobEngineConfig config) {
        return initCubingJob(seg, CubingJobTypeEnum.BUILD.toString(), submitter, config);
    }

    public static CubingJob createOptimizeJob(CubeSegment seg, String submitter, JobEngineConfig config) {
        return initCubingJob(seg, CubingJobTypeEnum.OPTIMIZE.toString(), submitter, config);
    }

    public static CubingJob createMergeJob(CubeSegment seg, String submitter, JobEngineConfig config) {
        return initCubingJob(seg, CubingJobTypeEnum.MERGE.toString(), submitter, config);
    }

    public static CubingJob createStreamJob(CubeSegment seg, String submitter, JobEngineConfig config) {
        return initCubingJob(seg, CubingJobTypeEnum.STREAM.toString(), submitter, config);
    }

    private static CubingJob initCubingJob(CubeSegment seg, String jobType, String submitter, JobEngineConfig config) {
        KylinConfig kylinConfig = config.getConfig();
        CubeInstance cube = seg.getCubeInstance();
        List<ProjectInstance> projList = ProjectManager.getInstance(kylinConfig).findProjects(cube.getType(),
                cube.getName());
        if (projList == null || projList.size() == 0) {
            throw new RuntimeException("Cannot find the project containing the cube " + cube.getName() + "!!!");
        } else if (projList.size() >= 2) {
            String msg = "Find more than one project containing the cube " + cube.getName()
                    + ". It does't meet the uniqueness requirement!!! ";
            if (!config.getConfig().allowCubeAppearInMultipleProjects()) {
                throw new RuntimeException(msg);
            } else {
                logger.warn(msg);
            }
        }

        CubingJob result = new CubingJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        result.setDeployEnvName(kylinConfig.getDeployEnv());
        result.setProjectName(projList.get(0).getName());
        result.setJobType(jobType);
        CubingExecutableUtil.setCubeName(seg.getCubeInstance().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setSegmentName(seg.getName(), result.getParams());
        result.setName(jobType + " CUBE - " + seg.getCubeInstance().getDisplayName() + " - " + seg.getName() + " - "
                + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);
        result.setNotifyList(seg.getCubeInstance().getDescriptor().getNotifyList());
        return result;
    }

    public CubingJob() {
        super();
    }

    @Override
    public int getDefaultPriority() {
        CubingJobTypeEnum jobType = CubingJobTypeEnum.getByName(getJobType());
        if (jobType == null) {
            return super.getDefaultPriority();
        }
        return jobType.getDefaultPriority();
    }

    protected void setDeployEnvName(String name) {
        setParam(DEPLOY_ENV_NAME, name);
    }

    public String getDeployEnvName() {
        return getParam(DEPLOY_ENV_NAME);
    }

    public String getJobType() {
        return getParam(JOB_TYPE);
    }

    public String getSegmentName() {
        return getParam(SEGMENT_NAME);
    }

    void setJobType(String jobType) {
        setParam(JOB_TYPE, jobType);
    }

    @Override
    protected Pair<String, String> formatNotifications(ExecutableContext context, ExecutableState state) {
        CubeInstance cubeInstance = CubeManager.getInstance(context.getConfig())
                .getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final Output output = getManager().getOutput(getId());
        if (state != ExecutableState.ERROR
                && !cubeInstance.getDescriptor().getStatusNeedNotify().contains(state.toString())) {
            logger.info("state:" + state + " no need to notify users");
            return null;
        }

        if (!MailNotificationUtil.hasMailNotification(state)) {
            logger.info("Cannot find email template for job state: " + state);
            return null;
        }

        Map<String, Object> dataMap = Maps.newHashMap();
        dataMap.put("job_name", getName());
        dataMap.put("env_name", getDeployEnvName());
        dataMap.put("submitter", StringUtil.noBlank(getSubmitter(), "missing submitter"));
        dataMap.put("job_engine", MailNotificationUtil.getLocalHostName());
        dataMap.put("project_name", getProjectName());
        dataMap.put("cube_name", cubeInstance.getName());
        dataMap.put("source_records_count", String.valueOf(findSourceRecordCount()));
        dataMap.put("start_time", new Date(getStartTime()).toString());
        dataMap.put("duration", getDuration() / 60000 + "mins");
        dataMap.put("mr_waiting", getMapReduceWaitTime() / 60000 + "mins");
        dataMap.put("last_update_time", new Date(getLastModified()).toString());

        if (state == ExecutableState.ERROR) {
            AbstractExecutable errorTask = null;
            Output errorOutput = null;
            for (AbstractExecutable task : getTasks()) {
                errorOutput = getManager().getOutput(task.getId());
                if (errorOutput.getState() == ExecutableState.ERROR) {
                    errorTask = task;
                    break;
                }
            }
            Preconditions.checkNotNull(errorTask,
                    "None of the sub tasks of cubing job " + getId() + " is error and this job should become success.");

            dataMap.put("error_step", errorTask.getName());
            if (errorTask instanceof MapReduceExecutable) {
                final String mrJobId = errorOutput.getExtra().get(ExecutableConstants.MR_JOB_ID);
                dataMap.put("mr_job_id", StringUtil.noBlank(mrJobId, "Not initialized"));
            } else {
                dataMap.put("mr_job_id", MailNotificationUtil.NA);
            }
            dataMap.put("error_log",
                    Matcher.quoteReplacement(StringUtil.noBlank(output.getVerboseMsg(), "no error message")));
        }

        String content = MailNotificationUtil.getMailContent(state, dataMap);
        String title = MailNotificationUtil.getMailTitle("JOB",
                state.toString(),
                context.getConfig().getClusterName(),
                getDeployEnvName(),
                getProjectName(),
                cubeInstance.getName());
        return Pair.newPair(title, content);
    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        KylinConfig.setAndUnsetThreadLocalConfig(getCubeSpecificConfig());
        super.onExecuteStart(executableContext);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        long time = 0L;
        for (AbstractExecutable task : getTasks()) {
            final ExecutableState status = task.getStatus();
            if (status != ExecutableState.SUCCEED) {
                break;
            }
            if (task instanceof MapReduceExecutable) {
                time += ((MapReduceExecutable) task).getMapReduceWaitTime();
            }
        }
        setMapReduceWaitTime(time);
        super.onExecuteFinished(result, executableContext);
    }

    protected void onStatusChange(ExecutableContext context, ExecuteResult result, ExecutableState state) {
        super.onStatusChange(context, result, state);

        updateMetrics(context, result, state);
    }

    protected void updateMetrics(ExecutableContext context, ExecuteResult result, ExecutableState state) {
        JobMetricsFacade.JobStatisticsResult jobStats = new JobMetricsFacade.JobStatisticsResult();
        jobStats.setWrapper(getSubmitter(), getProjectName(), CubingExecutableUtil.getCubeName(getParams()), getId(),
                getJobType(), getAlgorithm() == null ? "NULL" : getAlgorithm().toString());

        if (state == ExecutableState.SUCCEED) {
            jobStats.setJobStats(findSourceSizeBytes(), findCubeSizeBytes(), getDuration(), getMapReduceWaitTime(),
                    getPerBytesTimeCost(findSourceSizeBytes(), getDuration()));
            if (CubingJobTypeEnum.getByName(getJobType()) == CubingJobTypeEnum.BUILD) {
                jobStats.setJobStepStats(getTaskDurationByName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS),
                        getTaskDurationByName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY),
                        getTaskDurationByName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE),
                        getTaskDurationByName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE));
            }
        } else if (state == ExecutableState.ERROR) {
            jobStats.setJobException(result.getThrowable() != null ? result.getThrowable() : new Exception());
        }
        JobMetricsFacade.updateMetrics(jobStats);
    }

    private long getTaskDurationByName(String name) {
        AbstractExecutable task = getTaskByName(name);
        if (task != null) {
            return task.getDuration();
        } else {
            return 0;
        }
    }

    private static double getPerBytesTimeCost(long size, long timeCost) {
        if (size <= 0) {
            return 0;
        }
        if (size < MIN_SOURCE_SIZE) {
            size = MIN_SOURCE_SIZE;
        }
        return timeCost * 1.0 / size;
    }

    public void setAlgorithm(AlgorithmEnum alg) {
        addExtraInfo("algorithm", alg.name());
    }

    public AlgorithmEnum getAlgorithm() {
        String alg = getExtraInfo().get("algorithm");
        return alg == null ? null : AlgorithmEnum.valueOf(alg);
    }

    public boolean isLayerCubing() {
        return AlgorithmEnum.LAYER == getAlgorithm();
    }

    public boolean isInMemCubing() {
        return AlgorithmEnum.INMEM == getAlgorithm();
    }

    public long findSourceRecordCount() {
        return Long.parseLong(findExtraInfo(SOURCE_RECORD_COUNT, "0"));
    }

    public long findSourceSizeBytes() {
        return Long.parseLong(findExtraInfo(SOURCE_SIZE_BYTES, "0"));
    }

    public long findCubeSizeBytes() {
        // look for the info BACKWARD, let the last step that claims the cube size win
        return Long.parseLong(findExtraInfoBackward(CUBE_SIZE_BYTES, "0"));
    }

    public List<Double> findEstimateRatio(CubeSegment seg, KylinConfig config) {
        CubeInstance cubeInstance = seg.getCubeInstance();
        CuboidScheduler cuboidScheduler = cubeInstance.getCuboidScheduler();
        List<List<Long>> layeredCuboids = cuboidScheduler.getCuboidsByLayer();
        int totalLevels = cuboidScheduler.getBuildLevel();

        List<Double> result = Lists.newArrayList();

        Map<Long, Double> estimatedSizeMap;

        String cuboidRootPath = getCuboidRootPath(seg, config);

        try {
            estimatedSizeMap = new CubeStatsReader(seg, config).getCuboidSizeMap(true);
        } catch (IOException e) {
            logger.warn("Cannot get segment {} estimated size map", seg.getName());

            return null;
        }

        for (int level = 0; level <= totalLevels; level++) {
            double levelEstimatedSize = 0;
            for (Long cuboidId : layeredCuboids.get(level)) {
                levelEstimatedSize += estimatedSizeMap.get(cuboidId) == null ? 0.0 : estimatedSizeMap.get(cuboidId);
            }

            double levelRealSize = getRealSizeByLevel(cuboidRootPath, level);

            if (levelEstimatedSize == 0.0 || levelRealSize == 0.0){
                result.add(level, -1.0);
            } else {
                result.add(level, levelRealSize / levelEstimatedSize);
            }
        }

        return result;
    }


    private double getRealSizeByLevel(String rootPath, int level) {
        try {
            String levelPath = JobBuilderSupport.getCuboidOutputPathsByLevel(rootPath, level);
            FileSystem fs = HadoopUtil.getFileSystem(levelPath);
            return fs.getContentSummary(new Path(levelPath)).getLength() / (1024L * 1024L);
        } catch (Exception e) {
            logger.warn("get level real size failed." + e);
            return 0L;
        }
    }

    private String getCuboidRootPath(CubeSegment seg, KylinConfig kylinConfig) {
        String rootDir = kylinConfig.getHdfsWorkingDirectory();
        if (!rootDir.endsWith("/")) {
            rootDir = rootDir + "/";
        }
        String jobID = this.getId();
        return rootDir + "kylin-" + jobID + "/" + seg.getRealization().getName() + "/cuboid/";
    }

}
