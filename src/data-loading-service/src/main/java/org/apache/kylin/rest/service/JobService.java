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
package org.apache.kylin.rest.service;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.cluster.ClusterManagerFactory;
import org.apache.kylin.cluster.IClusterManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.rest.response.JobStatisticsResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.BuildAsyncProfileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import lombok.val;

@Component("jobService")
public class JobService extends BasicService {

    @Autowired
    private ProjectService projectService;

    private AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    @Autowired
    private JobInfoDao jobInfoDao;

    private static final Logger logger = LoggerFactory.getLogger(LogConstant.BUILD_CATEGORY);
    private static final Map<String, String> jobTypeMap = Maps.newHashMap();
    private static final String LAST_MODIFIED = "last_modified";
    private static final String CREATE_TIME = "create_time";
    private static final String DURATION = "duration";
    private static final String TOTAL_DURATION = "total_duration";
    private static final String TARGET_SUBJECT = "target_subject";
    private static final String JOB_NAME = "job_name";
    private static final String JOB_STATUS = "job_status";
    private static final String PROJECT = "project";

    public static final String EXCEPTION_CODE_PATH = "exception_to_code.json";
    public static final String EXCEPTION_CODE_DEFAULT = "KE-030001000";

    public static final String JOB_STEP_PREFIX = "job_step_";
    public static final String YARN_APP_SEPARATOR = "_";
    public static final String BUILD_JOB_PROFILING_PARAMETER = "kylin.engine.async-profiler-enabled";
    public static final String CHINESE_LANGUAGE = "zh";
    public static final String CHINESE_SIMPLE_LANGUAGE = "zh-CN";
    public static final String CHINESE_HK_LANGUAGE = "zh-HK";
    public static final String CHINESE_TW_LANGUAGE = "zh-TW";

    static {
        jobTypeMap.put("INDEX_REFRESH", "Refresh Data");
        jobTypeMap.put("INDEX_MERGE", "Merge Data");
        jobTypeMap.put("INDEX_BUILD", "Build Index");
        jobTypeMap.put("INC_BUILD", "Load Data");
        jobTypeMap.put("TABLE_SAMPLING", "Sample Table");
    }

    @Autowired
    public JobService setAclEvaluate(AclEvaluate aclEvaluate) {
        this.aclEvaluate = aclEvaluate;
        return this;
    }

    public JobStatisticsResponse getJobStats(String project, long startTime, long endTime) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        Pair<Integer, JobStatistics> stats = manager.getOverallJobStats(startTime, endTime);
        JobStatistics jobStatistics = stats.getSecond();
        return new JobStatisticsResponse(stats.getFirst(), jobStatistics.getTotalDuration(),
                jobStatistics.getTotalByteSize());
    }

    public Map<String, Integer> getJobCount(String project, long startTime, long endTime, String dimension) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        if (dimension.equals("model")) {
            return manager.getJobCountByModel(startTime, endTime);
        }

        return manager.getJobCountByTime(startTime, endTime, dimension);
    }

    public Map<String, Double> getJobDurationPerByte(String project, long startTime, long endTime, String dimension) {
        aclEvaluate.checkProjectOperationPermission(project);
        JobStatisticsManager manager = getManager(JobStatisticsManager.class, project);
        if (dimension.equals("model")) {
            return manager.getDurationPerByteByModel(startTime, endTime);
        }

        return manager.getDurationPerByteByTime(startTime, endTime, dimension);
    }

    public Map<String, Object> getEventsInfoGroupByModel(String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        Map<String, Object> result = Maps.newHashMap();
        result.put("data", null);
        result.put("size", 0);
        return result;
    }

    public Map<String, Object> getStepOutput(String project, String jobId, String stepId) {
        aclEvaluate.checkProjectOperationPermission(project);
        val executableManager = getManager(ExecutableManager.class, project);
        Output output = executableManager.getOutputFromHDFSByJobId(jobId, stepId);
        Map<String, Object> result = new HashMap<>();
        result.put("cmd_output", output.getVerboseMsg());

        Map<String, String> info = output.getExtra();
        List<String> servers = Lists.newArrayList();
        if (info != null && info.get("nodes") != null) {
            servers = Lists.newArrayList(info.get("nodes").split(","));
        }
        List<String> nodes = servers.stream().map(server -> {
            String[] split = server.split(":");
            return split[0] + ":" + split[1];
        }).collect(Collectors.toList());
        result.put("nodes", nodes);
        return result;
    }

    public void startProfileByProject(String project, String jobStepId, String params) {
        if (!KylinConfig.getInstanceFromEnv().buildJobProfilingEnabled()) {
            throw new KylinException(JobErrorCode.PROFILING_NOT_ENABLED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotEnabled(), BUILD_JOB_PROFILING_PARAMETER));
        }
        BuildAsyncProfileHelper.startProfile(project, jobStepId, params);
    }

    public void dumpProfileByProject(String project, String jobStepId, String params,
            Pair<InputStream, String> jobOutputAndDownloadFile) {
        if (!KylinConfig.getInstanceFromEnv().buildJobProfilingEnabled()) {
            throw new KylinException(JobErrorCode.PROFILING_NOT_ENABLED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotEnabled(), BUILD_JOB_PROFILING_PARAMETER));
        }
        InputStream jobOutput = BuildAsyncProfileHelper.dump(project, jobStepId, params);
        jobOutputAndDownloadFile.setFirst(jobOutput);
        String downloadFilename = String.format(Locale.ROOT, "%s_%s_dump.tar.gz", project, jobStepId);
        jobOutputAndDownloadFile.setSecond(downloadFilename);
    }

    public void startProfileByYarnAppId(String yarnAppId, String params) {
        if (!KylinConfig.getInstanceFromEnv().buildJobProfilingEnabled()) {
            throw new KylinException(JobErrorCode.PROFILING_NOT_ENABLED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotEnabled(), BUILD_JOB_PROFILING_PARAMETER));
        }
        Pair<String, String> projectNameAndJobStepId = getProjectNameAndJobStepId(yarnAppId);
        BuildAsyncProfileHelper.startProfile(projectNameAndJobStepId.getFirst(), projectNameAndJobStepId.getSecond(),
                params);
    }

    public void dumpProfileByYarnAppId(String yarnAppId, String params,
            Pair<InputStream, String> jobOutputAndDownloadFile) {
        if (!KylinConfig.getInstanceFromEnv().buildJobProfilingEnabled()) {
            throw new KylinException(JobErrorCode.PROFILING_NOT_ENABLED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getProfilingNotEnabled(), BUILD_JOB_PROFILING_PARAMETER));
        }
        Pair<String, String> projectNameAndJobStepId = getProjectNameAndJobStepId(yarnAppId);
        InputStream jobOutput = BuildAsyncProfileHelper.dump(projectNameAndJobStepId.getFirst(),
                projectNameAndJobStepId.getSecond(), params);
        jobOutputAndDownloadFile.setFirst(jobOutput);
        String downloadFilename = String.format(Locale.ROOT, "%s_%s_dump.tar.gz", projectNameAndJobStepId.getFirst(),
                projectNameAndJobStepId.getSecond());
        jobOutputAndDownloadFile.setSecond(downloadFilename);
    }

    /*
     * return as [projectName, jobStepId]
     */
    public Pair<String, String> getProjectNameAndJobStepId(String yarnAppId) {
        IClusterManager iClusterManager = ClusterManagerFactory.create(KylinConfig.getInstanceFromEnv());
        if (yarnAppId.contains(YARN_APP_SEPARATOR)) {
            // yarnAppId such as application_{timestamp}_30076
            String[] splits = yarnAppId.split(YARN_APP_SEPARATOR);
            if (splits.length == 3) {
                String appId = splits[2];
                // build applicationName such as job_step_{jobId}_01, sometimes maybe job_step_{jobId}_00
                String applicationName = iClusterManager.getApplicationNameById(Integer.parseInt(appId));

                if (applicationName.contains(JOB_STEP_PREFIX)) {
                    String jobStepId = StringUtils.replace(applicationName, JOB_STEP_PREFIX, "");
                    String jobId = applicationName.split(YARN_APP_SEPARATOR)[2];

                    String projectName = getProjectByJobId(jobId);
                    return Pair.newPair(projectName, jobStepId);
                } else {
                    throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getProfilingJobFinishedError()));
                }
            } else {
                throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getProfilingYarnAppIdError()));
            }
        } else {
            throw new KylinException(JobErrorCode.PROFILING_STATUS_ERROR,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getProfilingYarnAppIdError()));
        }
    }

    public String getProjectByJobId(String jobId) {
        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setJobId(jobId);
        List<JobInfo> jobInfoList = jobInfoDao.getJobInfoListByFilter(jobMapperFilter);
        if (CollectionUtils.isEmpty(jobInfoList)) {
            return null;
        }
        return jobInfoList.get(0).getProject();
    }

    public void setResponseLanguage(HttpServletRequest request) {
        aclEvaluate.checkIsGlobalAdmin();
        String languageToHandle = request.getHeader(HttpHeaders.ACCEPT_LANGUAGE);
        if (languageToHandle == null) {
            ErrorCode.setMsg("cn");
            MsgPicker.setMsg("cn");
            return;
        }
        // The user's browser may contain multiple language preferences, such as xx,xx;ss,ss
        String language = StringHelper.dropFirstSuffix(StringHelper.dropFirstSuffix(languageToHandle, ";"), ",");
        if (CHINESE_LANGUAGE.equals(language) || CHINESE_SIMPLE_LANGUAGE.equals(language)
                || CHINESE_HK_LANGUAGE.equals(language) || CHINESE_TW_LANGUAGE.equals(language)) {
            ErrorCode.setMsg("cn");
            MsgPicker.setMsg("cn");
        } else {
            ErrorCode.setMsg("en");
            MsgPicker.setMsg("en");
        }
    }
}
