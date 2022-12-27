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

package org.apache.kylin.job.execution;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.NonCustomProjectLevelConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.util.MailNotificationUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

@Getter
@Setter
public class EmailNotificationContent {

    protected static final Logger logger = LoggerFactory.getLogger(EmailNotificationContent.class);

    public static Pair<String, String> createContent(JobIssueEnum jobIssue, AbstractExecutable executable) {
        if (!checkState(jobIssue)) {
            logger.info("issue state: " + jobIssue.getDisplayName() + "not need to notify users");
            return null;
        }
        logger.info("notify on jobIssue change : {}", jobIssue);
        Map<String, Object> dataMap = getDataMap(executable);
        if (JobIssueEnum.SOURCE_RECORDS_CHANGE.equals(jobIssue)) {
            dataMap.put("start_time", DateFormat.formatToDateStr(executable.getStartTime(),
                    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS));
            dataMap.put("end_time", DateFormat.formatToDateStr(executable.getEndTime(),
                    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS));
        }
        return Pair.newPair(getMailTitle(jobIssue, executable), getMailContent(jobIssue, dataMap));
    }

    public static Pair<String, String> createContent(ExecutableState state, AbstractExecutable executable,
                                                     List<AbstractExecutable> tasks) {
        final Output output = executable.getManager().getOutput(executable.getId());
        if (!state.isFinalState() && state != ExecutableState.ERROR) {
            logger.info("state: " + state + "is not right,not need to notify users");
            return null;
        }
        logger.info("notify on execute change state: {}", state);
        String states = checkOverrideConfig(executable.getProject(),
                NonCustomProjectLevelConfig.JOB_NOTIFICATION_ENABLED_STATES.getValue());
        String[] notificationStates;
        if(states != null) {
            notificationStates = StringUtils.split(states, ",");
        } else {
            notificationStates = executable.getConfig().getJobNotificationStates();
        }

        if(notificationStates.length < 1 || !Arrays.asList(notificationStates).contains(state.toStringState())) {
            logger.info("state: " + state + " is not set,not need to notify users");
            return null;
        }

        Map<String, Object> dataMap = getDataMap(executable);
        dataMap.put("source_byte_size", String.valueOf(executable.getByteSize()));
        dataMap.put("start_time", new Date(executable.getStartTime()).toString());
        dataMap.put("duration", executable.getDuration() / 60000 + "mins");
        dataMap.put("last_update_time", new Date(executable.getLastModified()).toString());

        if (state == ExecutableState.ERROR) {
            AbstractExecutable errorTask = null;
            Output errorOutput = null;
            for (AbstractExecutable task : tasks) {
                errorOutput = executable.getManager().getOutput(task.getId());
                if (errorOutput.getState() == ExecutableState.ERROR) {
                    errorTask = task;
                    break;
                }
            }
            Preconditions.checkNotNull(errorTask,
                    "None of the sub tasks of cubing job " + executable.getId() + " is error and this job should become success.");
            dataMap.put("error_step", errorTask.getName());
            if (errorTask.getOutput().getExtra().containsKey(ExecutableConstants.MR_JOB_ID)) {
                final String mrJobId = errorOutput.getExtra().get(ExecutableConstants.MR_JOB_ID);
                dataMap.put("mr_job_id", StringUtil.noBlank(mrJobId, "Not initialized"));
            } else {
                dataMap.put("mr_job_id", MailNotificationUtil.NA);
            }
            dataMap.put("error_log",
                    Matcher.quoteReplacement(StringUtil.noBlank(output.getShortErrMsg(), "no error message")));
        }

        return Pair.newPair(getMailTitle(state, executable), getMailContent(state, dataMap));
    }

    public static Pair<String, String> createMetadataPersistExceptionContent(Throwable exception,
                                                                             AbstractExecutable executable) {
        logger.info("notify on metadata persist exception: {}", exception);
        Map<String, Object> dataMap = getDataMap(executable);
        dataMap.put("error_log", Matcher.quoteReplacement(StringUtil.noBlank(
                exception.getMessage(), "no error message")));

        String content = MailNotificationUtil.getMailContent(MailNotificationUtil.METADATA_PERSIST_FAIL, dataMap);
        String title = MailNotificationUtil.getMailTitle("METADATA PERSIST", "FAIL",
                executable.getConfig().getDeployEnv(), executable.getProject(), executable.getTargetSubjectAlias());
        return Pair.newPair(title, content);
    }

    private static Map<String, Object> getDataMap(AbstractExecutable executable) {
        Map<String, Object> dataMap = Maps.newHashMap();
        dataMap.put("job_name", executable.getName());
        dataMap.put("env_name", executable.getConfig().getDeployEnv());
        dataMap.put("submitter", StringUtil.noBlank(executable.getSubmitter(), "missing submitter"));
        dataMap.put("job_engine", MailNotificationUtil.getLocalHostName());
        dataMap.put("project_name", executable.getProject());
        dataMap.put("model_name", executable.getTargetSubjectAlias());
        return dataMap;
    }

    private static boolean checkState(JobIssueEnum jobIssue) {
        return JobIssueEnum.LOAD_EMPTY_DATA.equals(jobIssue)
                || JobIssueEnum.SOURCE_RECORDS_CHANGE.equals(jobIssue);
    }

    private static String getMailContent(ExecutableState state, Map<String, Object> dataMap) {
        return MailNotificationUtil.getMailContent(state, dataMap);
    }

    private static String getMailContent(JobIssueEnum jobIssueEnum, Map<String, Object> dataMap) {
        return MailNotificationUtil.getMailContent(jobIssueEnum, dataMap);
    }


    private static String getMailTitle(ExecutableState state, AbstractExecutable executable) {
        return MailNotificationUtil.getMailTitle("JOB",
                state.toString(),
                executable.getConfig().getMetadataUrlPrefix(),
                executable.getConfig().getDeployEnv(),
                executable.getProject(),
                executable.getTargetSubjectAlias());
    }

    private static String getMailTitle(JobIssueEnum issue, AbstractExecutable executable) {
        return MailNotificationUtil.getMailTitle("JOB",
                issue.getDisplayName(),
                executable.getConfig().getMetadataUrlPrefix(),
                executable.getConfig().getDeployEnv(),
                executable.getProject(),
                executable.getTargetSubjectAlias());
    }

    public static String checkOverrideConfig(String project, String overrideNotificationName) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        return projectInstance.getOverrideKylinProps().get(overrideNotificationName);
    }


}
