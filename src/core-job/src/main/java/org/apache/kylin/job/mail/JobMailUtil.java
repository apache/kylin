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

package org.apache.kylin.job.mail;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mail.MailNotificationCreator;
import org.apache.kylin.common.mail.MailNotificationType;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import freemarker.template.Template;

public class JobMailUtil {

    private static final Logger logger = LoggerFactory.getLogger(JobMailUtil.class);
    private static final String NA = "NA";

    private JobMailUtil() {
        throw new IllegalStateException("Utility class");
    }

    private static Map<String, Object> getCommonMailData(AbstractExecutable executable) {
        Map<String, Object> data = Maps.newHashMap();
        data.put("job_name", executable.getName());
        data.put("submitter", executable.getSubmitter());
        data.put("project_name", executable.getProject());
        data.put("object", Optional.ofNullable(executable.getTargetModelAlias()).orElse(executable.getTargetSubject()));
        data.put("start_time", DateFormat.formatToDateStr(executable.getStartTime(),
                DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS));
        data.put("end_time", DateFormat.formatToDateStr(executable.getLastModified(),
                DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS));
        return data;
    }

    private static void addErrorMsg(AbstractExecutable executable, Map<String, Object> mailData) {
        NExecutableManager executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                executable.getProject());
        final Output jobOutput = executableManager.getOutput(executable.getId());

        List<AbstractExecutable> tasks = ((DefaultExecutable) executable).getTasks();

        AbstractExecutable errorTask = null;
        Output errorTaskOutput = null;
        for (AbstractExecutable task : tasks) {
            errorTaskOutput = executableManager.getOutput(task.getId());
            if (errorTaskOutput.getState() == ExecutableState.ERROR
                    || errorTaskOutput.getState() == ExecutableState.SUICIDAL) {
                errorTask = task;
                break;
            }
        }

        Objects.requireNonNull(errorTask);
        Objects.requireNonNull(errorTaskOutput);

        mailData.put("error_step", errorTask.getName());

        if (errorTask.getOutput().getExtra().containsKey(ExecutableConstants.YARN_APP_ID)) {
            final String yarnApplicationId = errorTaskOutput.getExtra().get(ExecutableConstants.YARN_APP_ID);
            mailData.put(ExecutableConstants.YARN_APP_ID, StringUtils.defaultIfBlank(yarnApplicationId, NA));
        } else {
            mailData.put(ExecutableConstants.YARN_APP_ID, NA);
        }

        mailData.put("error_log",
                Matcher.quoteReplacement(StringUtils.defaultIfBlank(jobOutput.getFailedStack(), "no error message")));
    }

    private static Map<String, Object> createMailContent(MailNotificationType notificationType,
            AbstractExecutable executable) {
        Map<String, Object> mailData = getCommonMailData(executable);

        if (MailNotificationType.JOB_ERROR == notificationType) {
            addErrorMsg(executable, mailData);
        }

        return mailData;
    }

    public static Pair<String, String> createMail(MailNotificationType notificationType, AbstractExecutable job) {
        try {
            String mailTitle = MailNotificationCreator.createTitle(notificationType);
            Map<String, Object> mailData = createMailContent(notificationType, job);
            Template mailTemplate = MailNotificationCreator.MailTemplate
                    .getTemplate(notificationType.getCorrespondingTemplateName());
            String mailContent = MailNotificationCreator.createContent(mailTemplate, mailData);
            return new Pair<>(mailTitle, mailContent);
        } catch (Exception e) {
            logger.error("create mail [{}] failed!", notificationType.getDisplayName(), e);
        }

        return null;
    }
}
