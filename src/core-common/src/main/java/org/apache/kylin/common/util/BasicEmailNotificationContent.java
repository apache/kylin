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

package org.apache.kylin.common.util;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BasicEmailNotificationContent {

    public static final String NOTIFY_EMAIL_TITLE_TEMPLATE = "[Kyligence System Notification] ${issue}";
    public static final String NOTIFY_EMAIL_BODY_TEMPLATE = "<div style='display:block;word-wrap:break-word;width:80%;font-size:16px;font-family:Microsoft YaHei;'><b>Dear Kyligence Customer,</b><pre><p>"
            + "<p>${conclusion}</p>" + "<p>Issue: ${issue}<br>" + "Type: ${type}<br>" + "Time: ${time}<br>"
            + "Project: ${project}<br>" + "Solution: ${solution}</p>" + "<p>Yours sincerely,<br>" + "Kyligence team</p>"
            + "</pre><div/>";

    public static final String CONCLUSION_FOR_JOB_ERROR = "We found an error job happened in your Kyligence system as below. It won't affect your system stability and you may repair it by following instructions.";
    public static final String CONCLUSION_FOR_LOAD_EMPTY_DATA = "We found a job has loaded empty data in your Kyligence system as below. It won't affect your system stability and you may reload data by following instructions.";
    public static final String CONCLUSION_FOR_SOURCE_RECORDS_CHANGE = "We found some source records updated in your Kyligence system. You can reload updated records by following instructions. Ignore this issue may cause query result inconsistency over different indexes.";
    public static final String CONCLUSION_FOR_OVER_CAPACITY_THRESHOLD = "The amount of data volume used (${volume_used}/${volume_total}) has reached ${capacity_threshold}% of the license’s limit.";

    public static final String SOLUTION_FOR_JOB_ERROR = "You may resume the job first. If still won't work, please send the job's diagnostic package to kyligence technical support.";
    public static final String SOLUTION_FOR_LOAD_EMPTY_DATA = "You may refresh the empty segment of the model ${model_name} to reload data.";
    public static final String SOLUTION_FOR_SOURCE_RECORDS_CHANGE = "You may refresh the segment from ${start_time} to ${end_time} to apply source records change.";
    public static final String SOLUTION_FOR_OVER_CAPACITY_THRESHOLD = "To ensure the availability of your service, please contact Kyligence to get a new license, or try deleting some segments.";

    private String conclusion;
    private String issue;
    private String time;
    private String solution;

    private String jobType;
    private String project;

    public String getEmailTitle() {
        return NOTIFY_EMAIL_TITLE_TEMPLATE.replaceAll("\\$\\{issue\\}", issue);
    }

    public String getEmailBody() {
        return NOTIFY_EMAIL_BODY_TEMPLATE.replaceAll("\\$\\{conclusion\\}", conclusion)
                .replaceAll("\\$\\{issue\\}", issue).replaceAll("\\$\\{type\\}", jobType)
                .replaceAll("\\$\\{time\\}", time).replaceAll("\\$\\{project\\}", project)
                .replaceAll("\\$\\{solution\\}", solution);
    }

}