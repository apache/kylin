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

import java.time.Clock;
import java.time.LocalDate;
import java.util.Locale;

import org.apache.kylin.common.util.BasicEmailNotificationContent;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.job.constant.JobIssueEnum;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EmailNotificationContent extends BasicEmailNotificationContent {

    private AbstractExecutable executable;

    public static EmailNotificationContent createContent(JobIssueEnum issue, AbstractExecutable executable) {
        EmailNotificationContent content = new EmailNotificationContent();
        content.setIssue(issue.getDisplayName());
        content.setTime(LocalDate.now(Clock.systemDefaultZone()).toString());
        content.setJobType(executable.getJobType().toString());
        content.setProject(executable.getProject());
        content.setExecutable(executable);
        switch (issue) {
        case JOB_ERROR:
            content.setConclusion(CONCLUSION_FOR_JOB_ERROR);
            content.setSolution(SOLUTION_FOR_JOB_ERROR);
            break;
        case LOAD_EMPTY_DATA:
            content.setConclusion(CONCLUSION_FOR_LOAD_EMPTY_DATA);
            content.setSolution(
                    SOLUTION_FOR_LOAD_EMPTY_DATA.replaceAll("\\$\\{model_name\\}", executable.getTargetModelAlias()));
            break;
        case SOURCE_RECORDS_CHANGE:
            content.setConclusion(CONCLUSION_FOR_SOURCE_RECORDS_CHANGE);
            content.setSolution(SOLUTION_FOR_SOURCE_RECORDS_CHANGE
                    .replaceAll("\\$\\{start_time\\}",
                            DateFormat.formatToDateStr(executable.getDataRangeStart(),
                                    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS))
                    .replaceAll("\\$\\{end_time\\}", DateFormat.formatToDateStr(executable.getDataRangeEnd(),
                            DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS)));
            break;
        default:
            throw new IllegalArgumentException(String.format(Locale.ROOT, "no process for jobIssue: %s.", issue));
        }
        return content;
    }
}
