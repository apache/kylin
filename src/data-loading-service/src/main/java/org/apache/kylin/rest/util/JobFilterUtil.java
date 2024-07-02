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

package org.apache.kylin.rest.util;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.constant.JobStatusUtil;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.rest.JobFilter;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.TableExtService;
import org.sparkproject.guava.collect.Lists;

public class JobFilterUtil {

    public static JobMapperFilter getJobMapperFilter(final JobFilter jobFilter, int offset, int limit,
            ModelService modelService, TableExtService tableExtService, ProjectService projectService) {
        Date queryStartTime = getQueryStartTime(jobFilter.getTimeFilter());

        Set<String> subjects = new HashSet<>();
        if (StringUtils.isNotEmpty(jobFilter.getSubject())) {
            subjects.add(jobFilter.getSubject().trim());
        }
        List<String> convertKeyToSubjects = new ArrayList<>();
        // transform 'key' to subjects
        if (StringUtils.isNotEmpty(jobFilter.getKey())) {
            convertKeyToSubjects.addAll(modelService.getModelNamesByFuzzyName(jobFilter.getKey(),
                    jobFilter.getProject(), jobFilter.isExactMatch()));
            convertKeyToSubjects.addAll(tableExtService.getTableNamesByFuzzyKey(jobFilter.getProject(),
                    jobFilter.getKey(), jobFilter.isExactMatch()));
            convertKeyToSubjects.addAll(projectService
                    .getProjectsFilterByExactMatchAndPermission(jobFilter.getKey(), jobFilter.isExactMatch(),
                            AclPermissionEnum.READ)
                    .stream().map(ProjectInstance::getName).collect(Collectors.toList()));
            subjects.addAll(convertKeyToSubjects);
        }
        // if 'key' can not be transformed to 'subjects', then fuzzy query job id by 'key'
        String jobId = null;
        if (StringUtils.isNotEmpty(jobFilter.getKey()) && convertKeyToSubjects.isEmpty()) {
            jobId = "%" + jobFilter.getKey() + "%";
        }

        String orderByField = convertSortBy(jobFilter.getSortBy());

        String orderType = "ASC";
        if (jobFilter.isReverse()) {
            orderType = "DESC";
        }

        List<ExecutableState> scheduleStates = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(jobFilter.getStatuses())) {
            jobFilter.getStatuses()
                    .forEach(jobStatus -> scheduleStates.addAll(JobStatusUtil.mapJobStatusToScheduleState(jobStatus)));
        }

        return new JobMapperFilter(scheduleStates, jobFilter.getJobNames(), queryStartTime.getTime(),
                Lists.newArrayList(subjects), null, jobId, null, jobFilter.getProject(), orderByField, orderType,
                offset, limit, null, null);
    }

    private static Date getQueryStartTime(int timeFilter) {
        JobTimeFilterEnum filterEnum = JobTimeFilterEnum.getByCode(timeFilter);
        Preconditions.checkNotNull(filterEnum, "Can not find the JobTimeFilterEnum by code: %s", timeFilter);

        // prepare time range
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        calendar.setTime(new Date());
        Message msg = MsgPicker.getMsg();

        switch (filterEnum) {
        case LAST_ONE_DAY:
            calendar.add(Calendar.DAY_OF_MONTH, -1);
            return calendar.getTime();
        case LAST_ONE_WEEK:
            calendar.add(Calendar.WEEK_OF_MONTH, -1);
            return calendar.getTime();
        case LAST_ONE_MONTH:
            calendar.add(Calendar.MONTH, -1);
            return calendar.getTime();
        case LAST_ONE_YEAR:
            calendar.add(Calendar.YEAR, -1);
            return calendar.getTime();
        case ALL:
            return new Date(0);
        default:
            throw new KylinException(INVALID_PARAMETER, msg.getIllegalTimeFilter());
        }
    }

    private static String convertSortBy(String sortBy) {
        if (StringUtils.isEmpty(sortBy)) {
            return "update_time";
        }
        Message msg = MsgPicker.getMsg();
        switch (sortBy) {
        case "project":
        case "create_time":
        case "job_status":
            return sortBy;
        case "id":
            return "job_id";
        case "job_name":
            return "job_type";
        case "target_subject":
            return "model_id";
        case "duration":
        case "total_duration":
            return "job_duration_millis";
        case "last_modified":
            return "update_time";
        default:
            throw new KylinException(INVALID_PARAMETER,
                    String.format(Locale.ROOT, msg.getIllegalSortByFilter(), sortBy));
        }
    }
}
