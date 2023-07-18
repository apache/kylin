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

import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.common.response.MetricsResponse;
import org.apache.kylin.metadata.realization.HybridRealization;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.exception.UnsupportedQueryException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.JobStatisticsResponse;
import org.apache.kylin.rest.response.NDataModelOldParams;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.QueryStatisticsResponse;
import org.apache.kylin.rest.util.ModelUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Slf4j
@Component("dashboardService")
public class DashboardService extends BasicService {

    public static final Logger logger = LoggerFactory.getLogger(DashboardService.class);
    public static final String DAY = "day";
    public static final String AVG_QUERY_LATENCY = "AVG_QUERY_LATENCY";
    public static final String JOB = "JOB";
    public static final String AVG_JOB_BUILD_TIME = "AVG_JOB_BUILD_TIME";
    private static final String QUERY = "QUERY";
    private static final String QUERY_COUNT = "QUERY_COUNT";
    private static final String JOB_COUNT = "JOB_COUNT";
    @Autowired
    ModelService modelService;

    @Autowired
    QueryHistoryService queryHistoryService;

    @Autowired
    JobService jobService;

    public MetricsResponse getModelMetrics(String projectName, String modelName) {
        MetricsResponse modelMetrics = new MetricsResponse();
        long totalModelSize = 0;
        long totalRecordSize = 0;
        List<NDataModelResponse> models = modelService.getCubes0(modelName, projectName);//5.0 cube is model
        Integer totalModel = models.size();
        ProjectInstance project = getProjectManager().getProject(projectName);
        totalModel += project.getRealizationCount(HybridRealization.REALIZATION_TYPE);
        modelMetrics.increase("totalModel", totalModel.floatValue());

        Float minModelExpansion = Float.POSITIVE_INFINITY;
        Float maxModelExpansion = Float.NEGATIVE_INFINITY;

        for (NDataModelResponse dataModel : models) {
            NDataModelOldParams params = dataModel.getOldParams();
            if (params.getInputRecordSizeBytes() > 0) {
                totalModelSize += params.getSizeKB() * 1024;
                totalRecordSize += params.getInputRecordSizeBytes();//size / 1024 * 1024 * 1024 = x GB
                Float modelExpansion = Float.valueOf(dataModel.getExpansionrate());

                if (modelExpansion > maxModelExpansion) {
                    maxModelExpansion = modelExpansion;
                }
                if (modelExpansion < minModelExpansion) {
                    minModelExpansion = modelExpansion;
                }
            }
        }

        Float avgModelExpansion = 0f;
        if (totalRecordSize != 0) {
            avgModelExpansion = Float.valueOf(ModelUtils.computeExpansionRate(totalModelSize, totalRecordSize));
        }

        modelMetrics.increase("avgModelExpansion", avgModelExpansion);
        modelMetrics.increase("maxModelExpansion", maxModelExpansion);
        modelMetrics.increase("minModelExpansion", minModelExpansion);

        return modelMetrics;
    }

    public MetricsResponse getQueryMetrics(String projectName, String startTime, String endTime) {
        MetricsResponse queryMetrics = new MetricsResponse();
        QueryStatisticsResponse queryStatistics = queryHistoryService.getQueryStatisticsByRealization(projectName,
                convertToTimestamp(startTime), convertToTimestamp(endTime));
        Float queryCount = (float) queryStatistics.getCount();
        Float avgQueryLatency = (float) queryStatistics.getMean();
        queryMetrics.increase("queryCount", queryCount);
        queryMetrics.increase("avgQueryLatency", avgQueryLatency);
        return queryMetrics;
    }

    public MetricsResponse getJobMetrics(String projectName, String startTime, String endTime) {
        MetricsResponse jobMetrics = new MetricsResponse();
        JobStatisticsResponse jobStats = jobService.getJobStats(projectName, convertToTimestamp(startTime),
                convertToTimestamp(endTime));
        Float jobCount = (float) jobStats.getCount();
        Float jobTotalByteSize = (float) jobStats.getTotalByteSize();
        Float jobTotalLatency = (float) jobStats.getTotalDuration();
        jobMetrics.increase("jobCount", jobCount);
        jobMetrics.increase("jobTotalByteSize", jobTotalByteSize);
        jobMetrics.increase("jobTotalLatency", jobTotalLatency);
        return jobMetrics;
    }

    public MetricsResponse getChartData(String category, String projectName, String startTime, String endTime,
            String dimension, String metric) {
        long _startTime = convertToTimestamp(startTime);
        long _endTime = convertToTimestamp(endTime);
        switch (category) {
        case QUERY: {
            switch (metric) {
            case QUERY_COUNT:
                Map<String, Object> queryCounts = queryHistoryService.getQueryCountByRealization(projectName,
                        _startTime, _endTime, dimension.toLowerCase());
                return transformChartData(queryCounts);

            case AVG_QUERY_LATENCY:
                Map<String, Object> avgDurations = queryHistoryService.getAvgDurationByRealization(projectName,
                        _startTime, _endTime, dimension.toLowerCase());
                return transformChartData(avgDurations);
            default:
                throw new UnsupportedQueryException("Metric should be COUNT or AVG_QUERY_LATENCY");
            }
        }
        case JOB: {
            switch (metric) {
            case JOB_COUNT:
                Map<String, Integer> jobCounts = jobService.getJobCount(projectName, _startTime, _endTime,
                        dimension.toLowerCase());
                MetricsResponse counts = new MetricsResponse();
                jobCounts.forEach((k, v) -> counts.increase(k, Float.valueOf(v)));
                return counts;
            case AVG_JOB_BUILD_TIME:
                Map<String, Double> jobDurationPerByte = jobService.getJobDurationPerByte(projectName, _startTime,
                        _endTime, dimension.toLowerCase());
                MetricsResponse avgBuild = new MetricsResponse();
                jobDurationPerByte.forEach((k, v) -> avgBuild.increase(k, Float.valueOf(String.valueOf(v))));
                return avgBuild;
            default:
                throw new UnsupportedQueryException("Metric should be JOB_COUNT or AVG_JOB_BUILD_TIME");
            }
        }
        default:
            throw new UnsupportedQueryException("Category should either be QUERY or JOB");
        }
    }

    private MetricsResponse transformChartData(Map<String, Object> responses) {
        MetricsResponse metrics = new MetricsResponse();
        for (Map.Entry<String, Object> entry : responses.entrySet()) {
            String metric = entry.getKey();
            float value = Float.parseFloat(entry.getValue().toString());
            metrics.increase(metric, value);
        }
        return metrics;
    }

    private long convertToTimestamp(String time) {
        Date date;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT)).parse(time);
        } catch (ParseException e) {
            logger.error("parse time to timestamp error!");
            return 0L;
        }
        return date.getTime();
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    private void checkAuthorization(ProjectInstance project) throws AccessDeniedException {
        //for selected project
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    private void checkAuthorization() throws AccessDeniedException {
        //for no selected project
    }

    public void checkAuthorization(String projectName) {
        if (projectName != null && !projectName.isEmpty()) {
            ProjectInstance project = getProjectManager().getProject(projectName);
            try {
                checkAuthorization(project);
            } catch (AccessDeniedException e) {
                List<NDataModelResponse> models = modelService.getCubes0(null, projectName);
                if (models.isEmpty()) {
                    throw new AccessDeniedException("Access is denied");
                }
            }
        } else {
            checkAuthorization();
        }
    }
}
