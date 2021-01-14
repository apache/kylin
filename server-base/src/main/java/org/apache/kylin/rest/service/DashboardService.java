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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.apache.kylin.metrics.property.QuerySparkExecutionEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.response.MetricsResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

@Component("dashboardService")
public class DashboardService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(DashboardService.class);

    @Autowired
    private CubeService cubeService;

    public MetricsResponse getCubeMetrics(String projectName, String cubeName) {
        MetricsResponse cubeMetrics = new MetricsResponse();
        Float totalCubeSize = 0f;
        long totalRecoadSize = 0;
        List<CubeInstance> cubeInstances = cubeService.listAllCubes(cubeName, projectName, null, true);
        Integer totalCube = cubeInstances.size();
        if (projectName == null) {
            totalCube += getHybridManager().listHybridInstances().size();
        } else {
            ProjectInstance project = getProjectManager().getProject(projectName);
            totalCube += project.getRealizationCount(RealizationType.HYBRID);
        }
        Float minCubeExpansion = Float.POSITIVE_INFINITY;
        Float maxCubeExpansion = Float.NEGATIVE_INFINITY;
        cubeMetrics.increase("totalCube", totalCube.floatValue());
        for (CubeInstance cubeInstance : cubeInstances) {
            if (cubeInstance.getInputRecordSizeBytes() > 0) {
                totalCubeSize += cubeInstance.getSizeKB();
                totalRecoadSize += cubeInstance.getInputRecordSizeBytes();
                Float cubeExpansion = new Float(cubeInstance.getSizeKB()) * 1024
                        / cubeInstance.getInputRecordSizeBytes();
                if (cubeExpansion > maxCubeExpansion) {
                    maxCubeExpansion = cubeExpansion;
                }
                if (cubeExpansion < minCubeExpansion) {
                    minCubeExpansion = cubeExpansion;
                }
            }
        }
        Float avgCubeExpansion = 0f;
        if (totalRecoadSize != 0) {
            avgCubeExpansion = totalCubeSize * 1024 / totalRecoadSize;
        }
        cubeMetrics.increase("avgCubeExpansion", avgCubeExpansion);
        cubeMetrics.increase("maxCubeExpansion", maxCubeExpansion == Float.NEGATIVE_INFINITY ? 0 : maxCubeExpansion);
        cubeMetrics.increase("minCubeExpansion", minCubeExpansion == Float.POSITIVE_INFINITY ? 0 : minCubeExpansion);
        return cubeMetrics;
    }

    private List<CubeInstance> getCubeByHybrid(HybridInstance hybridInstance) {
        List<CubeInstance> cubeInstances = Lists.newArrayList();
        List<RealizationEntry> realizationEntries = hybridInstance.getRealizationEntries();
        for (RealizationEntry realizationEntry : realizationEntries) {
            String reName = realizationEntry.getRealization();
            if (RealizationType.CUBE == realizationEntry.getType()) {
                CubeInstance cubeInstance = getCubeManager().getCube(reName);
                cubeInstances.add(cubeInstance);
            } else if (RealizationType.HYBRID == realizationEntry.getType()) {
                HybridInstance innerHybridInstance = getHybridManager().getHybridInstance(reName);
                cubeInstances.addAll(getCubeByHybrid(innerHybridInstance));
            }
        }
        return cubeInstances;
    };

    public PrepareSqlRequest getQueryMetricsSQLRequest(String startTime, String endTime, String projectName,
            String cubeName) {
        String[] metrics = new String[] { QueryMetricEnum.QUERY_COUNT.toSQL(),
                QueryMetricEnum.AVG_QUERY_LATENCY.toSQL(), QueryMetricEnum.MAX_QUERY_LATENCY.toSQL(),
                QueryMetricEnum.MIN_QUERY_LATENCY.toSQL() };
        Map<String, String> filterMap = getBaseFilterMap(CategoryEnum.QUERY, projectName, startTime, endTime);
        filterMap.putAll(getCubeFilterMap(CategoryEnum.QUERY, cubeName));
        return createPrepareSqlRequest(null, metrics,
                getMetricsManager().getSystemTableFromSubject(getConfig().getKylinMetricsSubjectQueryExecution()), filterMap);
    };

    public PrepareSqlRequest getJobMetricsSQLRequest(String startTime, String endTime, String projectName,
            String cubeName) {
        String[] metrics = new String[] { JobMetricEnum.JOB_COUNT.toSQL(), JobMetricEnum.AVG_JOB_BUILD_TIME.toSQL(),
                JobMetricEnum.MAX_JOB_BUILD_TIME.toSQL(), JobMetricEnum.MIN_JOB_BUILD_TIME.toSQL() };
        Map<String, String> filterMap = getBaseFilterMap(CategoryEnum.JOB, projectName, startTime, endTime);
        filterMap.putAll(getCubeFilterMap(CategoryEnum.JOB, cubeName));
        return createPrepareSqlRequest(null, metrics,
                getMetricsManager().getSystemTableFromSubject(getConfig().getKylinMetricsSubjectJob()), filterMap);
    }

    public PrepareSqlRequest getChartSQLRequest(String startTime, String endTime, String projectName, String cubeName,
            String dimension, String metric, String category) {
        try {
            CategoryEnum categoryEnum = CategoryEnum.valueOf(category);
            String table = "";
            String[] dimensionSQL = null;
            String[] metricSQL = null;

            if (categoryEnum == CategoryEnum.QUERY) {
                dimensionSQL = new String[] { QueryDimensionEnum.valueOf(dimension).toSQL() };
                metricSQL = new String[] { QueryMetricEnum.valueOf(metric).toSQL() };
                table = getMetricsManager().getSystemTableFromSubject(getConfig().getKylinMetricsSubjectQueryExecution());
            } else if (categoryEnum == CategoryEnum.JOB) {
                dimensionSQL = new String[] { JobDimensionEnum.valueOf(dimension).toSQL() };
                metricSQL = new String[] { JobMetricEnum.valueOf(metric).toSQL() };
                table = getMetricsManager().getSystemTableFromSubject(getConfig().getKylinMetricsSubjectJob());
            }

            Map<String, String> filterMap = getBaseFilterMap(categoryEnum, projectName, startTime, endTime);
            filterMap.putAll(getCubeFilterMap(categoryEnum, cubeName));

            return createPrepareSqlRequest(dimensionSQL, metricSQL, table, filterMap);
        } catch (IllegalArgumentException e) {
            String message = "Generate dashboard chart sql failed. Please double check the input parameter: dimension, metric or category.";
            logger.error(message, e);
            throw new BadRequestException(message + " Caused by: " + e.getMessage(), null, e.getCause());
        }
    }

    public MetricsResponse transformChartData(SQLResponse sqlResponse) {
        if (!sqlResponse.getIsException()) {
            MetricsResponse metrics = new MetricsResponse();
            List<List<String>> results = sqlResponse.getResults();
            for (List<String> result : results) {
                String dimension = result.get(0);
                if (dimension != null && !dimension.isEmpty()) {
                    String metric = result.get(1);
                    metrics.increase(dimension, getMetricValue(metric));
                }
            }
            return metrics;
        }
        return null;
    }

    public Float getMetricValue(String value) {
        if (value == null || value.isEmpty()) {
            return 0f;
        } else {
            return Float.valueOf(value);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public void checkAuthorization(ProjectInstance project) throws AccessDeniedException {
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void checkAuthorization() throws AccessDeniedException {
    }

    private Map<String, String> getBaseFilterMap(CategoryEnum category, String projectName, String startTime,
            String endTime) {
        HashMap<String, String> filterMap = new HashMap<>();
        String project = "";
        if (category == CategoryEnum.QUERY) {
            project = QueryDimensionEnum.PROJECT.toSQL();
        } else {
            project = JobDimensionEnum.PROJECT.toSQL();
        }
        filterMap.put(TimePropertyEnum.DAY_DATE.toString() + " >= ?", startTime);
        filterMap.put(TimePropertyEnum.DAY_DATE.toString() + " <= ?", endTime);

        if (!Strings.isNullOrEmpty(projectName)) {
            filterMap.put(project + " = ?", projectName.toUpperCase(Locale.ROOT));
        } else {
            filterMap.put(project + " <> ?", MetricsManager.SYSTEM_PROJECT);
        }
        return filterMap;
    }

    private Map<String, String> getCubeFilterMap(CategoryEnum category, String cubeName) {
        HashMap<String, String> filterMap = new HashMap<>();

        if (category == CategoryEnum.QUERY) {
            filterMap.put(QuerySparkExecutionEnum.EXCEPTION.toString() + " = ?", "NULL");

            if (!Strings.isNullOrEmpty(cubeName)) {
                filterMap.put(QuerySparkExecutionEnum.REALIZATION + " = ?", cubeName);
            }
        } else if (category == CategoryEnum.JOB && !Strings.isNullOrEmpty(cubeName)) {
            HybridInstance hybridInstance = getHybridManager().getHybridInstance(cubeName);
            if (null != hybridInstance) {
                StringBuffer cubeNames = new StringBuffer();
                for (CubeInstance cube : getCubeByHybrid(hybridInstance)) {
                    cubeNames.append(",'" + cube.getName() + "'");
                }
                filterMap.put(JobPropertyEnum.CUBE.toString() + " IN (?)", cubeNames.substring(1));
            } else {
                filterMap.put(JobPropertyEnum.CUBE.toString() + " = ?", cubeName);
            }
        }
        return filterMap;
    }

    private PrepareSqlRequest createPrepareSqlRequest(String[] dimensions, String[] metrics, String category,
            Map<String, String> filterMap) {
        PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject(MetricsManager.SYSTEM_PROJECT);
        StringBuffer baseSQL = new StringBuffer("select ");
        StringBuffer groupBy = new StringBuffer("");
        if (dimensions != null && dimensions.length > 0) {
            groupBy.append(" group by ");
            StringBuffer dimensionSQL = new StringBuffer("");
            for (String dimension : dimensions) {
                dimensionSQL.append(",");
                dimensionSQL.append(dimension);
            }
            baseSQL.append(dimensionSQL.substring(1));
            groupBy.append(dimensionSQL.substring(1));
        }
        if (metrics != null && metrics.length > 0) {
            StringBuffer metricSQL = new StringBuffer("");
            for (String metric : metrics) {
                metricSQL.append(",");
                metricSQL.append(metric);
            }
            if (groupBy.length() > 0) {
                baseSQL.append(metricSQL);
            } else {
                baseSQL.append(metricSQL.substring(1));
            }
        }
        baseSQL.append(" from ");
        baseSQL.append(category);
        if (filterMap != null && filterMap.size() > 0) {
            PrepareSqlRequest.StateParam[] params = new PrepareSqlRequest.StateParam[filterMap.size()];
            int i = 0;
            StringBuffer filterSQL = new StringBuffer(" where ");
            Iterator<String> it = filterMap.keySet().iterator();
            String filter = it.next();
            filterSQL.append(filter);
            params[i] = new PrepareSqlRequest.StateParam();
            params[i].setClassName("java.lang.String");
            params[i++].setValue(filterMap.get(filter));

            while (it.hasNext()) {
                filter = it.next();
                filterSQL.append(" and ");
                filterSQL.append(filter);
                params[i] = new PrepareSqlRequest.StateParam();
                params[i].setClassName("java.lang.String");
                params[i++].setValue(filterMap.get(filter));
            }
            baseSQL.append(filterSQL.toString());
            sqlRequest.setParams(params);
        }
        baseSQL.append(groupBy);
        sqlRequest.setSql(baseSQL.toString());
        return sqlRequest;
    }

    private enum CategoryEnum {
        QUERY, JOB
    }

    private enum QueryDimensionEnum {
        PROJECT(QuerySparkExecutionEnum.PROJECT.toString()), //
        CUBE(QuerySparkExecutionEnum.REALIZATION.toString()), //
        DAY(TimePropertyEnum.DAY_DATE.toString()), //
        WEEK(TimePropertyEnum.WEEK_BEGIN_DATE.toString()), //
        MONTH(TimePropertyEnum.MONTH.toString());

        private final String sql;

        QueryDimensionEnum(String sql) {
            this.sql = sql;
        }

        public String toSQL() {
            return this.sql;
        }
    }

    private enum JobDimensionEnum {
        PROJECT(JobPropertyEnum.PROJECT.toString()), //
        CUBE(JobPropertyEnum.CUBE.toString()), //
        DAY(TimePropertyEnum.DAY_DATE.toString()), //
        WEEK(TimePropertyEnum.WEEK_BEGIN_DATE.toString()), //
        MONTH(TimePropertyEnum.MONTH.toString());

        private final String sql;

        JobDimensionEnum(String sql) {
            this.sql = sql;
        }

        public String toSQL() {
            return this.sql;
        }
    }

    private enum QueryMetricEnum {
        QUERY_COUNT("count(*)"), //
        AVG_QUERY_LATENCY("avg(" + QuerySparkExecutionEnum.TIME_COST.toString() + ")"), //
        MAX_QUERY_LATENCY("max(" + QuerySparkExecutionEnum.TIME_COST.toString() + ")"), //
        MIN_QUERY_LATENCY("min(" + QuerySparkExecutionEnum.TIME_COST.toString() + ")");

        private final String sql;

        QueryMetricEnum(String sql) {
            this.sql = sql;
        }

        public String toSQL() {
            return this.sql;
        }
    }

    private enum JobMetricEnum {
        JOB_COUNT("count(*)"), //
        AVG_JOB_BUILD_TIME("avg(" + JobPropertyEnum.PER_BYTES_TIME_COST.toString() + ")"), //
        MAX_JOB_BUILD_TIME("max(" + JobPropertyEnum.PER_BYTES_TIME_COST.toString() + ")"), //
        MIN_JOB_BUILD_TIME("min(" + JobPropertyEnum.PER_BYTES_TIME_COST.toString() + ")");

        private final String sql;

        JobMetricEnum(String sql) {
            this.sql = sql;
        }

        public String toSQL() {
            return this.sql;
        }
    }
}