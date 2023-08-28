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

package org.apache.kylin.job.metrics;




import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.job.metrics.util.JobMetricsUtil;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.mybatis.dynamic.sql.SqlBuilder.avg;
import static org.mybatis.dynamic.sql.SqlBuilder.count;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isGreaterThanOrEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.isLessThanOrEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.select;
import static org.mybatis.dynamic.sql.SqlBuilder.sum;

@Slf4j
public class JdbcJobMetricsStore {

    public static final String MONTH = "month";
    public static final String WEEK = "week";
    public static final String DAY = "day";
    public static final String COUNT = "count";
    public static final String JOB_METRICS_SUFFIX = "job_metrics";


    private JobMetricsTable jobMetricsTable;

    @VisibleForTesting
    @Getter
    private final SqlSessionFactory sqlSessionFactory;

    private final DataSource dataSource;

    String jobMetricsTableName;

    public JdbcJobMetricsStore(KylinConfig kylinConfig) throws Exception {
        StorageURL url = kylinConfig.getMetadataUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        dataSource = JdbcDataSource.getDataSource(props);
        jobMetricsTableName = StorageURL.replaceUrl(url) + "_" + JOB_METRICS_SUFFIX;
        jobMetricsTable = new JobMetricsTable(jobMetricsTableName);
        sqlSessionFactory = JobMetricsUtil.getSqlSessionFactory(dataSource, jobMetricsTableName);
    }

    public int insert(JobMetrics jobMetrics) {
        try(SqlSession session = sqlSessionFactory.openSession()) {
            JobMetricsMapper mapper = session.getMapper(JobMetricsMapper.class);
            InsertStatementProvider<JobMetrics> statementProvider = getInsertJobMetricsProvider(jobMetrics);
            int rows = mapper.insert(statementProvider);

            if (rows > 0) {
                log.info("Insert one Job Metric(job id:{}) into database.", jobMetrics.getJobId());
            }
            session.commit();
            return rows;
        }
    }


    public List<JobMetricsStatistics> JobCountAndTotalBuildCost(long startTime, long endTime, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            JobMetricsStatisticsMapper mapper = session.getMapper(JobMetricsStatisticsMapper.class);
            SelectStatementProvider statementProvider = select(count(jobMetricsTable.id).as(COUNT),
                    sum(jobMetricsTable.modelSize).as("model_size"),
                    sum(jobMetricsTable.duration).as("duration")).from(jobMetricsTable)
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime))
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime))
                    .and(jobMetricsTable.projectName, isEqualTo(project))
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<JobMetricsStatistics> jobCountByModel(long startTime, long endTime, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            JobMetricsStatisticsMapper mapper = session.getMapper(JobMetricsStatisticsMapper.class);
            SelectStatementProvider statementProvider = select(count(jobMetricsTable.id).as(COUNT),
                    (jobMetricsTable.model).as("model"))
                    .from(jobMetricsTable)
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime))
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime))
                    .and(jobMetricsTable.projectName, isEqualTo(project))
                    .groupBy(jobMetricsTable.model)
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<JobMetricsStatistics> jobCountByTime(long startTime, long endTime, String timeDimension, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            JobMetricsStatisticsMapper mapper = session.getMapper(JobMetricsStatisticsMapper.class);
            SelectStatementProvider statementProvider = jobCountByTimeProvider(startTime, endTime, timeDimension,
                    project);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<JobMetricsStatistics> jobBuildCostByModel(long startTime, long endTime, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            JobMetricsStatisticsMapper mapper = session.getMapper(JobMetricsStatisticsMapper.class);
            SelectStatementProvider statementProvider = select(count(jobMetricsTable.id).as(COUNT),
                    (jobMetricsTable.model).as("model"),
                    avg(jobMetricsTable.modelSize).as("model_size"),
                    avg(jobMetricsTable.duration).as("duration"))
                    .from(jobMetricsTable)
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime))
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime))
                    .and(jobMetricsTable.projectName, isEqualTo(project))
                    .groupBy(jobMetricsTable.model)
                    .build().render(RenderingStrategies.MYBATIS3);
            return mapper.selectMany(statementProvider);
        }
    }

    public List<JobMetricsStatistics> jobBuildCostByTime(long startTime, long endTime, String timeDimension, String project) {
        try (SqlSession session = sqlSessionFactory.openSession()) {
            JobMetricsStatisticsMapper mapper = session.getMapper(JobMetricsStatisticsMapper.class);
            SelectStatementProvider statementProvider = jobBuildCostByTimeProvider(startTime, endTime, timeDimension, project);
            return mapper.selectMany(statementProvider);
        }
    }

    private SelectStatementProvider jobCountByTimeProvider(long startTime, long endTime, String timeDimension,
                                                           String project) {
        if (timeDimension.equalsIgnoreCase(MONTH)) {
            return select(jobMetricsTable.buildFirstDayOfMonth.as("time"), count(jobMetricsTable.id).as(COUNT)) //
                    .from(jobMetricsTable) //
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime)) //
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime)) //
                    .and(jobMetricsTable.projectName, isEqualTo(project)) //
                    .groupBy(jobMetricsTable.buildFirstDayOfMonth) //
                    .build().render(RenderingStrategies.MYBATIS3);
        } else if (timeDimension.equalsIgnoreCase(WEEK)) {
            return select(jobMetricsTable.buildFirstDayOfWeek.as("time"), count(jobMetricsTable.id).as(COUNT)) //
                    .from(jobMetricsTable) //
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime)) //
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime)) //
                    .and(jobMetricsTable.projectName, isEqualTo(project)) //
                    .groupBy(jobMetricsTable.buildFirstDayOfWeek) //
                    .build().render(RenderingStrategies.MYBATIS3);
        } else if (timeDimension.equalsIgnoreCase(DAY)) {
            return select(jobMetricsTable.buildDay.as("time"), count(jobMetricsTable.id).as(COUNT)) //
                    .from(jobMetricsTable) //
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime)) //
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime)) //
                    .and(jobMetricsTable.projectName, isEqualTo(project)) //
                    .groupBy(jobMetricsTable.buildDay) //
                    .build().render(RenderingStrategies.MYBATIS3);
        } else {
            throw new IllegalStateException("Unsupported time window!");
        }
    }

    private SelectStatementProvider jobBuildCostByTimeProvider(long startTime, long endTime, String timeDimension,
                                                               String project) {
        if (timeDimension.equalsIgnoreCase(MONTH)) {
            return select(jobMetricsTable.buildFirstDayOfMonth.as("time"),
                    avg(jobMetricsTable.duration).as("duration"), //
                    avg(jobMetricsTable.modelSize).as("model_size"),
                    count(jobMetricsTable.id).as(COUNT))
                    .from(jobMetricsTable) //
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime)) //
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime)) //
                    .and(jobMetricsTable.projectName, isEqualTo(project)) //
                    .groupBy(jobMetricsTable.buildFirstDayOfMonth) //
                    .build().render(RenderingStrategies.MYBATIS3);
        } else if (timeDimension.equalsIgnoreCase(WEEK)) {
            return select(jobMetricsTable.buildFirstDayOfWeek.as("time"),
                    avg(jobMetricsTable.duration).as("duration"), //
                    avg(jobMetricsTable.modelSize).as("model_size"),
                    count(jobMetricsTable.id).as(COUNT))
                    .from(jobMetricsTable) //
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime)) //
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime)) //
                    .and(jobMetricsTable.projectName, isEqualTo(project)) //
                    .groupBy(jobMetricsTable.buildFirstDayOfWeek) //
                    .build().render(RenderingStrategies.MYBATIS3);
        } else if (timeDimension.equalsIgnoreCase(DAY)) {
            return select(jobMetricsTable.buildDay.as("time"),
                    avg(jobMetricsTable.duration).as("duration"), //
                    avg(jobMetricsTable.modelSize).as("model_size"),
                    count(jobMetricsTable.id).as(COUNT))
                    .from(jobMetricsTable) //
                    .where(jobMetricsTable.buildDay, isGreaterThanOrEqualTo(startTime)) //
                    .and(jobMetricsTable.buildDay, isLessThanOrEqualTo(endTime)) //
                    .and(jobMetricsTable.projectName, isEqualTo(project)) //
                    .groupBy(jobMetricsTable.buildDay) //
                    .build().render(RenderingStrategies.MYBATIS3);
        } else {
            throw new IllegalStateException("Unsupported time window!");
        }
    }

    public static void fillZeroForJobStatistics(List<JobMetricsStatistics> jobStatistics, long startTime, long endTime,
                                                String dimension) {
        if (!dimension.equalsIgnoreCase(DAY) && !dimension.equalsIgnoreCase(WEEK)) {
            return;
        }
        if (dimension.equalsIgnoreCase(WEEK)) {
            startTime = TimeUtil.getWeekStart(startTime);
            endTime = TimeUtil.getWeekStart(endTime);
        }
        Set<Instant> instantSet = jobStatistics.stream().map(JobMetricsStatistics::getTime).collect(Collectors.toSet());
        int rawOffsetTime = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();

        long startOffSetTime = Instant.ofEpochMilli(startTime).plusMillis(rawOffsetTime).toEpochMilli();
        Instant startInstant = Instant.ofEpochMilli(startOffSetTime - startOffSetTime % (1000 * 60 * 60 * 24));
        long endOffSetTime = Instant.ofEpochMilli(endTime).plusMillis(rawOffsetTime).toEpochMilli();
        Instant endInstant = Instant.ofEpochMilli(endOffSetTime - endOffSetTime % (1000 * 60 * 60 * 24));
        while (!startInstant.isAfter(endInstant)) {
            if (!instantSet.contains(startInstant)) {
                JobMetricsStatistics zeroStatistics = new JobMetricsStatistics();
                zeroStatistics.setCount(0);
                zeroStatistics.setTime(startInstant);
                jobStatistics.add(zeroStatistics);
            }
            if (dimension.equalsIgnoreCase(DAY)) {
                startInstant = startInstant.plus(Duration.ofDays(1));
            } else if (dimension.equalsIgnoreCase(WEEK)) {
                startInstant = startInstant.plus(Duration.ofDays(7));
            }
        }
    }



    InsertStatementProvider<JobMetrics> getInsertJobMetricsProvider(JobMetrics jobMetrics) {
        return SqlBuilder.insert(jobMetrics).into(jobMetricsTable)
                .map(jobMetricsTable.jobId).toPropertyWhenPresent("jobId", jobMetrics::getJobId)
                .map(jobMetricsTable.jobType).toPropertyWhenPresent("jobType", jobMetrics::getJobType)
                .map(jobMetricsTable.duration).toPropertyWhenPresent("duration", jobMetrics::getDuration)
                .map(jobMetricsTable.submitter).toPropertyWhenPresent("submitter", jobMetrics::getSubmitter)
                .map(jobMetricsTable.model).toPropertyWhenPresent("model", jobMetrics::getModel)
                .map(jobMetricsTable.projectName).toPropertyWhenPresent("projectName", jobMetrics::getProjectName)
                .map(jobMetricsTable.buildTime).toPropertyWhenPresent("buildTime", jobMetrics::getBuildTime)
                .map(jobMetricsTable.modelSize).toPropertyWhenPresent("modelSize", jobMetrics::getModelSize)
                .map(jobMetricsTable.waitTime).toPropertyWhenPresent("waitTime", jobMetrics::getWaitTime)
                .map(jobMetricsTable.buildFirstDayOfMonth)
                .toPropertyWhenPresent("buildFirstDayOfMonth", jobMetrics::getBuildFirstDayOfMonth)
                .map(jobMetricsTable.buildFirstDayOfWeek)
                .toPropertyWhenPresent("buildFirstDayOfWeek", jobMetrics::getBuildFirstDayOfWeek)
                .map(jobMetricsTable.buildDay).toPropertyWhenPresent("buildDay", jobMetrics::getBuildDay)
                .map(jobMetricsTable.buildDate).toPropertyWhenPresent("buildDate", jobMetrics::getBuildDate)
                .map(jobMetricsTable.jobState).toPropertyWhenPresent("jobState", jobMetrics::getJobState)
                .map(jobMetricsTable.errorType).toPropertyWhenPresent("errorType", jobMetrics::getErrorType)
                .map(jobMetricsTable.errorInfo).toPropertyWhenPresent("errorInfo", jobMetrics::getErrorInfo)
                .map(jobMetricsTable.perBytesTimeCost).toPropertyWhenPresent("perBytesTimeCost",
                        jobMetrics::getPerBytesTimeCost)
                .map(jobMetricsTable.jobEngine).toPropertyWhenPresent("jobEngine", jobMetrics::getJobEngine)
                .build().render(RenderingStrategies.MYBATIS3);
    }


}