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

package org.apache.kylin.metadata.query;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Setter;
import lombok.val;

public class RDBMSQueryHistoryDAO implements QueryHistoryDAO {
    private static final Logger logger = LoggerFactory.getLogger(RDBMSQueryHistoryDAO.class);
    @Setter
    private String queryMetricMeasurement;
    private String realizationMetricMeasurement;
    private JdbcQueryHistoryStore jdbcQueryHisStore;

    public static final String WEEK = "week";
    public static final String DAY = "day";

    public static RDBMSQueryHistoryDAO getInstance() {
        return Singletons.getInstance(RDBMSQueryHistoryDAO.class);
    }

    public RDBMSQueryHistoryDAO() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (!UnitOfWork.isAlreadyInTransaction()) {
            logger.info("Initializing RDBMSQueryHistoryDAO with KylinConfig Id: {} ", System.identityHashCode(config));
        }
        String metadataIdentifier = StorageURL.replaceUrl(config.getMetadataUrl());
        this.queryMetricMeasurement = metadataIdentifier + "_" + QueryHistory.QUERY_MEASUREMENT_SURFIX;
        this.realizationMetricMeasurement = metadataIdentifier + "_" + QueryHistory.REALIZATION_MEASUREMENT_SURFIX;
        jdbcQueryHisStore = new JdbcQueryHistoryStore(config);
    }

    @Override
    public String getQueryMetricMeasurement() {
        return queryMetricMeasurement;
    }

    @Override
    public String getRealizationMetricMeasurement() {
        return realizationMetricMeasurement;
    }

    public List<QueryDailyStatistic> getQueryDailyStatistic(long startTime, long endTime) {
        return jdbcQueryHisStore.queryHistoryDailyStatistic(startTime, endTime);
    }

    public int insert(QueryMetrics metrics) {
        return jdbcQueryHisStore.insert(metrics);
    }

    public void insert(List<QueryMetrics> metricsList) {
        jdbcQueryHisStore.insert(metricsList);
    }

    public void dropQueryHistoryTable() throws SQLException {
        jdbcQueryHisStore.dropQueryHistoryTable();
    }

    public void deleteAllQueryHistory() {
        jdbcQueryHisStore.deleteQueryHistory();
    }

    public void deleteQueryHistoryByProject(String project) {
        jdbcQueryHisStore.deleteQueryHistory(project);
    }

    public void deleteAllQueryHistoryRealizationForProject(String project) {
        jdbcQueryHisStore.deleteQueryHistoryRealization(project);
    }

    public void deleteQueryHistoriesIfMaxSizeReached() {
        QueryHistory queryHistory = jdbcQueryHisStore
                .queryOldestQueryHistory(KylinConfig.getInstanceFromEnv().getQueryHistoryMaxSize());
        if (Objects.nonNull(queryHistory)) {
            long time = queryHistory.getQueryTime();
            jdbcQueryHisStore.deleteQueryHistory(time);
            jdbcQueryHisStore.deleteQueryHistoryRealization(time);
        }
    }

    public QueryHistory getByQueryId(String queryId) {
        return jdbcQueryHisStore.queryByQueryId(queryId);
    }

    public void deleteQueryHistoriesIfProjectMaxSizeReached(String project) {
        QueryHistory queryHistory = jdbcQueryHisStore
                .queryOldestQueryHistory(KylinConfig.getInstanceFromEnv().getQueryHistoryProjectMaxSize(), project);
        if (Objects.nonNull(queryHistory)) {
            long time = queryHistory.getQueryTime();
            jdbcQueryHisStore.deleteQueryHistory(time, project);
            jdbcQueryHisStore.deleteQueryHistoryRealization(time, project);
        }
    }

    public void deleteQueryHistoriesIfRetainTimeReached() {
        long retainTime = getRetainTime();
        jdbcQueryHisStore.deleteQueryHistory(retainTime);
        jdbcQueryHisStore.deleteQueryHistoryRealization(retainTime);
    }

    public void batchUpdateQueryHistoriesInfo(List<Pair<Long, QueryHistoryInfo>> idToQHInfoList) {
        jdbcQueryHisStore.updateQueryHistoryInfo(idToQHInfoList);
    }

    public static long getRetainTime() {
        return new Date(
                System.currentTimeMillis() - KylinConfig.getInstanceFromEnv().getQueryHistorySurvivalThreshold())
                        .getTime();
    }

    public void dropProjectMeasurement(String project) {
        jdbcQueryHisStore.deleteQueryHistory(project);
        jdbcQueryHisStore.deleteQueryHistoryRealization(project);
    }

    public List<QueryHistory> getAllQueryHistories() {
        return jdbcQueryHisStore.queryAllQueryHistories();
    }

    public List<QueryHistory> queryQueryHistoriesByIdOffset(long idOffset, int batchSize, String project) {
        return jdbcQueryHisStore.queryQueryHistoriesByIdOffset(idOffset, batchSize, project);
    }

    public List<QueryHistory> getQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int page) {
        return jdbcQueryHisStore.queryQueryHistoriesByConditions(request, limit, page * limit);
    }

    public List<QueryHistory> getQueryHistoriesByConditionsWithOffset(QueryHistoryRequest request, int limit,
            int offset) {
        return jdbcQueryHisStore.queryQueryHistoriesByConditions(request, limit, offset);
    }

    public long getQueryHistoriesSize(QueryHistoryRequest request, String project) {
        return jdbcQueryHisStore.queryQueryHistoriesSize(request).getCount();
    }

    public List<QueryHistory> getQueryHistoriesSubmitters(QueryHistoryRequest request, int size) {
        return jdbcQueryHisStore.queryQueryHistoriesSubmitters(request, size);
    }

    public List<QueryStatistics> getQueryHistoriesModelIds(QueryHistoryRequest request, int size) {
        return jdbcQueryHisStore.queryQueryHistoriesModelIds(request, size);
    }

    public QueryStatistics getQueryCountAndAvgDuration(long startTime, long endTime, String project) {
        List<QueryStatistics> result = jdbcQueryHisStore.queryCountAndAvgDuration(startTime, endTime, project);
        if (CollectionUtils.isEmpty(result))
            return new QueryStatistics();
        return result.get(0);
    }

    public List<QueryStatistics> getQueryCountByModel(long startTime, long endTime, String project) {
        return jdbcQueryHisStore.queryCountByModel(startTime, endTime, project);
    }

    public QueryStatistics getQueryCountByRange(long startTime, long endTime, String project) {
        return jdbcQueryHisStore.queryRecentQueryCount(startTime, endTime, project);
    }

    public long getQueryHistoryCountBeyondOffset(long offset, String project) {
        return jdbcQueryHisStore.queryQueryHistoryCountBeyondOffset(offset, project);
    }

    public long getQueryHistoryMaxId(String project) {
        return jdbcQueryHisStore.queryQueryHistoryMaxId(project);
    }

    public List<QueryStatistics> getQueryCountByTime(long startTime, long endTime, String timeDimension,
            String project) {
        return jdbcQueryHisStore.queryCountByTime(startTime, endTime, timeDimension, project);
    }

    public List<QueryStatistics> getAvgDurationByModel(long startTime, long endTime, String project) {
        return jdbcQueryHisStore.queryAvgDurationByModel(startTime, endTime, project);
    }

    public List<QueryStatistics> getAvgDurationByTime(long startTime, long endTime, String timeDimension,
            String project) {
        return jdbcQueryHisStore.queryAvgDurationByTime(startTime, endTime, timeDimension, project);
    }

    public static void fillZeroForQueryStatistics(List<QueryStatistics> queryStatistics, long startTime, long endTime,
            String dimension) {
        if (!dimension.equalsIgnoreCase(DAY) && !dimension.equalsIgnoreCase(WEEK)) {
            return;
        }
        if (dimension.equalsIgnoreCase(WEEK)) {
            startTime = TimeUtil.getWeekStart(startTime);
            endTime = TimeUtil.getWeekStart(endTime);
        }
        Set<Instant> instantSet = queryStatistics.stream().map(QueryStatistics::getTime).collect(Collectors.toSet());
        int rawOffsetTime = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone()).getRawOffset();

        long startOffSetTime = Instant.ofEpochMilli(startTime).plusMillis(rawOffsetTime).toEpochMilli();
        Instant startInstant = Instant.ofEpochMilli(startOffSetTime - startOffSetTime % (1000 * 60 * 60 * 24));
        long endOffSetTime = Instant.ofEpochMilli(endTime).plusMillis(rawOffsetTime).toEpochMilli();
        Instant endInstant = Instant.ofEpochMilli(endOffSetTime - endOffSetTime % (1000 * 60 * 60 * 24));
        while (!startInstant.isAfter(endInstant)) {
            if (!instantSet.contains(startInstant)) {
                QueryStatistics zeroStatistics = new QueryStatistics();
                zeroStatistics.setCount(0);
                zeroStatistics.setTime(startInstant);
                queryStatistics.add(zeroStatistics);
            }
            if (dimension.equalsIgnoreCase(DAY)) {
                startInstant = startInstant.plus(Duration.ofDays(1));
            } else if (dimension.equalsIgnoreCase(WEEK)) {
                startInstant = startInstant.plus(Duration.ofDays(7));
            }
        }
    }
}
