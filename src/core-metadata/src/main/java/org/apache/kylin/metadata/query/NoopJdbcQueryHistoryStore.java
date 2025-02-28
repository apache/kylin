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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.query.QueryMetrics.RealizationMetrics;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.select.QueryExpressionDSL;
import org.mybatis.dynamic.sql.select.SelectModel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopJdbcQueryHistoryStore extends JdbcQueryHistoryStore {

    public NoopJdbcQueryHistoryStore() {
        // nothing to do
    }

    @Override
    public void dropQueryHistoryTable() {
        // nothing to do
    }

    @Override
    public int insert(QueryMetrics queryMetrics) {
        return 0;
    }

    @Override
    public void insert(List<QueryMetrics> queryMetricsList) {
        // nothing to do
    }

    @Override
    public List<QueryHistory> queryQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int offset) {
        return Collections.emptyList();
    }

    @Override
    public QueryStatistics queryQueryHistoriesSize(QueryHistoryRequest request) {
        return new QueryStatistics();
    }

    @Override
    public List<QueryDailyStatistic> queryHistoryDailyStatistic(long startTime, long endTime) {
        return Collections.emptyList();
    }

    @Override
    public List<QueryHistory> queryQueryHistoriesSubmitters(QueryHistoryRequest request, int size) {
        return Collections.emptyList();
    }

    @Override
    public List<QueryStatistics> queryQueryHistoriesModelIds(QueryHistoryRequest request) {
        return Collections.emptyList();
    }

    @Override
    public QueryHistory getOldestQueryHistory(long index) {
        return null;
    }

    @Override
    public QueryHistory getOldestQueryHistory(String project, long index) {
        return null;
    }

    @Override
    public Long getCountOnQueryHistory() {
        return 0L;
    }

    @Override
    public Long getCountOnQueryHistory(long retainTime) {
        return 0L;
    }

    @Override
    public Map<String, Long> getCountGroupByProject() {
        return Collections.emptyMap();
    }

    @Override
    public QueryHistory queryByQueryId(String queryId) {
        return null;
    }

    @Override
    public List<QueryHistory> queryByQueryIds(List<String> queryIds) {
        return Collections.emptyList();
    }

    @Override
    public List<QueryHistory> queryAllQueryHistories() {
        return Collections.emptyList();
    }

    @Override
    public List<QueryHistory> queryQueryHistoriesByIdOffset(long id, int batchSize, String project) {
        return Collections.emptyList();
    }

    @Override
    public List<QueryStatistics> queryCountAndAvgDuration(long startTime, long endTime, String project) {
        return Collections.emptyList();
    }

    @Override
    public List<QueryStatistics> queryCountByModel(long startTime, long endTime, String project) {
        return Collections.emptyList();
    }

    @Override
    public long queryQueryHistoryCountBeyondOffset(long offset, String project) {
        return 0;
    }

    @Override
    public long queryQueryHistoryMaxId(String project) {
        return 0;
    }

    @Override
    protected Long queryQueryHistoryMinQueryTime() {
        return 0L;
    }

    @Override
    public QueryStatistics queryRecentQueryCount(long startTime, long endTime, String project) {
        return null;
    }

    @Override
    public List<QueryStatistics> queryCountByTime(long startTime, long endTime, String timeDimension, String project) {
        return Collections.emptyList();
    }

    @Override
    public List<QueryStatistics> queryAvgDurationByModel(long startTime, long endTime, String project) {
        return Collections.emptyList();
    }

    @Override
    public List<QueryStatistics> queryAvgDurationByTime(long startTime, long endTime, String timeDimension, String project) {
        return Collections.emptyList();
    }

    @Override
    public void deleteQueryHistory() {
        // nothing to do
    }

    @Override
    public int deleteQueryHistory(long id) {
        return 0;
    }

    @Override
    public int deleteQueryHistory(String project, long id) {
        return 0;
    }

    @Override
    public void deleteQueryHistory(String project) {
        // nothing to do
    }

    @Override
    public void deleteQueryHistoryRealization(long queryTime) {
        // nothing to do
    }

    @Override
    public void deleteQueryHistoryRealization(String project, long queryTime) {
        // nothing to do
    }

    @Override
    public void deleteQueryHistoryRealization(String project) {
        // nothing to do
    }

    @Override
    public void updateQueryHistoryInfo(List<Pair<Long, QueryHistoryInfo>> idToQHInfoList) {
        // nothing to do
    }

    @Override
    InsertStatementProvider<QueryMetrics> getInsertQhProvider(QueryMetrics queryMetrics) {
        return null;
    }

    @Override
    InsertStatementProvider<RealizationMetrics> getInsertQhRealizationProvider(RealizationMetrics realizationMetrics) {
        return null;
    }

    @Override
    QueryExpressionDSL<SelectModel>.QueryExpressionWhereBuilder filterModelsByConditions(QueryExpressionDSL<SelectModel> selectSql, List<String> modelIds) {
        return null;
    }
}
