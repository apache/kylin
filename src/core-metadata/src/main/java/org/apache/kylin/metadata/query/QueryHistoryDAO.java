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

import java.util.List;
import java.util.Map;

public interface QueryHistoryDAO {

    List<QueryHistory> getQueryHistoriesByConditions(QueryHistoryRequest request, int limit, int page);

    List<QueryHistory> getQueryHistoriesByConditionsWithOffset(QueryHistoryRequest request, int limit, int offset);

    QueryStatistics getQueryCountAndAvgDuration(long startTime, long endTime, String project);

    QueryStatistics getQueryCountByRange(long startTime, long endTime, String project);

    long getQueryHistoryCountBeyondOffset(long offset, String project);

    List<QueryStatistics> getQueryCountByModel(long startTime, long endTime, String project);

    List<QueryStatistics> getQueryCountByTime(long startTime, long endTime, String timeDimension, String project);

    List<QueryStatistics> getAvgDurationByModel(long startTime, long endTime, String project);

    List<QueryStatistics> getAvgDurationByTime(long startTime, long endTime, String timeDimension, String project);

    String getQueryMetricMeasurement();

    void deleteQueryHistoriesIfMaxSizeReached();

    void deleteQueryHistoriesIfRetainTimeReached();

    void deleteOldestQueryHistoriesByProject(String project, int deleteCount);

    long getQueryHistoriesSize(QueryHistoryRequest request, String project);

    QueryHistory getByQueryId(String queryId);

    List<QueryHistory> getQueryHistoriesSubmitters(QueryHistoryRequest request, int size);

    List<QueryStatistics> getQueryHistoriesModelIds(QueryHistoryRequest request, int size);

    String getRealizationMetricMeasurement();

    List<QueryDailyStatistic> getQueryDailyStatistic(long startTime, long endTime);

    Map<String, Long> getQueryCountByProject();

}
