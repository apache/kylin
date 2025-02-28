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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;

import lombok.Setter;

public class MockedQueryHistoryDao extends RDBMSQueryHistoryDAO {
    // current time is 2018-01-02 00:00:00
    private long currentTime;

    @Setter
    private List<QueryHistory> overallQueryHistories = Lists.newArrayList();

    public MockedQueryHistoryDao() throws Exception {
        super();
        init();
    }

    private void init() {
        String currentDate = "2018-01-02";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        try {
            currentTime = format.parse(currentDate).getTime();
        } catch (ParseException e) {
            // ignore
        }

        // these are expected to be marked as favorite queries
        for (int i = 0; i < 6; i++) {
            QueryHistory queryHistory = new QueryHistory("select * from sql_pattern" + i,
                    QueryHistory.QUERY_HISTORY_SUCCEEDED, "ADMIN", System.currentTimeMillis(), 6000L);
            queryHistory.setInsertTime(currentTime + 30 * i * 1000L);
            queryHistory.setEngineType("HIVE");
            QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo();
            queryHistoryInfo.setRealizationMetrics(Lists.newArrayList(
                    new QueryMetrics.RealizationMetrics("1", "Agg Index", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                            Lists.newArrayList()),
                    new QueryMetrics.RealizationMetrics("1", "Agg Index", "82fa7671-a935-45f5-8779-85703601f49a",
                            Lists.newArrayList())));
            queryHistory.setQueryHistoryInfo(queryHistoryInfo);
            if (i == 4)
                queryHistory.setSqlPattern("SELECT *\nFROM \"TEST_KYLIN_FACT\"");
            if (i == 5)
                queryHistory.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
            overallQueryHistories.add(queryHistory);
        }

        // These are three sql patterns that are already loaded in database
        for (int i = 0; i < 3; i++) {
            QueryHistory queryHistoryForUpdate = new QueryHistory("select * from sql" + (i + 1),
                    QueryHistory.QUERY_HISTORY_SUCCEEDED, "ADMIN", System.currentTimeMillis(), 6000L);
            queryHistoryForUpdate.setInsertTime(currentTime + 30 * i * 1000L);
            queryHistoryForUpdate.setEngineType("HIVE");
            overallQueryHistories.add(queryHistoryForUpdate);
        }
    }

    public void insert(QueryHistory queryHistory) {
        overallQueryHistories.add(queryHistory);
    }

    public long getCurrentTime() {
        return currentTime;
    }
}
