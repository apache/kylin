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

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.scheduler.SchedulerEventNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryMetrics extends SchedulerEventNotifier {

    protected static final Logger logger = LoggerFactory.getLogger(QueryMetrics.class);

    protected static final KapConfig kapConfig = KapConfig.getInstanceFromEnv();
    public static final String UNKNOWN = "Unknown";

    public static final String AGG_INDEX = "Agg Index";
    public static final String TABLE_INDEX = "Table Index";
    public static final String TABLE_SNAPSHOT = "Table Snapshot";
    public static final String TOTAL_SCAN_COUNT = "totalScanCount";
    public static final String TOTAL_SCAN_BYTES = "totalScanBytes";

    // fields below are columns in InfluxDB table which records down query history
    protected long id;
    protected final String queryId;
    protected long queryTime;
    protected String projectName;

    protected String sql;
    // KE-36662 Using sql_pattern as normalized_sql storage
    protected String sqlPattern;

    protected String submitter;
    protected String server;

    protected long queryDuration;
    protected long totalScanBytes;
    protected long totalScanCount;
    protected long resultRowCount;
    protected long queryJobCount;
    protected long queryStageCount;
    protected long queryTaskCount;

    protected boolean isPushdown;
    protected String engineType;

    protected boolean isCacheHit;
    protected String cacheType;
    protected String queryMsg;
    protected boolean isIndexHit;
    protected boolean isTimeout;

    protected String errorType;
    protected String queryStatus;

    protected String month;
    protected long queryFirstDayOfMonth;
    protected long queryFirstDayOfWeek;
    protected long queryDay;

    protected boolean tableIndexUsed;
    protected boolean aggIndexUsed;
    protected boolean tableSnapshotUsed;

    protected String defaultServer;

    protected QueryHistoryInfo queryHistoryInfo;

    public QueryMetrics(String queryId) {
        this.queryId = queryId;
    }

    public QueryMetrics(String queryId, String defaultServer) {
        this.queryId = queryId;
        this.defaultServer = defaultServer;
    }

    public List<RealizationMetrics> getRealizationMetrics() {
        return ImmutableList.copyOf(queryHistoryInfo.realizationMetrics);
    }

    public boolean isSucceed() {
        return QueryHistory.QUERY_HISTORY_SUCCEEDED.equals(queryStatus);
    }

    public boolean isSecondStorage() {
        for (RealizationMetrics metrics: getRealizationMetrics()) {
            if (metrics.isSecondStorage)
                return true;
        }
        return false;
    }

    @Getter
    @Setter
    // fields in this class are columns in InfluxDB table which records down query history's realization info
    public static class RealizationMetrics implements Serializable {

        protected String queryId;

        protected long duration;

        protected String layoutId;

        protected String indexType;

        protected String modelId;

        protected long queryTime;

        protected String projectName;

        protected boolean isSecondStorage;

        protected boolean isStreamingLayout;

        protected List<String> snapshots;

        // For serialize
        public RealizationMetrics() {}

        public RealizationMetrics(String layoutId, String indexType, String modelId, List<String> snapshots) {
            this.layoutId = layoutId;
            this.indexType = indexType;
            this.modelId = modelId;
            this.snapshots = snapshots;
        }
    }
}
