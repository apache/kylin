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
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueryMetrics extends SchedulerEventNotifier {

    protected static final Logger logger = LoggerFactory.getLogger(QueryMetrics.class);

    protected static final KapConfig kapConfig = KapConfig.getInstanceFromEnv();
    public static final String UNKNOWN = "Unknown";

    public static final String FILTER_CONFLICT = "Filter Conflict";
    public static final String AGG_INDEX = "Agg Index";
    public static final String TABLE_INDEX = "Table Index";
    public static final String TABLE_SNAPSHOT = "Table Snapshot";
    public static final String INTERNAL_TABLE = "Internal Table";
    public static final String TOTAL_SCAN_COUNT = "totalScanCount";
    public static final String TOTAL_SCAN_BYTES = "totalScanBytes";
    public static final String SOURCE_RESULT_COUNT = "sourceResultCount";
    public static final String QUERY_RESPONSE_TIME = "QUERY_RESPONSE_TIME";

    // fields below are columns in InfluxDB table which records down query history
    protected long id;
    protected final String queryId;
    protected long queryTime;
    protected String projectName;

    protected String sql;
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
    protected long cpuTime;

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

    protected boolean isUpdateMetrics = false;

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

        protected boolean isStreamingLayout;

        protected List<String> snapshots;

        protected long queryFirstDayOfMonth;

        protected long queryFirstDayOfWeek;

        protected long queryDay;

        // For serialize
        public RealizationMetrics() {
        }

        public RealizationMetrics(String layoutId, String indexType, String modelId, List<String> snapshots) {
            this.layoutId = layoutId;
            this.indexType = indexType;
            this.modelId = modelId;
            this.snapshots = snapshots;
        }
    }

    @Getter
    @Setter
    // for query metric extensions
    public static class QueryMetric implements Serializable {

        protected String name;

        protected Serializable value;

        public QueryMetric() {
        }

        public QueryMetric(String name, Serializable value) {
            this.name = name;
            this.value = value;
        }
    }
}
