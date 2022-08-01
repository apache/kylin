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

package org.apache.kylin.metadata.streaming;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

public class StreamingJobStatsManager {
    private static final Logger logger = LoggerFactory.getLogger(StreamingJobStatsManager.class);
    @Setter
    @Getter
    private String sJSMetricMeasurement;

    private JdbcStreamingJobStatsStore jdbcSJSStore;

    public static StreamingJobStatsManager getInstance() {
        return Singletons.getInstance(StreamingJobStatsManager.class);
    }

    public StreamingJobStatsManager() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing StreamingJobStatsManager with KylinConfig Id: {} ",
                    System.identityHashCode(config));
        String metadataIdentifier = StorageURL.replaceUrl(config.getMetadataUrl());
        this.sJSMetricMeasurement = metadataIdentifier + "_" + StreamingJobStats.STREAMING_JOB_STATS_SUFFIX;
        jdbcSJSStore = new JdbcStreamingJobStatsStore(config);
    }

    public int insert(StreamingJobStats stats) {
        return jdbcSJSStore.insert(stats);
    }

    public void insert(List<StreamingJobStats> statsList) {
        jdbcSJSStore.insert(statsList);
    }

    public void dropTable() throws SQLException {
        jdbcSJSStore.dropTable();
    }

    public void deleteAllStreamingJobStats() {
        jdbcSJSStore.deleteStreamingJobStats(-1L);
    }

    public void deleteSJSIfRetainTimeReached() {
        long retainTime = getRetainTime();
        jdbcSJSStore.deleteStreamingJobStats(retainTime);
    }

    public static long getRetainTime() {
        return new Date(System.currentTimeMillis()
                - KylinConfig.getInstanceFromEnv().getStreamingJobStatsSurvivalThreshold() * 24 * 60 * 60 * 1000)
                        .getTime();
    }

    public List<RowCountDetailByTime> queryRowCountDetailByTime(long startTime, String jobId) {
        return jdbcSJSStore.queryRowCountDetailByTime(startTime, jobId);
    }

    public ConsumptionRateStats countAvgConsumptionRate(long startTime, String jobId) {
        return jdbcSJSStore.queryAvgConsumptionRate(startTime, jobId);
    }

    public List<StreamingJobStats> queryStreamingJobStats(long startTime, String jobId) {
        return jdbcSJSStore.queryByJobId(startTime, jobId);
    }

    public StreamingJobStats getLatestOneByJobId(String jobId) {
        return jdbcSJSStore.getLatestOneByJobId(jobId);
    }

    public Map<String, Long> queryDataLatenciesByJobIds(List<String> jobIds) {
        if (jobIds.isEmpty()) {
            return Collections.emptyMap();
        }
        return jdbcSJSStore.queryDataLatenciesByJobIds(jobIds);
    }
}
