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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;

public class BigQueryThresholdUpdater {
    protected static final Logger logger = LoggerFactory.getLogger(BigQueryThresholdUpdater.class);

    private static final double SLOPE = 0.000000756178;
    private static final double OFFSET = 0.0000000195538;

    private static final ThreadPoolExecutor updateThreadPool = new ThreadPoolExecutor(0, 1, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(), new DaemonThreadFactory("big-query-threshold-updater-thread"));

    private static long lastUpdateTime = System.currentTimeMillis();
    private static long bigQueryThreshold = KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold();
    private static final Map<Long, StatInfo> queryScanRowsToStatsMap = new HashMap<>();

    private BigQueryThresholdUpdater() {

    }

    public static void initBigQueryThresholdBySparkResource(int instance, int core) {
        // user don't set the threshold value, init it using spark resource
        if (bigQueryThreshold < 0) {
            double timeToRowCountRatio = OFFSET + SLOPE / (instance * core);
            bigQueryThreshold = (long) (KapConfig.getInstanceFromEnv().getBigQuerySecond() / timeToRowCountRatio);
            logger.info(
                    "Init big query threshold auto, spark instanceNum: {}, coreNum:{}, timeToRowCountRatio:{}, bigQueryRowCountThreshold:{}",
                    instance, core, timeToRowCountRatio, bigQueryThreshold);
        }
    }

    // for unit test
    public static void resetBigQueryThreshold() {
        bigQueryThreshold = KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold();
    }

    public static void setLastUpdateTime(long updateTime) {
        lastUpdateTime = updateTime;
    }

    public static long getBigQueryThreshold() {
        return bigQueryThreshold;
    }

    public static void collectQueryScanRowsAndTime(long duration, long scanRows) {
        if (duration < KapConfig.getInstanceFromEnv().getBigQuerySecond() * 1000 || scanRows <= 0) {
            return;
        }
        updateThreadPool.submit(() -> {
            StatInfo statInfo;
            if (queryScanRowsToStatsMap.containsKey(scanRows)) {
                statInfo = queryScanRowsToStatsMap.get(scanRows);
            } else {
                statInfo = new StatInfo();
                queryScanRowsToStatsMap.put(scanRows, statInfo);
            }
            statInfo.addDuration(duration);

            long now = System.currentTimeMillis();
            if (now - lastUpdateTime >= KapConfig.getInstanceFromEnv().getBigQueryThresholdUpdateIntervalSecond()
                    * 1000) {
                StringBuilder logRecord = new StringBuilder();
                logRecord.append("SampleCnt:").append(queryScanRowsToStatsMap.size()).append(";");
                long bigQueryThresholdUpdate = Long.MAX_VALUE;
                for (Map.Entry<Long, StatInfo> entry : queryScanRowsToStatsMap.entrySet()) {
                    StatInfo curStatInfo = entry.getValue();
                    if (curStatInfo.getCount() > 0) {
                        long recordScanRows = entry.getKey();
                        double durationAvg = curStatInfo.getDurationAvg();
                        double durationVariance = curStatInfo.getDurationVariance();
                        logRecord.append("scanRows:").append(recordScanRows).append(",cnt:").append(curStatInfo.count)
                                .append(",avgTime:").append(durationAvg).append(",varTime:").append(durationVariance)
                                .append(";");
                        bigQueryThresholdUpdate = Math.min(bigQueryThresholdUpdate, recordScanRows);
                    }
                }
                if (bigQueryThresholdUpdate < Long.MAX_VALUE) {
                    bigQueryThreshold = bigQueryThresholdUpdate;
                    logRecord.append("updateThreshold:").append(bigQueryThreshold);
                }
                logger.info(logRecord.toString());
                setLastUpdateTime(now);
                queryScanRowsToStatsMap.clear();
            }
        });
    }

    public static class StatInfo {
        @Getter
        private int count;
        private double durationSum;
        private double durationSquareSum;

        public void addDuration(long durationMillis) {
            double durationSec = (double) durationMillis / 1000;
            count++;
            durationSum += durationSec;
            durationSquareSum += durationSec * durationSec;
        }

        public double getDurationAvg() {
            return durationSum / count;
        }

        public double getDurationVariance() {
            double durationAvg = getDurationAvg();
            return durationSquareSum / count + durationAvg * durationAvg;
        }
    }

}
