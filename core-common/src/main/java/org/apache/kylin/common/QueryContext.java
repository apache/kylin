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

package org.apache.kylin.common;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Holds per query information and statistics.
 */
public class QueryContext {

    private static final Logger logger = LoggerFactory.getLogger(QueryContext.class);

    public interface QueryStopListener {
        void stop(QueryContext query);
    }

    private long queryStartMillis;
    private long deadline = Long.MAX_VALUE;

    private final String queryId;
    private String username;
    private Set<String> groups;
    private AtomicLong scannedRows = new AtomicLong();
    private AtomicLong returnedRows = new AtomicLong();
    private AtomicLong scannedBytes = new AtomicLong();

    private AtomicBoolean isRunning = new AtomicBoolean(true);
    private volatile Throwable throwable;
    private String stopReason;
    private List<QueryStopListener> stopListeners = Lists.newCopyOnWriteArrayList();

    private List<RPCStatistics> rpcStatisticsList = Lists.newCopyOnWriteArrayList();
    private Map<Integer, CubeSegmentStatisticsResult> cubeSegmentStatisticsResultMap = Maps.newConcurrentMap();

    QueryContext() {
        this(System.currentTimeMillis());
    }

    QueryContext(long startMills) {
        queryId = UUID.randomUUID().toString();
        queryStartMillis = startMills;
    }

    public long getQueryStartMillis() {
        return queryStartMillis;
    }

    public void setDeadline(long timeoutMillis) {
        if (timeoutMillis > 0) {
            deadline = queryStartMillis + timeoutMillis;
        }
    }

    public long getDeadline() {
        return deadline;
    }

    /**
     * @return millis before deadline
     * @throws KylinTimeoutException if deadline has passed
     */
    public long checkMillisBeforeDeadline() {
        long remain = deadline - System.currentTimeMillis();
        if (remain <= 0) {
            throw new KylinTimeoutException("Query timeout");
        }
        return remain;
    }

    public String getQueryId() {
        return queryId == null ? "" : queryId;
    }

    public long getAccumulatedMillis() {
        return System.currentTimeMillis() - queryStartMillis;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public long getScannedRows() {
        return scannedRows.get();
    }

    public long addAndGetScannedRows(long deltaRows) {
        return scannedRows.addAndGet(deltaRows);
    }

    public long getReturnedRows() {
        return returnedRows.get();
    }

    public long addAndGetReturnedRows(long deltaRows) {
        return returnedRows.addAndGet(deltaRows);
    }

    public long getScannedBytes() {
        return scannedBytes.get();
    }

    public long addAndGetScannedBytes(long deltaBytes) {
        return scannedBytes.addAndGet(deltaBytes);
    }

    public void addQueryStopListener(QueryStopListener listener) {
        this.stopListeners.add(listener);
    }

    public boolean isStopped() {
        return !isRunning.get();
    }

    public String getStopReason() {
        return stopReason;
    }

    /**
     * stop the whole query and related sub threads
     */
    public void stop(Throwable t) {
        stopQuery(t, t.getMessage());
    }

    /**
     * stop the whole query by rest call
     */
    public void stopEarly(String reason) {
        stopQuery(null, reason);
    }

    private void stopQuery(Throwable t, String reason) {
        if (!isRunning.compareAndSet(true, false)) {
            return;
        }
        this.throwable = t;
        this.stopReason = reason;
        for (QueryStopListener stopListener : stopListeners) {
            stopListener.stop(this);
        }
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public void addContext(int ctxId, String type, boolean ifCube) {
        Map<String, Map<String, CubeSegmentStatistics>> cubeSegmentStatisticsMap = null;
        if (ifCube) {
            cubeSegmentStatisticsMap = Maps.newConcurrentMap();
        }
        cubeSegmentStatisticsResultMap.put(ctxId, new CubeSegmentStatisticsResult(type, cubeSegmentStatisticsMap));
    }

    public void setContextRealization(int ctxId, String realizationName, int realizationType) {
        CubeSegmentStatisticsResult cubeSegmentStatisticsResult = cubeSegmentStatisticsResultMap.get(ctxId);
        if (cubeSegmentStatisticsResult == null) {
            logger.warn("Cannot find CubeSegmentStatisticsResult for context " + ctxId);
            return;
        }
        cubeSegmentStatisticsResult.setRealization(realizationName);
        cubeSegmentStatisticsResult.setRealizationType(realizationType);
    }

    public List<RPCStatistics> getRpcStatisticsList() {
        return rpcStatisticsList;
    }

    public List<CubeSegmentStatisticsResult> getCubeSegmentStatisticsResultList() {
        return Lists.newArrayList(cubeSegmentStatisticsResultMap.values());
    }

    public void addRPCStatistics(int ctxId, String rpcServer, String cubeName, String segmentName, long sourceCuboidId,
            long targetCuboidId, long filterMask, Exception e, long rpcCallTimeMs, long skippedRows, long scannedRows,
            long returnedRows, long aggregatedRows, long scannedBytes) {
        RPCStatistics rpcStatistics = new RPCStatistics();
        rpcStatistics.setWrapper(cubeName, rpcServer);
        rpcStatistics.setStats(rpcCallTimeMs, skippedRows, scannedRows, returnedRows, aggregatedRows, scannedBytes);
        rpcStatistics.setException(e);
        rpcStatisticsList.add(rpcStatistics);

        CubeSegmentStatisticsResult cubeSegmentStatisticsResult = cubeSegmentStatisticsResultMap.get(ctxId);
        if (cubeSegmentStatisticsResult == null) {
            logger.warn("CubeSegmentStatisticsResult should be initialized for context " + ctxId);
            return;
        }
        Map<String, Map<String, CubeSegmentStatistics>> cubeSegmentStatisticsMap = cubeSegmentStatisticsResult.cubeSegmentStatisticsMap;
        if (cubeSegmentStatisticsMap == null) {
            logger.warn(
                    "cubeSegmentStatisticsMap should be initialized for CubeSegmentStatisticsResult with query type "
                            + cubeSegmentStatisticsResult.queryType);
            return;
        }
        Map<String, CubeSegmentStatistics> segmentStatisticsMap = cubeSegmentStatisticsMap.get(cubeName);
        if (segmentStatisticsMap == null) {
            segmentStatisticsMap = Maps.newConcurrentMap();
            cubeSegmentStatisticsMap.put(cubeName, segmentStatisticsMap);
        }
        CubeSegmentStatistics segmentStatistics = segmentStatisticsMap.get(segmentName);
        if (segmentStatistics == null) {
            segmentStatistics = new CubeSegmentStatistics();
            segmentStatisticsMap.put(segmentName, segmentStatistics);
            segmentStatistics.setWrapper(cubeName, segmentName, sourceCuboidId, targetCuboidId, filterMask);
        }
        if (segmentStatistics.sourceCuboidId != sourceCuboidId || segmentStatistics.targetCuboidId != targetCuboidId
                || segmentStatistics.filterMask != filterMask) {
            StringBuilder inconsistency = new StringBuilder();
            if (segmentStatistics.sourceCuboidId != sourceCuboidId) {
                inconsistency.append(
                        "sourceCuboidId exist " + segmentStatistics.sourceCuboidId + " input " + sourceCuboidId);
            }
            if (segmentStatistics.targetCuboidId != targetCuboidId) {
                inconsistency.append(
                        "targetCuboidId exist " + segmentStatistics.targetCuboidId + " input " + targetCuboidId);
            }
            if (segmentStatistics.filterMask != filterMask) {
                inconsistency.append("filterMask exist " + segmentStatistics.filterMask + " input " + filterMask);
            }
            logger.error("cube segment statistics wrapper is not consistent due to " + inconsistency.toString());
            return;
        }
        segmentStatistics.addRPCStats(rpcCallTimeMs, skippedRows, scannedRows, returnedRows, aggregatedRows,
                scannedBytes, e == null);
    }

    public static class RPCStatistics implements Serializable {
        protected static final long serialVersionUID = 1L;

        private String realizationName;
        private String rpcServer;

        private Exception exception;

        private long callTimeMs;
        private long skippedRows;
        private long scannedRows;
        private long returnedRows;
        private long aggregatedRows;

        private long scannedBytes;

        public void setWrapper(String realizationName, String rpcServer) {
            this.realizationName = realizationName;
            this.rpcServer = rpcServer;
        }

        public void setStats(long callTimeMs, long skipCount, long scanCount, long returnCount, long aggrCount,
                long scanBytes) {
            this.callTimeMs = callTimeMs;
            this.skippedRows = skipCount;
            this.scannedRows = scanCount;
            this.returnedRows = returnCount;
            this.aggregatedRows = aggrCount;

            this.scannedBytes = scanBytes;
        }

        public void setException(Exception e) {
            exception = e;
        }

        public String getRealizationName() {
            return realizationName;
        }

        public String getRpcServer() {
            return rpcServer;
        }

        public Exception getException() {
            return exception;
        }

        public long getCallTimeMs() {
            return callTimeMs;
        }

        public long getSkippedRows() {
            return skippedRows;
        }

        public void setRealizationName(String realizationName) {
            this.realizationName = realizationName;
        }

        public void setRpcServer(String rpcServer) {
            this.rpcServer = rpcServer;
        }

        public void setCallTimeMs(long callTimeMs) {
            this.callTimeMs = callTimeMs;
        }

        public void setSkippedRows(long skippedRows) {
            this.skippedRows = skippedRows;
        }

        public void setScannedRows(long scannedRows) {
            this.scannedRows = scannedRows;
        }

        public void setReturnedRows(long returnedRows) {
            this.returnedRows = returnedRows;
        }

        public void setAggregatedRows(long aggregatedRows) {
            this.aggregatedRows = aggregatedRows;
        }

        public void setScannedBytes(long scannedBytes) {
            this.scannedBytes = scannedBytes;
        }

        public long getScannedRows() {
            return scannedRows;
        }

        public long getReturnedRows() {
            return returnedRows;
        }

        public long getAggregatedRows() {
            return aggregatedRows;
        }

        public long getScannedBytes() {
            return scannedBytes;
        }

        @Override
        public String toString() {
            return "RPCStatistics [rpcServer=" + rpcServer + ",realizationName=" + realizationName + "]";
        }
    }

    public static class CubeSegmentStatistics implements Serializable {
        protected static final long serialVersionUID = 1L;

        private String cubeName;
        private String segmentName;
        private long sourceCuboidId;
        private long targetCuboidId;
        private long filterMask;

        private boolean ifSuccess = true;

        private long callCount = 0L;
        private long callTimeSum = 0L;
        private long callTimeMax = 0L;
        private long storageSkippedRows = 0L;
        private long storageScannedRows = 0L;
        private long storageReturnedRows = 0L;
        private long storageAggregatedRows = 0L;

        private long storageScannedBytes = 0L;

        public void setWrapper(String cubeName, String segmentName, long sourceCuboidId, long targetCuboidId,
                long filterMask) {
            this.cubeName = cubeName;
            this.segmentName = segmentName;
            this.sourceCuboidId = sourceCuboidId;
            this.targetCuboidId = targetCuboidId;
            this.filterMask = filterMask;
        }

        public void addRPCStats(long callTimeMs, long skipCount, long scanCount, long returnCount, long aggrCount,
                long scanBytes, boolean ifSuccess) {
            this.callCount++;
            this.callTimeSum += callTimeMs;
            if (this.callTimeMax < callTimeMs) {
                this.callTimeMax = callTimeMs;
            }
            this.storageSkippedRows += skipCount;
            this.storageScannedRows += scanCount;
            this.storageReturnedRows += returnCount;
            this.storageAggregatedRows += aggrCount;
            this.ifSuccess = this.ifSuccess && ifSuccess;

            this.storageScannedBytes += scanBytes;
        }

        public void setCubeName(String cubeName) {
            this.cubeName = cubeName;
        }

        public void setSegmentName(String segmentName) {
            this.segmentName = segmentName;
        }

        public void setSourceCuboidId(long sourceCuboidId) {
            this.sourceCuboidId = sourceCuboidId;
        }

        public void setTargetCuboidId(long targetCuboidId) {
            this.targetCuboidId = targetCuboidId;
        }

        public void setFilterMask(long filterMask) {
            this.filterMask = filterMask;
        }

        public void setIfSuccess(boolean ifSuccess) {
            this.ifSuccess = ifSuccess;
        }

        public void setCallCount(long callCount) {
            this.callCount = callCount;
        }

        public void setCallTimeSum(long callTimeSum) {
            this.callTimeSum = callTimeSum;
        }

        public void setCallTimeMax(long callTimeMax) {
            this.callTimeMax = callTimeMax;
        }

        public void setStorageSkippedRows(long storageSkippedRows) {
            this.storageSkippedRows = storageSkippedRows;
        }

        public void setStorageScannedRows(long storageScannedRows) {
            this.storageScannedRows = storageScannedRows;
        }

        public void setStorageReturnedRows(long storageReturnedRows) {
            this.storageReturnedRows = storageReturnedRows;
        }

        public void setStorageAggregatedRows(long storageAggregatedRows) {
            this.storageAggregatedRows = storageAggregatedRows;
        }

        public void setStorageScannedBytes(long storageScannedBytes) {
            this.storageScannedBytes = storageScannedBytes;
        }

        public String getCubeName() {
            return cubeName;
        }

        public long getStorageScannedBytes() {
            return storageScannedBytes;
        }

        public long getStorageAggregatedRows() {
            return storageAggregatedRows;
        }

        public long getStorageReturnedRows() {
            return storageReturnedRows;
        }

        public long getStorageSkippedRows() {
            return storageSkippedRows;
        }

        public long getStorageScannedRows() {
            return storageScannedRows;
        }

        public long getCallTimeMax() {
            return callTimeMax;
        }

        public long getCallTimeSum() {
            return callTimeSum;
        }

        public long getCallCount() {
            return callCount;
        }

        public boolean isIfSuccess() {
            return ifSuccess;
        }

        public long getFilterMask() {
            return filterMask;
        }

        public long getTargetCuboidId() {
            return targetCuboidId;
        }

        public long getSourceCuboidId() {
            return sourceCuboidId;
        }

        public String getSegmentName() {
            return segmentName;
        }

        @Override
        public String toString() {
            return "CubeSegmentStatistics [cubeName=" + cubeName + ",segmentName=" + segmentName + ",sourceCuboidId="
                    + sourceCuboidId + ",targetCuboidId=" + targetCuboidId + ",filterMask=" + filterMask + "]";
        }
    }

    public static class CubeSegmentStatisticsResult implements Serializable {
        protected static final long serialVersionUID = 1L;

        private String queryType;
        private Map<String, Map<String, CubeSegmentStatistics>> cubeSegmentStatisticsMap;
        private String realization;
        private int realizationType;

        public CubeSegmentStatisticsResult() {
        }

        public CubeSegmentStatisticsResult(String queryType,
                Map<String, Map<String, CubeSegmentStatistics>> cubeSegmentStatisticsMap) {
            this.queryType = queryType;
            this.cubeSegmentStatisticsMap = cubeSegmentStatisticsMap;
        }

        public void setRealization(String realization) {
            this.realization = realization;
        }

        public String getRealization() {
            return realization;
        }

        public int getRealizationType() {
            return realizationType;
        }

        public void setRealizationType(int realizationType) {
            this.realizationType = realizationType;
        }

        public void setQueryType(String queryType) {
            this.queryType = queryType;
        }

        public void setCubeSegmentStatisticsMap(
                Map<String, Map<String, CubeSegmentStatistics>> cubeSegmentStatisticsMap) {
            this.cubeSegmentStatisticsMap = cubeSegmentStatisticsMap;
        }

        public String getQueryType() {
            return queryType;

        }

        public Map<String, Map<String, CubeSegmentStatistics>> getCubeSegmentStatisticsMap() {
            return cubeSegmentStatisticsMap;
        }

        @Override
        public String toString() {
            return "CubeSegmentStatisticsResult [queryType=" + queryType + ",realization=" + realization
                    + ",realizationType=" + realizationType + ",cubeSegmentStatisticsMap=" + cubeSegmentStatisticsMap
                    + "]";
        }
    }
}
