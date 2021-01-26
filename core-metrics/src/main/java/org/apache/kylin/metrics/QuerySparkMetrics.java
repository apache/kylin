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

package org.apache.kylin.metrics;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimedRecordEvent;
import org.apache.kylin.metrics.property.QuerySparkExecutionEnum;
import org.apache.kylin.metrics.property.QuerySparkJobEnum;
import org.apache.kylin.metrics.property.QuerySparkStageEnum;
import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.cache.Cache;
import org.apache.kylin.shaded.com.google.common.cache.CacheBuilder;
import org.apache.kylin.shaded.com.google.common.cache.RemovalListener;
import org.apache.kylin.shaded.com.google.common.cache.RemovalNotification;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class QuerySparkMetrics {
    private static final Logger logger = LoggerFactory.getLogger(QuerySparkMetrics.class);
    private static ScheduledExecutorService scheduledExecutor = null;
    private static QuerySparkMetrics instance =
            new QuerySparkMetrics(new QuerySparkMetricsRemovalListener());
    private static final int sparkMetricsNum = 10;
    private org.apache.kylin.shaded.com.google.common.cache.Cache<String, QueryExecutionMetrics> queryExecutionMetricsMap;

    // default removal listener
    private static class QuerySparkMetricsRemovalListener implements RemovalListener<String,
            QueryExecutionMetrics> {
        @Override
        public void onRemoval(RemovalNotification<String, QueryExecutionMetrics> notification) {
            try {
                updateMetricsToReservoir(notification.getKey(), notification.getValue());
                logger.info("Query metrics {} is removed due to {}, update to metrics reservoir successful",
                        notification.getKey(), notification.getCause());
            } catch (Exception e) {
                logger.warn("Query metrics {} is removed due to {}, update to metrics reservoir failed",
                        notification.getKey(), notification.getCause());
            }
        }
    }

    private QuerySparkMetrics(RemovalListener removalListener) {
        if (queryExecutionMetricsMap != null) {
            queryExecutionMetricsMap.cleanUp();
            queryExecutionMetricsMap = null;
        }
        queryExecutionMetricsMap = CacheBuilder.newBuilder()
                .maximumSize(KylinConfig.getInstanceFromEnv().getKylinMetricsCacheMaxEntries())
                .expireAfterWrite(KylinConfig.getInstanceFromEnv().getKylinMetricsCacheExpireSeconds(),
                        TimeUnit.SECONDS)
                .removalListener(removalListener).build();

        if (scheduledExecutor != null && !scheduledExecutor.isShutdown()) {
            scheduledExecutor.shutdown();
            scheduledExecutor = null;
        }
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
                                                     @Override
                                                     public void run() {
                                                         queryExecutionMetricsMap.cleanUp();
                                                     }
                                                 },
                KylinConfig.getInstanceFromEnv().getKylinMetricsCacheExpireSeconds(),
                KylinConfig.getInstanceFromEnv().getKylinMetricsCacheExpireSeconds(), TimeUnit.SECONDS);
    }

    private void shutdown() {
        queryExecutionMetricsMap.invalidateAll();
    }

    // only for test case
    @VisibleForTesting
    public static void init(RemovalListener removalListener) {
        instance = new QuerySparkMetrics(removalListener);
    }

    public static QuerySparkMetrics getInstance() {
        return instance;
    }

    public void onJobStart(String queryId, String sparderName, long executionId, long executionStartTime, int jobId,
            long jobStartTime) {
        QueryExecutionMetrics queryExecutionMetrics = queryExecutionMetricsMap.getIfPresent(queryId);
        if (queryExecutionMetrics == null) {
            queryExecutionMetrics = new QueryExecutionMetrics();
            ConcurrentMap<Integer, SparkJobMetrics> sparkJobMetricsMap = Maps.newConcurrentMap();
            queryExecutionMetrics.setQueryId(queryId);
            queryExecutionMetrics.setSparderName(sparderName);
            queryExecutionMetrics.setExecutionId(executionId);
            queryExecutionMetrics.setStartTime(executionStartTime);
            queryExecutionMetrics.setSparkJobMetricsMap(sparkJobMetricsMap);
            queryExecutionMetricsMap.put(queryId, queryExecutionMetrics);
        }
        SparkJobMetrics sparkJobMetrics = new SparkJobMetrics();
        sparkJobMetrics.setExecutionId(executionId);
        sparkJobMetrics.setJobId(jobId);
        sparkJobMetrics.setStartTime(jobStartTime);

        ConcurrentMap<Integer, SparkStageMetrics> sparkStageMetricsMap = Maps.newConcurrentMap();
        sparkJobMetrics.setSparkStageMetricsMap(sparkStageMetricsMap);

        queryExecutionMetrics.getSparkJobMetricsMap().put(jobId, sparkJobMetrics);
    }

    public void onSparkStageStart(String queryId, int jobId, int stageId, String stageType, long submitTime) {
        QueryExecutionMetrics queryExecutionMetrics = queryExecutionMetricsMap.getIfPresent(queryId);
        if (queryExecutionMetrics != null && queryExecutionMetrics.getSparkJobMetricsMap().get(jobId) != null) {
            SparkStageMetrics sparkStageMetrics = new SparkStageMetrics();
            sparkStageMetrics.setStageId(stageId);
            sparkStageMetrics.setStageType(stageType);
            sparkStageMetrics.setSubmitTime(submitTime);
            queryExecutionMetrics.getSparkJobMetricsMap().get(jobId).getSparkStageMetricsMap().put(stageId,
                    sparkStageMetrics);
        }
    }

    public void updateSparkStageMetrics(String queryId, int jobId, int stageId, boolean isSuccess,
            SparkStageMetrics sparkStageMetricsEnd) {
        QueryExecutionMetrics queryExecutionMetrics = queryExecutionMetricsMap.getIfPresent(queryId);
        if (queryExecutionMetrics != null) {
            SparkJobMetrics sparkJobMetrics = queryExecutionMetrics.getSparkJobMetricsMap().get(jobId);
            if (sparkJobMetrics != null) {
                SparkStageMetrics sparkStageMetrics = sparkJobMetrics.getSparkStageMetricsMap().get(stageId);
                if (sparkStageMetrics != null) {
                    sparkStageMetrics.setSuccess(isSuccess);
                    sparkStageMetrics.setMetrics(sparkStageMetricsEnd.getResultSize(),
                            sparkStageMetricsEnd.getExecutorDeserializeTime(),
                            sparkStageMetricsEnd.getExecutorDeserializeCpuTime(),
                            sparkStageMetricsEnd.getExecutorRunTime(), sparkStageMetricsEnd.getExecutorCpuTime(),
                            sparkStageMetricsEnd.getJvmGCTime(), sparkStageMetricsEnd.getResultSerializationTime(),
                            sparkStageMetricsEnd.getMemoryBytesSpilled(), sparkStageMetricsEnd.getDiskBytesSpilled(),
                            sparkStageMetricsEnd.getPeakExecutionMemory());
                }
            }
        }
    }

    public void updateSparkJobMetrics(String queryId, int jobId, long jobEndTime, boolean isSuccess) {
        QueryExecutionMetrics queryExecutionMetrics = queryExecutionMetricsMap.getIfPresent(queryId);
        if (queryExecutionMetrics != null && queryExecutionMetrics.getSparkJobMetricsMap().get(jobId) != null) {
            SparkJobMetrics sparkJobMetrics = queryExecutionMetrics.getSparkJobMetricsMap().get(jobId);
            sparkJobMetrics.setEndTime(jobEndTime);
            sparkJobMetrics.setSuccess(isSuccess);
        }
    }

    public void updateExecutionMetrics(String queryId, long executionEndTime) {
        QueryExecutionMetrics queryExecutionMetrics = queryExecutionMetricsMap.getIfPresent(queryId);
        if (queryExecutionMetrics != null) {
            queryExecutionMetrics.setEndTime(executionEndTime);
        }
    }

    public Cache<String, QueryExecutionMetrics> getQueryExecutionMetricsMap() {
        return queryExecutionMetricsMap;
    }

    public QueryExecutionMetrics getQueryExecutionMetrics(String queryId) {
        return queryExecutionMetricsMap.getIfPresent(queryId);
    }

    /**
     * report query related metrics
     */
    public static void updateMetricsToReservoir(String queryId,
                                                QueryExecutionMetrics queryExecutionMetrics) {
        if (!KylinConfig.getInstanceFromEnv().isKylinMetricsReporterForQueryEnabled()) {
            return;
        }
        if (queryExecutionMetrics != null) {
            RecordEvent queryExecutionMetricsEvent = new TimedRecordEvent(
                    KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryExecution());

            setQueryWrapper(queryExecutionMetricsEvent, queryExecutionMetrics.getUser(),
                    queryExecutionMetrics.getSqlIdCode(), queryExecutionMetrics.getQueryType(),
                    queryId, queryExecutionMetrics.getProject(),
                    queryExecutionMetrics.getException());

            setSparkExecutionWrapper(queryExecutionMetricsEvent, queryExecutionMetrics.getSparderName(),
                    queryExecutionMetrics.getExecutionId(), queryExecutionMetrics.getRealization(),
                    queryExecutionMetrics.getRealizationTypes(),
                    queryExecutionMetrics.getCuboidIds(),
                    queryExecutionMetrics.getStartTime(), queryExecutionMetrics.getEndTime());

            setQueryMetrics(queryExecutionMetricsEvent, queryExecutionMetrics.getSqlDuration(),
                    queryExecutionMetrics.getTotalScanCount(), queryExecutionMetrics.getTotalScanBytes(),
                    queryExecutionMetrics.getResultCount());

            long[] queryExecutionMetricsList = new long[sparkMetricsNum];
            for (Map.Entry<Integer, QuerySparkMetrics.SparkJobMetrics> sparkJobMetricsEntry : queryExecutionMetrics
                    .getSparkJobMetricsMap().entrySet()) {
                RecordEvent sparkJobMetricsEvent = new TimedRecordEvent(
                        KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQuerySparkJob());

                setSparkJobWrapper(sparkJobMetricsEvent, queryExecutionMetrics.getProject(),
                        queryId, queryExecutionMetrics.getExecutionId(),
                        sparkJobMetricsEntry.getValue().getJobId(), sparkJobMetricsEntry.getValue().getStartTime(),
                        sparkJobMetricsEntry.getValue().getEndTime(), sparkJobMetricsEntry.getValue().isSuccess());

                long[] sparkJobMetricsList = new long[sparkMetricsNum];
                for (Map.Entry<Integer, QuerySparkMetrics.SparkStageMetrics> sparkStageMetricsEntry : sparkJobMetricsEntry
                        .getValue().getSparkStageMetricsMap().entrySet()) {
                    RecordEvent sparkStageMetricsEvent = new TimedRecordEvent(
                            KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQuerySparkStage());
                    QuerySparkMetrics.SparkStageMetrics sparkStageMetrics = sparkStageMetricsEntry.getValue();
                    setStageWrapper(sparkStageMetricsEvent, queryExecutionMetrics.getProject(), null,
                            queryId, queryExecutionMetrics.getExecutionId(),
                            sparkJobMetricsEntry.getValue().getJobId(), sparkStageMetrics.getStageId(),
                            sparkStageMetrics.getSubmitTime(), sparkStageMetrics.isSuccess());
                    setStageMetrics(sparkStageMetricsEvent, sparkStageMetrics.getResultSize(),
                            sparkStageMetrics.getExecutorDeserializeTime(),
                            sparkStageMetrics.getExecutorDeserializeCpuTime(), sparkStageMetrics.getExecutorRunTime(),
                            sparkStageMetrics.getExecutorCpuTime(), sparkStageMetrics.getJvmGCTime(),
                            sparkStageMetrics.getResultSerializationTime(), sparkStageMetrics.getMemoryBytesSpilled(),
                            sparkStageMetrics.getDiskBytesSpilled(), sparkStageMetrics.getPeakExecutionMemory());
                    //Update spark stage level metrics
                    MetricsManager.getInstance().update(sparkStageMetricsEvent);

                    sparkJobMetricsList[0] += sparkStageMetrics.getResultSize();
                    sparkJobMetricsList[1] += sparkStageMetrics.getExecutorDeserializeTime();
                    sparkJobMetricsList[2] += sparkStageMetrics.getExecutorDeserializeCpuTime();
                    sparkJobMetricsList[3] += sparkStageMetrics.getExecutorRunTime();
                    sparkJobMetricsList[4] += sparkStageMetrics.getExecutorCpuTime();
                    sparkJobMetricsList[5] += sparkStageMetrics.getJvmGCTime();
                    sparkJobMetricsList[6] += sparkStageMetrics.getResultSerializationTime();
                    sparkJobMetricsList[7] += sparkStageMetrics.getMemoryBytesSpilled();
                    sparkJobMetricsList[8] += sparkStageMetrics.getDiskBytesSpilled();
                    sparkJobMetricsList[9] += sparkStageMetrics.getPeakExecutionMemory();
                }
                setSparkJobMetrics(sparkJobMetricsEvent, sparkJobMetricsList[0], sparkJobMetricsList[1],
                        sparkJobMetricsList[2], sparkJobMetricsList[3], sparkJobMetricsList[4], sparkJobMetricsList[5],
                        sparkJobMetricsList[6], sparkJobMetricsList[7], sparkJobMetricsList[8], sparkJobMetricsList[9]);
                //Update spark job level metrics
                MetricsManager.getInstance().update(sparkJobMetricsEvent);

                for (int i = 0; i < sparkMetricsNum; i++) {
                    queryExecutionMetricsList[i] += sparkJobMetricsList[i];
                }
            }
            setSparkExecutionMetrics(queryExecutionMetricsEvent,
                    queryExecutionMetrics.getEndTime() - queryExecutionMetrics.getStartTime(),
                    queryExecutionMetricsList[0], queryExecutionMetricsList[1], queryExecutionMetricsList[2],
                    queryExecutionMetricsList[3], queryExecutionMetricsList[4], queryExecutionMetricsList[5],
                    queryExecutionMetricsList[6], queryExecutionMetricsList[7], queryExecutionMetricsList[8],
                    queryExecutionMetricsList[9]);
            //Update execution level metrics
            MetricsManager.getInstance().update(queryExecutionMetricsEvent);
        }
    }

    private static void setQueryWrapper(RecordEvent metricsEvent, String user, long sqlIdCode, String queryType,
            String queryId, String project, String exception) {
        metricsEvent.put(QuerySparkExecutionEnum.USER.toString(), user);
        metricsEvent.put(QuerySparkExecutionEnum.ID_CODE.toString(), sqlIdCode);
        metricsEvent.put(QuerySparkExecutionEnum.TYPE.toString(), queryType);
        metricsEvent.put(QuerySparkExecutionEnum.QUERY_ID.toString(), queryId);
        metricsEvent.put(QuerySparkExecutionEnum.PROJECT.toString(), project);
        metricsEvent.put(QuerySparkExecutionEnum.EXCEPTION.toString(), exception);
    }

    private static void setSparkExecutionWrapper(RecordEvent metricsEvent, String sparderName,
                                                 long executionId, String realizationName,
                                                 String realizationType, String cuboidIds,
                                                 long startTime, long endTime) {
        metricsEvent.put(QuerySparkExecutionEnum.SPARDER_NAME.toString(), sparderName);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTION_ID.toString(), executionId);
        metricsEvent.put(QuerySparkExecutionEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QuerySparkExecutionEnum.REALIZATION_TYPE.toString(), realizationType);
        metricsEvent.put(QuerySparkExecutionEnum.CUBOID_IDS.toString(), cuboidIds);
        metricsEvent.put(QuerySparkExecutionEnum.START_TIME.toString(), startTime);
        metricsEvent.put(QuerySparkExecutionEnum.END_TIME.toString(), endTime);
    }

    private static void setQueryMetrics(RecordEvent metricsEvent, long sqlDuration, long totalScanCount,
            long totalScanBytes, long resultCount) {
        metricsEvent.put(QuerySparkExecutionEnum.TIME_COST.toString(), sqlDuration);
        metricsEvent.put(QuerySparkExecutionEnum.TOTAL_SCAN_COUNT.toString(), totalScanCount);
        metricsEvent.put(QuerySparkExecutionEnum.TOTAL_SCAN_BYTES.toString(), totalScanBytes);
        metricsEvent.put(QuerySparkExecutionEnum.RESULT_COUNT.toString(), resultCount);
    }

    private static void setSparkExecutionMetrics(RecordEvent metricsEvent, long executionDuration, long resultSize,
            long executorDeserializeTime, long executorDeserializeCpuTime, long executorRunTime, long executorCpuTime,
            long jvmGCTime, long resultSerializationTime, long memoryBytesSpilled, long diskBytesSpilled,
            long peakExecutionMemory) {
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTION_DURATION.toString(), executionDuration);

        metricsEvent.put(QuerySparkExecutionEnum.RESULT_SIZE.toString(), resultSize);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTOR_DESERIALIZE_TIME.toString(), executorDeserializeTime);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), executorDeserializeCpuTime);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTOR_RUN_TIME.toString(), executorRunTime);
        metricsEvent.put(QuerySparkExecutionEnum.EXECUTOR_CPU_TIME.toString(), executorCpuTime);
        metricsEvent.put(QuerySparkExecutionEnum.JVM_GC_TIME.toString(), jvmGCTime);
        metricsEvent.put(QuerySparkExecutionEnum.RESULT_SERIALIZATION_TIME.toString(), resultSerializationTime);
        metricsEvent.put(QuerySparkExecutionEnum.MEMORY_BYTE_SPILLED.toString(), memoryBytesSpilled);
        metricsEvent.put(QuerySparkExecutionEnum.DISK_BYTES_SPILLED.toString(), diskBytesSpilled);
        metricsEvent.put(QuerySparkExecutionEnum.PEAK_EXECUTION_MEMORY.toString(), peakExecutionMemory);
    }

    private static void setSparkJobMetrics(RecordEvent metricsEvent, long resultSize, long executorDeserializeTime,
            long executorDeserializeCpuTime, long executorRunTime, long executorCpuTime, long jvmGCTime,
            long resultSerializationTime, long memoryBytesSpilled, long diskBytesSpilled, long peakExecutionMemory) {
        metricsEvent.put(QuerySparkJobEnum.RESULT_SIZE.toString(), resultSize);
        metricsEvent.put(QuerySparkJobEnum.EXECUTOR_DESERIALIZE_TIME.toString(), executorDeserializeTime);
        metricsEvent.put(QuerySparkJobEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), executorDeserializeCpuTime);
        metricsEvent.put(QuerySparkJobEnum.EXECUTOR_RUN_TIME.toString(), executorRunTime);
        metricsEvent.put(QuerySparkJobEnum.EXECUTOR_CPU_TIME.toString(), executorCpuTime);
        metricsEvent.put(QuerySparkJobEnum.JVM_GC_TIME.toString(), jvmGCTime);
        metricsEvent.put(QuerySparkJobEnum.RESULT_SERIALIZATION_TIME.toString(), resultSerializationTime);
        metricsEvent.put(QuerySparkJobEnum.MEMORY_BYTE_SPILLED.toString(), memoryBytesSpilled);
        metricsEvent.put(QuerySparkJobEnum.DISK_BYTES_SPILLED.toString(), diskBytesSpilled);
        metricsEvent.put(QuerySparkJobEnum.PEAK_EXECUTION_MEMORY.toString(), peakExecutionMemory);
    }

    private static void setStageMetrics(RecordEvent metricsEvent, long resultSize, long executorDeserializeTime,
            long executorDeserializeCpuTime, long executorRunTime, long executorCpuTime, long jvmGCTime,
            long resultSerializationTime, long memoryBytesSpilled, long diskBytesSpilled, long peakExecutionMemory) {
        metricsEvent.put(QuerySparkStageEnum.RESULT_SIZE.toString(), resultSize);
        metricsEvent.put(QuerySparkStageEnum.EXECUTOR_DESERIALIZE_TIME.toString(), executorDeserializeTime);
        metricsEvent.put(QuerySparkStageEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), executorDeserializeCpuTime);
        metricsEvent.put(QuerySparkStageEnum.EXECUTOR_RUN_TIME.toString(), executorRunTime);
        metricsEvent.put(QuerySparkStageEnum.EXECUTOR_CPU_TIME.toString(), executorCpuTime);
        metricsEvent.put(QuerySparkStageEnum.JVM_GC_TIME.toString(), jvmGCTime);
        metricsEvent.put(QuerySparkStageEnum.RESULT_SERIALIZATION_TIME.toString(), resultSerializationTime);
        metricsEvent.put(QuerySparkStageEnum.MEMORY_BYTE_SPILLED.toString(), memoryBytesSpilled);
        metricsEvent.put(QuerySparkStageEnum.DISK_BYTES_SPILLED.toString(), diskBytesSpilled);
        metricsEvent.put(QuerySparkStageEnum.PEAK_EXECUTION_MEMORY.toString(), peakExecutionMemory);
    }

    private static void setStageWrapper(RecordEvent metricsEvent, String projectName, String realizationName,
            String queryId, long executionId, int jobId, int stageId, long submitTime, boolean isSuccess) {
        metricsEvent.put(QuerySparkStageEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QuerySparkStageEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QuerySparkStageEnum.QUERY_ID.toString(), queryId);
        metricsEvent.put(QuerySparkStageEnum.EXECUTION_ID.toString(), executionId);
        metricsEvent.put(QuerySparkStageEnum.JOB_ID.toString(), jobId);
        metricsEvent.put(QuerySparkStageEnum.STAGE_ID.toString(), stageId);
        metricsEvent.put(QuerySparkStageEnum.SUBMIT_TIME.toString(), submitTime);
        metricsEvent.put(QuerySparkStageEnum.IF_SUCCESS.toString(), isSuccess);
    }

    private static void setSparkJobWrapper(RecordEvent metricsEvent, String projectName, String queryId,
            long executionId, int jobId, long startTime, long endTime, boolean isSuccess) {
        metricsEvent.put(QuerySparkJobEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QuerySparkJobEnum.QUERY_ID.toString(), queryId);
        metricsEvent.put(QuerySparkJobEnum.EXECUTION_ID.toString(), executionId);
        metricsEvent.put(QuerySparkJobEnum.JOB_ID.toString(), jobId);
        metricsEvent.put(QuerySparkJobEnum.START_TIME.toString(), startTime);
        metricsEvent.put(QuerySparkJobEnum.END_TIME.toString(), endTime);
        metricsEvent.put(QuerySparkJobEnum.IF_SUCCESS.toString(), isSuccess);
    }

    public static class QueryExecutionMetrics implements Serializable {
        private long sqlIdCode;
        private String user;
        private String queryType;
        private String project;
        private String exception;
        private long executionId;
        private String sparderName;
        private long executionDuration;
        private String queryId;
        private String realization;
        private String realizationTypes;
        private String cuboidIds;
        private long startTime;
        private long endTime;
        private ConcurrentMap<Integer, SparkJobMetrics> sparkJobMetricsMap;

        private long sqlDuration;
        private long totalScanCount;
        private long totalScanBytes;
        private int resultCount;

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public int getResultCount() {
            return resultCount;
        }

        public long getSqlDuration() {
            return sqlDuration;
        }

        public long getTotalScanBytes() {
            return totalScanBytes;
        }

        public long getTotalScanCount() {
            return totalScanCount;
        }

        public void setResultCount(int resultCount) {
            this.resultCount = resultCount;
        }

        public void setSqlDuration(long sqlDuration) {
            this.sqlDuration = sqlDuration;
        }

        public void setTotalScanBytes(long totalScanBytes) {
            this.totalScanBytes = totalScanBytes;
        }

        public void setTotalScanCount(long totalScanCount) {
            this.totalScanCount = totalScanCount;
        }

        public String getException() {
            return exception;
        }

        public void setException(String exception) {
            this.exception = exception;
        }

        public void setProject(String project) {
            this.project = project;
        }

        public String getProject() {
            return project;
        }

        public String getQueryType() {
            return queryType;
        }

        public long getSqlIdCode() {
            return sqlIdCode;
        }

        public void setQueryType(String queryType) {
            this.queryType = queryType;
        }

        public void setSqlIdCode(long sqlIdCode) {
            this.sqlIdCode = sqlIdCode;
        }

        public long getEndTime() {
            return endTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }

        public String getQueryId() {
            return queryId;
        }

        public long getExecutionDuration() {
            return executionDuration;
        }

        public void setExecutionDuration(long executionDuration) {
            this.executionDuration = executionDuration;
        }

        public ConcurrentMap<Integer, SparkJobMetrics> getSparkJobMetricsMap() {
            return sparkJobMetricsMap;
        }

        public long getExecutionId() {
            return executionId;
        }

        public String getSparderName() {
            return sparderName;
        }

        public void setExecutionId(long executionId) {
            this.executionId = executionId;
        }

        public void setSparderName(String sparderName) {
            this.sparderName = sparderName;
        }

        public String getCuboidIds() {
            return cuboidIds;
        }

        public void setCuboidIds(String cuboidIds) {
            this.cuboidIds = cuboidIds;
        }

        public String getRealization() {
            return realization;
        }

        public String getRealizationTypes() {
            return realizationTypes;
        }

        public void setRealization(String realization) {
            this.realization = realization;
        }

        public void setRealizationTypes(String realizationTypes) {
            this.realizationTypes = realizationTypes;
        }

        public void setSparkJobMetricsMap(ConcurrentMap<Integer, SparkJobMetrics> sparkJobMetricsMap) {
            this.sparkJobMetricsMap = sparkJobMetricsMap;
        }
    }

    public static class SparkJobMetrics implements Serializable {
        private long executionId;
        private int jobId;
        private long startTime;
        private long endTime;
        private boolean isSuccess;
        private ConcurrentMap<Integer, SparkStageMetrics> sparkStageMetricsMap;

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setExecutionId(long executionId) {
            this.executionId = executionId;
        }

        public long getExecutionId() {
            return executionId;
        }

        public void setSparkStageMetricsMap(ConcurrentMap<Integer, SparkStageMetrics> sparkStageMetricsMap) {
            this.sparkStageMetricsMap = sparkStageMetricsMap;
        }

        public void setJobId(int jobId) {
            this.jobId = jobId;
        }

        public void setSuccess(boolean success) {
            isSuccess = success;
        }

        public boolean isSuccess() {
            return isSuccess;
        }

        public ConcurrentMap<Integer, SparkStageMetrics> getSparkStageMetricsMap() {
            return sparkStageMetricsMap;
        }

        public int getJobId() {
            return jobId;
        }
    }

    public static class SparkStageMetrics implements Serializable {
        private int stageId;
        private String stageType;
        private long submitTime;
        private long endTime;
        private boolean isSuccess;
        private long resultSize;
        private long executorDeserializeTime;
        private long executorDeserializeCpuTime;
        private long executorRunTime;
        private long executorCpuTime;
        private long jvmGCTime;
        private long resultSerializationTime;
        private long memoryBytesSpilled;
        private long diskBytesSpilled;
        private long peakExecutionMemory;

        public void setMetrics(long resultSize, long executorDeserializeTime, long executorDeserializeCpuTime,
                long executorRunTime, long executorCpuTime, long jvmGCTime, long resultSerializationTime,
                long memoryBytesSpilled, long diskBytesSpilled, long peakExecutionMemory) {
            this.resultSize = resultSize;
            this.executorDeserializeTime = executorDeserializeTime;
            this.executorDeserializeCpuTime = executorDeserializeCpuTime;
            this.executorRunTime = executorRunTime;
            this.executorCpuTime = executorCpuTime;
            this.jvmGCTime = jvmGCTime;
            this.resultSerializationTime = resultSerializationTime;
            this.memoryBytesSpilled = memoryBytesSpilled;
            this.diskBytesSpilled = diskBytesSpilled;
            this.peakExecutionMemory = peakExecutionMemory;
        }

        public long getEndTime() {
            return endTime;
        }

        public long getSubmitTime() {
            return submitTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public void setSubmitTime(long submitTime) {
            this.submitTime = submitTime;
        }

        public boolean isSuccess() {
            return isSuccess;
        }

        public void setSuccess(boolean success) {
            isSuccess = success;
        }

        public void setStageType(String stageType) {
            this.stageType = stageType;
        }

        public void setStageId(int stageId) {
            this.stageId = stageId;
        }

        public void setResultSize(long resultSize) {
            this.resultSize = resultSize;
        }

        public void setResultSerializationTime(long resultSerializationTime) {
            this.resultSerializationTime = resultSerializationTime;
        }

        public void setPeakExecutionMemory(long peakExecutionMemory) {
            this.peakExecutionMemory = peakExecutionMemory;
        }

        public void setMemoryBytesSpilled(long memoryBytesSpilled) {
            this.memoryBytesSpilled = memoryBytesSpilled;
        }

        public void setJvmGCTime(long jvmGCTime) {
            this.jvmGCTime = jvmGCTime;
        }

        public void setExecutorRunTime(long executorRunTime) {
            this.executorRunTime = executorRunTime;
        }

        public void setExecutorDeserializeTime(long executorDeserializeTime) {
            this.executorDeserializeTime = executorDeserializeTime;
        }

        public void setExecutorDeserializeCpuTime(long executorDeserializeCpuTime) {
            this.executorDeserializeCpuTime = executorDeserializeCpuTime;
        }

        public void setExecutorCpuTime(long executorCpuTime) {
            this.executorCpuTime = executorCpuTime;
        }

        public void setDiskBytesSpilled(long diskBytesSpilled) {
            this.diskBytesSpilled = diskBytesSpilled;
        }

        public String getStageType() {
            return stageType;
        }

        public long getResultSize() {
            return resultSize;
        }

        public long getResultSerializationTime() {
            return resultSerializationTime;
        }

        public long getPeakExecutionMemory() {
            return peakExecutionMemory;
        }

        public long getMemoryBytesSpilled() {
            return memoryBytesSpilled;
        }

        public long getJvmGCTime() {
            return jvmGCTime;
        }

        public long getExecutorRunTime() {
            return executorRunTime;
        }

        public long getExecutorDeserializeTime() {
            return executorDeserializeTime;
        }

        public long getExecutorDeserializeCpuTime() {
            return executorDeserializeCpuTime;
        }

        public long getExecutorCpuTime() {
            return executorCpuTime;
        }

        public long getDiskBytesSpilled() {
            return diskBytesSpilled;
        }

        public int getStageId() {
            return stageId;
        }
    }
}
