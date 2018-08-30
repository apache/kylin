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

package org.apache.kylin.rest.metrics;

import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimedRecordEvent;
import org.apache.kylin.metrics.property.QueryCubePropertyEnum;
import org.apache.kylin.metrics.property.QueryPropertyEnum;
import org.apache.kylin.metrics.property.QueryRPCPropertyEnum;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * The entrance of metrics features.
 */
@ThreadSafe
public class QueryMetricsFacade {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsFacade.class);
    private static final HashFunction hashFunc = Hashing.murmur3_128();

    private static boolean enabled = false;
    private static ConcurrentHashMap<String, QueryMetrics> metricsMap = new ConcurrentHashMap<String, QueryMetrics>();

    public static void init() {
        enabled = KylinConfig.getInstanceFromEnv().getQueryMetricsEnabled();
        if (!enabled)
            return;

        DefaultMetricsSystem.initialize("Kylin");
    }

    public static long getSqlHashCode(String sql) {
        return hashFunc.hashString(sql, Charset.forName("UTF-8")).asLong();
    }

    public static void updateMetrics(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        updateMetricsToLocal(sqlRequest, sqlResponse);
        updateMetricsToReservoir(sqlRequest, sqlResponse);
    }

    private static void updateMetricsToLocal(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (!enabled)
            return;

        String projectName = sqlRequest.getProject();
        String cubeName = sqlResponse.getCube();

        update(getQueryMetrics("Server_Total"), sqlResponse);

        update(getQueryMetrics(projectName), sqlResponse);

        String cubeMetricName = projectName + ",sub=" + cubeName;
        update(getQueryMetrics(cubeMetricName), sqlResponse);
    }

    /**
     * report query related metrics
     */
    private static void updateMetricsToReservoir(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (!KylinConfig.getInstanceFromEnv().isKylinMetricsReporterForQueryEnabled()) {
            return;
        }
        String user = SecurityContextHolder.getContext().getAuthentication().getName();
        if (user == null) {
            user = "unknown";
        }
        for (QueryContext.RPCStatistics entry : QueryContextFacade.current().getRpcStatisticsList()) {
            RecordEvent rpcMetricsEvent = new TimedRecordEvent(
                    KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryRpcCall());
            setRPCWrapper(rpcMetricsEvent, //
                    norm(sqlRequest.getProject()), entry.getRealizationName(), entry.getRpcServer(),
                    entry.getException());
            setRPCStats(rpcMetricsEvent, //
                    entry.getCallTimeMs(), entry.getSkippedRows(), entry.getScannedRows(), entry.getReturnedRows(),
                    entry.getAggregatedRows());
            //For update rpc level related metrics
            MetricsManager.getInstance().update(rpcMetricsEvent);
        }
        long sqlHashCode = getSqlHashCode(sqlRequest.getSql());
        for (QueryContext.CubeSegmentStatisticsResult contextEntry : sqlResponse.getCubeSegmentStatisticsList()) {
            RecordEvent queryMetricsEvent = new TimedRecordEvent(
                    KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQuery());
            setQueryWrapper(queryMetricsEvent, //
                    user, sqlHashCode, sqlResponse.isStorageCacheUsed() ? "CACHE" : contextEntry.getQueryType(),
                    norm(sqlRequest.getProject()), contextEntry.getRealization(), contextEntry.getRealizationType(),
                    sqlResponse.getThrowable());

            long totalStorageReturnCount = 0L;
            for (Map<String, QueryContext.CubeSegmentStatistics> cubeEntry : contextEntry.getCubeSegmentStatisticsMap()
                    .values()) {
                for (QueryContext.CubeSegmentStatistics segmentEntry : cubeEntry.values()) {
                    RecordEvent cubeSegmentMetricsEvent = new TimedRecordEvent(
                            KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryCube());

                    setCubeWrapper(cubeSegmentMetricsEvent, //
                            norm(sqlRequest.getProject()), segmentEntry.getCubeName(), segmentEntry.getSegmentName(),
                            segmentEntry.getSourceCuboidId(), segmentEntry.getTargetCuboidId(),
                            segmentEntry.getFilterMask());

                    setCubeStats(cubeSegmentMetricsEvent, //
                            segmentEntry.getCallCount(), segmentEntry.getCallTimeSum(), segmentEntry.getCallTimeMax(),
                            segmentEntry.getStorageSkippedRows(), segmentEntry.getStorageScannedRows(),
                            segmentEntry.getStorageReturnedRows(), segmentEntry.getStorageAggregatedRows(),
                            segmentEntry.isIfSuccess(), 1.0 / cubeEntry.size());

                    totalStorageReturnCount += segmentEntry.getStorageReturnedRows();
                    //For update cube segment level related query metrics
                    MetricsManager.getInstance().update(cubeSegmentMetricsEvent);
                }
            }
            setQueryStats(queryMetricsEvent, //
                    sqlResponse.getDuration(), sqlResponse.getResults().size(), totalStorageReturnCount);
            //For update query level metrics
            MetricsManager.getInstance().update(queryMetricsEvent);
        }
    }

    private static String norm(String project) {
        return project.toUpperCase(Locale.ROOT);
    }

    private static void setRPCWrapper(RecordEvent metricsEvent, String projectName, String realizationName,
            String rpcServer, Throwable throwable) {
        metricsEvent.put(QueryRPCPropertyEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QueryRPCPropertyEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QueryRPCPropertyEnum.RPC_SERVER.toString(), rpcServer);
        metricsEvent.put(QueryRPCPropertyEnum.EXCEPTION.toString(),
                throwable == null ? "NULL" : throwable.getClass().getName());
    }

    private static void setRPCStats(RecordEvent metricsEvent, long callTimeMs, long skipCount, long scanCount,
            long returnCount, long aggrCount) {
        metricsEvent.put(QueryRPCPropertyEnum.CALL_TIME.toString(), callTimeMs);
        metricsEvent.put(QueryRPCPropertyEnum.SKIP_COUNT.toString(), skipCount); //Number of skips on region servers based on region meta or fuzzy filter
        metricsEvent.put(QueryRPCPropertyEnum.SCAN_COUNT.toString(), scanCount); //Count scanned by region server
        metricsEvent.put(QueryRPCPropertyEnum.RETURN_COUNT.toString(), returnCount);//Count returned by region server
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_FILTER_COUNT.toString(), scanCount - returnCount); //Count filtered & aggregated by coprocessor
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_COUNT.toString(), aggrCount); //Count aggregated by coprocessor
    }

    private static void setCubeWrapper(RecordEvent metricsEvent, String projectName, String cubeName,
            String segmentName, long sourceCuboidId, long targetCuboidId, long filterMask) {
        metricsEvent.put(QueryCubePropertyEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QueryCubePropertyEnum.CUBE.toString(), cubeName);
        metricsEvent.put(QueryCubePropertyEnum.SEGMENT.toString(), segmentName);
        metricsEvent.put(QueryCubePropertyEnum.CUBOID_SOURCE.toString(), sourceCuboidId);
        metricsEvent.put(QueryCubePropertyEnum.CUBOID_TARGET.toString(), targetCuboidId);
        metricsEvent.put(QueryCubePropertyEnum.IF_MATCH.toString(), sourceCuboidId == targetCuboidId);
        metricsEvent.put(QueryCubePropertyEnum.FILTER_MASK.toString(), filterMask);
    }

    private static void setCubeStats(RecordEvent metricsEvent, long callCount, long callTimeSum, long callTimeMax,
            long skipCount, long scanCount, long returnCount, long aggrCount, boolean ifSuccess, double weightPerHit) {
        metricsEvent.put(QueryCubePropertyEnum.CALL_COUNT.toString(), callCount);
        metricsEvent.put(QueryCubePropertyEnum.TIME_SUM.toString(), callTimeSum);
        metricsEvent.put(QueryCubePropertyEnum.TIME_MAX.toString(), callTimeMax);
        metricsEvent.put(QueryCubePropertyEnum.SKIP_COUNT.toString(), skipCount);
        metricsEvent.put(QueryCubePropertyEnum.SCAN_COUNT.toString(), scanCount);
        metricsEvent.put(QueryCubePropertyEnum.RETURN_COUNT.toString(), returnCount);
        metricsEvent.put(QueryCubePropertyEnum.AGGR_FILTER_COUNT.toString(), scanCount - returnCount);
        metricsEvent.put(QueryCubePropertyEnum.AGGR_COUNT.toString(), aggrCount);
        metricsEvent.put(QueryCubePropertyEnum.IF_SUCCESS.toString(), ifSuccess);
        metricsEvent.put(QueryCubePropertyEnum.WEIGHT_PER_HIT.toString(), weightPerHit);
    }

    private static void setQueryWrapper(RecordEvent metricsEvent, String user, long queryHashCode, String queryType,
            String projectName, String realizationName, int realizationType, Throwable throwable) {
        metricsEvent.put(QueryPropertyEnum.USER.toString(), user);
        metricsEvent.put(QueryPropertyEnum.ID_CODE.toString(), queryHashCode);
        metricsEvent.put(QueryPropertyEnum.TYPE.toString(), queryType);
        metricsEvent.put(QueryPropertyEnum.PROJECT.toString(), projectName);
        metricsEvent.put(QueryPropertyEnum.REALIZATION.toString(), realizationName);
        metricsEvent.put(QueryPropertyEnum.REALIZATION_TYPE.toString(), realizationType);
        metricsEvent.put(QueryPropertyEnum.EXCEPTION.toString(),
                throwable == null ? "NULL" : throwable.getClass().getName());
    }

    private static void setQueryStats(RecordEvent metricsEvent, long callTimeMs, long returnCountByCalcite,
            long returnCountByStorage) {
        metricsEvent.put(QueryPropertyEnum.TIME_COST.toString(), callTimeMs);
        metricsEvent.put(QueryPropertyEnum.CALCITE_RETURN_COUNT.toString(), returnCountByCalcite);
        metricsEvent.put(QueryPropertyEnum.STORAGE_RETURN_COUNT.toString(), returnCountByStorage);
        long countAggrAndFilter = returnCountByStorage - returnCountByCalcite;
        if (countAggrAndFilter < 0) {
            countAggrAndFilter = 0;
            logger.warn(returnCountByStorage + " rows returned by storage less than " + returnCountByCalcite
                    + " rows returned by calcite");
        }
        metricsEvent.put(QueryPropertyEnum.AGGR_FILTER_COUNT.toString(), countAggrAndFilter);
    }

    private static void update(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        try {
            incrQueryCount(queryMetrics, sqlResponse);
            incrCacheHitCount(queryMetrics, sqlResponse);

            if (!sqlResponse.getIsException()) {
                queryMetrics.addQueryLatency(sqlResponse.getDuration());
                queryMetrics.addScanRowCount(sqlResponse.getTotalScanCount());
                queryMetrics.addResultRowCount(sqlResponse.getResults().size());
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    private static void incrQueryCount(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        if (!sqlResponse.isHitExceptionCache() && !sqlResponse.getIsException()) {
            queryMetrics.incrQuerySuccessCount();
        } else {
            queryMetrics.incrQueryFailCount();
        }
        queryMetrics.incrQueryCount();
    }

    private static void incrCacheHitCount(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        if (sqlResponse.isStorageCacheUsed()) {
            queryMetrics.addCacheHitCount(1);
        }
    }

    private static QueryMetrics getQueryMetrics(String name) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int[] intervals = config.getQueryMetricsPercentilesIntervals();

        QueryMetrics queryMetrics = metricsMap.get(name);
        if (queryMetrics != null) {
            return queryMetrics;
        }

        synchronized (QueryMetricsFacade.class) {
            queryMetrics = metricsMap.get(name);
            if (queryMetrics != null) {
                return queryMetrics;
            }

            try {
                queryMetrics = new QueryMetrics(intervals).registerWith(name);
                metricsMap.put(name, queryMetrics);
                return queryMetrics;
            } catch (MetricsException e) {
                logger.warn(name + " register error: ", e);
            }
        }
        return queryMetrics;
    }
}
