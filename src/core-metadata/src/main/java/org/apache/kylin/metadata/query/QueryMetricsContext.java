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

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryMetricsContext extends QueryMetrics {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsContext.class);

    private static final ThreadLocal<QueryMetricsContext> contexts = new ThreadLocal<>();

    private QueryMetricsContext(String queryId, String defaultServer) {
        super(queryId, defaultServer);
    }

    public static void start(final String queryId, final String defaultServer) {
        if (isStarted()) {
            logger.warn("Query metric context already started in thread named {}", Thread.currentThread().getName());
            return;
        }
        contexts.set(new QueryMetricsContext(queryId, defaultServer));
    }

    public static boolean isStarted() {
        return contexts.get() != null;
    }

    public static QueryMetricsContext collect(final QueryContext context) {
        final QueryMetricsContext current = obtainCurrentQueryMetrics();

        current.doCollect(context);

        return current;
    }

    public static void reset() {
        contexts.remove();
    }

    private static QueryMetricsContext obtainCurrentQueryMetrics() {
        QueryMetricsContext current = null;
        Preconditions.checkState((current = contexts.get()) != null, "Query metric context is not started.");
        return current;
    }

    private void doCollect(final QueryContext context) {
        // set sql
        this.sql = context.getMetrics().getCorrectedSql();
        this.sqlPattern = context.getMetrics().getSqlPattern();
        this.queryTime = context.getMetrics().getQueryStartTime();

        // for query stats
        TimeZone timeZone = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone());
        LocalDate date = Instant.ofEpochMilli(this.queryTime).atZone(timeZone.toZoneId()).toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM", Locale.getDefault(Locale.Category.FORMAT));
        this.month = date.withDayOfMonth(1).format(formatter);
        this.queryFirstDayOfMonth = TimeUtil.getMonthStart(this.queryTime);
        this.queryDay = TimeUtil.getDayStart(this.queryTime);
        this.queryFirstDayOfWeek = TimeUtil.getWeekStart(this.queryTime);

        this.submitter = context.getAclInfo().getUsername();

        this.server = context.getMetrics().getServer();

        if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
            QueryContext.currentTrace().endLastSpan();
            this.queryDuration = System.currentTimeMillis() - queryTime;
        } else if (QueryContext.current().getQueryTagInfo().isStorageCacheUsed()) {
            this.queryDuration = 0;
        } else {
            this.queryDuration = QueryContext.currentMetrics().duration();
        }
        this.totalScanBytes = context.getMetrics().getTotalScanBytes();
        this.totalScanCount = context.getMetrics().getTotalScanRows();
        this.queryJobCount = context.getMetrics().getQueryJobCount();
        this.queryStageCount = context.getMetrics().getQueryStageCount();
        this.queryTaskCount = context.getMetrics().getQueryTaskCount();
        this.cpuTime = context.getMetrics().getCpuTime();
        this.isPushdown = context.getQueryTagInfo().isPushdown();
        this.isTimeout = context.getQueryTagInfo().isTimeout();
        if (context.getQueryTagInfo().isStorageCacheUsed() && context.getEngineType() != null) {
            this.engineType = context.getEngineType();
        } else {
            if (context.getQueryTagInfo().isPushdown()) {
                this.engineType = context.getPushdownEngine();
            } else if (context.getQueryTagInfo().isConstantQuery()) {
                this.engineType = QueryHistory.EngineType.CONSTANTS.name();
            } else if (!context.getMetrics().isException()) {
                this.engineType = QueryHistory.EngineType.NATIVE.name();
            }
        }

        this.queryStatus = context.getMetrics().isException() ? QueryHistory.QUERY_HISTORY_FAILED
                : QueryHistory.QUERY_HISTORY_SUCCEEDED;

        if (context.getQueryTagInfo().isHitExceptionCache() || context.getQueryTagInfo().isStorageCacheUsed()) {
            this.isCacheHit = true;
            this.cacheType = context.getQueryTagInfo().getStorageCacheType();
        }
        this.resultRowCount = context.getMetrics().getResultRowCount();
        this.queryMsg = context.getMetrics().getQueryMsg();

        this.isIndexHit = !context.getMetrics().isException() && !context.getQueryTagInfo().isPushdown()
                && !this.engineType.equals(QueryHistory.EngineType.CONSTANTS.name());
        this.projectName = context.getProject();

        collectErrorType(context);
        List<RealizationMetrics> realizationMetricList = collectRealizationMetrics(
                QueryContext.current().getQueryRealizations());

        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(context.getMetrics().isExactlyMatch(),
                context.getMetrics().getSegCount(),
                Objects.nonNull(this.errorType) && !this.errorType.equals(QueryHistory.NO_REALIZATION_FOUND_ERROR));
        queryHistoryInfo.setRealizationMetrics(realizationMetricList);

        queryHistoryInfo.setQueryMetrics(collectQueryMetrics());

        List<List<String>> querySnapshots = new ArrayList<>();
        for (NativeQueryRealization qcReal : QueryContext.current().getQueryRealizations()) {
            if (CollectionUtils.isEmpty(qcReal.getLookupTables())) {
                continue;
            }
            querySnapshots.add(qcReal.getLookupTables());
        }
        queryHistoryInfo.setQuerySnapshots(querySnapshots);
        queryHistoryInfo.setCacheType(this.cacheType);
        queryHistoryInfo.setQueryMsg(this.queryMsg);
        queryHistoryInfo.setHostName(AddressUtil.getHostName());
        queryHistoryInfo.setPort(KylinConfig.getInstanceFromEnv().getServerPort());
        this.queryHistoryInfo = queryHistoryInfo;

        this.queryHistoryInfo.setTraces(createTraces(context));
    }

    public static List<QueryHistoryInfo.QueryTraceSpan> createTraces(final QueryContext context) {
        return context.getQueryTrace().spans().stream().map(span -> {
            if (!KapConfig.getInstanceFromEnv().isQuerySparkJobTraceEnabled()
                    && QueryTrace.PREPARE_AND_SUBMIT_JOB.equals(span.getName())) {
                return new QueryHistoryInfo.QueryTraceSpan(QueryTrace.SPARK_JOB_EXECUTION,
                        QueryTrace.SPAN_GROUPS.get(QueryTrace.SPARK_JOB_EXECUTION), span.getDuration());
            } else {
                return new QueryHistoryInfo.QueryTraceSpan(span.getName(), span.getGroup(), span.getDuration());
            }
        }).collect(Collectors.toList());
    }

    private void collectErrorType(final QueryContext context) {
        Throwable olapErrorCause = context.getMetrics().getOlapCause();
        Throwable cause = context.getMetrics().getFinalCause();

        while (olapErrorCause != null) {
            if (olapErrorCause instanceof NoRealizationFoundException) {
                this.errorType = QueryHistory.NO_REALIZATION_FOUND_ERROR;
                return;
            }

            if (olapErrorCause instanceof RoutingIndicatorException) {
                this.errorType = QueryHistory.NOT_SUPPORTED_SQL_BY_OLAP_ERROR;
                return;
            }

            olapErrorCause = olapErrorCause.getCause();
        }

        while (cause != null) {
            if (cause instanceof SqlValidatorException || cause instanceof SqlParseException
                    || cause.getClass().getName().contains("ParseException")) {
                this.errorType = QueryHistory.SYNTAX_ERROR;
                return;
            }

            cause = cause.getCause();
        }

        if (context.getMetrics().getFinalCause() != null) {
            this.errorType = QueryHistory.OTHER_ERROR;
        }
    }

    public List<QueryMetric> collectQueryMetrics() {
        List<QueryMetric> queryMetrics = new ArrayList<>();
        queryMetrics.add(new QueryMetric(QueryHistory.CPU_TIME,this.cpuTime));
        return queryMetrics;
    }

    public List<RealizationMetrics> collectRealizationMetrics(List<NativeQueryRealization> queryRealizations) {
        List<RealizationMetrics> realizationMetricList = new ArrayList<>();
        if (CollectionUtils.isEmpty(queryRealizations)) {
            return realizationMetricList;
        }

        for (NativeQueryRealization realization : queryRealizations) {
            RealizationMetrics realizationMetrics = new RealizationMetrics(
                    Objects.toString(realization.getLayoutId(), null), realization.getType(), realization.getModelId(),
                    realization.getLookupTables());
            realizationMetrics.setQueryId(queryId);
            realizationMetrics.setDuration(queryDuration);
            realizationMetrics.setQueryTime(queryTime);
            realizationMetrics.setProjectName(projectName);
            realizationMetrics.setQueryDay(queryDay);
            realizationMetrics.setQueryFirstDayOfWeek(queryFirstDayOfWeek);
            realizationMetrics.setQueryFirstDayOfMonth(queryFirstDayOfMonth);
            realizationMetrics.setStreamingLayout(realization.isStreamingLayout());
            realizationMetrics.setSnapshots(realization.getLookupTables());
            realizationMetricList.add(realizationMetrics);

            if (realization.getType() == null) {
                continue;
            }

            switch (realization.getType()) {
            case QueryMetrics.TABLE_INDEX:
                tableIndexUsed = true;
                break;
            case QueryMetrics.AGG_INDEX:
                aggIndexUsed = true;
                break;
            case QueryMetrics.TABLE_SNAPSHOT:
                tableSnapshotUsed = true;
                break;
            case QueryMetrics.INTERNAL_TABLE:
                // need change table schema
                break;
            }
        }
        return realizationMetricList;
    }
}
