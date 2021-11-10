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

import static org.apache.kylin.common.util.CheckUtil.checkCondition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.OnlyPrepareEarlyAbortException;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.kylin.cache.cachemanager.MemcachedCacheManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.metrics.QuerySparkMetrics;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ServerMode;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.ColumnMetaWithType;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryInfoCollector;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.TempStatementUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.metrics.QueryMetrics2Facade;
import org.apache.kylin.rest.metrics.QueryMetricsFacade;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.response.SQLResponseTrace;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.QueryRequestLimits;
import org.apache.kylin.rest.util.SQLResponseSignatureUtil;
import org.apache.kylin.rest.util.TableauInterceptor;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.apache.spark.sql.SparderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService extends BasicService {

    public static final String QUERY_CACHE = "StorageCache";
    public static final String QUERY_STORE_PATH_PREFIX = "/query/";
    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);
    final BadQueryDetector badQueryDetector = new BadQueryDetector();
    final ResourceStore queryStore;

    @Autowired
    protected CacheManager cacheManager;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("TableAclService")
    private TableACLService tableAclService;

    @Autowired
    private AclEvaluate aclEvaluate;

    private GenericKeyedObjectPool<PreparedContextKey, PreparedContext> preparedContextPool;

    public QueryService() {
        queryStore = ResourceStore.getStore(getConfig());
        preparedContextPool = createPreparedContextPool();
        badQueryDetector.start();
    }

    private GenericKeyedObjectPool<PreparedContextKey, PreparedContext> createPreparedContextPool() {
        PreparedContextFactory factory = new PreparedContextFactory();
        KylinConfig kylinConfig = getConfig();
        GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
        config.setMaxTotalPerKey(kylinConfig.getQueryMaxCacheStatementInstancePerKey());
        config.setMaxTotal(kylinConfig.getQueryMaxCacheStatementNum());
        config.setBlockWhenExhausted(false);
        config.setMinEvictableIdleTimeMillis(10 * 60 * 1000L); // cached statement will be evict if idle for 10 minutes
        config.setTimeBetweenEvictionRunsMillis(60 * 1000L); 
        GenericKeyedObjectPool<PreparedContextKey, PreparedContext> pool = new GenericKeyedObjectPool<>(factory,
                config);
        return pool;
    }

    protected static void close(ResultSet resultSet, Statement stat, Connection conn) {
        OLAPContext.clearParameter();
        DBUtils.closeQuietly(resultSet);
        DBUtils.closeQuietly(stat);
        DBUtils.closeQuietly(conn);
    }

    private static String getQueryKeyById(String creator) {
        return QUERY_STORE_PATH_PREFIX + creator;
    }

    @PostConstruct
    public void init() throws IOException {
        Preconditions.checkNotNull(cacheManager, "cacheManager is not injected yet");
    }

    public List<TableMeta> getMetadataFilterByUser(String project) throws SQLException, IOException {
        return tableAclService.filterTableMetasByAcl(getMetadata(project), project);
    }

    public List<TableMeta> getMetadata(String project) throws SQLException {
        return getMetadata(getCubeManager(), project);
    }

    public SQLResponse query(SQLRequest sqlRequest, String queryId) throws Exception {
        SQLResponse ret = null;
        try {
            final String user = SecurityContextHolder.getContext().getAuthentication().getName();
            badQueryDetector.queryStart(Thread.currentThread(), sqlRequest, user, queryId);

            ret = queryWithSqlMassage(sqlRequest);
            ret.setTraces(QueryContextFacade.current().getQueryTrace().spans().stream()
                    .map(span -> new SQLResponseTrace(span.getName(), span.getGroup(), span.getDuration()))
                    .collect(Collectors.toList()));
            return ret;

        } finally {
            String badReason = (ret != null && ret.isPushDown()) ? BadQueryEntry.ADJ_PUSHDOWN : null;
            badQueryDetector.queryEnd(Thread.currentThread(), badReason);
            Thread.interrupted(); //reset if interrupted
        }
    }

    public SQLResponse update(SQLRequest sqlRequest) throws Exception {
        // non select operations, only supported when enable pushdown
        logger.debug("Query pushdown enabled, redirect the non-select query to pushdown engine.");
        Connection conn = null;
        try {
            conn = QueryConnection.getConnection(sqlRequest.getProject());
            Pair<List<List<String>>, List<SelectedColumnMeta>> r = PushDownUtil.tryPushDownNonSelectQuery(
                    sqlRequest.getProject(), sqlRequest.getSql(), conn.getSchema(), BackdoorToggles.getPrepareOnly());

            List<SelectedColumnMeta> columnMetas = Lists.newArrayList();
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0", "c0",
                    null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false));

            return buildSqlResponse(sqlRequest.getProject(), true, r.getFirst(), columnMetas);

        } catch (Exception e) {
            logger.info("pushdown engine failed to finish current non-select query");
            throw e;
        } finally {
            close(null, null, conn);
        }
    }

    public void saveQuery(final String creator, final Query query) throws IOException {
        List<Query> queries = getQueries(creator);
        queries.add(query);
        Query[] queryArray = new Query[queries.size()];
        QueryRecord record = new QueryRecord(queries.toArray(queryArray));
        queryStore.putResource(getQueryKeyById(creator), record, System.currentTimeMillis(),
                QueryRecordSerializer.getInstance());
        return;
    }

    public void removeQuery(final String creator, final String id) throws IOException {
        List<Query> queries = getQueries(creator);
        Iterator<Query> queryIter = queries.iterator();

        boolean changed = false;
        while (queryIter.hasNext()) {
            Query temp = queryIter.next();
            if (temp.getId().equals(id)) {
                queryIter.remove();
                changed = true;
                break;
            }
        }

        if (!changed) {
            return;
        }
        Query[] queryArray = new Query[queries.size()];
        QueryRecord record = new QueryRecord(queries.toArray(queryArray));
        queryStore.putResource(getQueryKeyById(creator), record, System.currentTimeMillis(),
                QueryRecordSerializer.getInstance());
        return;
    }

    public List<Query> getQueries(final String creator) throws IOException {
        return getQueries(creator, null);
    }

    public List<Query> getQueries(final String creator, final String project) throws IOException {
        if (null == creator) {
            return null;
        }
        List<Query> queries = new ArrayList<>();
        QueryRecord record = queryStore.getResource(getQueryKeyById(creator), QueryRecordSerializer.getInstance());
        if (record != null) {
            for (Query query : record.getQueries()) {
                if (project == null || query.getProject().equals(project))
                    queries.add(query);
            }
        }
        return queries;
    }

    public void logQuery(final String queryId, final SQLRequest request, final SQLResponse response) {
        final String user = aclEvaluate.getCurrentUserName();
        final List<String> realizationNames = new LinkedList<>();
        final List<Long> cuboidIds = new LinkedList<>();
        final List<Boolean> isExactlyMatchSet = new LinkedList<>();
        float duration = response.getDuration() / (float) 1000;
        boolean storageCacheUsed = response.isStorageCacheUsed();
        boolean isPushDown = response.isPushDown();

        if (!response.isHitExceptionCache() && null != OLAPContext.getThreadLocalContexts()) {
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                Cuboid cuboid = ctx.storageContext.getCuboid();
                if (cuboid != null) {
                    //Some queries do not involve cuboid, e.g. lookup table query
                    cuboidIds.add(cuboid.getId());
                }
                isExactlyMatchSet.add(ctx.isExactlyAggregate);

                if (ctx.realization != null) {
                    realizationNames.add(ctx.realization.getCanonicalName());
                }

            }
        }

        // if Realization Names is empty, get value from SQLResponse.
        if (realizationNames.isEmpty()) {
            if (!Strings.isNullOrEmpty(response.getCube())) {
                realizationNames.addAll(Lists.newArrayList(StringUtil.splitByComma(response.getCube())));
            }
        }

        // if Cuboid Ids is empty, get value from SQLResponse.
        if (cuboidIds.isEmpty()) {
            List<QueryContext.CubeSegmentStatisticsResult> cubeSegmentStatisticsList =
                    response.getCubeSegmentStatisticsList();
            if (CollectionUtils.isNotEmpty(cubeSegmentStatisticsList)) {
                cubeSegmentStatisticsList.forEach(cubeSegmentStatResult -> {
                    if (MapUtils.isNotEmpty(cubeSegmentStatResult.getCubeSegmentStatisticsMap())) {
                        cubeSegmentStatResult.getCubeSegmentStatisticsMap().values().forEach(cubeSegmentStatMap -> {
                            cubeSegmentStatMap.values().forEach(cubeSegmentStat -> {
                                cuboidIds.add(cubeSegmentStat.getTargetCuboidId());
                            });
                        });
                    }
                });
            }
        }

        int resultRowCount = 0;
        if (!response.getIsException() && response.getResults() != null) {
            resultRowCount = response.getResults().size();
        }

        String newLine = System.getProperty("line.separator");
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(newLine);
        stringBuilder.append("==========================[QUERY]===============================").append(newLine);
        stringBuilder.append("Query Id: ").append(queryId).append(newLine);
        stringBuilder.append("SQL: ").append(request.getSql()).append(newLine);
        stringBuilder.append("User: ").append(user).append(newLine);
        stringBuilder.append("Success: ").append((null == response.getExceptionMessage())).append(newLine);
        stringBuilder.append("Duration: ").append(duration).append(newLine);
        stringBuilder.append("Project: ").append(request.getProject()).append(newLine);
        stringBuilder.append("Realization Names: ").append(realizationNames).append(newLine);
        stringBuilder.append("Cuboid Ids: ").append(cuboidIds).append(newLine);
        stringBuilder.append("Is Exactly Matched: ").append(isExactlyMatchSet).append(newLine);
        stringBuilder.append("Total scan count: ").append(response.getTotalScanCount()).append(newLine);
        stringBuilder.append("Total scan files: ").append(response.getTotalScanFiles()).append(newLine);
        stringBuilder.append("Total metadata time: ").append(response.getMetadataTime()).append("ms").append(newLine);
        stringBuilder.append("Total spark scan time: ").append(response.getTotalSparkScanTime()).append("ms").append(newLine);
        stringBuilder.append("Total scan bytes: ").append(response.getTotalScanBytes()).append(newLine);
        stringBuilder.append("Result row count: ").append(resultRowCount).append(newLine);
//        stringBuilder.append("Accept Partial: ").append(request.isAcceptPartial()).append(newLine);
//        stringBuilder.append("Is Partial Result: ").append(response.isPartial()).append(newLine);
//        stringBuilder.append("Hit Exception Cache: ").append(response.isHitExceptionCache()).append(newLine);
        stringBuilder.append("Storage cache used: ").append(storageCacheUsed).append(newLine);
        stringBuilder.append("Is Query Push-Down: ").append(isPushDown).append(newLine);
        stringBuilder.append("Is Prepare: ").append(BackdoorToggles.getPrepareOnly()).append(newLine);
        stringBuilder.append("Used Spark pool: ").append(response.getSparkPool()).append(newLine);
        stringBuilder.append("Trace URL: ").append(response.getTraceUrl()).append(newLine);
        stringBuilder.append("Message: ").append(response.getExceptionMessage()).append(newLine);
        if (response.getTraces() != null) {
            stringBuilder.append("Time consuming for each query stage: -----------------").append(newLine);
            response.getTraces().forEach(trace -> stringBuilder.append(trace.getName() + " : " + trace.getDuration() + "ms").append(newLine));
            stringBuilder.append("Time consuming for each query stage: -----------------").append(newLine);
        }
        stringBuilder.append("==========================[QUERY]===============================").append(newLine);

        logger.info(stringBuilder.toString());
    }

    public SQLResponse querySystemCube(String sql) {
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setProject(MetricsManager.SYSTEM_PROJECT);
        sqlRequest.setSql(sql);
        return doQueryWithCache(sqlRequest, false);
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest) {
        long t = System.currentTimeMillis();
        aclEvaluate.checkProjectReadPermission(sqlRequest.getProject());
        logger.info("Check query permission in " + (System.currentTimeMillis() - t) + " ms.");
        return doQueryWithCache(sqlRequest, false);
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest, boolean isQueryInspect) {
        Message msg = MsgPicker.getMsg();
        sqlRequest.setUsername(getUserName());
        final QueryContext queryContext = QueryContextFacade.current();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        queryContext.getQueryTrace().startSpan(QueryTrace.SQL_TRANSFORMATION);
        if (!ServerMode.SERVER_MODE.canServeQuery()) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getQUERY_NOT_ALLOWED(), kylinConfig.getServerMode()));
        }
        if (StringUtils.isBlank(sqlRequest.getProject())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }
        // project not found
        ProjectManager mgr = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (mgr.getProject(sqlRequest.getProject()) == null) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getPROJECT_NOT_FOUND(), sqlRequest.getProject()));
        }
        if (StringUtils.isBlank(sqlRequest.getSql())) {
            throw new BadRequestException(msg.getNULL_EMPTY_SQL());
        }

        if (sqlRequest.getBackdoorToggles() != null)
            BackdoorToggles.addToggles(sqlRequest.getBackdoorToggles());

        try (SetThreadName ignored = new SetThreadName("Query %s", queryContext.getQueryId())) {
            // force clear the query context before a new query
            OLAPContext.clearThreadLocalContexts();

            SQLResponse sqlResponse = null;
            String sql = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            boolean isQueryCacheEnabled = isQueryCacheEnabled(kylinConfig);
            logger.info("Using project: " + project);
            logger.info("The original query:  " + sql);

            sql = QueryUtil.removeCommentInSql(sql);

            Pair<Boolean, String> result = TempStatementUtil.handleTempStatement(sql, kylinConfig);
            boolean isCreateTempStatement = result.getFirst();
            sql = result.getSecond();
            sqlRequest.setSql(sql);

            try (QueryRequestLimits limit = new QueryRequestLimits(sqlRequest.getProject())) {
                // try some cheap executions
                if (sqlResponse == null && isQueryInspect) {
                    sqlResponse = new SQLResponse(null, null, 0, false, sqlRequest.getSql());
                }

                if (sqlResponse == null && isCreateTempStatement) {
                    sqlResponse = new SQLResponse(null, null, 0, false, null);
                }

                if (sqlResponse == null && isQueryCacheEnabled) {
                    sqlResponse = searchQueryInCache(sqlRequest);
                }

                // real execution if required
                if (sqlResponse == null) {
                    sqlResponse = queryAndUpdateCache(sqlRequest, isQueryCacheEnabled);
                }
            }

            sqlResponse.setDuration(queryContext.getAccumulatedMillis());
            if (QuerySparkMetrics.getInstance().getQueryExecutionMetrics(queryContext.getQueryId()) != null) {
                String sqlTraceUrl = SparderContext.appMasterTrackURL() + "/SQL/execution/?id=" +
                        QuerySparkMetrics.getInstance().getQueryExecutionMetrics(queryContext.getQueryId()).getExecutionId();
                sqlResponse.setTraceUrl(sqlTraceUrl);
            }
            logQuery(queryContext.getQueryId(), sqlRequest, sqlResponse);
            try {
                recordMetric(queryContext.getQueryId(), sqlRequest, sqlResponse);
            } catch (Throwable th) {
                logger.warn("Write metric error.", th);
            }
            if (sqlResponse.getIsException())
                throw new InternalErrorException(sqlResponse.getExceptionMessage(), sqlResponse.getThrowable());

            return sqlResponse;

        } finally {
            BackdoorToggles.cleanToggles();
            QueryContextFacade.resetCurrent();
            QueryInfoCollector.reset();
        }
    }

    private SQLResponse queryAndUpdateCache(SQLRequest sqlRequest, boolean queryCacheEnabled) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Message msg = MsgPicker.getMsg();
        final QueryContext queryContext = QueryContextFacade.current();

        boolean isDummyResponseEnabled = queryCacheEnabled && kylinConfig.isLazyQueryEnabled();
        SQLResponse sqlResponse = null;
        try {
            // Add dummy response which will be updated or evicted when query finishes
            if (isDummyResponseEnabled) {
                SQLResponse dummyResponse = new SQLResponse();
                dummyResponse.setLazyQueryStartTime(System.currentTimeMillis());
                cacheManager.getCache(QUERY_CACHE).put(sqlRequest.getCacheKey(), dummyResponse);
            }

            final boolean isSelect = QueryUtil.isSelectStatement(sqlRequest.getSql());
            if (isSelect) {
                sqlResponse = query(sqlRequest, queryContext.getQueryId());
            } else if (kylinConfig.isPushDownEnabled() && kylinConfig.isPushDownUpdateEnabled()) {
                sqlResponse = update(sqlRequest);
            } else {
                logger.debug("Directly return exception as the sql is unsupported, and query pushdown is disabled");
                throw new BadRequestException(msg.getNOT_SUPPORTED_SQL());
            }

            long durationThreshold = kylinConfig.getQueryDurationCacheThreshold();
            long scanCountThreshold = kylinConfig.getQueryScanCountCacheThreshold();
            long scanBytesThreshold = kylinConfig.getQueryScanBytesCacheThreshold();
            sqlResponse.setDuration(queryContext.getAccumulatedMillis());
            logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                    String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()),
                    String.valueOf(sqlResponse.getTotalScanCount()));

            boolean realtimeQuery = false;
            Collection<OLAPContext> olapContexts = OLAPContext.getThreadLocalContexts();
//            if (olapContexts != null) {
//                for (OLAPContext ctx : olapContexts) {
////                    try {
////                        if (ctx.storageContext.getStorageQuery() instanceof StreamStorageQuery) {
////                            realtimeQuery = true;
////                            logger.debug("Shutdown query cache for realtime.");
////                        }
////                    } catch (Exception e) {
////                        logger.error("Error", e);
////                    }
//
//                }
//            }

            if (checkCondition(queryCacheEnabled, "query cache is disabled") //
                    && checkCondition(!Strings.isNullOrEmpty(sqlResponse.getCube()),
                            "query does not hit cube nor hybrid") //
                    && checkCondition(!sqlResponse.getIsException(), "query has exception") //
                    && checkCondition(
                            !(sqlResponse.isPushDown()
                                    && (isSelect == false || kylinConfig.isPushdownQueryCacheEnabled() == false)),
                            "query is executed with pushdown, but it is non-select, or the cache for pushdown is disabled") //
                    && checkCondition(
                            cacheManager.getCache(QUERY_CACHE) instanceof MemcachedCacheManager.MemCachedCacheAdaptor
                                    || sqlResponse.getDuration() > durationThreshold
                                    || sqlResponse.getTotalScanCount() > scanCountThreshold
                                    || sqlResponse.getTotalScanBytes() > scanBytesThreshold, //
                            "query is too lightweight with duration: {} (threshold {}), scan count: {} (threshold {}), scan bytes: {} (threshold {})",
                            sqlResponse.getDuration(), durationThreshold, sqlResponse.getTotalScanCount(),
                            scanCountThreshold, sqlResponse.getTotalScanBytes(), scanBytesThreshold)
                    && checkCondition(sqlResponse.getResults().size() < kylinConfig.getLargeQueryThreshold(),
                            "query response is too large: {} ({})", sqlResponse.getResults().size(),
                            kylinConfig.getLargeQueryThreshold())) {

                if (!realtimeQuery) {
                    cacheManager.getCache(QUERY_CACHE).put(sqlRequest.getCacheKey(), sqlResponse);
                }
            } else if (isDummyResponseEnabled) {
                cacheManager.getCache(QUERY_CACHE).evict(sqlRequest.getCacheKey());
            }

        } catch (Throwable e) { // calcite may throw AssertError
            queryContext.stop(e);

            logger.error("Exception while executing query", e);
            String errMsg = makeErrorMsgUserFriendly(e);

            sqlResponse = buildSqlResponse(sqlRequest.getProject(), false, null, null, true, errMsg);
            sqlResponse.setThrowable(e.getCause() == null ? e : ExceptionUtils.getRootCause(e));

            if (queryCacheEnabled && e.getCause() != null
                    && ExceptionUtils.getRootCause(e) instanceof ResourceLimitExceededException) {
                Cache exceptionCache = cacheManager.getCache(QUERY_CACHE);
                exceptionCache.put(sqlRequest.getCacheKey(), sqlResponse);
            } else if (isDummyResponseEnabled) {
                // evict dummy response to avoid caching too many bad queries
                Cache exceptionCache = cacheManager.getCache(QUERY_CACHE);
                exceptionCache.evict(sqlRequest.getCacheKey());
            }
        }
        return sqlResponse;
    }

    private boolean isQueryCacheEnabled(KylinConfig kylinConfig) {
        return checkCondition(kylinConfig.isQueryCacheEnabled(), "query cache disabled in KylinConfig") && //
                checkCondition(!BackdoorToggles.getDisableCache(), "query cache disabled in BackdoorToggles");
    }

    protected void recordMetric(String queryId, SQLRequest sqlRequest, SQLResponse sqlResponse) throws UnknownHostException {
        QueryMetricsFacade.updateMetrics(queryId, sqlRequest, sqlResponse);
        QueryMetrics2Facade.updateMetrics(sqlRequest, sqlResponse);
    }

    private String getUserName() {
        String username = SecurityContextHolder.getContext().getAuthentication().getName();
        if (StringUtils.isEmpty(username)) {
            username = "";
        }
        return username;
    }

    public SQLResponse searchQueryInCache(SQLRequest sqlRequest) {
        Cache cache = cacheManager.getCache(QUERY_CACHE);
        Cache.ValueWrapper wrapper = cache.get(sqlRequest.getCacheKey());
        if (wrapper == null) {
            return null;
        }
        SQLResponse response = (SQLResponse) wrapper.get();
        if (response == null) {
            return null;
        }

        // Check whether duplicate query exists
        while (response.isRunning()) {
            // Wait at most one minute
            if (System.currentTimeMillis() - response.getLazyQueryStartTime() >= getConfig()
                    .getLazyQueryWaitingTimeoutMilliSeconds()) {
                cache.evict(sqlRequest.getCacheKey());
                return null;
            }
            logger.info("Duplicated SQL request is running, waiting...");
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
            }
            wrapper = cache.get(sqlRequest.getCacheKey());
            if (wrapper == null) {
                return null;
            }
            response = (SQLResponse) wrapper.get();
            if (response == null) {
                return null;
            }
        }
        logger.info("The sqlResponse is found in QUERY_CACHE");
        if (getConfig().isQueryCacheSignatureEnabled()
                && !SQLResponseSignatureUtil.checkSignature(getConfig(), response, sqlRequest.getProject())) {
            logger.info("The sql response signature is changed. Remove it from QUERY_CACHE.");
            cache.evict(sqlRequest.getCacheKey());
            return null;
        }
        if (response.getIsException()) {
            response.setHitExceptionCache(true);
        } else {
            response.setStorageCacheUsed(true);
        }
        return response;
    }

    private SQLResponse queryWithSqlMassage(SQLRequest sqlRequest) throws Exception {
        Connection conn = null;
        boolean isPrepareRequest = isPrepareStatementWithParams(sqlRequest);
        boolean borrowPrepareContext = false;
        PreparedContextKey preparedContextKey = null;
        PreparedContext preparedContext = null;

        try {
            conn = QueryConnection.getConnection(sqlRequest.getProject());
            String userInfo = SecurityContextHolder.getContext().getAuthentication().getName();
            QueryContext context = QueryContextFacade.current();
            context.setUsername(userInfo);
            context.setGroups(AclPermissionUtil.getCurrentUserGroups());
            context.setProject(sqlRequest.getProject());
            final Collection<? extends GrantedAuthority> grantedAuthorities = SecurityContextHolder.getContext()
                    .getAuthentication().getAuthorities();
            for (GrantedAuthority grantedAuthority : grantedAuthorities) {
                userInfo += ",";
                userInfo += grantedAuthority.getAuthority();
            }

            SQLResponse fakeResponse = TableauInterceptor.tableauIntercept(sqlRequest.getSql());
            if (null != fakeResponse) {
                logger.debug("Return fake response, is exception? " + fakeResponse.getIsException());
                return fakeResponse;
            }
            String correctedSql = QueryUtil.massageSql(sqlRequest.getSql(), sqlRequest.getProject(),
                    sqlRequest.getLimit(), sqlRequest.getOffset(), conn.getSchema(), Constant.FakeCatalogName);
            if (!correctedSql.equals(sqlRequest.getSql())) {
                logger.info("The corrected query: " + correctedSql);

                //CAUTION: should not change sqlRequest content!
                //sqlRequest.setSql(correctedSql);
            }

            // add extra parameters into olap context, like acceptPartial
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put(OLAPContext.PRM_USER_AUTHEN_INFO, userInfo);
            parameters.put(OLAPContext.PRM_ACCEPT_PARTIAL_RESULT, String.valueOf(sqlRequest.isAcceptPartial()));
            OLAPContext.setParameters(parameters);

            // special case for prepare query.
            List<List<String>> results = Lists.newArrayList();
            List<SelectedColumnMeta> columnMetas = Lists.newArrayList();
            if (BackdoorToggles.getPrepareOnly()) {
                return getPrepareOnlySqlResponse(sqlRequest.getProject(), correctedSql, conn, false, results,
                        columnMetas);
            }
            if (!isPrepareRequest) {
                return executeRequest(correctedSql, sqlRequest, conn);
            } else {
                long prjLastModifyTime = getProjectManager().getProject(sqlRequest.getProject()).getLastModified();
                preparedContextKey = new PreparedContextKey(sqlRequest.getProject(), prjLastModifyTime, correctedSql);
                PrepareSqlRequest prepareSqlRequest = (PrepareSqlRequest) sqlRequest;
                if (getConfig().isQueryPreparedStatementCacheEnable() && prepareSqlRequest.isEnableStatementCache()) {
                    try {
                        preparedContext = preparedContextPool.borrowObject(preparedContextKey);

                        // preparedContext initialized by current thread, put relNode and resultType into cache
                        if (preparedContext.olapRel == null) {
                            preparedContext.olapRel = QueryContextFacade.current().getOlapRel();
                            preparedContext.resultType = (QueryContextFacade.current().getResultType());
                        } else {
                            //set cached RelNode and ResultType into current QueryContext
                            QueryContextFacade.current().setOlapRel(preparedContext.olapRel);
                            QueryContextFacade.current().setResultType(preparedContext.resultType);
                        }

                        borrowPrepareContext = true;
                    } catch (NoSuchElementException noElementException) {
                        borrowPrepareContext = false;
                        preparedContext = createPreparedContext(sqlRequest.getProject(), sqlRequest.getSql());
                    }

                    for (OLAPContext olapContext : preparedContext.olapContexts) {
                        resetRealizationInContext(olapContext);
                        OLAPContext.registerContext(olapContext);
                    }
                } else {
                    preparedContext = createPreparedContext(sqlRequest.getProject(), sqlRequest.getSql());
                }
                return executePrepareRequest(correctedSql, prepareSqlRequest, preparedContext);
            }

        } finally {
            DBUtils.closeQuietly(conn);
            if (preparedContext != null) {
                if (borrowPrepareContext) {
                    // Set tag isBorrowedContext true, when return preparedContext back
                    for (OLAPContext olapContext : preparedContext.olapContexts) {
                        if (borrowPrepareContext) {
                            olapContext.isBorrowedContext = true;
                        }
                    }

                    preparedContextPool.returnObject(preparedContextKey, preparedContext);
                } else {
                    preparedContext.close();
                }
            }
        }
    }

    private void resetRealizationInContext(OLAPContext olapContext) {
        IRealization realization = olapContext.realization;
        if (realization == null) {
            return;
        }
        KylinConfig config = getConfig();
        HybridInstance hybridInstance = HybridManager.getInstance(config).getHybridInstance(realization.getName());
        if (hybridInstance != null) {
            olapContext.realization = hybridInstance;
            return;
        }
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(realization.getName());
        if (cubeInstance != null) {
            olapContext.realization = cubeInstance;
        }
    }

    protected List<TableMeta> getMetadata(CubeManager cubeMgr, String project) throws SQLException {

        Connection conn = null;
        ResultSet columnMeta = null;
        List<TableMeta> tableMetas = null;
        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }
        ResultSet JDBCTableMeta = null;
        try {
            conn = QueryConnection.getConnection(project);
            DatabaseMetaData metaData = conn.getMetaData();

            JDBCTableMeta = metaData.getTables(null, null, null, null);

            tableMetas = new LinkedList<TableMeta>();
            Map<String, TableMeta> tableMap = new HashMap<String, TableMeta>();
            while (JDBCTableMeta.next()) {
                String catalogName = JDBCTableMeta.getString(1);
                String schemaName = JDBCTableMeta.getString(2);

                // Not every JDBC data provider offers full 10 columns, e.g., PostgreSQL has only 5
                TableMeta tblMeta = new TableMeta(catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, JDBCTableMeta.getString(3),
                        JDBCTableMeta.getString(4), JDBCTableMeta.getString(5), null, null, null, null, null);

                if (!"metadata".equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                    tableMetas.add(tblMeta);
                    tableMap.put(tblMeta.getTABLE_SCHEM() + "#" + tblMeta.getTABLE_NAME(), tblMeta);
                }
            }

            columnMeta = metaData.getColumns(null, null, null, null);

            while (columnMeta.next()) {
                String catalogName = columnMeta.getString(1);
                String schemaName = columnMeta.getString(2);

                // kylin(optiq) is not strictly following JDBC specification
                ColumnMeta colmnMeta = new ColumnMeta(catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, columnMeta.getString(3),
                        columnMeta.getString(4), columnMeta.getInt(5), columnMeta.getString(6), columnMeta.getInt(7),
                        getInt(columnMeta.getString(8)), columnMeta.getInt(9), columnMeta.getInt(10),
                        columnMeta.getInt(11), columnMeta.getString(12), columnMeta.getString(13),
                        getInt(columnMeta.getString(14)), getInt(columnMeta.getString(15)), columnMeta.getInt(16),
                        columnMeta.getInt(17), columnMeta.getString(18), columnMeta.getString(19),
                        columnMeta.getString(20), columnMeta.getString(21), getShort(columnMeta.getString(22)),
                        columnMeta.getString(23));

                if (!"metadata".equalsIgnoreCase(colmnMeta.getTABLE_SCHEM())
                        && !colmnMeta.getCOLUMN_NAME().toUpperCase(Locale.ROOT).startsWith("_KY_")) {
                    tableMap.get(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME()).addColumn(colmnMeta);
                }
            }
        } finally {
            close(columnMeta, null, conn);
            if (JDBCTableMeta != null) {
                JDBCTableMeta.close();
            }
        }

        return tableMetas;
    }

    public List<TableMetaWithType> getMetadataV2(String project) throws SQLException, IOException {
        return getMetadataV2(getCubeManager(), project);
    }

    @SuppressWarnings("checkstyle:methodlength")
    protected List<TableMetaWithType> getMetadataV2(CubeManager cubeMgr, String project)
            throws SQLException, IOException {
        //Message msg = MsgPicker.getMsg();

        Connection conn = null;
        ResultSet columnMeta = null;
        List<TableMetaWithType> tableMetas = null;
        Map<String, TableMetaWithType> tableMap = null;
        Map<String, ColumnMetaWithType> columnMap = null;
        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }
        ResultSet JDBCTableMeta = null;
        try {
            conn = QueryConnection.getConnection(project);
            DatabaseMetaData metaData = conn.getMetaData();

            JDBCTableMeta = metaData.getTables(null, null, null, null);

            tableMetas = new LinkedList<TableMetaWithType>();
            tableMap = new HashMap<String, TableMetaWithType>();
            columnMap = new HashMap<String, ColumnMetaWithType>();
            while (JDBCTableMeta.next()) {
                String catalogName = JDBCTableMeta.getString(1);
                String schemaName = JDBCTableMeta.getString(2);

                // Not every JDBC data provider offers full 10 columns, e.g., PostgreSQL has only 5
                TableMetaWithType tblMeta = new TableMetaWithType(
                        catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, JDBCTableMeta.getString(3),
                        JDBCTableMeta.getString(4), JDBCTableMeta.getString(5), null, null, null, null, null);

                if (!"metadata".equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                    tableMetas.add(tblMeta);
                    tableMap.put(tblMeta.getTABLE_SCHEM() + "#" + tblMeta.getTABLE_NAME(), tblMeta);
                }
            }

            columnMeta = metaData.getColumns(null, null, null, null);

            while (columnMeta.next()) {
                String catalogName = columnMeta.getString(1);
                String schemaName = columnMeta.getString(2);

                // kylin(optiq) is not strictly following JDBC specification
                ColumnMetaWithType colmnMeta = new ColumnMetaWithType(
                        catalogName == null ? Constant.FakeCatalogName : catalogName,
                        schemaName == null ? Constant.FakeSchemaName : schemaName, columnMeta.getString(3),
                        columnMeta.getString(4), columnMeta.getInt(5), columnMeta.getString(6), columnMeta.getInt(7),
                        getInt(columnMeta.getString(8)), columnMeta.getInt(9), columnMeta.getInt(10),
                        columnMeta.getInt(11), columnMeta.getString(12), columnMeta.getString(13),
                        getInt(columnMeta.getString(14)), getInt(columnMeta.getString(15)), columnMeta.getInt(16),
                        columnMeta.getInt(17), columnMeta.getString(18), columnMeta.getString(19),
                        columnMeta.getString(20), columnMeta.getString(21), getShort(columnMeta.getString(22)),
                        columnMeta.getString(23));

                if (!"metadata".equalsIgnoreCase(colmnMeta.getTABLE_SCHEM())
                        && !colmnMeta.getCOLUMN_NAME().toUpperCase(Locale.ROOT).startsWith("_KY_")) {
                    tableMap.get(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME()).addColumn(colmnMeta);
                    columnMap.put(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME() + "#"
                            + colmnMeta.getCOLUMN_NAME(), colmnMeta);
                }
            }

        } finally {
            close(columnMeta, null, conn);
            if (JDBCTableMeta != null) {
                JDBCTableMeta.close();
            }
        }

        ProjectInstance projectInstance = getProjectManager().getProject(project);
        for (String modelName : projectInstance.getModels()) {

            DataModelDesc dataModelDesc = modelService.getModel(modelName, project);
            if (dataModelDesc != null && !dataModelDesc.isDraft()) {

                // update table type: FACT
                for (TableRef factTable : dataModelDesc.getFactTables()) {
                    String factTableName = factTable.getTableIdentity().replace('.', '#');
                    if (tableMap.containsKey(factTableName)) {
                        tableMap.get(factTableName).getTYPE().add(TableMetaWithType.tableTypeEnum.FACT);
                    } else {
                        // should be used after JDBC exposes all tables and columns
                        // throw new BadRequestException(msg.getTABLE_META_INCONSISTENT());
                    }
                }

                // update table type: LOOKUP
                for (TableRef lookupTable : dataModelDesc.getLookupTables()) {
                    String lookupTableName = lookupTable.getTableIdentity().replace('.', '#');
                    if (tableMap.containsKey(lookupTableName)) {
                        tableMap.get(lookupTableName).getTYPE().add(TableMetaWithType.tableTypeEnum.LOOKUP);
                    } else {
                        // throw new BadRequestException(msg.getTABLE_META_INCONSISTENT());
                    }
                }

                // update column type: PK and FK
                for (JoinTableDesc joinTableDesc : dataModelDesc.getJoinTables()) {
                    JoinDesc joinDesc = joinTableDesc.getJoin();
                    for (String pk : joinDesc.getPrimaryKey()) {
                        String columnIdentity = (dataModelDesc.findTable(pk.substring(0, pk.indexOf(".")))
                                .getTableIdentity() + pk.substring(pk.indexOf("."))).replace('.', '#');
                        if (columnMap.containsKey(columnIdentity)) {
                            columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.PK);
                        } else {
                            // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                        }
                    }

                    for (String fk : joinDesc.getForeignKey()) {
                        String columnIdentity = (dataModelDesc.findTable(fk.substring(0, fk.indexOf(".")))
                                .getTableIdentity() + fk.substring(fk.indexOf("."))).replace('.', '#');
                        if (columnMap.containsKey(columnIdentity)) {
                            columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.FK);
                        } else {
                            // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                        }
                    }
                }

                // update column type: DIMENSION AND MEASURE
                List<ModelDimensionDesc> dimensions = dataModelDesc.getDimensions();
                for (ModelDimensionDesc dimension : dimensions) {
                    for (String column : dimension.getColumns()) {
                        String columnIdentity = (dataModelDesc.findTable(dimension.getTable()).getTableIdentity() + "."
                                + column).replace('.', '#');
                        if (columnMap.containsKey(columnIdentity)) {
                            columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.DIMENSION);
                        } else {
                            // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                        }

                    }
                }

                String[] measures = dataModelDesc.getMetrics();
                for (String measure : measures) {
                    String columnIdentity = (dataModelDesc.findTable(measure.substring(0, measure.indexOf(".")))
                            .getTableIdentity() + measure.substring(measure.indexOf("."))).replace('.', '#');
                    if (columnMap.containsKey(columnIdentity)) {
                        columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.MEASURE);
                    } else {
                        // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                    }
                }
            }
        }

        return tableMetas;
    }

    protected void processStatementAttr(Statement s, SQLRequest sqlRequest) throws SQLException {
        Integer statementMaxRows = BackdoorToggles.getStatementMaxRows();
        if (statementMaxRows != null) {
            logger.info("Setting current statement's max rows to {}", statementMaxRows);
            s.setMaxRows(statementMaxRows);
        }
    }

    /**
     * @param correctedSql
     * @param sqlRequest
     * @return
     * @throws Exception
     */
    private SQLResponse executeRequest(String correctedSql, SQLRequest sqlRequest, Connection conn) throws Exception {
        Statement stat = null;
        ResultSet resultSet = null;
        boolean isPushDown = false;

        Pair<List<List<String>>, List<SelectedColumnMeta>> r = null;
        try {
            stat = conn.createStatement();
            processStatementAttr(stat, sqlRequest);
            QueryContextFacade.current().getQueryTrace().startSpan(QueryTrace.SQL_PARSE_AND_OPTIMIZE);
            resultSet = stat.executeQuery(correctedSql);

            r = createResponseFromResultSet(resultSet);

        } catch (SQLException sqlException) {
            r = pushDownQuery(sqlRequest, correctedSql, conn, sqlException);
            if (r == null)
                throw sqlException;

            isPushDown = true;
        } finally {
            close(resultSet, stat, null); //conn is passed in, not my duty to close
        }

        return buildSqlResponse(sqlRequest.getProject(), isPushDown, r.getFirst(), r.getSecond());
    }

    private SQLResponse executePrepareRequest(String correctedSql, PrepareSqlRequest sqlRequest,
            PreparedContext preparedContext) throws Exception {
        ResultSet resultSet = null;
        boolean isPushDown = false;

        Pair<List<List<String>>, List<SelectedColumnMeta>> r = null;
        Connection conn = preparedContext.conn;
        try {
            PreparedStatement preparedStatement = preparedContext.preparedStatement;
            processStatementAttr(preparedStatement, sqlRequest);
            for (int i = 0; i < sqlRequest.getParams().length; i++) {
                setParam(preparedStatement, i + 1, (sqlRequest.getParams()[i]));
            }
            resultSet = preparedStatement.executeQuery();
            r = createResponseFromResultSet(resultSet);
        } catch (SQLException sqlException) {
            r = pushDownQuery(sqlRequest, correctedSql, conn, sqlException);
            if (r == null)
                throw sqlException;

            isPushDown = true;
        } finally {
            DBUtils.closeQuietly(resultSet);
        }

        return buildSqlResponse(sqlRequest.getProject(), isPushDown, r.getFirst(), r.getSecond());
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownQuery(SQLRequest sqlRequest, String correctedSql,
            Connection conn, SQLException sqlException) throws Exception {
        try {
            return PushDownUtil.tryPushDownSelectQuery(sqlRequest.getProject(), correctedSql, conn.getSchema(),
                    sqlException, BackdoorToggles.getPrepareOnly());
        } catch (Exception e2) {
            logger.error("pushdown engine failed current query too", e2);
            //exception in pushdown, throw it instead of exception in calcite
            throw e2;
        }
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> createResponseFromResultSet(ResultSet resultSet)
            throws Exception {
        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Fill in selected column meta
        for (int i = 1; i <= columnCount; ++i) {
            columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i),
                    metaData.isSearchable(i), metaData.isCurrency(i), metaData.isNullable(i), metaData.isSigned(i),
                    metaData.getColumnDisplaySize(i), metaData.getColumnLabel(i), metaData.getColumnName(i),
                    metaData.getSchemaName(i), metaData.getCatalogName(i), metaData.getTableName(i),
                    metaData.getPrecision(i), metaData.getScale(i), metaData.getColumnType(i),
                    metaData.getColumnTypeName(i), metaData.isReadOnly(i), metaData.isWritable(i),
                    metaData.isDefinitelyWritable(i)));
        }

        // fill in results
        while (resultSet.next()) {
            List<String> oneRow = Lists.newArrayListWithCapacity(columnCount);
            for (int i = 0; i < columnCount; i++) {
                oneRow.add((resultSet.getString(i + 1)));
            }

            results.add(oneRow);
        }

        return new Pair<>(results, columnMetas);
    }

    protected String makeErrorMsgUserFriendly(Throwable e) {
        return QueryUtil.makeErrorMsgUserFriendly(e);
    }

    private SQLResponse getPrepareOnlySqlResponse(String projectName, String correctedSql, Connection conn,
            Boolean isPushDown, List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws SQLException {

        CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(true);

        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(correctedSql);
            throw new IllegalStateException("Should have thrown OnlyPrepareEarlyAbortException");
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause != null && rootCause instanceof OnlyPrepareEarlyAbortException) {
                OnlyPrepareEarlyAbortException abortException = (OnlyPrepareEarlyAbortException) rootCause;
                CalcitePrepare.Context context = abortException.getContext();
                CalcitePrepare.ParseResult preparedResult = abortException.getPreparedResult();
                List<RelDataTypeField> fieldList = preparedResult.rowType.getFieldList();

                CalciteConnectionConfig config = context.config();

                // Fill in selected column meta
                for (int i = 0; i < fieldList.size(); ++i) {

                    RelDataTypeField field = fieldList.get(i);
                    String columnName = field.getKey();

                    if (columnName.startsWith("_KY_")) {
                        continue;
                    }
                    BasicSqlType basicSqlType = (BasicSqlType) field.getValue();

                    columnMetas.add(new SelectedColumnMeta(false, config.caseSensitive(), false, false,
                            basicSqlType.isNullable() ? 1 : 0, true, basicSqlType.getPrecision(), columnName,
                            columnName, null, null, null, basicSqlType.getPrecision(),
                            basicSqlType.getScale() < 0 ? 0 : basicSqlType.getScale(),
                            basicSqlType.getSqlTypeName().getJdbcOrdinal(), basicSqlType.getSqlTypeName().getName(),
                            true, false, false));
                }

            } else {
                throw e;
            }
        } finally {
            CalcitePrepareImpl.KYLIN_ONLY_PREPARE.set(false);
            DBUtils.closeQuietly(preparedStatement);
        }

        return buildSqlResponse(projectName, isPushDown, results, columnMetas);
    }

    private boolean isPrepareStatementWithParams(SQLRequest sqlRequest) {
        if (sqlRequest instanceof PrepareSqlRequest && ((PrepareSqlRequest) sqlRequest).getParams() != null
                && ((PrepareSqlRequest) sqlRequest).getParams().length > 0)
            return true;
        return false;
    }

    private SQLResponse buildSqlResponse(String projectName, Boolean isPushDown, List<List<String>> results,
            List<SelectedColumnMeta> columnMetas) {
        return buildSqlResponse(projectName, isPushDown, results, columnMetas, false, null);
    }

    private SQLResponse buildSqlResponse(String projectName, Boolean isPushDown, List<List<String>> results,
            List<SelectedColumnMeta> columnMetas, boolean isException, String exceptionMessage) {

        boolean isPartialResult = false;

        List<String> realizations = Lists.newLinkedList();
        StringBuilder cubeSb = new StringBuilder();
        StringBuilder cuboidIdsSb = new StringBuilder();
        StringBuilder realizationTypeSb = new StringBuilder();
        StringBuilder logSb = new StringBuilder("Processed rows for each storageContext: ");
        QueryContext queryContext = QueryContextFacade.current();
        if (OLAPContext.getThreadLocalContexts() != null) { // contexts can be null in case of 'explain plan for'
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                String realizationName = "NULL";
                int realizationType = -1;
                if (ctx.realization != null) {
                    isPartialResult |= ctx.storageContext.isPartialResultReturned();
                    if (cubeSb.length() > 0) {
                        cubeSb.append(",");
                    }
                    cubeSb.append(ctx.realization.getCanonicalName());
                    Cuboid cuboid = ctx.storageContext.getCuboid();
                    if (cuboid != null) {
                        //Some queries do not involve cuboid, e.g. lookup table query
                        if(cuboidIdsSb.length() >0) {
                            cuboidIdsSb.append(",");
                        }
                        cuboidIdsSb.append(cuboid.getId());
                    }
                    logSb.append(ctx.storageContext.getProcessedRowCount()).append(" ");

                    realizationName = ctx.realization.getName();
                    realizationType = ctx.realization.getStorageType();
                    if (realizationTypeSb.length() > 0) {
                        realizationTypeSb.append(",");
                    }
                    realizationTypeSb.append(realizationType);

                    realizations.add(realizationName);
                }
            }
        }
        logger.info(logSb.toString());

        SQLResponse response = new SQLResponse(columnMetas, results, cubeSb.toString(), 0, isException,
                exceptionMessage, isPartialResult, isPushDown);
        response.setCuboidIds(cuboidIdsSb.toString());
        response.setRealizationTypes(realizationTypeSb.toString());
        response.setTotalScanCount(queryContext.getScannedRows());
        response.setTotalScanFiles((queryContext.getScanFiles() < 0) ? -1 :
                queryContext.getScanFiles());
        response.setMetadataTime((queryContext.getMedataTime() < 0) ? -1 :
                queryContext.getMedataTime());
        response.setTotalSparkScanTime((queryContext.getScanTime() < 0) ? -1 :
                queryContext.getScanTime());
        response.setTotalScanBytes((queryContext.getScannedBytes() < 1) ?
                (queryContext.getSourceScanBytes() < 1 ? -1 : queryContext.getSourceScanBytes()) : queryContext.getScannedBytes());
        response.setCubeSegmentStatisticsList(queryContext.getCubeSegmentStatisticsResultList());
        response.setSparkPool(queryContext.getSparkPool());
        if (getConfig().isQueryCacheSignatureEnabled()) {
            response.setSignature(SQLResponseSignatureUtil.createSignature(getConfig(), response, projectName));
        }
        return response;
    }

    /**
     * @param preparedState
     * @param param
     * @throws SQLException
     */
    private void setParam(PreparedStatement preparedState, int index, PrepareSqlRequest.StateParam param)
            throws SQLException {
        boolean isNull = (null == param.getValue());

        Class<?> clazz = getValidClass(param.getClassName());

        Rep rep = Rep.of(clazz);

        switch (rep) {
        case PRIMITIVE_CHAR:
        case CHARACTER:
        case STRING:
            preparedState.setString(index, isNull ? null : String.valueOf(param.getValue()));
            break;
        case PRIMITIVE_INT:
        case INTEGER:
            preparedState.setInt(index, isNull ? 0 : Integer.parseInt(param.getValue()));
            break;
        case PRIMITIVE_SHORT:
        case SHORT:
            preparedState.setShort(index, isNull ? 0 : Short.parseShort(param.getValue()));
            break;
        case PRIMITIVE_LONG:
        case LONG:
            preparedState.setLong(index, isNull ? 0 : Long.parseLong(param.getValue()));
            break;
        case PRIMITIVE_FLOAT:
        case FLOAT:
            preparedState.setFloat(index, isNull ? 0 : Float.parseFloat(param.getValue()));
            break;
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            preparedState.setDouble(index, isNull ? 0 : Double.parseDouble(param.getValue()));
            break;
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            preparedState.setBoolean(index, !isNull && Boolean.parseBoolean(param.getValue()));
            break;
        case PRIMITIVE_BYTE:
        case BYTE:
            preparedState.setByte(index, isNull ? 0 : Byte.parseByte(param.getValue()));
            break;
        case JAVA_UTIL_DATE:
        case JAVA_SQL_DATE:
            preparedState.setDate(index, isNull ? null : java.sql.Date.valueOf(param.getValue()));
            break;
        case JAVA_SQL_TIME:
            preparedState.setTime(index, isNull ? null : Time.valueOf(param.getValue()));
            break;
        case JAVA_SQL_TIMESTAMP:
            preparedState.setTimestamp(index, isNull ? null : Timestamp.valueOf(param.getValue()));
            break;
        default:
            preparedState.setObject(index, isNull ? null : param.getValue());
        }
    }

    protected int getInt(String content) {
        try {
            return Integer.parseInt(content);
        } catch (Exception e) {
            return -1;
        }
    }

    protected short getShort(String content) {
        try {
            return Short.parseShort(content);
        } catch (Exception e) {
            return -1;
        }
    }

    public Class<?> getValidClass(String className) {
        Class<?> clazz;
        try {
            List<String> classList = new ArrayList<>();
            Rep.VALUE_MAP.keySet().forEach(key -> {
                classList.add(key.getName());
            });
            if (classList.contains(className)) {
                clazz = Class.forName(className);
            } else {
                clazz = Class.forName("java.lang.Object");
            }
            logger.debug("Class parameter for sql is: " + clazz.getName());
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException(e);
        }
        return clazz;
    }

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    private static PreparedContext createPreparedContext(String project, String sql) throws Exception {
        Connection conn = QueryConnection.getConnection(project);
        PreparedStatement preparedStatement = conn.prepareStatement(sql);
        Collection<OLAPContext> olapContexts = OLAPContext.getThreadLocalContexts();
        // If the preparedContext is first initialized, then set the borrowed tag to false
        for (OLAPContext olapContext : olapContexts) {
            olapContext.isBorrowedContext = false;
        }
        return new PreparedContext(conn, preparedStatement, olapContexts);
    }

    private static class QueryRecordSerializer implements Serializer<QueryRecord> {

        private static final QueryRecordSerializer serializer = new QueryRecordSerializer();

        QueryRecordSerializer() {

        }

        public static QueryRecordSerializer getInstance() {
            return serializer;
        }

        @Override
        public void serialize(QueryRecord record, DataOutputStream out) throws IOException {
            String jsonStr = JsonUtil.writeValueAsString(record);
            out.writeUTF(jsonStr);
        }

        @Override
        public QueryRecord deserialize(DataInputStream in) throws IOException {
            String jsonStr = in.readUTF();
            return JsonUtil.readValue(jsonStr, QueryRecord.class);
        }
    }

    private static class PreparedContextFactory
            extends BaseKeyedPooledObjectFactory<PreparedContextKey, PreparedContext> {

        @Override
        public PreparedContext create(PreparedContextKey key) throws Exception {
            return createPreparedContext(key.project, key.sql);
        }

        @Override
        public PooledObject<PreparedContext> wrap(PreparedContext value) {
            return new DefaultPooledObject<>(value);
        }

        @Override
        public void destroyObject(final PreparedContextKey key, final PooledObject<PreparedContext> p) {
            PreparedContext cachedContext = p.getObject();
            cachedContext.close();
        }

        @Override
        public boolean validateObject(final PreparedContextKey key, final PooledObject<PreparedContext> p) {
            return true;
        }
    }

    private static class PreparedContextKey {
        private String project;
        private long prjLastModifyTime;
        private String sql;

        public PreparedContextKey(String project, long prjLastModifyTime, String sql) {
            this.project = project;
            this.prjLastModifyTime = prjLastModifyTime;
            this.sql = sql;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PreparedContextKey that = (PreparedContextKey) o;

            if (prjLastModifyTime != that.prjLastModifyTime)
                return false;
            if (project != null ? !project.equals(that.project) : that.project != null)
                return false;
            return sql != null ? sql.equals(that.sql) : that.sql == null;

        }

        @Override
        public int hashCode() {
            int result = project != null ? project.hashCode() : 0;
            result = 31 * result + (int) (prjLastModifyTime ^ (prjLastModifyTime >>> 32));
            result = 31 * result + (sql != null ? sql.hashCode() : 0);
            return result;
        }
    }

    private static class PreparedContext {
        private Connection conn;
        private PreparedStatement preparedStatement;
        private Collection<OLAPContext> olapContexts;
        private Object olapRel;
        private Object resultType;

        public PreparedContext(Connection conn, PreparedStatement preparedStatement,
                Collection<OLAPContext> olapContexts) {
            this.conn = conn;
            this.preparedStatement = preparedStatement;
            this.olapContexts = olapContexts;
        }

        public void close() {
            if (conn != null) {
                DBUtils.closeQuietly(conn);
            }
            if (preparedStatement != null) {
                DBUtils.closeQuietly(preparedStatement);
            }
        }
    }

}

@SuppressWarnings("serial")
class QueryRecord extends RootPersistentEntity {

    @JsonProperty()
    private Query[] queries;

    public QueryRecord() {

    }

    public QueryRecord(Query[] queries) {
        this.queries = queries;
    }

    public Query[] getQueries() {
        return queries;
    }

    public void setQueries(Query[] queries) {
        this.queries = queries;
    }

}
