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

import static org.apache.kylin.common.QueryTrace.GET_ACL_INFO;
import static org.apache.kylin.common.QueryTrace.SPARK_JOB_EXECUTION;
import static org.apache.kylin.common.exception.ServerErrorCode.ACCESS_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_EXCEEDED_CONCURRENT_LIMIT;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_QUERY_REJECTED;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_SQL_EXPRESSION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.SAVE_QUERY_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeSystem.JOB_NODE_QUERY_API_INVALID;
import static org.apache.kylin.common.util.CheckUtil.checkCondition;
import static org.springframework.security.acls.domain.BasePermission.ADMINISTRATION;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.ForceToTieredStorage;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.NewQueryRefuseException;
import org.apache.kylin.common.exception.ResourceLimitExceededException;
import org.apache.kylin.common.hystrix.NCircuitBreaker;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.engine.spark.filter.BloomFilterSkipCollector;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.collect.Collections2;
import org.apache.kylin.guava30.shaded.common.collect.HashMultimap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.SetMultimap;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.BigQueryThresholdUpdater;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistorySql;
import org.apache.kylin.metadata.query.QueryHistorySqlParam;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.metadata.query.StructField;
import org.apache.kylin.metadata.query.util.QueryHistoryUtil;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.ColumnMetaWithType;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.apache.kylin.query.blacklist.SQLBlacklistItem;
import org.apache.kylin.query.blacklist.SQLBlacklistManager;
import org.apache.kylin.query.calcite.KEDialect;
import org.apache.kylin.query.engine.AsyncQueryJob;
import org.apache.kylin.query.engine.PrepareSqlStateParam;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.QueryRoutingEngine;
import org.apache.kylin.query.engine.SchemaMetaData;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.engine.data.TableSchema;
import org.apache.kylin.query.exception.NotSupportedSQLException;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryLimiter;
import org.apache.kylin.query.util.QueryModelPriorities;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.RawSql;
import org.apache.kylin.query.util.RawSqlParser;
import org.apache.kylin.query.util.SlowQueryDetector;
import org.apache.kylin.query.util.TokenMgrError;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.config.AppConfig;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.response.SQLResponseTrace;
import org.apache.kylin.rest.response.TableMetaCacheResult;
import org.apache.kylin.rest.response.TableMetaCacheResultV2;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.query.util.PrepareSQLUtils;
import org.apache.kylin.rest.util.QueryCacheSignatureUtil;
import org.apache.kylin.rest.util.QueryRequestLimits;
import org.apache.kylin.rest.util.QueryUtils;
import org.apache.kylin.rest.util.SparderUIUtil;
import org.apache.kylin.rest.util.TableauInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService extends BasicService implements CacheSignatureQuerySupporter {

    public static final String QUERY_STORE_PATH_PREFIX = "/query/";
    private static final String JDBC_METADATA_SCHEMA = "metadata";
    private static final Logger logger = LoggerFactory.getLogger(LogConstant.QUERY_CATEGORY);
    final SlowQueryDetector slowQueryDetector = new SlowQueryDetector();

    @Autowired
    private QueryCacheManager queryCacheManager;

    @Autowired
    protected AclEvaluate aclEvaluate;

    @Autowired
    private AccessService accessService;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private AppConfig appConfig;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRServiceSupporter aclTCRService;

    protected QueryRoutingEngine queryRoutingEngine;

    public QueryService() {
        slowQueryDetector.start();
        queryRoutingEngine = new QueryRoutingEngine();
    }

    private static String getQueryKeyById(String project, String creator) {
        return "/" + project + QUERY_STORE_PATH_PREFIX + creator + MetadataConstants.FILE_SURFIX;
    }

    public ForceToTieredStorage getForcedToTieredStorage(String project, ForceToTieredStorage api) {
        switch (api) {
        case CH_FAIL_TO_DFS:
        case CH_FAIL_TO_PUSH_DOWN:
        case CH_FAIL_TO_RETURN:
            return api;
        case CH_FAIL_TAIL:
            break;
        default:
            // Do nothing
        }

        try {
            //project level config
            api = NProjectManager.getProjectConfig(project).getProjectForcedToTieredStorage();
            switch (api) {
            case CH_FAIL_TO_DFS:
            case CH_FAIL_TO_PUSH_DOWN:
            case CH_FAIL_TO_RETURN:
                return api;
            default:
                // Do nothing
            }
        } catch (Exception e) {
            // no define or invalid do nothing
        }

        try {
            //system
            api = KylinConfig.getInstanceFromEnv().getSystemForcedToTieredStorage();
            switch (api) {
            case CH_FAIL_TO_DFS:
            case CH_FAIL_TO_PUSH_DOWN:
            case CH_FAIL_TO_RETURN:
                return api;
            default:
                return ForceToTieredStorage.CH_FAIL_TO_DFS;
            }
        } catch (Exception e) {
            return ForceToTieredStorage.CH_FAIL_TO_DFS;
        }
    }

    public SQLResponse query(SQLRequest sqlRequest) throws Exception {
        try {
            slowQueryDetector.queryStart(sqlRequest.getStopId());
            markHighPriorityQueryIfNeeded();

            QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(sqlRequest.getProject()),
                    sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(), true,
                    sqlRequest.getExecuteAs());
            queryParams.setForcedToPushDown(sqlRequest.isForcedToPushDown());
            queryParams.setForcedToIndex(sqlRequest.isForcedToIndex());
            queryParams.setPrepareStatementWithParams(QueryUtils.isPrepareStatementWithParams(sqlRequest));
            queryParams.setPartialMatchIndex(sqlRequest.isPartialMatchIndex());
            queryParams.setAcceptPartial(sqlRequest.isAcceptPartial());
            queryParams.setSelect(true);

            if (queryParams.isPrepareStatementWithParams()) {
                queryParams.setPrepareSql(PrepareSQLUtils.fillInParams(queryParams.getSql(),
                        ((PrepareSqlRequest) sqlRequest).getParams()));
                queryParams.setParams(((PrepareSqlRequest) sqlRequest).getParams());
            }
            queryParams.setAclInfo(getExecuteAclInfo(queryParams.getProject(), queryParams.getExecuteAs()));
            queryParams.setACLDisabledOrAdmin(isACLDisabledOrAdmin(queryParams.getProject(), queryParams.getAclInfo()));
            int forcedToTieredStorage;
            ForceToTieredStorage enumForcedToTieredStorage;
            try {
                forcedToTieredStorage = sqlRequest.getForcedToTieredStorage();
                enumForcedToTieredStorage = getForcedToTieredStorage(sqlRequest.getProject(),
                        ForceToTieredStorage.values()[forcedToTieredStorage]);
            } catch (NullPointerException e) {
                enumForcedToTieredStorage = getForcedToTieredStorage(sqlRequest.getProject(),
                        ForceToTieredStorage.CH_FAIL_TAIL);
            }
            logger.debug("forcedToTieredStorage={}", enumForcedToTieredStorage);
            queryParams.setForcedToTieredStorage(enumForcedToTieredStorage);
            QueryContext.current().setForcedToTieredStorage(enumForcedToTieredStorage);
            QueryContext.current().setForceTableIndex(queryParams.isForcedToIndex());

            if (QueryContext.current().getQueryTagInfo().isAsyncQuery()
                    && NProjectManager.getProjectConfig(sqlRequest.getProject()).isUniqueAsyncQueryYarnQueue()) {
                logger.info("This query is an async query in project: {}", sqlRequest.getProject());
                if (StringUtils.isNotEmpty(sqlRequest.getSparkQueue())) {
                    queryParams.setSparkQueue(sqlRequest.getSparkQueue());
                }
                AsyncQueryJob asyncQueryJob = new AsyncQueryJob();
                asyncQueryJob.setProject(queryParams.getProject());
                slowQueryDetector.addJobIdForAsyncQueryJob(asyncQueryJob.getId());
                ExecuteResult result = asyncQueryJob.submit(queryParams);
                if (!result.succeed()) {
                    throw (Exception) result.getThrowable();

                }
                return buildSqlResponse(false, Collections.emptyList(), 0, Lists.newArrayList(),
                        sqlRequest.getProject());
            }

            SQLResponse fakeResponse = TableauInterceptor.tableauIntercept(queryParams.getSql());
            if (null != fakeResponse) {
                logger.debug("Return fake response, is exception? {}", fakeResponse.isException());
                fakeResponse.setEngineType(QueryHistory.EngineType.CONSTANTS.name());
                QueryContext.current().getQueryTagInfo().setConstantQuery(true);
                QueryContext.currentTrace().startSpan(SPARK_JOB_EXECUTION);
                return fakeResponse;
            }

            QueryResult result = queryRoutingEngine.queryWithSqlMassage(queryParams);
            if (!QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
                QueryContext.current().getMetrics().setResultRowCount(result.getSize());
            }
            return buildSqlResponse(QueryContext.current().getQueryTagInfo().isPushdown(), result.getRowsIterable(),
                    result.getSize(), result.getColumnMetas(), sqlRequest.getProject());
        } finally {
            slowQueryDetector.queryEnd();
            Thread.interrupted(); //reset if interrupted
        }
    }

    public void stopQuery(String id) {
        slowQueryDetector.stopQuery(id);
    }

    private void markHighPriorityQueryIfNeeded() {
        String vipRoleName = KylinConfig.getInstanceFromEnv().getQueryVIPRole();
        if (!StringUtils.isBlank(vipRoleName) && SecurityContextHolder.getContext() //
                .getAuthentication() //
                .getAuthorities() //
                .contains(new SimpleGrantedAuthority(vipRoleName))) { //
            QueryContext.current().getQueryTagInfo().setHighPriorityQuery(true);
        }
    }

    @Transaction(project = 1)
    public void saveQuery(final String creator, final String project, final Query query) throws IOException {
        aclEvaluate.checkProjectQueryPermission(project);
        Message msg = MsgPicker.getMsg();
        val record = getSavedQueries(creator, project);
        List<Query> currentQueries = record.getQueries();
        if (currentQueries.stream().map(Query::getName).collect(Collectors.toSet()).contains(query.getName()))
            throw new KylinException(SAVE_QUERY_FAILED,
                    String.format(Locale.ROOT, msg.getDuplicateQueryName(), query.getName()));

        currentQueries.add(query);
        getStore().checkAndPutResource(getQueryKeyById(project, creator), record, QueryRecordSerializer.getInstance());
    }

    @Transaction(project = 1)
    public void removeSavedQuery(final String creator, final String project, final String id) throws IOException {
        aclEvaluate.checkProjectQueryPermission(project);
        val record = getSavedQueries(creator, project);
        record.setQueries(record.getQueries().stream().filter(q -> !q.getId().equals(id)).collect(Collectors.toList()));
        getStore().checkAndPutResource(getQueryKeyById(project, creator), record, QueryRecordSerializer.getInstance());
    }

    public QueryRecord getSavedQueries(final String creator, final String project) throws IOException {
        aclEvaluate.checkProjectQueryPermission(project);
        if (null == creator) {
            return null;
        }
        QueryRecord record = getStore().getResource(getQueryKeyById(project, creator),
                QueryRecordSerializer.getInstance());
        if (record == null) {
            return new QueryRecord();
        }
        val resource = getStore().getResource(getQueryKeyById(project, creator));
        val copy = JsonUtil.deepCopy(record, QueryRecord.class);
        copy.setMvcc(resource.getMvcc());
        return copy;
    }

    public String logQuery(final SQLRequest request, final SQLResponse response) {
        final String user = aclEvaluate.getCurrentUserName();
        Collection<String> modelNames = Lists.newArrayList();
        Collection<String> layoutIds = Lists.newArrayList();
        Collection<String> isPartialMatchModel = Lists.newArrayList();
        float duration = response.getDuration() / (float) 1000;

        if (CollectionUtils.isNotEmpty(response.getNativeRealizations())) {
            modelNames = response.getNativeRealizations().stream().map(NativeQueryRealization::getModelAlias)
                    .collect(Collectors.toList());
            layoutIds = Collections2.transform(response.getNativeRealizations(),
                    realization -> String.valueOf(realization.getLayoutId()));
            isPartialMatchModel = Collections2.transform(response.getNativeRealizations(),
                    realization -> String.valueOf(realization.isPartialMatchModel()));
        }

        int resultRowCount = 0;
        if (!response.isException() && response.getResults() != null) {
            resultRowCount = (int) response.getResultRowCount();
        }
        String sql = QueryContext.current().getUserSQL();
        if (StringUtils.isEmpty(sql))
            sql = request.getSql();

        Collection<String> snapShots;
        Collection<String> snapShotFilters;
        if (response.getNativeRealizations() == null) {
            snapShots = Lists.newArrayList();
            snapShotFilters = Lists.newArrayList();
        } else {
            snapShots = response.getNativeRealizations().stream()
                    .flatMap(nativeQueryRealization -> nativeQueryRealization.getSnapshots().stream()).distinct()
                    .collect(Collectors.toList());
            snapShotFilters = ContextUtil
                    .listContexts().stream().flatMap(ctx -> ctx.filterColumns.stream()
                            .filter(col -> snapShots.contains(col.getTable())).map(TblColRef::getCanonicalName))
                    .collect(Collectors.toList());
        }
        boolean isDerived = !snapShots.isEmpty() && layoutIds.stream().anyMatch(id -> !StringUtils.equals("-1", id));

        String errorMsg = response.getExceptionMessage();
        if (StringUtils.isNotBlank(errorMsg)) {
            int maxLength = 5000;
            errorMsg = errorMsg.length() > maxLength ? errorMsg.substring(0, maxLength) : errorMsg;
        }

        BloomFilterSkipCollector.logAndCleanStatus(QueryContext.current().getQueryId());
        LogReport report = new LogReport().put(LogReport.QUERY_ID, QueryContext.current().getQueryId())
                .put(LogReport.SQL, sql).put(LogReport.USER, user)
                .put(LogReport.SUCCESS, null == response.getExceptionMessage()).put(LogReport.DURATION, duration)
                .put(LogReport.PROJECT, request.getProject()).put(LogReport.REALIZATION_NAMES, modelNames)
                .put(LogReport.INDEX_LAYOUT_IDS, layoutIds).put(LogReport.IS_PARTIAL_MATCH_MODEL, isPartialMatchModel)
                .put(LogReport.SCAN_ROWS, response.getScanRows())
                .put(LogReport.TOTAL_SCAN_ROWS, response.getTotalScanRows()).put(LogReport.IS_DERIVED, isDerived)
                .put(LogReport.SNAPSHOTS, snapShots).put(LogReport.SNAPSHOT_FILTERS, snapShotFilters)
                .put(LogReport.SCAN_BYTES, response.getScanBytes())
                .put(LogReport.TOTAL_SCAN_BYTES, response.getTotalScanBytes())
                .put(LogReport.RESULT_ROW_COUNT, resultRowCount)
                .put(LogReport.SHUFFLE_PARTITIONS, response.getShufflePartitions())
                .put(LogReport.ACCEPT_PARTIAL, request.isAcceptPartial())
                .put(LogReport.PARTIAL_RESULT, response.isPartial())
                .put(LogReport.HIT_EXCEPTION_CACHE, response.isHitExceptionCache())
                .put(LogReport.STORAGE_CACHE_USED, response.isStorageCacheUsed())
                .put(LogReport.STORAGE_CACHE_TYPE, response.getStorageCacheType())
                .put(LogReport.PUSH_DOWN, response.isQueryPushDown()).put(LogReport.IS_PREPARE, response.isPrepare())
                .put(LogReport.TIMEOUT, response.isTimeout())
                .put(LogReport.TIMELINE_SCHEMA, QueryContext.current().getSchema())
                .put(LogReport.TIMELINE, QueryContext.current().getTimeLine()).put(LogReport.ERROR_MSG, errorMsg)
                .put(LogReport.USER_TAG, request.getUser_defined_tag())
                .put(LogReport.PUSH_DOWN_FORCED, request.isForcedToPushDown())
                .put(LogReport.INDEX_FORCED, request.isForcedToIndex())
                .put(LogReport.USER_AGENT, request.getUserAgent())
                .put(LogReport.BACK_DOOR_TOGGLES, request.getBackdoorToggles())
                .put(LogReport.SCAN_SEGMENT_COUNT, QueryContext.current().getMetrics().getSegCount())
                .put(LogReport.SCAN_FILE_COUNT, QueryContext.current().getMetrics().getFileCount())
                .put(LogReport.REFUSE, response.isRefused());
        String log = report.oldStyleLog();
        if (!(QueryContext.current().getQueryTagInfo().isAsyncQuery()
                && NProjectManager.getProjectConfig(request.getProject()).isUniqueAsyncQueryYarnQueue())) {
            logger.info(log);
            logger.debug(report.jsonStyleLog());
            if (request.getExecuteAs() != null)
                logger.info("[EXECUTE AS USER]: User [{}] executes the sql as user [{}].", user,
                        request.getExecuteAs());
        }
        return log;
    }

    public SQLResponse queryWithCache(SQLRequest sqlRequest) {
        aclEvaluate.checkProjectQueryPermission(sqlRequest.getProject());
        checkIfExecuteUserValid(sqlRequest);
        final QueryContext queryContext = QueryContext.current();
        queryContext.setProject(sqlRequest.getProject());
        queryContext.setLimit(sqlRequest.getLimit());
        queryContext.setOffset(sqlRequest.getOffset());
        if (StringUtils.isNotEmpty(sqlRequest.getQueryId())) {
            // validate queryId with UUID.fromString
            queryContext.setQueryId(UUID.fromString(sqlRequest.getQueryId()).toString());
        }
        try (SetThreadName ignored = new SetThreadName("Query %s", queryContext.getQueryId());
                SetLogCategory ignored2 = new SetLogCategory(LogConstant.QUERY_CATEGORY)) {
            logger.info("Start query in project: {}", sqlRequest.getProject());
            if (sqlRequest.getExecuteAs() != null)
                sqlRequest.setUsername(sqlRequest.getExecuteAs());
            else
                sqlRequest.setUsername(getUsername());
            QueryLimiter.tryAcquire();
            SQLResponse response = doQueryWithCache(sqlRequest);
            response.setTraces(QueryContext.currentTrace().spans().stream().map(span -> {
                if (QueryTrace.PREPARE_AND_SUBMIT_JOB.equals(span.getName())) {
                    return new SQLResponseTrace(QueryTrace.SPARK_JOB_EXECUTION,
                            QueryTrace.SPAN_GROUPS.get(QueryTrace.SPARK_JOB_EXECUTION), span.getDuration());
                } else {
                    return new SQLResponseTrace(span.getName(), span.getGroup(), span.getDuration());
                }
            }).collect(Collectors.toList()));
            if (null == response.getExceptionMessage()) {
                removeExceptionCache(sqlRequest);
            }
            return response;
        } finally {
            QueryLimiter.release();
            QueryContext.current().close();
        }
    }

    private void checkIfExecuteUserValid(SQLRequest sqlRequest) {
        String executeUser = sqlRequest.getExecuteAs();
        if (executeUser == null)
            return;
        if (!KylinConfig.getInstanceFromEnv().isExecuteAsEnabled()) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getExecuteAsNotEnabled());
        }
        // check whether service account has all read privileges
        final AclTCRManager aclTCRManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(),
                sqlRequest.getProject());
        String username = AclPermissionUtil.getCurrentUsername();
        Set<String> groups = getCurrentUserGroups();
        if (!AclPermissionUtil.hasProjectAdminPermission(sqlRequest.getProject(), groups)
                && !aclTCRManager.isAllTablesAuthorized(username, groups))
            throw new KylinException(PERMISSION_DENIED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getServiceAccountNotAllowed(), username, sqlRequest.getProject()));

        // check whether execute user has project read permission
        List<String> grantedProjects;
        try {
            grantedProjects = accessService.getGrantedProjectsOfUser(executeUser);
        } catch (IOException e) {
            throw new KylinException(ACCESS_DENIED, e);
        }
        if (!grantedProjects.contains(sqlRequest.getProject())) {
            throw new KylinException(ACCESS_DENIED, "Access is denied.");
        }
    }

    private void checkSqlRequest(SQLRequest sqlRequest) {
        Message msg = MsgPicker.getMsg();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isQueryNode()) {
            throw new KylinException(JOB_NODE_QUERY_API_INVALID);
        }
        checkSqlRequestProject(sqlRequest, msg);
        if (NProjectManager.getInstance(kylinConfig).getProject(sqlRequest.getProject()) == null) {
            throw new KylinException(PROJECT_NOT_EXIST, sqlRequest.getProject());
        }
        if (StringUtils.isBlank(sqlRequest.getSql())) {
            throw new KylinException(EMPTY_SQL_EXPRESSION, msg.getNullEmptySql());
        }
    }

    private void checkSqlRequestProject(SQLRequest sqlRequest, Message msg) {
        if (StringUtils.isBlank(sqlRequest.getProject())) {
            throw new KylinException(EMPTY_PROJECT_NAME, msg.getEmptyProjectName());
        }
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest) {
        checkSqlRequest(sqlRequest);

        if (sqlRequest.getBackdoorToggles() != null)
            BackdoorToggles.addToggles(sqlRequest.getBackdoorToggles());

        QueryContext queryContext = QueryContext.current();
        QueryMetricsContext.start(queryContext.getQueryId(), getDefaultServer());

        final String project = sqlRequest.getProject();
        SQLResponse sqlResponse = null;
        try {
            QueryContext.currentTrace().startSpan(GET_ACL_INFO);
            queryContext.setAclInfo(getExecuteAclInfo(project, sqlRequest.getExecuteAs()));
            QueryContext.currentTrace().startSpan(QueryTrace.SQL_TRANSFORMATION);
            queryContext.getMetrics().setServer(clusterManager.getLocalServer());
            queryContext.setProject(project);

            KylinConfig kylinConfig = NProjectManager.getProjectConfig(project);
            // Parsing user sql by RawSqlParser
            RawSql rawSql = new RawSqlParser(sqlRequest.getSql()).parse();
            rawSql.autoAppendLimit(kylinConfig, sqlRequest.getLimit(), sqlRequest.getOffset());

            // Reset request sql for code compatibility
            sqlRequest.setSql(rawSql.getStatementString());
            // Set user sql for log & record purpose
            queryContext.setUserSQL(rawSql.getFullTextString());

            // Apply model priority if provided
            applyModelPriority(queryContext, rawSql.getFullTextString());

            // Apply sql black list check if matching
            applyQuerySqlBlacklist(project, rawSql.getStatementString());

            // convert CREATE ... to WITH ...
            sqlResponse = QueryUtils.handleTempStatement(sqlRequest, kylinConfig);

            // search cache
            if (sqlResponse == null && kylinConfig.isQueryCacheEnabled() && !sqlRequest.isForcedToPushDown()) {
                sqlResponse = searchCache(sqlRequest, kylinConfig);
            }

            // real execution if required
            if (sqlResponse == null) {
                try (QueryRequestLimits ignored = new QueryRequestLimits(project)) {
                    sqlResponse = queryAndUpdateCache(sqlRequest, kylinConfig);
                }
            }

            QueryUtils.updateQueryContextSQLMetrics(rawSql.getStatementString());
            QueryContext.currentTrace().amendLast(QueryTrace.PREPARE_AND_SUBMIT_JOB, System.currentTimeMillis());
            QueryContext.currentTrace().endLastSpan();
            QueryContext.current().record("update_metrics_time");
            QueryContext.currentMetrics().setQueryEndTime(System.currentTimeMillis());

            sqlResponse.setServer(clusterManager.getLocalServer());
            sqlResponse.setQueryId(QueryContext.current().getQueryId());
            if (sqlResponse.isStorageCacheUsed() || sqlResponse.isHitExceptionCache()) {
                sqlResponse.setDuration(0);
            } else {
                sqlResponse.setDuration(QueryContext.currentMetrics().duration());
            }
            logQuery(sqlRequest, sqlResponse);

            addToQueryHistory(sqlRequest, sqlResponse, rawSql.getFullTextString());

            if (isCollectQueryScanRowsAndTimeEnabled()) {
                BigQueryThresholdUpdater.collectQueryScanRowsAndTime(QueryContext.currentMetrics().duration(),
                        QueryContext.currentMetrics().getTotalScanRows());
            }

            val fusionManager = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    sqlRequest.getProject());
            if (CollectionUtils.isNotEmpty(sqlResponse.getNativeRealizations())) {
                sqlResponse.getNativeRealizations().stream()
                        .forEach(realization -> realization.setModelId(fusionManager.getModelId(realization)));
            }
            //check query result row count
            NCircuitBreaker.verifyQueryResultRowCount(sqlResponse.getResultRowCount());

            return sqlResponse;

        } catch (Exception e) {
            QueryContext.current().getMetrics().setException(true);
            if (sqlResponse != null) {
                sqlResponse.setException(true);
                sqlResponse.setExceptionMessage(e.getMessage());
                sqlResponse.setResults(null);
                return sqlResponse;
            } else {
                return new SQLResponse(null, null, 0, true, e.getMessage());
            }
        } catch (TokenMgrError t) {
            QueryContext.current().getMetrics().setException(true);
            return new SQLResponse(null, null, 0, true, t.getMessage());
        } finally {
            BackdoorToggles.cleanToggles();
            if (QueryMetricsContext.isStarted()) {
                QueryMetricsContext.reset();
            }
        }
    }

    public boolean isCollectQueryScanRowsAndTimeEnabled() {
        return KapConfig.getInstanceFromEnv().isAutoAdjustBigQueryRowsThresholdEnabled()
                && !QueryContext.current().getQueryTagInfo().isAsyncQuery()
                && !QueryContext.current().getQueryTagInfo().isStorageCacheUsed();
    }

    protected SQLResponse searchCache(SQLRequest sqlRequest, KylinConfig kylinConfig) {
        SQLResponse response = searchFailedCache(sqlRequest, kylinConfig);
        if (response == null) {
            response = searchSuccessCache(sqlRequest);
        }
        if (response != null) {
            response.setDuration(0);
            collectToQueryContext(response);
            QueryContext.currentTrace().clear();
            QueryContext.currentTrace().startSpan(QueryTrace.HIT_CACHE);
            QueryContext.currentTrace().endLastSpan();
        }
        return response;
    }

    private SQLResponse searchFailedCache(SQLRequest sqlRequest, KylinConfig kylinConfig) {
        SQLResponse response = queryCacheManager.getFromExceptionCache(sqlRequest);
        if (response != null && isFailTimesExceedThreshold(response, kylinConfig)) {
            logger.info("The sqlResponse is found in EXCEPTION_QUERY_CACHE");
            response.setHitExceptionCache(true);
            QueryContext.current().getMetrics().setException(true);
            QueryContext.current().getMetrics().setFinalCause(response.getThrowable());
            QueryContext.current().getMetrics().setQueryMsg(response.getExceptionMessage());
            return response;
        }
        return null;
    }

    private SQLResponse searchSuccessCache(SQLRequest sqlRequest) {
        SQLResponse response = queryCacheManager.searchSuccessCache(sqlRequest);
        if (response != null) {
            logger.info("The sqlResponse is found in SUCCESS_QUERY_CACHE");
            response.setStorageCacheUsed(true);
        }
        return response;
    }

    private void addToQueryHistory(SQLRequest sqlRequest, SQLResponse sqlResponse, String originalSql) {
        if (!(QueryContext.current().getQueryTagInfo().isAsyncQuery()
                && NProjectManager.getProjectConfig(sqlRequest.getProject()).isUniqueAsyncQueryYarnQueue())) {
            try {
                if (!sqlResponse.isPrepare() && QueryMetricsContext.isStarted()) {
                    val queryMetricsContext = QueryMetricsContext.collect(QueryContext.current());
                    // KE-35556 Set stored sql a structured format json string
                    queryMetricsContext.setSql(constructQueryHistorySqlText(sqlRequest, sqlResponse, originalSql));
                    // KE-36662 Using sql_pattern as normalized_sql storage
                    String normalizedSql = QueryContext.currentMetrics().getCorrectedSql();
                    queryMetricsContext.setSqlPattern(normalizedSql);
                    QueryHistoryScheduler queryHistoryScheduler = QueryHistoryScheduler.getInstance();
                    queryHistoryScheduler.offerQueryHistoryQueue(queryMetricsContext);
                    EventBusFactory.getInstance().postAsync(queryMetricsContext);
                }
            } catch (Throwable th) {
                logger.warn("Write metric error.", th);
            }
        }
    }

    private String constructQueryHistorySqlText(SQLRequest sqlRequest, SQLResponse sqlResponse, String originalSql)
            throws JsonProcessingException, ClassNotFoundException {
        // Fill in params if available
        QueryUtils.fillInPrepareStatParams(sqlRequest, sqlResponse.isQueryPushDown());

        List<QueryHistorySqlParam> params = null;
        if (QueryUtils.isPrepareStatementWithParams(sqlRequest)) {
            params = new ArrayList<>();
            PrepareSqlStateParam[] requestParams = ((PrepareSqlRequest) sqlRequest).getParams();
            for (int i = 0; i < requestParams.length; i++) {
                PrepareSqlStateParam p = requestParams[i];
                String dataType = QueryHistoryUtil.toDataType(p.getClassName());
                QueryHistorySqlParam param = new QueryHistorySqlParam(i + 1, p.getClassName(), dataType, p.getValue());
                params.add(param);
            }
        }

        // KE-36662 Do not store normalized_sql in sql_text, as it may exceed storage limitation
        return QueryHistoryUtil.toQueryHistorySqlText(new QueryHistorySql(originalSql, null, params));
    }

    private void collectToQueryContext(SQLResponse sqlResponse) {
        QueryContext queryContext = QueryContext.current();
        if (sqlResponse.getEngineType() != null) {
            queryContext.setEngineType(sqlResponse.getEngineType());
        }
        queryContext.getMetrics().setScanBytes(sqlResponse.getScanBytes());
        queryContext.getMetrics().setScanRows(sqlResponse.getScanRows());
        queryContext.getMetrics().setResultRowCount(sqlResponse.getResultRowCount());

        List<QueryContext.NativeQueryRealization> nativeQueryRealizationList = Lists.newArrayList();
        if (sqlResponse.getNativeRealizations() != null) {
            for (NativeQueryRealization nqReal : sqlResponse.getNativeRealizations()) {
                nativeQueryRealizationList.add(new QueryContext.NativeQueryRealization(nqReal.getModelId(),
                        nqReal.getModelAlias(), nqReal.getLayoutId(), nqReal.getIndexType(),
                        nqReal.isPartialMatchModel(), nqReal.isValid(), nqReal.isLayoutExist(),
                        nqReal.isStreamingLayout(), nqReal.getSnapshots()));
            }
        }
        queryContext.setNativeQueryRealizationList(nativeQueryRealizationList);
    }

    private String getDefaultServer() {
        return AddressUtil.getLocalHostExactAddress() + ":" + appConfig.getPort();
    }

    @VisibleForTesting
    protected SQLResponse queryAndUpdateCache(SQLRequest sqlRequest, KylinConfig kylinConfig) {
        boolean queryCacheEnabled = isQueryCacheEnabled(kylinConfig);
        SQLResponse sqlResponse;
        try {
            final boolean isSelect = QueryUtil.isSelectStatement(sqlRequest.getSql());
            if (isSelect) {
                sqlResponse = query(sqlRequest);
            } else {
                throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getnotSupportedSql());
            }
            if (checkCondition(queryCacheEnabled, "query cache is disabled")) {
                // set duration for caching condition checking
                sqlResponse.setDuration(QueryContext.currentMetrics().duration());
                queryCacheManager.cacheSuccessQuery(sqlRequest, sqlResponse);
            }
        } catch (Throwable e) { // calcite may throw AssertError
            logger.error("Exception while executing query", e);
            QueryContext.current().getMetrics().setException(true);
            String errMsg = makeErrorMsgUserFriendly(e);
            if (errMsg == null) {
                // in case makeErrorMsgUserFriendly fails to get a err msg, try e.getMessage()
                // in case getMessage is null as well, set the err msg as the full exception class name
                errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getCanonicalName();
            }
            sqlResponse = new SQLResponse(null, null, 0, true, errMsg, false, false);
            QueryContext queryContext = QueryContext.current();
            queryContext.getMetrics().setFinalCause(e);
            queryContext.getMetrics().setQueryMsg(errMsg);
            queryContext.getQueryTagInfo().setPushdown(false);

            if (e.getCause() != null && NewQueryRefuseException.causedByRefuse(e)) {
                queryContext.getQueryTagInfo().setRefused(true);
            }

            if (e.getCause() != null && KylinTimeoutException.causedByTimeout(e)) {
                queryContext.getQueryTagInfo().setTimeout(true);
            }

            if (UserStopQueryException.causedByUserStop(e)) {
                sqlResponse.setStopByUser(true);
                sqlResponse.setColumnMetas(Lists.newArrayList());
                sqlResponse.setExceptionMessage(MsgPicker.getMsg().getStopByUserErrorMessage());
            }

            sqlResponse.wrapResultOfQueryContext(queryContext);
            sqlResponse.setRefused(queryContext.getQueryTagInfo().isRefused());
            sqlResponse.setTimeout(queryContext.getQueryTagInfo().isTimeout());
            setAppMaterURL(sqlResponse);
            sqlResponse.setDuration(QueryContext.currentMetrics().duration());
            if (queryCacheEnabled && e.getCause() != null) {
                putIntoExceptionCache(sqlRequest, sqlResponse, e);
            }
        }
        return sqlResponse;
    }

    @VisibleForTesting
    public void putIntoExceptionCache(SQLRequest sqlRequest, SQLResponse sqlResponse, Throwable e) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // always cache ResourceLimitExceededException
        if (e.getCause() != null && ExceptionUtils.getRootCause(e) instanceof ResourceLimitExceededException) {
            queryCacheManager.cacheFailedQuery(sqlRequest, sqlResponse);
            return;
        }
        if (!isQueryExceptionCacheEnabled(kylinConfig)) {
            return;
        }
        if (noNeedCache(sqlResponse, kylinConfig, e)) {
            return;
        }
        SQLResponse cachedSqlResponse = queryCacheManager.getFromExceptionCache(sqlRequest);
        if (null == cachedSqlResponse) {
            sqlResponse.setFailTimes(1);
            queryCacheManager.putIntoExceptionCache(sqlRequest, sqlResponse);
        } else {
            int failCount = cachedSqlResponse.getFailTimes();
            sqlResponse.setFailTimes(failCount + 1);
            queryCacheManager.updateIntoExceptionCache(sqlRequest, sqlResponse);
        }
    }

    private boolean noNeedCache(SQLResponse sqlResponse, KylinConfig kylinConfig, Throwable e) {
        if (sqlResponse.getDuration() < kylinConfig.getQueryExceptionCacheThresholdDuration()) {
            logger.info("Query duration has not exceed threshold, will not cache.");
            return true;
        }
        Throwable rootCause = ExceptionUtils.getRootCause(e);
        return rootCause instanceof NoRealizationFoundException || rootCause instanceof RoutingIndicatorException
                || rootCause instanceof NotSupportedSQLException || rootCause instanceof SqlValidatorException;
    }

    private boolean isFailTimesExceedThreshold(SQLResponse sqlResponse, KylinConfig kylinConfig) {
        int failTimes = sqlResponse.getFailTimes();
        // always return cached ResourceLimitExceededException
        if (failTimes < 0) {
            return true;
        }
        return kylinConfig.isQueryExceptionCacheEnabled()
                && failTimes >= kylinConfig.getQueryExceptionCacheThresholdTimes();
    }

    private void removeExceptionCache(SQLRequest sqlRequest) {
        if (!isQueryExceptionCacheEnabled(KylinConfig.getInstanceFromEnv())) {
            return;
        }
        if (null == queryCacheManager.getFromExceptionCache(sqlRequest)) {
            return;
        }
        if (!queryCacheManager.getCache().remove(CommonQueryCacheSupporter.Type.EXCEPTION_QUERY_CACHE.rootCacheName,
                sqlRequest.getProject(), sqlRequest.getCacheKey())) {
            logger.info("Remove cache failed");
        }
    }

    private boolean isQueryCacheEnabled(KylinConfig kylinConfig) {
        return checkCondition(kylinConfig.isQueryCacheEnabled(), "query cache disabled in KylinConfig") //
                && checkCondition(!BackdoorToggles.getDisableCache(), "query cache disabled in BackdoorToggles");
    }

    private boolean isQueryExceptionCacheEnabled(KylinConfig kylinConfig) {
        return checkCondition(kylinConfig.isQueryExceptionCacheEnabled(),
                "query exception cache disabled in KylinConfig") //
                && checkCondition(!BackdoorToggles.getDisableCache(), "query cache disabled in BackdoorToggles");
    }

    private void applyQuerySqlBlacklist(String project, String sql) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isQueryBlacklistEnabled()) {
            return;
        }
        SQLBlacklistItem sqlBlacklistItem = matchSqlBlacklist(project, sql);
        if (null == sqlBlacklistItem) {
            return;
        }
        int concurrentLimit = sqlBlacklistItem.getConcurrentLimit();
        if (concurrentLimit == 0) {
            throw new KylinException(BLACKLIST_QUERY_REJECTED, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSqlBlacklistQueryRejected(), sqlBlacklistItem.getId()));
        }
        if (getSqlConcurrentCount(sqlBlacklistItem) >= concurrentLimit) {
            throw new KylinException(BLACKLIST_EXCEEDED_CONCURRENT_LIMIT,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSqlBlackListQueryConcurrentLimitExceeded(),
                            sqlBlacklistItem.getId(), concurrentLimit));
        }
    }

    private SQLBlacklistItem matchSqlBlacklist(String project, String sql) {
        try {
            SQLBlacklistManager sqlBlacklistManager = SQLBlacklistManager.getInstance(KylinConfig.getInstanceFromEnv());
            return sqlBlacklistManager.matchSqlBlacklist(project, sql);
        } catch (Exception e) {
            logger.error("Match sql blacklist failed.", e);
            return null;
        }
    }

    private int getSqlConcurrentCount(SQLBlacklistItem sqlBlacklistItem) {
        int concurrentCount = 0;
        Collection<SlowQueryDetector.QueryEntry> runningQueries = SlowQueryDetector.getRunningQueries().values();
        for (SlowQueryDetector.QueryEntry query : runningQueries) {
            if (sqlBlacklistItem.match(query.getSql())) {
                concurrentCount++;
            }
        }
        return concurrentCount;
    }

    boolean isACLDisabledOrAdmin(String project, QueryContext.AclInfo aclInfo) {
        if (!NProjectManager.getProjectConfig(project).isAclTCREnabled()) {
            return true;
        }

        // is role admin
        if (aclInfo != null && aclInfo.getGroups() != null && AclPermissionUtil.isAdmin(aclInfo.getGroups())) {
            return true;
        }

        // is project admin
        return aclInfo != null && aclInfo.isHasAdminPermission();
    }

    public void clearThreadLocalContexts() {
        OLAPContext.clearThreadLocalContexts();
    }

    public QueryExec newQueryExec(String project) {
        return newQueryExec(project, null);
    }

    public QueryExec newQueryExec(String project, String executeAs) {
        QueryContext.current().setAclInfo(getExecuteAclInfo(project, executeAs));
        return new QueryExec(project, NProjectManager.getProjectConfig(project), true);
    }

    protected QueryContext.AclInfo getExecuteAclInfo(String project) {
        return getExecuteAclInfo(project, null);
    }

    @VisibleForTesting
    public QueryContext.AclInfo getExecuteAclInfo(String project, String executeAs) {
        if (executeAs == null) {
            executeAs = AclPermissionUtil.getCurrentUsername();
        }

        // check if it is cached in query context
        if (QueryContext.current().getAclInfo() != null) {
            val aclInfo = QueryContext.current().getAclInfo();
            if (executeAs != null && executeAs.equals(aclInfo.getUsername())) {
                return aclInfo;
            }
        }

        Set<String> groupsOfExecuteUser;
        boolean hasAdminPermission;
        try {
            groupsOfExecuteUser = accessService.getGroupsOfExecuteUser(executeAs);
            MutableAclRecord acl = AclPermissionUtil.getProjectAcl(project);
            Set<String> groupsInProject = AclPermissionUtil.filterGroupsInProject(groupsOfExecuteUser, acl);
            hasAdminPermission = AclPermissionUtil.isSpecificPermissionInProject(executeAs, groupsInProject,
                    ADMINISTRATION, acl);
        } catch (UsernameNotFoundException e) {
            throw new KylinException(INVALID_USER_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidExecuteAsUser(), executeAs));
        }
        return new QueryContext.AclInfo(executeAs, groupsOfExecuteUser, hasAdminPermission);
    }

    public List<TableMeta> getMetadata(String project) {
        if (!NProjectManager.getProjectConfig(project).isSchemaCacheEnabled()) {
            return doGetMetadata(project, null);
        }

        String userName = AclPermissionUtil.getCurrentUsername();
        List<TableMeta> cached = queryCacheManager.getSchemaCache(project, userName);
        if (cached != null) {
            logger.info("[schema cache log] Get meta data from cache, project: {}, username: {}.", project, userName);
            return cached;
        }

        List<TableMeta> tableMetas = doGetMetadata(project, null);
        List<String> tables = tableMetas.stream().map(meta -> meta.getTABLE_SCHEM() + "." + meta.getTABLE_NAME())
                .collect(Collectors.toList());
        val cacheResult = new TableMetaCacheResult(tableMetas,
                QueryCacheSignatureUtil.createCacheSignature(tables, project, null));
        queryCacheManager.putSchemaCache(project, userName, cacheResult);
        return tableMetas;
    }

    public List<TableMeta> getMetadata(String project, String modelAlias) {
        return doGetMetadata(project, modelAlias);
    }

    private List<TableMeta> doGetMetadata(String project, String targetModelName) {

        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }

        List<NDataModel> models = getModels(project, targetModelName);
        List<String> targetModelTables = getTargetModelTables(targetModelName, models);
        List<String> targetModelColumns = getTargetModelColumns(targetModelName, models, project);

        QueryContext.current().setAclInfo(getExecuteAclInfo(project));
        SchemaMetaData schemaMetaData = new SchemaMetaData(project, NProjectManager.getProjectConfig(project));

        List<TableMeta> tableMetas = new LinkedList<>();
        SetMultimap<String, String> tbl2ccNames = collectComputedColumns(project);
        for (TableSchema tableSchema : schemaMetaData.getTables()) {
            TableMeta tblMeta = new TableMeta(tableSchema.getCatalog(), tableSchema.getSchema(), tableSchema.getTable(),
                    tableSchema.getType(), tableSchema.getRemarks(), null, null, null, null, null);

            if (JDBC_METADATA_SCHEMA.equalsIgnoreCase(tblMeta.getTABLE_SCHEM()) || (targetModelTables != null
                    && !targetModelTables.contains(tblMeta.getTABLE_SCHEM() + "." + tblMeta.getTABLE_NAME()))) {
                continue;
            }

            tableMetas.add(tblMeta);

            int columnOrdinal = 1;
            for (StructField field : tableSchema.getFields()) {
                ColumnMeta columnMeta = constructColumnMeta(tableSchema, field, columnOrdinal);
                columnOrdinal++;

                if (!shouldExposeColumn(project, columnMeta, tbl2ccNames)) {
                    continue;
                }

                String qualifiedCol = columnMeta.getTABLE_SCHEM() + "." + columnMeta.getTABLE_NAME() + "."
                        + columnMeta.getCOLUMN_NAME();
                if (!columnMeta.getCOLUMN_NAME().toUpperCase(Locale.ROOT).startsWith("_KY_")
                        && isQualifiedColumn(targetModelColumns, qualifiedCol)) {
                    tblMeta.addColumn(columnMeta);
                }
            }

        }

        return tableMetas;
    }

    private boolean isQualifiedColumn(List<String> targetModelColumns, String qualifiedCol) {
        return targetModelColumns == null || targetModelColumns.contains(qualifiedCol);
    }

    public List<TableMetaWithType> getMetadataV2(String project, String modelAlias) {
        if (!NProjectManager.getProjectConfig(project).isSchemaCacheEnabled()) {
            return doGetMetadataV2(project, modelAlias);
        }

        String userName = AclPermissionUtil.getCurrentUsername();
        List<TableMetaWithType> cached = queryCacheManager.getSchemaV2Cache(project, modelAlias, userName);
        if (cached != null) {
            logger.info("[schema cache log] Get meta data v2 from cache, project: {}, username: {}.", project,
                    userName);
            return cached;
        }

        List<TableMetaWithType> tableMetas = doGetMetadataV2(project, modelAlias);
        List<String> tables = tableMetas.stream().map(meta -> meta.getTABLE_SCHEM() + "." + meta.getTABLE_NAME())
                .collect(Collectors.toList());
        val cacheResult = new TableMetaCacheResultV2(tableMetas,
                QueryCacheSignatureUtil.createCacheSignature(tables, project, modelAlias));
        queryCacheManager.putSchemaV2Cache(project, modelAlias, userName, cacheResult);
        return tableMetas;
    }

    public List<String> format(List<String> sqls) {
        List<Pair<Integer, String>> pairs = Lists.newArrayList();
        int index = 0;
        for (String sql : sqls) {
            pairs.add(Pair.newPair(index, sql));
        }
        return pairs.parallelStream().map(pair -> {
            try {
                val node = CalciteParser.parse(pair.getSecond());
                val writer = new SqlPrettyWriter(KEDialect.DEFAULT);
                writer.setIndentation(2);
                writer.setSelectListExtraIndentFlag(true);
                writer.setSelectListItemsOnSeparateLines(true);
                return Pair.newPair(pair.getFirst(), writer.format(node));
            } catch (SqlParseException e) {
                logger.info("Sql {} cannot be formatted", pair.getSecond());
                return pair;
            }
        }).sorted(Comparator.comparingInt(Pair::getFirst)).map(Pair::getSecond).collect(Collectors.toList());
    }

    @SuppressWarnings("checkstyle:methodlength")
    private List<TableMetaWithType> doGetMetadataV2(String project, String targetModelName) {
        aclEvaluate.checkProjectQueryPermission(project);
        if (StringUtils.isBlank(project))
            return Collections.emptyList();

        List<NDataModel> models = getModels(project, targetModelName);
        List<String> targetModelTables = getTargetModelTables(targetModelName, models);
        List<String> targetModelColumns = getTargetModelColumns(targetModelName, models, project);

        QueryContext.current().setAclInfo(getExecuteAclInfo(project));
        SchemaMetaData schemaMetaData = new SchemaMetaData(project, NProjectManager.getProjectConfig(project));
        Map<TableMetaIdentify, TableMetaWithType> tableMap = constructTableMeta(schemaMetaData, targetModelTables);
        Map<ColumnMetaIdentify, ColumnMetaWithType> columnMap = constructTblColMeta(schemaMetaData, project,
                targetModelColumns);
        addColsToTblMeta(tableMap, columnMap);

        for (NDataModel model : models) {
            clarifyTblTypeToFactOrLookup(model, tableMap);
            clarifyPkFkCols(model, columnMap);
        }

        List<TableMetaWithType> tableMetas = Lists.newArrayList();
        tableMap.forEach((name, tableMeta) -> tableMetas.add(tableMeta));

        return tableMetas;
    }

    List<String> getTargetModelColumns(String targetModelName, List<NDataModel> models, String project) {
        List<String> targetModelColumns = null;
        if (targetModelName != null) {
            NIndexPlanManager indexPlanManager = getManager(NIndexPlanManager.class, project);
            targetModelColumns = NProjectManager.getProjectConfig(project).exposeAllModelRelatedColumns()
                    ? models.stream()
                            .flatMap(m -> m.getEffectiveCols().values().stream()
                                    .map(TblColRef::getColumnWithTableAndSchema))
                            .collect(Collectors.toList())
                    : models.stream().map(model -> {
                        Set<Integer> relatedColIds = indexPlanManager.getIndexPlan(model.getId()).getRelatedColIds();
                        return relatedColIds.stream().map(id -> model.getColRef(id).getColumnWithTableAndSchema())
                                .collect(Collectors.toList());
                    }).flatMap(List::stream).collect(Collectors.toList());
        }
        return targetModelColumns;
    }

    private List<String> getTargetModelTables(String targetModelName, List<NDataModel> models) {
        return targetModelName == null ? null
                : models.stream().flatMap(m -> m.getAllTableRefs().stream().map(TableRef::getTableIdentity))
                        .collect(Collectors.toList());
    }

    private List<NDataModel> getModels(String project, String targetModelName) {
        return NDataModelManager.getInstance(getConfig(), project).listAllModels().stream()
                .filter(m -> !m.isBroken() && (targetModelName == null || m.getAlias().equals(targetModelName)))
                .collect(Collectors.toList());
    }

    private LinkedHashMap<TableMetaIdentify, TableMetaWithType> constructTableMeta(SchemaMetaData schemaMetaData,
            List<String> targetModelTables) {
        LinkedHashMap<TableMetaIdentify, TableMetaWithType> tableMap = Maps.newLinkedHashMap();
        for (TableSchema tableSchema : schemaMetaData.getTables()) {
            TableMetaWithType tblMeta = new TableMetaWithType(tableSchema.getCatalog(), tableSchema.getSchema(),
                    tableSchema.getTable(), tableSchema.getType(), tableSchema.getRemarks(), null, null, null, null,
                    null);

            if (!JDBC_METADATA_SCHEMA.equalsIgnoreCase(tblMeta.getTABLE_SCHEM()) && (targetModelTables == null
                    || targetModelTables.contains(tblMeta.getTABLE_SCHEM() + "." + tblMeta.getTABLE_NAME()))) {
                tableMap.put(new TableMetaIdentify(tblMeta.getTABLE_SCHEM(), tblMeta.getTABLE_NAME()), tblMeta);
            }
        }

        return tableMap;
    }

    private LinkedHashMap<ColumnMetaIdentify, ColumnMetaWithType> constructTblColMeta(SchemaMetaData schemaMetaData,
            String project, List<String> targetModelColumns) {
        LinkedHashMap<ColumnMetaIdentify, ColumnMetaWithType> columnMap = Maps.newLinkedHashMap();
        SetMultimap<String, String> tbl2ccNames = collectComputedColumns(project);

        for (TableSchema tableSchema : schemaMetaData.getTables()) {
            int columnOrdinal = 1;
            for (StructField field : tableSchema.getFields()) {
                ColumnMetaWithType columnMeta = ColumnMetaWithType
                        .ofColumnMeta(constructColumnMeta(tableSchema, field, columnOrdinal));
                columnOrdinal++;

                if (!shouldExposeColumn(project, columnMeta, tbl2ccNames)) {
                    continue;
                }

                if (!JDBC_METADATA_SCHEMA.equalsIgnoreCase(columnMeta.getTABLE_SCHEM())
                        && !columnMeta.getCOLUMN_NAME().toUpperCase(Locale.ROOT).startsWith("_KY_")
                        && (targetModelColumns == null || targetModelColumns.contains(columnMeta.getTABLE_SCHEM() + "."
                                + columnMeta.getTABLE_NAME() + "." + columnMeta.getCOLUMN_NAME()))) {
                    columnMap.put(new ColumnMetaIdentify(columnMeta.getTABLE_SCHEM(), columnMeta.getTABLE_NAME(),
                            columnMeta.getCOLUMN_NAME()), columnMeta);
                }
            }
        }
        return columnMap;
    }

    private ColumnMeta constructColumnMeta(TableSchema tableSchema, StructField field, int columnOrdinal) {
        final int NUM_PREC_RADIX = 10;
        int columnSize = -1;
        if (field.getDataType() == Types.TIMESTAMP || field.getDataType() == Types.DECIMAL
                || field.getDataType() == Types.VARCHAR || field.getDataType() == Types.CHAR) {
            columnSize = field.getPrecision();
        }
        final int charOctetLength = columnSize;
        final int decimalDigit = field.getDataType() == Types.DECIMAL ? field.getScale() : 0;
        final int nullable = field.isNullable() ? 1 : 0;
        final String isNullable = field.isNullable() ? "YES" : "NO";
        final short sourceDataType = -1;

        return new ColumnMeta(tableSchema.getCatalog(), tableSchema.getSchema(), tableSchema.getTable(),
                field.getName(), field.getDataType(), field.getDataTypeName(), columnSize, // COLUMN_SIZE
                -1, // BUFFER_LENGTH
                decimalDigit, // DECIMAL_DIGIT
                NUM_PREC_RADIX, // NUM_PREC_RADIX
                nullable, // NULLABLE
                null, null, -1, -1, // REMAKRS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB
                charOctetLength, // CHAR_OCTET_LENGTH
                columnOrdinal, isNullable, null, null, null, sourceDataType, "");
    }

    private SetMultimap<String, String> collectComputedColumns(String project) {
        SetMultimap<String, String> tbl2ccNames = HashMultimap.create();
        getManager(NProjectManager.class).listAllRealizations(project).forEach(rea -> {
            val upperCaseCcNames = rea.getModel().getComputedColumnNames().stream()
                    .map(str -> str.toUpperCase(Locale.ROOT)).collect(Collectors.toList());
            tbl2ccNames.putAll(rea.getModel().getRootFactTable().getAlias().toUpperCase(Locale.ROOT), upperCaseCcNames);
            tbl2ccNames.putAll(rea.getModel().getRootFactTableName().toUpperCase(Locale.ROOT), upperCaseCcNames);
        });
        return tbl2ccNames;
    }

    private boolean shouldExposeColumn(String project, ColumnMeta columnMeta, SetMultimap<String, String> tbl2ccNames) {
        // check for cc exposing
        // exposeComputedColumn=True, expose columns anyway
        if (NProjectManager.getProjectConfig(project).exposeComputedColumn()) {
            return true;
        }

        // only check cc expose when exposeComputedColumn=False
        // do not expose column if it is a computed column
        return !isComputedColumn(columnMeta.getCOLUMN_NAME().toUpperCase(Locale.ROOT), columnMeta.getTABLE_NAME(),
                tbl2ccNames);
    }

    /**
     * @param ccName
     * @param table   only support table alias like "TEST_COUNT" or table indentity "default.TEST_COUNT"
     * @return
     */
    private boolean isComputedColumn(String ccName, String table, SetMultimap<String, String> tbl2ccNames) {

        return CollectionUtils.isNotEmpty(tbl2ccNames.get(table.toUpperCase(Locale.ROOT)))
                && tbl2ccNames.get(table.toUpperCase(Locale.ROOT)).contains(ccName.toUpperCase(Locale.ROOT));
    }

    static void addColsToTblMeta(Map<TableMetaIdentify, TableMetaWithType> tblMap,
            Map<ColumnMetaIdentify, ColumnMetaWithType> columnMetaWithTypeMap) {
        columnMetaWithTypeMap.forEach((identify, columnMetaWithType) -> {
            TableMetaIdentify tableMetaIdentify = new TableMetaIdentify(identify.getTableSchema(),
                    identify.getTableName());
            tblMap.get(tableMetaIdentify).addColumn(columnMetaWithType);
        });
    }

    private void clarifyTblTypeToFactOrLookup(NDataModel dataModelDesc,
            Map<TableMetaIdentify, TableMetaWithType> tableMap) {
        // update table type: FACT
        for (TableRef factTable : dataModelDesc.getFactTables()) {
            String tableSchema = factTable.getTableIdentity().split("\\.")[0];
            String tableName = factTable.getTableIdentity().split("\\.")[1];
            TableMetaIdentify tableMetaIdentify = new TableMetaIdentify(tableSchema, tableName);

            if (tableMap.containsKey(tableMetaIdentify)) {
                tableMap.get(tableMetaIdentify).getTYPE().add(TableMetaWithType.tableTypeEnum.FACT);
            }
        }

        // update table type: LOOKUP
        for (TableRef lookupTable : dataModelDesc.getLookupTables()) {
            String tableSchema = lookupTable.getTableIdentity().split("\\.")[0];
            String tableName = lookupTable.getTableIdentity().split("\\.")[1];

            TableMetaIdentify tableMetaIdentify = new TableMetaIdentify(tableSchema, tableName);
            if (tableMap.containsKey(tableMetaIdentify)) {
                tableMap.get(tableMetaIdentify).getTYPE().add(TableMetaWithType.tableTypeEnum.LOOKUP);
            }
        }
    }

    private void clarifyPkFkCols(NDataModel dataModelDesc, Map<ColumnMetaIdentify, ColumnMetaWithType> columnMap) {
        for (JoinTableDesc joinTableDesc : dataModelDesc.getJoinTables()) {
            JoinDesc joinDesc = joinTableDesc.getJoin();
            for (String pk : joinDesc.getPrimaryKey()) {
                ColumnMetaIdentify columnMetaIdentify = getColumnMetaIdentify(dataModelDesc, pk);
                if (columnMap.containsKey(columnMetaIdentify)) {
                    columnMap.get(columnMetaIdentify).getTYPE().add(ColumnMetaWithType.columnTypeEnum.PK);
                }
            }

            for (String fk : joinDesc.getForeignKey()) {
                ColumnMetaIdentify columnMetaIdentify = getColumnMetaIdentify(dataModelDesc, fk);
                if (columnMap.containsKey(columnMetaIdentify)) {
                    columnMap.get(columnMetaIdentify).getTYPE().add(ColumnMetaWithType.columnTypeEnum.FK);
                }
            }
        }
    }

    private ColumnMetaIdentify getColumnMetaIdentify(NDataModel model, String joinKey) {
        String tableName = joinKey.substring(0, joinKey.indexOf('.'));
        String tableSchema = model.findTable(tableName).getTableIdentity().split("\\.")[0];
        String columnName = joinKey.substring(joinKey.indexOf('.') + 1);
        return new ColumnMetaIdentify(tableSchema, tableName, columnName);
    }

    protected String makeErrorMsgUserFriendly(Throwable e) {
        return QueryUtil.makeErrorMsgUserFriendly(e);
    }

    private SQLResponse buildSqlResponse(boolean isPushDown, Iterable<List<String>> results, int resultSize,
            List<SelectedColumnMeta> columnMetas, String project) {
        SQLResponse response = new SQLResponse(columnMetas, results, resultSize, 0, false, null,
                QueryContext.current().getQueryTagInfo().isPartial(), isPushDown);
        QueryContext queryContext = QueryContext.current();

        response.wrapResultOfQueryContext(queryContext);

        response.setNativeRealizations(OLAPContext.getNativeRealizations());

        if (!queryContext.getQueryTagInfo().isVacant()) {
            setAppMaterURL(response);
        }

        if (isPushDown) {
            response.setNativeRealizations(Lists.newArrayList());
            response.setEngineType(queryContext.getPushdownEngine());
            return response;
        }

        // case of query like select * from table where 1 <> 1
        if (CollectionUtils.isEmpty(response.getNativeRealizations())) {
            QueryContext.current().getQueryTagInfo().setConstantQuery(true);
            response.setEngineType(QueryHistory.EngineType.CONSTANTS.name());
            return response;
        }

        response.setEngineType(QueryHistory.EngineType.NATIVE.name());
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));
        response.setVacant(QueryContext.current().getQueryTagInfo().isVacant());

        if (QueryContext.current().getMetrics().getQueryExecutedPlan() != null) {
            response.setExecutedPlan(QueryContext.current().getMetrics().getQueryExecutedPlan());
        }

        return response;
    }

    @Autowired
    @Qualifier("sparderUIUtil")
    private SparderUIUtil sparderUIUtil;

    private void setAppMaterURL(SQLResponse response) {
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            try {
                String executionID = QueryContext.current().getExecutionID();
                if (executionID != null && !executionID.isEmpty()) {
                    response.setAppMasterURL(sparderUIUtil.getSQLTrackingPath(executionID));
                }
            } catch (Throwable th) {
                logger.error("Get app master for sql failed", th);
            }
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

    private static ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
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
            JsonUtil.writeValueIndent(out, record);
        }

        @Override
        public QueryRecord deserialize(DataInputStream in) throws IOException {
            return JsonUtil.readValue(in, QueryRecord.class);
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @SuppressWarnings("serial")
    public static class QueryRecord extends RootPersistentEntity {

        @JsonProperty
        private List<Query> queries = Lists.newArrayList();
    }

    public static class LogReport {
        static final String QUERY_ID = "id";
        static final String SQL = "sql";
        static final String USER = "user";
        static final String SUCCESS = "success";
        static final String DURATION = "duration";
        static final String PROJECT = "project";
        static final String REALIZATION_NAMES = "realization";
        static final String INDEX_LAYOUT_IDS = "layout";
        static final String SNAPSHOTS = "snapshots";
        static final String IS_DERIVED = "is_derived";
        static final String SNAPSHOT_FILTERS = "snapshot_filters";

        static final String IS_PARTIAL_MATCH_MODEL = "is_partial_match";
        static final String SCAN_ROWS = "scan_rows";
        static final String TOTAL_SCAN_ROWS = "total_scan_rows";
        static final String SCAN_BYTES = "scan_bytes";
        static final String TOTAL_SCAN_BYTES = "total_scan_bytes";
        static final String RESULT_ROW_COUNT = "result_row_count";
        static final String SHUFFLE_PARTITIONS = "shuffle_partitions";
        static final String ACCEPT_PARTIAL = "accept_partial";
        static final String PARTIAL_RESULT = "is_partial_result";
        static final String HIT_EXCEPTION_CACHE = "hit_exception_cache";
        static final String STORAGE_CACHE_USED = "storage_cache_used";
        static final String STORAGE_CACHE_TYPE = "storage_cache_type";
        static final String PUSH_DOWN = "push_down";
        static final String IS_PREPARE = "is_prepare";
        static final String TIMEOUT = "timeout";
        static final String TRACE_URL = "trace_url";
        static final String TIMELINE_SCHEMA = "timeline_schema";
        static final String TIMELINE = "timeline";
        static final String ERROR_MSG = "error_msg";
        static final String USER_TAG = "user_defined_tag";
        static final String PUSH_DOWN_FORCED = "push_down_forced";
        static final String INDEX_FORCED = "index_forced";
        static final String USER_AGENT = "user_agent";
        static final String BACK_DOOR_TOGGLES = "back_door_toggles";
        static final String SCAN_SEGMENT_COUNT = "scan_segment_count";
        static final String SCAN_FILE_COUNT = "scan_file_count";
        static final String REFUSE = "refuse";

        static final ImmutableMap<String, String> O2N = new ImmutableMap.Builder<String, String>()
                .put(QUERY_ID, "Query Id: ").put(SQL, "SQL: ").put(USER, "User: ").put(SUCCESS, "Success: ")
                .put(DURATION, "Duration: ").put(PROJECT, "Project: ").put(REALIZATION_NAMES, "Realization Names: ")
                .put(INDEX_LAYOUT_IDS, "Index Layout Ids: ").put(IS_DERIVED, "Is Dervied: ")
                .put(SNAPSHOTS, "Snapshot Names: ").put(SNAPSHOT_FILTERS, "Snapshot Filter: ")
                .put(IS_PARTIAL_MATCH_MODEL, "Is Partial Match Model: ").put(SCAN_ROWS, "Scan rows: ")
                .put(TOTAL_SCAN_ROWS, "Total Scan rows: ").put(SCAN_BYTES, "Scan bytes: ")
                .put(TOTAL_SCAN_BYTES, "Total Scan Bytes: ").put(RESULT_ROW_COUNT, "Result Row Count: ")
                .put(SHUFFLE_PARTITIONS, "Shuffle partitions: ").put(ACCEPT_PARTIAL, "Accept Partial: ")
                .put(PARTIAL_RESULT, "Is Partial Result: ").put(HIT_EXCEPTION_CACHE, "Hit Exception Cache: ")
                .put(STORAGE_CACHE_USED, "Storage Cache Used: ").put(STORAGE_CACHE_TYPE, "Storage Cache Type: ")
                .put(PUSH_DOWN, "Is Query Push-Down: ").put(IS_PREPARE, "Is Prepare: ").put(TIMEOUT, "Is Timeout: ")
                .put(TRACE_URL, "Trace URL: ").put(TIMELINE_SCHEMA, "Time Line Schema: ").put(TIMELINE, "Time Line: ")
                .put(ERROR_MSG, "Message: ").put(USER_TAG, "User Defined Tag: ")
                .put(PUSH_DOWN_FORCED, "Is forced to Push-Down: ").put(USER_AGENT, "User Agent: ")
                .put(BACK_DOOR_TOGGLES, "Back door toggles: ").put(SCAN_SEGMENT_COUNT, "Scan Segment Count: ")
                .put(SCAN_FILE_COUNT, "Scan File Count: ").put(REFUSE, "Is Refused: ").build();

        private Map<String, Object> logs = new HashMap<>(100);

        public LogReport put(String key, String value) {
            if (!StringUtils.isEmpty(value))
                logs.put(key, value);
            return this;
        }

        public LogReport put(String key, Object value) {
            if (value != null)
                logs.put(key, value);
            return this;
        }

        private String get(String key) {
            Object value = logs.get(key);
            if (value == null)
                return "null";
            return value.toString();
        }

        public String oldStyleLog() {
            String newLine = System.getProperty("line.separator");
            String delimiter = "==========================[QUERY]===============================";

            return newLine + delimiter + newLine //
                    + O2N.get(QUERY_ID) + get(QUERY_ID) + newLine //
                    + O2N.get(SQL) + get(SQL) + newLine //
                    + O2N.get(USER) + get(USER) + newLine //
                    + O2N.get(SUCCESS) + get(SUCCESS) + newLine //
                    + O2N.get(DURATION) + get(DURATION) + newLine //
                    + O2N.get(PROJECT) + get(PROJECT) + newLine //
                    + O2N.get(REALIZATION_NAMES) + get(REALIZATION_NAMES) + newLine //
                    + O2N.get(INDEX_LAYOUT_IDS) + get(INDEX_LAYOUT_IDS) + newLine //
                    + O2N.get(IS_DERIVED) + get(IS_DERIVED) + newLine //
                    + O2N.get(SNAPSHOTS) + get(SNAPSHOTS) + newLine //
                    + O2N.get(SNAPSHOT_FILTERS) + get(SNAPSHOT_FILTERS) + newLine //
                    + O2N.get(IS_PARTIAL_MATCH_MODEL) + get(IS_PARTIAL_MATCH_MODEL) + newLine //
                    + O2N.get(SCAN_ROWS) + get(SCAN_ROWS) + newLine //
                    + O2N.get(TOTAL_SCAN_ROWS) + get(TOTAL_SCAN_ROWS) + newLine //
                    + O2N.get(SCAN_BYTES) + get(SCAN_BYTES) + newLine //
                    + O2N.get(TOTAL_SCAN_BYTES) + get(TOTAL_SCAN_BYTES) + newLine //
                    + O2N.get(RESULT_ROW_COUNT) + get(RESULT_ROW_COUNT) + newLine //
                    + O2N.get(SHUFFLE_PARTITIONS) + get(SHUFFLE_PARTITIONS) + newLine //
                    + O2N.get(ACCEPT_PARTIAL) + get(ACCEPT_PARTIAL) + newLine //
                    + O2N.get(PARTIAL_RESULT) + get(PARTIAL_RESULT) + newLine //
                    + O2N.get(HIT_EXCEPTION_CACHE) + get(HIT_EXCEPTION_CACHE) + newLine //
                    + O2N.get(STORAGE_CACHE_USED) + get(STORAGE_CACHE_USED) + newLine //
                    + O2N.get(STORAGE_CACHE_TYPE) + get(STORAGE_CACHE_TYPE) + newLine //
                    + O2N.get(PUSH_DOWN) + get(PUSH_DOWN) + newLine //
                    + O2N.get(IS_PREPARE) + get(IS_PREPARE) + newLine //
                    + O2N.get(TIMEOUT) + get(TIMEOUT) + newLine //
                    + O2N.get(TRACE_URL) + get(TRACE_URL) + newLine //
                    + O2N.get(TIMELINE_SCHEMA) + get(TIMELINE_SCHEMA) + newLine //
                    + O2N.get(TIMELINE) + get(TIMELINE) + newLine //
                    + O2N.get(ERROR_MSG) + get(ERROR_MSG) + newLine //
                    + O2N.get(USER_TAG) + get(USER_TAG) + newLine //
                    + O2N.get(PUSH_DOWN_FORCED) + get(PUSH_DOWN_FORCED) + newLine //
                    + O2N.get(USER_AGENT) + get(USER_AGENT) + newLine //
                    + O2N.get(BACK_DOOR_TOGGLES) + get(BACK_DOOR_TOGGLES) + newLine //
                    + O2N.get(SCAN_SEGMENT_COUNT) + get(SCAN_SEGMENT_COUNT) + newLine //
                    + O2N.get(SCAN_FILE_COUNT) + get(SCAN_FILE_COUNT) + newLine //
                    + O2N.get(REFUSE) + get(REFUSE) + newLine //
                    + delimiter + newLine;
        }

        public String jsonStyleLog() {
            return "[QUERY SUMMARY]: ".concat(new Gson().toJson(logs));
        }
    }

    @Data
    @AllArgsConstructor
    public static class TableMetaIdentify {
        private String tableSchema;
        private String tableName;
    }

    @Data
    @AllArgsConstructor
    public static class ColumnMetaIdentify {
        private String tableSchema;
        private String tableName;
        private String columnName;
    }

    @Override
    public String onCreateAclSignature(String project) throws IOException {
        return createAclSignature(project);
    }

    public String createAclSignature(String project) throws IOException {
        List<Long> aclTimes = Lists.newLinkedList();
        List<String> aclNames = Lists.newLinkedList();
        QueryContext.AclInfo aclInfo = QueryContext.current().getAclInfo();
        if (aclInfo == null) {
            aclInfo = getExecuteAclInfo(project);
        }
        String username = aclInfo.getUsername();
        if (aclTCRService.hasAdminPermissionInProject(username, true, project)) {
            return "ADMIN";
        }
        val aclManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Set<String> userGroups = aclInfo.getGroups();
        List<AclTCR> aclGroups = aclManager.getAclTCRs(username, userGroups);
        for (AclTCR group : aclGroups) {
            aclNames.add(group.resourceName());
            aclTimes.add(group.getLastModified());
        }

        return Joiner.on(";").join(Joiner.on("_").join(aclNames), Joiner.on("_").join(aclTimes));
    }

    private void applyModelPriority(QueryContext queryContext, String sql) {
        queryContext.setModelPriorities(QueryModelPriorities.getModelPrioritiesFromComment(sql));
    }

    public List<TableMetaWithType> getMetadataAddType(String project, String modelAlias) {
        List<TableMeta> tableMetas = getMetadata(project, modelAlias);

        Map<TableMetaIdentify, TableMetaWithType> tableMap = Maps.newLinkedHashMap();
        Map<ColumnMetaIdentify, ColumnMetaWithType> columnMap = Maps.newLinkedHashMap();

        for (TableMeta tableMeta : tableMetas) {
            TableMetaWithType tblMeta = TableMetaWithType.ofColumnMeta(tableMeta);
            tableMap.put(new TableMetaIdentify(tblMeta.getTABLE_SCHEM(), tblMeta.getTABLE_NAME()), tblMeta);

            for (ColumnMeta columnMeta : tblMeta.getColumns()) {
                columnMap.put(new ColumnMetaIdentify(columnMeta.getTABLE_SCHEM(), columnMeta.getTABLE_NAME(),
                        columnMeta.getCOLUMN_NAME()), (ColumnMetaWithType) columnMeta);
            }
        }

        List<NDataModel> models = getModels(project, modelAlias);

        for (NDataModel model : models) {
            clarifyTblTypeToFactOrLookup(model, tableMap);
            clarifyPkFkCols(model, columnMap);
        }

        return Lists.newArrayList(tableMap.values());
    }
}
