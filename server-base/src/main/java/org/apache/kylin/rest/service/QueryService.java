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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.OnlyPrepareEarlyAbortException;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SetThreadName;
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
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.ColumnMetaWithType;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryUtil;
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
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.TableauInterceptor;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService extends BasicService {

    public static final String SUCCESS_QUERY_CACHE = "StorageCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";
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
    private AclEvaluate aclEvaluate;

    public QueryService() {
        queryStore = ResourceStore.getStore(getConfig());
        badQueryDetector.start();
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

    public List<TableMeta> getMetadata(String project) throws SQLException {
        return getMetadata(getCubeManager(), project, true);
    }

    public SQLResponse query(SQLRequest sqlRequest) throws Exception {
        SQLResponse ret = null;
        try {
            final String user = SecurityContextHolder.getContext().getAuthentication().getName();
            badQueryDetector.queryStart(Thread.currentThread(), sqlRequest, user);

            ret = queryWithSqlMassage(sqlRequest);
            return ret;

        } finally {
            String badReason = (ret != null && ret.isPushDown()) ? BadQueryEntry.ADJ_PUSHDOWN : null;
            badQueryDetector.queryEnd(Thread.currentThread(), badReason);
        }
    }

    public SQLResponse update(SQLRequest sqlRequest) throws Exception {
        // non select operations, only supported when enable pushdown
        logger.debug("Query pushdown enabled, redirect the non-select query to pushdown engine.");
        Connection conn = null;
        try {
            conn = QueryConnection.getConnection(sqlRequest.getProject());
            Pair<List<List<String>>, List<SelectedColumnMeta>> r = PushDownUtil
                    .tryPushDownNonSelectQuery(sqlRequest.getProject(), sqlRequest.getSql(), conn.getSchema());

            List<SelectedColumnMeta> columnMetas = Lists.newArrayList();
            columnMetas.add(new SelectedColumnMeta(false, false, false, false, 1, false, Integer.MAX_VALUE, "c0", "c0",
                    null, null, null, Integer.MAX_VALUE, 128, 1, "char", false, false, false));

            return buildSqlResponse(true, r.getFirst(), columnMetas);

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
        queryStore.putResourceWithoutCheck(getQueryKeyById(creator), record, System.currentTimeMillis(),
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
        queryStore.putResourceWithoutCheck(getQueryKeyById(creator), record, System.currentTimeMillis(),
                QueryRecordSerializer.getInstance());
        return;
    }

    public List<Query> getQueries(final String creator) throws IOException {
        if (null == creator) {
            return null;
        }
        List<Query> queries = new ArrayList<Query>();
        QueryRecord record = queryStore.getResource(getQueryKeyById(creator), QueryRecord.class,
                QueryRecordSerializer.getInstance());
        if (record != null) {
            for (Query query : record.getQueries()) {
                queries.add(query);
            }
        }
        return queries;
    }

    public void logQuery(final SQLRequest request, final SQLResponse response) {
        final String user = aclEvaluate.getCurrentUserName();
        final List<String> realizationNames = new LinkedList<>();
        final Set<Long> cuboidIds = new HashSet<Long>();
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

                if (ctx.realization != null) {
                    realizationNames.add(ctx.realization.getCanonicalName());
                }

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
        stringBuilder.append("Query Id: ").append(QueryContext.current().getQueryId()).append(newLine);
        stringBuilder.append("SQL: ").append(request.getSql()).append(newLine);
        stringBuilder.append("User: ").append(user).append(newLine);
        stringBuilder.append("Success: ").append((null == response.getExceptionMessage())).append(newLine);
        stringBuilder.append("Duration: ").append(duration).append(newLine);
        stringBuilder.append("Project: ").append(request.getProject()).append(newLine);
        stringBuilder.append("Realization Names: ").append(realizationNames).append(newLine);
        stringBuilder.append("Cuboid Ids: ").append(cuboidIds).append(newLine);
        stringBuilder.append("Total scan count: ").append(response.getTotalScanCount()).append(newLine);
        stringBuilder.append("Total scan bytes: ").append(response.getTotalScanBytes()).append(newLine);
        stringBuilder.append("Result row count: ").append(resultRowCount).append(newLine);
        stringBuilder.append("Accept Partial: ").append(request.isAcceptPartial()).append(newLine);
        stringBuilder.append("Is Partial Result: ").append(response.isPartial()).append(newLine);
        stringBuilder.append("Hit Exception Cache: ").append(response.isHitExceptionCache()).append(newLine);
        stringBuilder.append("Storage cache used: ").append(storageCacheUsed).append(newLine);
        stringBuilder.append("Is Query Push-Down: ").append(isPushDown).append(newLine);
        stringBuilder.append("Message: ").append(response.getExceptionMessage()).append(newLine);
        stringBuilder.append("==========================[QUERY]===============================").append(newLine);

        logger.info(stringBuilder.toString());
    }

    public void checkAuthorization(SQLResponse sqlResponse, String project) throws AccessDeniedException {

        //project 
        ProjectInstance projectInstance = getProjectManager().getProject(project);
        try {
            if (aclEvaluate.hasProjectReadPermission(projectInstance)) {
                return;
            }
        } catch (AccessDeniedException e) {
            logger.warn(
                    "Current user {} has no READ permission on current project {}, please ask Administrator for permission granting.");
            //just continue
        }

        String realizationsStr = sqlResponse.getCube();//CUBE[name=abc],HYBRID[name=xyz]

        if (StringUtils.isEmpty(realizationsStr)) {
            throw new AccessDeniedException(
                    "Query pushdown requires having READ permission on project, please ask Administrator to grant you permissions");
        }

        String[] splits = StringUtils.split(realizationsStr, ",");

        for (String split : splits) {

            Iterable<String> parts = Splitter.on(CharMatcher.anyOf("[]=,")).split(split);
            String[] partsStr = Iterables.toArray(parts, String.class);

            if (RealizationType.HYBRID.toString().equals(partsStr[0])) {
                // special care for hybrid
                HybridInstance hybridInstance = getHybridManager().getHybridInstance(partsStr[2]);
                Preconditions.checkNotNull(hybridInstance);
                checkHybridAuthorization(hybridInstance);
            } else {
                CubeInstance cubeInstance = getCubeManager().getCube(partsStr[2]);
                checkCubeAuthorization(cubeInstance);
            }
        }
    }

    private void checkCubeAuthorization(CubeInstance cube) throws AccessDeniedException {
        Preconditions.checkState(aclEvaluate.hasCubeReadPermission(cube));
    }

    private void checkHybridAuthorization(HybridInstance hybridInstance) throws AccessDeniedException {
        List<RealizationEntry> realizationEntries = hybridInstance.getRealizationEntries();
        for (RealizationEntry realizationEntry : realizationEntries) {
            String reName = realizationEntry.getRealization();
            if (RealizationType.CUBE == realizationEntry.getType()) {
                CubeInstance cubeInstance = getCubeManager().getCube(reName);
                checkCubeAuthorization(cubeInstance);
            } else if (RealizationType.HYBRID == realizationEntry.getType()) {
                HybridInstance innerHybridInstance = getHybridManager().getHybridInstance(reName);
                checkHybridAuthorization(innerHybridInstance);
            }
        }
    }

    public SQLResponse doQueryWithCache(SQLRequest sqlRequest) {
        Message msg = MsgPicker.getMsg();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String serverMode = kylinConfig.getServerMode();
        if (!(Constant.SERVER_MODE_QUERY.equals(serverMode.toLowerCase())
                || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase()))) {
            throw new BadRequestException(String.format(msg.getQUERY_NOT_ALLOWED(), serverMode));
        }
        if (StringUtils.isBlank(sqlRequest.getProject())) {
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        if (sqlRequest.getBackdoorToggles() != null)
            BackdoorToggles.addToggles(sqlRequest.getBackdoorToggles());

        final QueryContext queryContext = QueryContext.current();

        try (SetThreadName ignored = new SetThreadName("Query %s", queryContext.getQueryId())) {
            String sql = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            logger.info("Using project: " + project);
            logger.info("The original query:  " + sql);

            final boolean isSelect = QueryUtil.isSelectStatement(sql);

            long startTime = System.currentTimeMillis();

            SQLResponse sqlResponse = null;
            boolean queryCacheEnabled = checkCondition(kylinConfig.isQueryCacheEnabled(),
                    "query cache disabled in KylinConfig") && //
                    checkCondition(!BackdoorToggles.getDisableCache(), "query cache disabled in BackdoorToggles");

            if (queryCacheEnabled) {
                sqlResponse = searchQueryInCache(sqlRequest);
            }

            try {
                if (null == sqlResponse) {
                    if (isSelect) {
                        sqlResponse = query(sqlRequest);
                    } else if (kylinConfig.isPushDownEnabled()) {
                        sqlResponse = update(sqlRequest);
                    } else {
                        logger.debug(
                                "Directly return exception as the sql is unsupported, and query pushdown is disabled");
                        throw new BadRequestException(msg.getNOT_SUPPORTED_SQL());
                    }

                    long durationThreshold = kylinConfig.getQueryDurationCacheThreshold();
                    long scanCountThreshold = kylinConfig.getQueryScanCountCacheThreshold();
                    long scanBytesThreshold = kylinConfig.getQueryScanBytesCacheThreshold();
                    sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                    logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                            String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()),
                            String.valueOf(sqlResponse.getTotalScanCount()));
                    if (checkCondition(queryCacheEnabled, "query cache is disabled") //
                            && checkCondition(!sqlResponse.getIsException(), "query has exception") //
                            && checkCondition(!(sqlResponse.isPushDown()
                                    && (isSelect == false || kylinConfig.isPushdownQueryCacheEnabled() == false)),
                                    "query is executed with pushdown, but it is non-select, or the cache for pushdown is disabled") //
                            && checkCondition(
                                    sqlResponse.getDuration() > durationThreshold
                                            || sqlResponse.getTotalScanCount() > scanCountThreshold
                                            || sqlResponse.getTotalScanBytes() > scanBytesThreshold, //
                                    "query is too lightweight with duration: {} (threshold {}), scan count: {} (threshold {}), scan bytes: {} (threshold {})",
                                    sqlResponse.getDuration(), durationThreshold, sqlResponse.getTotalScanCount(),
                                    scanCountThreshold, sqlResponse.getTotalScanBytes(), scanBytesThreshold)
                            && checkCondition(sqlResponse.getResults().size() < kylinConfig.getLargeQueryThreshold(),
                                    "query response is too large: {} ({})", sqlResponse.getResults().size(),
                                    kylinConfig.getLargeQueryThreshold())) {
                        cacheManager.getCache(SUCCESS_QUERY_CACHE)
                                .put(new Element(sqlRequest.getCacheKey(), sqlResponse));
                    }

                } else {
                    sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                    sqlResponse.setTotalScanCount(0);
                    sqlResponse.setTotalScanBytes(0);
                }

                checkQueryAuth(sqlResponse, project);

            } catch (Throwable e) { // calcite may throw AssertError
                logger.error("Exception while executing query", e);
                String errMsg = makeErrorMsgUserFriendly(e);

                sqlResponse = new SQLResponse(null, null, 0, true, errMsg);
                sqlResponse.setTotalScanCount(queryContext.getScannedRows());
                sqlResponse.setTotalScanBytes(queryContext.getScannedBytes());

                if (queryCacheEnabled && e.getCause() != null
                        && ExceptionUtils.getRootCause(e) instanceof ResourceLimitExceededException) {
                    Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
                    exceptionCache.put(new Element(sqlRequest.getCacheKey(), sqlResponse));
                }
            }

            logQuery(sqlRequest, sqlResponse);

            QueryMetricsFacade.updateMetrics(sqlRequest, sqlResponse);
            QueryMetrics2Facade.updateMetrics(sqlRequest, sqlResponse);

            if (sqlResponse.getIsException())
                throw new InternalErrorException(sqlResponse.getExceptionMessage());

            return sqlResponse;

        } finally {
            BackdoorToggles.cleanToggles();
            QueryContext.reset();
        }
    }

    public SQLResponse searchQueryInCache(SQLRequest sqlRequest) {
        SQLResponse response = null;
        Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
        Cache successCache = cacheManager.getCache(SUCCESS_QUERY_CACHE);

        Element element = null;
        if ((element = exceptionCache.get(sqlRequest.getCacheKey())) != null) {
            logger.info("The sqlResponse is found in EXCEPTION_QUERY_CACHE");
            response = (SQLResponse) element.getObjectValue();
            response.setHitExceptionCache(true);
        } else if ((element = successCache.get(sqlRequest.getCacheKey())) != null) {
            logger.info("The sqlResponse is found in SUCCESS_QUERY_CACHE");
            response = (SQLResponse) element.getObjectValue();
            response.setStorageCacheUsed(true);
        }

        return response;
    }

    protected void checkQueryAuth(SQLResponse sqlResponse, String project) throws AccessDeniedException {
        if (!sqlResponse.getIsException() && KylinConfig.getInstanceFromEnv().isQuerySecureEnabled()) {
            checkAuthorization(sqlResponse, project);
        }
    }

    private SQLResponse queryWithSqlMassage(SQLRequest sqlRequest) throws Exception {
        Connection conn = null;

        try {
            conn = QueryConnection.getConnection(sqlRequest.getProject());

            String userInfo = SecurityContextHolder.getContext().getAuthentication().getName();
            QueryContext context = QueryContext.current();
            context.setUsername(userInfo);
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
                    sqlRequest.getLimit(), sqlRequest.getOffset(), conn.getSchema());
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
            // force clear the query context before a new query
            OLAPContext.clearThreadLocalContexts();

            return execute(correctedSql, sqlRequest, conn);

        } finally {
            DBUtils.closeQuietly(conn);
        }
    }

    protected List<TableMeta> getMetadata(CubeManager cubeMgr, String project, boolean cubedOnly) throws SQLException {

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

                if (!cubedOnly
                        || getProjectManager().isExposedTable(project, schemaName + "." + tblMeta.getTABLE_NAME())) {
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

                if (!cubedOnly || getProjectManager().isExposedColumn(project,
                        schemaName + "." + colmnMeta.getTABLE_NAME(), colmnMeta.getCOLUMN_NAME())) {
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
        return getMetadataV2(getCubeManager(), project, true);
    }

    @SuppressWarnings("checkstyle:methodlength")
    protected List<TableMetaWithType> getMetadataV2(CubeManager cubeMgr, String project, boolean cubedOnly)
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

                if (!cubedOnly
                        || getProjectManager().isExposedTable(project, schemaName + "." + tblMeta.getTABLE_NAME())) {
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

                if (!cubedOnly || getProjectManager().isExposedColumn(project,
                        schemaName + "." + colmnMeta.getTABLE_NAME(), colmnMeta.getCOLUMN_NAME())) {
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
            DataModelDesc dataModelDesc = modelService.listAllModels(modelName, project, true).get(0);
            if (!dataModelDesc.isDraft()) {

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
    private SQLResponse execute(String correctedSql, SQLRequest sqlRequest, Connection conn) throws Exception {
        Statement stat = null;
        ResultSet resultSet = null;
        boolean isPushDown = false;

        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        try {

            // special case for prepare query.
            if (BackdoorToggles.getPrepareOnly()) {
                return getPrepareOnlySqlResponse(correctedSql, conn, isPushDown, results, columnMetas);
            }

            if (isPrepareStatementWithParams(sqlRequest)) {

                PreparedStatement preparedState = conn.prepareStatement(correctedSql);
                processStatementAttr(preparedState, sqlRequest);
                for (int i = 0; i < ((PrepareSqlRequest) sqlRequest).getParams().length; i++) {
                    setParam(preparedState, i + 1, ((PrepareSqlRequest) sqlRequest).getParams()[i]);
                }
                resultSet = preparedState.executeQuery();
            } else {
                stat = conn.createStatement();
                processStatementAttr(stat, sqlRequest);
                resultSet = stat.executeQuery(correctedSql);
            }

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

        } catch (SQLException sqlException) {
            Pair<List<List<String>>, List<SelectedColumnMeta>> r = null;
            try {
                r = PushDownUtil.tryPushDownSelectQuery(sqlRequest.getProject(), correctedSql, conn.getSchema(),
                        sqlException);
            } catch (Exception e2) {
                //exception in pushdown engine will be printed, but we'll not re-throw it, we'll 
                logger.info("pushdown engine failed current query too", e2);
            }

            if (r == null)
                throw sqlException;

            isPushDown = true;
            results = r.getFirst();
            columnMetas = r.getSecond();

        } finally {
            close(resultSet, stat, null); //conn is passed in, not my duty to close
        }

        return buildSqlResponse(isPushDown, results, columnMetas);
    }

    protected String makeErrorMsgUserFriendly(Throwable e) {
        return QueryUtil.makeErrorMsgUserFriendly(e);
    }

    private SQLResponse getPrepareOnlySqlResponse(String correctedSql, Connection conn, Boolean isPushDown,
            List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws SQLException {

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

        return buildSqlResponse(isPushDown, results, columnMetas);
    }

    private boolean isPrepareStatementWithParams(SQLRequest sqlRequest) {
        if (sqlRequest instanceof PrepareSqlRequest && ((PrepareSqlRequest) sqlRequest).getParams() != null
                && ((PrepareSqlRequest) sqlRequest).getParams().length > 0)
            return true;
        return false;
    }

    private SQLResponse buildSqlResponse(Boolean isPushDown, List<List<String>> results,
            List<SelectedColumnMeta> columnMetas) {

        boolean isPartialResult = false;
        StringBuilder cubeSb = new StringBuilder();
        StringBuilder logSb = new StringBuilder("Processed rows for each storageContext: ");
        if (OLAPContext.getThreadLocalContexts() != null) { // contexts can be null in case of 'explain plan for'
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                if (ctx.realization != null) {
                    isPartialResult |= ctx.storageContext.isPartialResultReturned();
                    if (cubeSb.length() > 0) {
                        cubeSb.append(",");
                    }
                    cubeSb.append(ctx.realization.getCanonicalName());
                    logSb.append(ctx.storageContext.getProcessedRowCount()).append(" ");
                }
            }
        }
        logger.info(logSb.toString());

        SQLResponse response = new SQLResponse(columnMetas, results, cubeSb.toString(), 0, false, null, isPartialResult,
                isPushDown);
        response.setTotalScanCount(QueryContext.current().getScannedRows());
        response.setTotalScanBytes(QueryContext.current().getScannedBytes());
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

        Class<?> clazz;
        try {
            clazz = Class.forName(param.getClassName());
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException(e);
        }

        Rep rep = Rep.of(clazz);

        switch (rep) {
        case PRIMITIVE_CHAR:
        case CHARACTER:
        case STRING:
            preparedState.setString(index, isNull ? null : String.valueOf(param.getValue()));
            break;
        case PRIMITIVE_INT:
        case INTEGER:
            preparedState.setInt(index, isNull ? 0 : Integer.valueOf(param.getValue()));
            break;
        case PRIMITIVE_SHORT:
        case SHORT:
            preparedState.setShort(index, isNull ? 0 : Short.valueOf(param.getValue()));
            break;
        case PRIMITIVE_LONG:
        case LONG:
            preparedState.setLong(index, isNull ? 0 : Long.valueOf(param.getValue()));
            break;
        case PRIMITIVE_FLOAT:
        case FLOAT:
            preparedState.setFloat(index, isNull ? 0 : Float.valueOf(param.getValue()));
            break;
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            preparedState.setDouble(index, isNull ? 0 : Double.valueOf(param.getValue()));
            break;
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            preparedState.setBoolean(index, !isNull && Boolean.parseBoolean(param.getValue()));
            break;
        case PRIMITIVE_BYTE:
        case BYTE:
            preparedState.setByte(index, isNull ? 0 : Byte.valueOf(param.getValue()));
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

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
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
