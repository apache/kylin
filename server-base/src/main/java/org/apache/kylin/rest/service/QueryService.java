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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.metrics.QueryMetricsFacade;
import org.apache.kylin.rest.model.ColumnMeta;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.model.SelectedColumnMeta;
import org.apache.kylin.rest.model.TableMeta;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.QueryUtil;
import org.apache.kylin.rest.util.Serializer;
import org.apache.kylin.rest.util.TableauInterceptor;
import org.apache.kylin.storage.exception.ScanOutOfLimitException;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);

    public static final String USER_QUERY_FAMILY = "q";
    private static final String USER_TABLE_NAME = "_user";
    private static final String USER_QUERY_COLUMN = "c";

    public static final String SUCCESS_QUERY_CACHE = "StorageCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";

    private final Serializer<Query[]> querySerializer = new Serializer<Query[]>(Query[].class);
    private final BadQueryDetector badQueryDetector = new BadQueryDetector();

    private final String hbaseUrl;
    private final String userTableName;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private CacheService cacheService;

    @PostConstruct
    public void init() throws IOException {
        Preconditions.checkNotNull(cacheManager, "cacheManager is not injected yet");
    }

    public QueryService() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String metadataUrl = kylinConfig.getMetadataUrl();
        // split TABLE@HBASE_URL
        int cut = metadataUrl.indexOf('@');
        hbaseUrl = cut < 0 ? metadataUrl : metadataUrl.substring(cut + 1);
        String tableNameBase = kylinConfig.getMetadataUrlPrefix();
        userTableName = tableNameBase + USER_TABLE_NAME;

        badQueryDetector.start();
    }

    public List<TableMeta> getMetadata(String project) throws SQLException {
        return getMetadata(getCubeManager(), project, true);
    }

    public SQLResponse query(SQLRequest sqlRequest) throws Exception {
        try {
            final String user = SecurityContextHolder.getContext().getAuthentication().getName();
            badQueryDetector.queryStart(Thread.currentThread(), sqlRequest, user);

            return queryWithSqlMassage(sqlRequest);

        } finally {
            badQueryDetector.queryEnd(Thread.currentThread());
        }
    }

    public void saveQuery(final String creator, final Query query) throws IOException {
        List<Query> queries = getQueries(creator);
        queries.add(query);
        Query[] queryArray = new Query[queries.size()];

        byte[] bytes = querySerializer.serialize(queries.toArray(queryArray));
        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));
            Put put = new Put(Bytes.toBytes(creator));
            put.addColumn(Bytes.toBytes(USER_QUERY_FAMILY), Bytes.toBytes(USER_QUERY_COLUMN), bytes);

            htable.put(put);
        } finally {
            IOUtils.closeQuietly(htable);
        }
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
        byte[] bytes = querySerializer.serialize(queries.toArray(queryArray));
        Table htable = null;
        try {
            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));
            Put put = new Put(Bytes.toBytes(creator));
            put.addColumn(Bytes.toBytes(USER_QUERY_FAMILY), Bytes.toBytes(USER_QUERY_COLUMN), bytes);

            htable.put(put);
        } finally {
            IOUtils.closeQuietly(htable);
        }
    }

    public List<Query> getQueries(final String creator) throws IOException {
        if (null == creator) {
            return null;
        }

        List<Query> queries = new ArrayList<Query>();
        Table htable = null;
        try {
            org.apache.hadoop.hbase.client.Connection conn = HBaseConnection.get(hbaseUrl);
            HBaseConnection.createHTableIfNeeded(conn, userTableName, USER_QUERY_FAMILY);

            htable = HBaseConnection.get(hbaseUrl).getTable(TableName.valueOf(userTableName));
            Get get = new Get(Bytes.toBytes(creator));
            get.addFamily(Bytes.toBytes(USER_QUERY_FAMILY));
            Result result = htable.get(get);
            Query[] query = querySerializer.deserialize(result.getValue(Bytes.toBytes(USER_QUERY_FAMILY), Bytes.toBytes(USER_QUERY_COLUMN)));

            if (null != query) {
                queries.addAll(Arrays.asList(query));
            }
        } finally {
            IOUtils.closeQuietly(htable);
        }

        return queries;
    }

    public void logQuery(final SQLRequest request, final SQLResponse response) {
        final String user = SecurityContextHolder.getContext().getAuthentication().getName();
        final Set<String> realizationNames = new HashSet<String>();
        final Set<Long> cuboidIds = new HashSet<Long>();
        float duration = response.getDuration() / (float) 1000;
        boolean storageCacheUsed = response.isStorageCacheUsed();

        if (!response.isHitExceptionCache() && null != OLAPContext.getThreadLocalContexts()) {
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                Cuboid cuboid = ctx.storageContext.getCuboid();
                if (cuboid != null) {
                    //Some queries do not involve cuboid, e.g. lookup table query
                    cuboidIds.add(cuboid.getId());
                }

                if (ctx.realization != null) {
                    String realizationName = ctx.realization.getName();
                    realizationNames.add(realizationName);
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
        stringBuilder.append("Query Id: ").append(QueryContext.getQueryId()).append(newLine);
        stringBuilder.append("SQL: ").append(request.getSql()).append(newLine);
        stringBuilder.append("User: ").append(user).append(newLine);
        stringBuilder.append("Success: ").append((null == response.getExceptionMessage())).append(newLine);
        stringBuilder.append("Duration: ").append(duration).append(newLine);
        stringBuilder.append("Project: ").append(request.getProject()).append(newLine);
        stringBuilder.append("Realization Names: ").append(realizationNames).append(newLine);
        stringBuilder.append("Cuboid Ids: ").append(cuboidIds).append(newLine);
        stringBuilder.append("Total scan count: ").append(response.getTotalScanCount()).append(newLine);
        stringBuilder.append("Result row count: ").append(resultRowCount).append(newLine);
        stringBuilder.append("Accept Partial: ").append(request.isAcceptPartial()).append(newLine);
        stringBuilder.append("Is Partial Result: ").append(response.isPartial()).append(newLine);
        stringBuilder.append("Hit Exception Cache: ").append(response.isHitExceptionCache()).append(newLine);
        stringBuilder.append("Storage cache used: ").append(storageCacheUsed).append(newLine);
        stringBuilder.append("Message: ").append(response.getExceptionMessage()).append(newLine);
        stringBuilder.append("==========================[QUERY]===============================").append(newLine);

        logger.info(stringBuilder.toString());
    }

    public void checkAuthorization(String cubeName) throws AccessDeniedException {
        // special care for hybrid
        HybridInstance hybridInstance = getHybridManager().getHybridInstance(cubeName);
        if (hybridInstance != null) {
            checkHybridAuthorization(hybridInstance);
            return;
        }

        CubeInstance cubeInstance = getCubeManager().getCube(cubeName);
        checkCubeAuthorization(cubeInstance);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')" + " or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'READ')")
    private void checkCubeAuthorization(CubeInstance cube) throws AccessDeniedException {
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
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String serverMode = kylinConfig.getServerMode();
        if (!(Constant.SERVER_MODE_QUERY.equals(serverMode.toLowerCase()) || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase()))) {
            throw new InternalErrorException("Query is not allowed in " + serverMode + " mode.");
        }
        if (StringUtils.isBlank(sqlRequest.getProject())) {
            throw new InternalErrorException("Project cannot be empty. Please select a project.");
        }

        final String queryId = UUID.randomUUID().toString();
        if (sqlRequest.getBackdoorToggles() != null)
            BackdoorToggles.addToggles(sqlRequest.getBackdoorToggles());
        QueryContext.setQueryId(queryId);

        try (SetThreadName ignored = new SetThreadName("Query %s", queryId)) {
            String sql = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            logger.info("Using project: " + project);
            logger.info("The original query:  " + sql);

            if (!sql.toLowerCase().contains("select")) {
                logger.debug("Directly return exception as not supported");
                throw new InternalErrorException("Not Supported SQL.");
            }

            long startTime = System.currentTimeMillis();

            SQLResponse sqlResponse = null;
            boolean queryCacheEnabled = checkCondition(kylinConfig.isQueryCacheEnabled(), "query cache disabled in KylinConfig") && //
                    checkCondition(!BackdoorToggles.getDisableCache(), "query cache disabled in BackdoorToggles");

            if (queryCacheEnabled) {
                sqlResponse = searchQueryInCache(sqlRequest);
            }

            try {
                if (null == sqlResponse) {
                    sqlResponse = query(sqlRequest);

                    long durationThreshold = kylinConfig.getQueryDurationCacheThreshold();
                    long scancountThreshold = kylinConfig.getQueryScanCountCacheThreshold();
                    sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                    logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                            String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()), String.valueOf(sqlResponse.getTotalScanCount()));
                    if (checkCondition(queryCacheEnabled, "query cache is disabled") && //
                            checkCondition(!sqlResponse.getIsException(), "query has exception") && //
                            checkCondition(sqlResponse.getDuration() > durationThreshold || sqlResponse.getTotalScanCount() > scancountThreshold, "query is too lightweight with duration: {} ({}), scan count: {} ({})", sqlResponse.getDuration(), durationThreshold, sqlResponse.getTotalScanCount(), scancountThreshold) && // 
                            checkCondition(sqlResponse.getResults().size() < kylinConfig.getLargeQueryThreshold(), "query response is too large: {} ({})", sqlResponse.getResults().size(), kylinConfig.getLargeQueryThreshold())) {
                        cacheManager.getCache(SUCCESS_QUERY_CACHE).put(new Element(sqlRequest, sqlResponse));
                    }

                } else {
                    sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                }

                checkQueryAuth(sqlResponse);

            } catch (Throwable e) { // calcite may throw AssertError
                logger.error("Exception when execute sql", e);
                String errMsg = QueryUtil.makeErrorMsgUserFriendly(e);

                sqlResponse = new SQLResponse(null, null, 0, true, errMsg);

                // for exception queries, only cache ScanOutOfLimitException
                if (queryCacheEnabled && e instanceof ScanOutOfLimitException) {
                    Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
                    exceptionCache.put(new Element(sqlRequest, sqlResponse));
                }
            }

            logQuery(sqlRequest, sqlResponse);

            QueryMetricsFacade.updateMetrics(sqlRequest, sqlResponse);

            if (sqlResponse.getIsException())
                throw new InternalErrorException(sqlResponse.getExceptionMessage());

            return sqlResponse;

        } finally {
            BackdoorToggles.cleanToggles();
        }
    }

    public SQLResponse searchQueryInCache(SQLRequest sqlRequest) {
        SQLResponse response = null;
        Cache exceptionCache = cacheManager.getCache(EXCEPTION_QUERY_CACHE);
        Cache successCache = cacheManager.getCache(SUCCESS_QUERY_CACHE);

        if (exceptionCache.get(sqlRequest) != null) {
            logger.info("The sqlResponse is found in EXCEPTION_QUERY_CACHE");
            Element element = exceptionCache.get(sqlRequest);
            response = (SQLResponse) element.getObjectValue();
            response.setHitExceptionCache(true);
        } else if (successCache.get(sqlRequest) != null) {
            logger.info("The sqlResponse is found in SUCCESS_QUERY_CACHE");
            Element element = successCache.get(sqlRequest);
            response = (SQLResponse) element.getObjectValue();
            response.setStorageCacheUsed(true);
        }

        return response;
    }

    private void checkQueryAuth(SQLResponse sqlResponse) throws AccessDeniedException {
        if (!sqlResponse.getIsException() && KylinConfig.getInstanceFromEnv().isQuerySecureEnabled()) {
            checkAuthorization(sqlResponse.getCube());
        }
    }

    private SQLResponse queryWithSqlMassage(SQLRequest sqlRequest) throws Exception {
        String userInfo = SecurityContextHolder.getContext().getAuthentication().getName();
        final Collection<? extends GrantedAuthority> grantedAuthorities = SecurityContextHolder.getContext().getAuthentication().getAuthorities();
        for (GrantedAuthority grantedAuthority : grantedAuthorities) {
            userInfo += ",";
            userInfo += grantedAuthority.getAuthority();
        }

        SQLResponse fakeResponse = TableauInterceptor.tableauIntercept(sqlRequest.getSql());
        if (null != fakeResponse) {
            logger.debug("Return fake response, is exception? " + fakeResponse.getIsException());
            return fakeResponse;
        }

        String correctedSql = QueryUtil.massageSql(sqlRequest);
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

        return execute(correctedSql, sqlRequest);

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
            DataSource dataSource = cacheService.getOLAPDataSource(project);
            conn = dataSource.getConnection();
            DatabaseMetaData metaData = conn.getMetaData();

            JDBCTableMeta = metaData.getTables(null, null, null, null);

            tableMetas = new LinkedList<TableMeta>();
            Map<String, TableMeta> tableMap = new HashMap<String, TableMeta>();
            while (JDBCTableMeta.next()) {
                String catalogName = JDBCTableMeta.getString(1);
                String schemaName = JDBCTableMeta.getString(2);

                // Not every JDBC data provider offers full 10 columns, e.g., PostgreSQL has only 5
                TableMeta tblMeta = new TableMeta(catalogName == null ? Constant.FakeCatalogName : catalogName, schemaName == null ? Constant.FakeSchemaName : schemaName, JDBCTableMeta.getString(3), JDBCTableMeta.getString(4), JDBCTableMeta.getString(5), null, null, null, null, null);

                if (!cubedOnly || getProjectManager().isExposedTable(project, schemaName + "." + tblMeta.getTABLE_NAME())) {
                    tableMetas.add(tblMeta);
                    tableMap.put(tblMeta.getTABLE_SCHEM() + "#" + tblMeta.getTABLE_NAME(), tblMeta);
                }
            }

            columnMeta = metaData.getColumns(null, null, null, null);

            while (columnMeta.next()) {
                String catalogName = columnMeta.getString(1);
                String schemaName = columnMeta.getString(2);

                // kylin(optiq) is not strictly following JDBC specification
                ColumnMeta colmnMeta = new ColumnMeta(catalogName == null ? Constant.FakeCatalogName : catalogName, schemaName == null ? Constant.FakeSchemaName : schemaName, columnMeta.getString(3), columnMeta.getString(4), columnMeta.getInt(5), columnMeta.getString(6), columnMeta.getInt(7), getInt(columnMeta.getString(8)), columnMeta.getInt(9), columnMeta.getInt(10), columnMeta.getInt(11), columnMeta.getString(12), columnMeta.getString(13), getInt(columnMeta.getString(14)), getInt(columnMeta.getString(15)), columnMeta.getInt(16), columnMeta.getInt(17), columnMeta.getString(18), columnMeta.getString(19), columnMeta.getString(20), columnMeta.getString(21), getShort(columnMeta.getString(22)), columnMeta.getString(23));

                if (!cubedOnly || getProjectManager().isExposedColumn(project, schemaName + "." + colmnMeta.getTABLE_NAME(), colmnMeta.getCOLUMN_NAME())) {
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

    private void processStatementAttr(Statement s, SQLRequest sqlRequest) throws SQLException {
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
    private SQLResponse execute(String correctedSql, SQLRequest sqlRequest) throws Exception {
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;

        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        try {
            conn = cacheService.getOLAPDataSource(sqlRequest.getProject()).getConnection();

            if (sqlRequest instanceof PrepareSqlRequest) {
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
                columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i), metaData.isSearchable(i), metaData.isCurrency(i), metaData.isNullable(i), metaData.isSigned(i), metaData.getColumnDisplaySize(i), metaData.getColumnLabel(i), metaData.getColumnName(i), metaData.getSchemaName(i), metaData.getCatalogName(i), metaData.getTableName(i), metaData.getPrecision(i), metaData.getScale(i), metaData.getColumnType(i), metaData.getColumnTypeName(i), metaData.isReadOnly(i), metaData.isWritable(i), metaData.isDefinitelyWritable(i)));
            }

            // fill in results
            while (resultSet.next()) {
                List<String> oneRow = Lists.newArrayListWithCapacity(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    oneRow.add((resultSet.getString(i + 1)));
                }

                results.add(oneRow);
            }
        } finally {
            close(resultSet, stat, conn);
        }

        boolean isPartialResult = false;
        String cube = "";
        StringBuilder sb = new StringBuilder("Scan count for each storageContext: ");
        long totalScanCount = 0;
        if (OLAPContext.getThreadLocalContexts() != null) { // contexts can be null in case of 'explain plan for'
            for (OLAPContext ctx : OLAPContext.getThreadLocalContexts()) {
                if (ctx.realization != null) {
                    isPartialResult |= ctx.storageContext.isPartialResultReturned();
                    cube = ctx.realization.getName();
                    totalScanCount += ctx.storageContext.getTotalScanCount();
                    sb.append(ctx.storageContext.getTotalScanCount() + ",");
                }
            }
        }
        logger.info(sb.toString());

        SQLResponse response = new SQLResponse(columnMetas, results, cube, 0, false, null, isPartialResult);
        response.setTotalScanCount(totalScanCount);

        return response;
    }

    /**
     * @param preparedState
     * @param param
     * @throws SQLException
     */
    private void setParam(PreparedStatement preparedState, int index, PrepareSqlRequest.StateParam param) throws SQLException {
        boolean isNull = (null == param.getValue());

        Class<?> clazz;
        try {
            clazz = Class.forName(param.getClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
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

    private int getInt(String content) {
        try {
            return Integer.parseInt(content);
        } catch (Exception e) {
            return -1;
        }
    }

    private short getShort(String content) {
        try {
            return Short.parseShort(content);
        } catch (Exception e) {
            return -1;
        }
    }

    private static void close(ResultSet resultSet, Statement stat, Connection conn) {
        OLAPContext.clearParameter();
        DBUtils.closeQuietly(resultSet);
        DBUtils.closeQuietly(stat);
        DBUtils.closeQuietly(conn);
    }

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
}
