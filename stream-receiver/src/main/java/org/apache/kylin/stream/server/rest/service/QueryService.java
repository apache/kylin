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

package org.apache.kylin.stream.server.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.stream.server.rest.model.PrepareSqlRequest;
import org.apache.kylin.stream.server.rest.model.SQLRequest;
import org.apache.kylin.stream.server.rest.model.SQLResponse;
import org.apache.kylin.stream.server.rest.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * @author xduo
 */
@Component("queryService")
public class QueryService {

    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);

    public QueryService() {
    }

    private static void close(ResultSet resultSet, Statement stat, Connection conn) {
        OLAPContext.clearParameter();

        if (resultSet != null)
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.error("failed to close", e);
            }
        if (stat != null)
            try {
                stat.close();
            } catch (SQLException e) {
                logger.error("failed to close", e);
            }
        if (conn != null)
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("failed to close", e);
            }
    }

    public SQLResponse query(SQLRequest sqlRequest) throws Exception {
        return queryWithSqlMassage(sqlRequest);
    }

    private SQLResponse queryWithSqlMassage(SQLRequest sqlRequest) throws Exception {
        SQLResponse fakeResponse = QueryUtil.tableauIntercept(sqlRequest.getSql());
        if (null != fakeResponse) {
            logger.debug("Return fake response, is exception? " + fakeResponse.getIsException());
            return fakeResponse;
        }

        String correctedSql = QueryUtil.massageSql(sqlRequest);
        if (correctedSql.equals(sqlRequest.getSql()) == false)
            logger.info("The corrected query: " + correctedSql);

        // add extra parameters into olap context, like acceptPartial
        Map<String, String> parameters = new HashMap<String, String>();
        parameters.put(OLAPContext.PRM_ACCEPT_PARTIAL_RESULT, String.valueOf(sqlRequest.isAcceptPartial()));
        OLAPContext.setParameters(parameters);

        return execute(correctedSql, sqlRequest);
    }

    /**
     * @param sql
     * @param sqlRequest
     * @return
     * @throws Exception
     */
    private SQLResponse execute(String sql, SQLRequest sqlRequest) throws Exception {
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;

        List<List<String>> results = Lists.newArrayList();
        List<SelectedColumnMeta> columnMetas = Lists.newArrayList();

        try {
            conn = QueryConnection.getConnection(sqlRequest.getProject());
            if (sqlRequest instanceof PrepareSqlRequest) {
                PreparedStatement preparedState = conn.prepareStatement(sql);

                for (int i = 0; i < ((PrepareSqlRequest) sqlRequest).getParams().length; i++) {
                    setParam(preparedState, i + 1, ((PrepareSqlRequest) sqlRequest).getParams()[i]);
                }

                resultSet = preparedState.executeQuery();
            } else {
                stat = conn.createStatement();
                resultSet = stat.executeQuery(sql);
            }

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Fill in selected column meta
            for (int i = 1; i <= columnCount; ++i) {
                columnMetas.add(new SelectedColumnMeta(metaData.isAutoIncrement(i), metaData.isCaseSensitive(i),
                        metaData.isSearchable(i), metaData.isCurrency(i), metaData.isNullable(i), metaData.isSigned(i),
                        metaData.getColumnDisplaySize(i), metaData.getColumnLabel(i), metaData.getColumnName(i),
                        metaData.getSchemaName(i), metaData.getCatalogName(i), metaData.getTableName(i), metaData
                                .getPrecision(i), metaData.getScale(i), metaData.getColumnType(i), metaData
                                .getColumnTypeName(i), metaData.isReadOnly(i), metaData.isWritable(i), metaData
                                .isDefinitelyWritable(i)));
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
        QueryContext queryContext = QueryContextFacade.current();
        StringBuilder cubeSb = new StringBuilder();
        StringBuilder logSb = new StringBuilder("Processed rows for each storageContext: ");
        boolean isPartialResult = false;

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
                    logSb.append(ctx.storageContext.getProcessedRowCount()).append(" ");

                    realizationName = ctx.realization.getName();
                    realizationType = ctx.realization.getStorageType();
                }
                queryContext.setContextRealization(ctx.id, realizationName, realizationType);
            }
        }

        SQLResponse response = new SQLResponse(columnMetas, results, cubeSb.toString(), 0, false, null, isPartialResult);
        response.setTotalScanCount(queryContext.getScannedRows());

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

}
