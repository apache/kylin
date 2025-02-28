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

package org.apache.kylin.jdbc;

import static org.apache.kylin.jdbc.LoggerUtils.entry;
import static org.apache.kylin.jdbc.LoggerUtils.exit;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.util.AbstractCursor;
import org.apache.calcite.avatica.util.KylinDelegateCursor;
import org.apache.kylin.jdbc.IRemoteClient.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinResultSet extends AvaticaResultSet {

    private static final Logger logger = LoggerFactory.getLogger(KylinResultSet.class);

    private String queryId;
    private long resultRowCount;
    private long duration;

    public KylinResultSet(AvaticaStatement statement, QueryState state, Signature signature,
            ResultSetMetaData resultSetMetaData, TimeZone timeZone, Frame firstFrame) throws SQLException {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
        entry(logger);
        exit(logger);
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getQueryId() {
        return this.queryId;
    }

    public long getResultRowCount() {
        return resultRowCount;
    }

    public void setResultRowCount(long resultRowCount) {
        this.resultRowCount = resultRowCount;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    @Override
    protected AvaticaResultSet execute() throws SQLException {

        entry(logger);
        // skip execution if result is already there (case of meta data lookup)
        if (this.firstFrame != null) {
            return super.execute();
        }

        String sql = signature.sql;
        List<AvaticaParameter> params = signature.parameters;
        List<Object> paramValues = null;
        if (!(statement instanceof KylinPreparedStatement)) {
            params = null;
        } else if (params != null && params.size() > 0) {
            paramValues = ((KylinPreparedStatement) statement).getParameterJDBCValues();
        }

        String queryId = null;
        if (statement instanceof KylinPreparedStatement) {
            queryId = ((KylinPreparedStatement) statement).getQueryId();
            ((KylinPreparedStatement) statement).resetQueryId();
        } else if (statement instanceof KylinStatement) {
            queryId = ((KylinStatement) statement).getQueryId();
            ((KylinStatement) statement).resetQueryId();
        }

        IRemoteClient client = ((KylinConnection) statement.connection).getRemoteClient();

        Map<String, String> queryToggles = new HashMap<>();
        int maxRows = statement.getMaxRows();
        queryToggles.put("ATTR_STATEMENT_MAX_ROWS", String.valueOf(maxRows));

        QueryResult result;
        try {
            result = client.executeQuery(sql, params, paramValues, queryToggles, queryId);
        } catch (IOException e) {
            logger.error("Query failed: ", e);
            throw new SQLException(e);
        }

        columnMetaDataList.clear();
        columnMetaDataList.addAll(result.columnMeta);

        cursor = MetaImpl.createCursor(signature.cursorFactory, result.iterable);
        if (cursor instanceof AbstractCursor) {
            cursor = new KylinDelegateCursor((AbstractCursor) cursor);
        }
        AvaticaResultSet resultSet = super.execute2(cursor, columnMetaDataList);
        KylinResultSet kylinResultSet = (KylinResultSet) resultSet;
        kylinResultSet.setQueryId(result.queryId);
        kylinResultSet.setDuration(result.duration);
        kylinResultSet.setResultRowCount(result.resultRowCount);
        exit(logger);
        return kylinResultSet;
    }

}
