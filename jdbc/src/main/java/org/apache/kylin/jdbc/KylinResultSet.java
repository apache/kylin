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

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.kylin.jdbc.IRemoteClient.QueryResult;

public class KylinResultSet extends AvaticaResultSet {

    public KylinResultSet(AvaticaStatement statement, QueryState state, Signature signature, ResultSetMetaData resultSetMetaData, TimeZone timeZone, Frame firstFrame) {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    }

    @Override
    protected AvaticaResultSet execute() throws SQLException {

        // skip execution if result is already there (case of meta data lookup)
        if (this.firstFrame != null) {
            return super.execute();
        }

        String sql = signature.sql;
        List<AvaticaParameter> params = signature.parameters;
        List<Object> paramValues = null;
        if (params != null && params.size() > 0) {
            paramValues = ((KylinPreparedStatement) statement).getParameterValues2();
        }

        IRemoteClient client = ((KylinConnection) statement.connection).getRemoteClient();
        QueryResult result;
        try {
            result = client.executeQuery(sql, params, paramValues);
        } catch (IOException e) {
            throw new SQLException(e);
        }

        columnMetaDataList.clear();
        columnMetaDataList.addAll(result.columnMeta);

        cursor = MetaImpl.createCursor(signature.cursorFactory, result.iterable);
        return super.execute2(cursor, columnMetaDataList);
    }

}
