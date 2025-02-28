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

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.remote.TypedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinPreparedStatement extends AvaticaPreparedStatement {

    private static final Logger logger = LoggerFactory.getLogger(KylinPreparedStatement.class);

    private String queryId;

    protected KylinPreparedStatement(AvaticaConnection connection, StatementHandle h, Signature signature,
            int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        super(connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
        entry(logger);
        if (this.handle.signature == null) {
            this.handle.signature = signature;
        }
        exit(logger);
    }

    protected List<Object> getParameterJDBCValues() {
        entry(logger);
        List<TypedValue> typeValues = getParameterValues();
        List<Object> jdbcValues = new ArrayList<>(typeValues.size());
        for (TypedValue typeValue : typeValues) {
            Object jdbcValue = typeValue == null ? null : typeValue.toJdbc(getCalendar());
            jdbcValues.add(jdbcValue);
        }
        exit(logger);
        return jdbcValues;
    }

    // ============================================================================

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        getSite(parameterIndex).setRowId(x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        getSite(parameterIndex).setNString(value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        getSite(parameterIndex).setNCharacterStream(value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        getSite(parameterIndex).setNClob(value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getSite(parameterIndex).setClob(reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        getSite(parameterIndex).setBlob(inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getSite(parameterIndex).setNClob(reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        getSite(parameterIndex).setSQLXML(xmlObject);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getSite(parameterIndex).setAsciiStream(x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getSite(parameterIndex).setBinaryStream(x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        getSite(parameterIndex).setCharacterStream(reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        getSite(parameterIndex).setAsciiStream(x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        getSite(parameterIndex).setBinaryStream(x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        getSite(parameterIndex).setCharacterStream(reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        getSite(parameterIndex).setNCharacterStream(value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        getSite(parameterIndex).setClob(reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        getSite(parameterIndex).setBlob(inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        getSite(parameterIndex).setNClob(reader);
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        logger.debug("Query id is set manually to {}", queryId);
        this.queryId = queryId;
    }

    public void resetQueryId() {
        logger.trace("Reset query id");
        this.queryId = null;
    }
}
