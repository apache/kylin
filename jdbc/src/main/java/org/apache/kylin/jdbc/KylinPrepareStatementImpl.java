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

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Collections;
import java.util.List;

import net.hydromatic.avatica.AvaticaConnection;
import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;
import net.hydromatic.avatica.AvaticaResultSet;

/**
 * Kylin prepare statement. <br>
 * Supported operations:
 * <ul>
 * <li>setString</li>
 * <li>setInt</li>
 * <li>setShort</li>
 * <li>setLong</li>
 * <li>setFloat</li>
 * <li>setDouble</li>
 * <li>setBoolean</li>
 * <li>setByte</li>
 * <li>setDate</li>
 * <li>setTime</li>
 * <li>setTimestamp</li>
 * </ul>
 * 
 * @author xduo
 * 
 */
public abstract class KylinPrepareStatementImpl extends AvaticaPreparedStatement {

    /**
     * Before real query,
     */
    protected AvaticaPrepareResult prequeryResult;

    protected KylinPrepareStatementImpl(AvaticaConnection connection, AvaticaPrepareResult prepareResult, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        super(connection, prepareResult, resultSetType, resultSetConcurrency, resultSetHoldability);

        this.prequeryResult = prepareResult;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        AvaticaPrepareResult queriedResult = ((KylinConnectionImpl) this.connection).getMeta().prepare(this, this.prequeryResult.getSql());

        return executeQueryInternal(queriedResult);
    }

    @Override
    protected void close_() {
        if (!closed) {
            closed = true;
            final KylinConnectionImpl connection_ = (KylinConnectionImpl) connection;
            connection_.statements.remove(this);
            if (openResultSet != null) {
                AvaticaResultSet c = openResultSet;
                openResultSet = null;
                c.close();
            }
            // If onStatementClose throws, this method will throw an
            // exception (later
            // converted to SQLException), but this statement still gets
            // closed.
            connection_.getDriver().handler.onStatementClose(this);
        }
    }

    public List<Object> getParameterValues() {
        return (List<Object>) Collections.unmodifiableList(super.getParameterValues());
    }

    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        getParameter(parameterIndex).setRowId(x);
    }

    public void setNString(int parameterIndex, String value) throws SQLException {
        getParameter(parameterIndex).setNString(value);
    }

    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        getParameter(parameterIndex).setNCharacterStream(value, length);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        getParameter(parameterIndex).setNClob(value);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getParameter(parameterIndex).setClob(reader, length);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        getParameter(parameterIndex).setBlob(inputStream, length);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        getParameter(parameterIndex).setNClob(reader, length);
    }

    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        getParameter(parameterIndex).setSQLXML(xmlObject);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getParameter(parameterIndex).setAsciiStream(x, length);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        getParameter(parameterIndex).setBinaryStream(x, length);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        getParameter(parameterIndex).setCharacterStream(reader, length);
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        getParameter(parameterIndex).setAsciiStream(x);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        getParameter(parameterIndex).setBinaryStream(x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        getParameter(parameterIndex).setCharacterStream(reader);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        getParameter(parameterIndex).setNCharacterStream(value);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        getParameter(parameterIndex).setClob(reader);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        getParameter(parameterIndex).setBlob(inputStream);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        getParameter(parameterIndex).setNClob(reader);
    }
}
