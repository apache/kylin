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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPrepareResult;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.UnregisteredDriver;

import org.apache.kylin.jdbc.stub.KylinClient;
import org.apache.kylin.jdbc.stub.RemoteClient;

/**
 * Kylin JDBC factory.
 * 
 * @author xduo
 * 
 */
public class KylinJdbc41Factory implements AvaticaFactory {
    private final int major;
    private final int minor;

    /** Creates a JDBC factory. */
    public KylinJdbc41Factory() {
        this(4, 1);
    }

    /** Creates a JDBC factory with given major/minor version number. */
    protected KylinJdbc41Factory(int major, int minor) {
        this.major = major;
        this.minor = minor;
    }

    public int getJdbcMajorVersion() {
        return major;
    }

    public int getJdbcMinorVersion() {
        return minor;
    }

    public AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
        return new KylinJdbc41Connection(driver, factory, url, info);
    }

    public AvaticaDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
        return new AvaticaJdbc41DatabaseMetaData(connection);
    }

    public AvaticaStatement newStatement(AvaticaConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        return new KylinJdbc41Statement(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection, AvaticaPrepareResult prepareResult, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new KylinJdbc41PreparedStatement(connection, prepareResult, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public AvaticaResultSet newResultSet(AvaticaStatement statement, AvaticaPrepareResult prepareResult, TimeZone timeZone) {
        final ResultSetMetaData metaData = newResultSetMetaData(statement, prepareResult.getColumnList());
        return new KylinResultSet(statement, prepareResult, metaData, timeZone);
    }

    public AvaticaResultSetMetaData newResultSetMetaData(AvaticaStatement statement, List<ColumnMetaData> columnMetaDataList) {
        return new AvaticaResultSetMetaData(statement, null, columnMetaDataList);
    }

    // ~ kylin sepcified
    public RemoteClient newRemoteClient(KylinConnectionImpl connection) {
        return new KylinClient(connection);
    }

    /** Implementation of Connection for JDBC 4.1. */
    private static class KylinJdbc41Connection extends KylinConnectionImpl {
        KylinJdbc41Connection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info) {
            super(driver, (KylinJdbc41Factory) factory, url, info);
        }
    }

    /** Implementation of Statement for JDBC 4.1. */
    public static class KylinJdbc41Statement extends KylinStatementImpl {
        public KylinJdbc41Statement(AvaticaConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
            super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
    }

    /** Implementation of PreparedStatement for JDBC 4.1. */
    public static class KylinJdbc41PreparedStatement extends KylinPrepareStatementImpl {
        KylinJdbc41PreparedStatement(AvaticaConnection connection, AvaticaPrepareResult sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            super(connection, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }
    }

    /** Implementation of DatabaseMetaData for JDBC 4.1. */
    private static class AvaticaJdbc41DatabaseMetaData extends AvaticaDatabaseMetaData {
        AvaticaJdbc41DatabaseMetaData(AvaticaConnection connection) {
            super(connection);
        }
    }
}
