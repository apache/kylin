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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinConnection extends AvaticaConnection {

    private static final Logger logger = LoggerFactory.getLogger(KylinConnection.class);

    private final String baseUrl;
    private final String project;
    private final IRemoteClient remoteClient;

    protected KylinConnection(UnregisteredDriver driver, KylinJdbcFactory factory, String url, Properties info) throws SQLException {
        super(driver, factory, url, info);

        String odbcUrl = url;
        odbcUrl = odbcUrl.replace(Driver.CONNECT_STRING_PREFIX + "//", "");
        String[] temps = odbcUrl.split("/");

        assert temps.length == 2;

        this.baseUrl = temps[0];
        this.project = temps[1];

        logger.debug("Kylin base url " + this.baseUrl + ", project name " + this.project);

        this.remoteClient = factory.newRemoteClient(this);

        try {
            this.remoteClient.connect();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    String getBaseUrl() {
        return baseUrl;
    }

    String getProject() {
        return project;
    }

    Properties getConnectionProperties() {
        return info;
    }

    public boolean getAutoCommit() throws SQLException {
        if (meta.connectionSync(handle, new ConnectionPropertiesImpl()).isAutoCommit() == null)
            setAutoCommit(true);
        return super.getAutoCommit();
    }

    public boolean isReadOnly() throws SQLException {
        if (meta.connectionSync(handle, new ConnectionPropertiesImpl()).isReadOnly() == null)
            setReadOnly(true);
        return super.isReadOnly();
    }

    @Override
    public AvaticaStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return super.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        Meta.Signature sig = mockPreparedSignature(sql);
        return factory().newPreparedStatement(this, null, sig, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    // TODO add restful API to prepare SQL, get back expected ResultSetMetaData
    Signature mockPreparedSignature(String sql) {
        List<AvaticaParameter> params = new ArrayList<AvaticaParameter>();
        int startIndex = 0;
        while (sql.indexOf("?", startIndex) >= 0) {
            AvaticaParameter param = new AvaticaParameter(false, 0, 0, 0, null, null, null);
            params.add(param);
            startIndex = sql.indexOf("?", startIndex) + 1;
        }

        ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
        Map<String, Object> internalParams = Collections.<String, Object> emptyMap();

        return new Meta.Signature(columns, sql, params, internalParams, CursorFactory.ARRAY, Meta.StatementType.SELECT);
    }

    private KylinJdbcFactory factory() {
        return (KylinJdbcFactory) factory;
    }

    public IRemoteClient getRemoteClient() {
        return remoteClient;
    }

    @Override
    public void close() throws SQLException {
        super.close();
        try {
            remoteClient.close();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }
}
