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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
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

    protected KylinConnection(UnregisteredDriver driver, KylinJdbcFactory factory, String url, Properties info)
            throws SQLException {
        super(driver, factory, url, info);
        entry(logger);
        checkProperties();
        String odbcUrl = url;
        odbcUrl = odbcUrl.replaceAll(Driver.CONNECT_STRING_PREFIX + "([A-Za-z0-9\\_]+?=.*?;)*?//", "");

        String[] temps0 = odbcUrl.split("\\?");
        odbcUrl = temps0[0];

        String[] temps = odbcUrl.split("/");
        assert temps.length == 2;

        this.baseUrl = temps[0];
        this.project = temps[1];

        logger.info("------------------------");
        logger.info("Kylin Connection Info:");
        logger.info("base url: {}", this.baseUrl);
        logger.info("project name: {}", this.project);
        info.entrySet().stream().filter(infoEntry -> !String.valueOf(infoEntry.getKey()).equalsIgnoreCase("password"))
                .forEach(infoEntry -> logger.info("{}: {}", infoEntry.getKey(), infoEntry.getValue()));
        logger.info("------------------------");

        this.remoteClient = factory.newRemoteClient(this);

        try {
            this.remoteClient.connect();
        } catch (IOException e) {
            try {
                this.remoteClient.close();
            } catch (Exception eClose) {
                // close quietly
            }
            logger.error("Connect Kylin failed: ", e);
            throw new SQLException(e);
        }
        exit(logger);
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

    private void checkProperties() {
        String executeAs = info.getProperty("EXECUTE_AS_USER_ID");
        if (executeAs != null) {
            if (executeAs.isEmpty()) {
                IllegalArgumentException e = new IllegalArgumentException("The parameter “EXECUTE_AS_USER_ID” can’t be empty. "
                        + "please contact your system admin and check its value in the connection string for Kylin.");
                logger.error(e.getMessage());
                throw e;
            }

            if (executeAs.length() > 1024) {
                IllegalArgumentException e = new IllegalArgumentException("The value of parameter EXECUTE_AS_USER_ID can’t exceed 1024 characters. "
                        + "Please shorten your user id value.");
                logger.error(e.getMessage());
                throw e;
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        entry(logger);
        if (meta.connectionSync(handle, new ConnectionPropertiesImpl()).isAutoCommit() == null)
            setAutoCommit(true);
        boolean isAutoCommit = super.getAutoCommit();
        exit(logger);
        return isAutoCommit;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        entry(logger);
        if (meta.connectionSync(handle, new ConnectionPropertiesImpl()).isReadOnly() == null)
            setReadOnly(true);
        boolean isReadOnly = super.isReadOnly();
        exit(logger);
        return isReadOnly;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        entry(logger);
        Meta.Signature sig = mockPreparedSignature(sql);
        AvaticaPreparedStatement preparedStatement = factory().newPreparedStatement(
                this, null, sig, resultSetType, resultSetConcurrency, resultSetHoldability);
        exit(logger);
        return preparedStatement;
    }

    // TODO add restful API to prepare SQL, get back expected ResultSetMetaData
    Signature mockPreparedSignature(String sql) {
        List<AvaticaParameter> params = new ArrayList<>();
        int placeholderCount = KylinCheckSql.countDynamicPlaceholder(sql);
        for (int i = 1; i <= placeholderCount; i++) {
            AvaticaParameter param = new AvaticaParameter(false, 0, 0, 0, null, null, null);
            params.add(param);
        }

        ArrayList<ColumnMetaData> columns = new ArrayList<>();
        Map<String, Object> internalParams = Collections.emptyMap();

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
        entry(logger);
        super.close();
        try {
            remoteClient.close();
        } catch (IOException e) {
            throw new SQLException(e);
        }
        exit(logger);
    }
}
