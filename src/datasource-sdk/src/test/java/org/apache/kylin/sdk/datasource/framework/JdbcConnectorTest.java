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
package org.apache.kylin.sdk.datasource.framework;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;

import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.sdk.datasource.framework.conv.SqlConverter;
import org.apache.kylin.source.jdbc.H2Database;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JdbcConnectorTest extends NLocalFileMetadataTestCase {
    protected static JdbcConnector connector = null;
    protected static Connection h2Conn = null;
    protected static H2Database h2Db = null;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "testing");

        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        h2Conn = connector.getConnection();

        h2Db = new H2Database(h2Conn, getTestConfig(), "default");
        h2Db.loadAllTables();
    }

    @After
    public void after() throws Exception {
        h2Db.dropAll();
        DBUtils.closeQuietly(h2Conn);

        cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws SQLException {
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        Assert.assertNotNull(connector);
        Assert.assertNotNull(connector.getJdbcDriver());
        Assert.assertNotNull(connector.getJdbcUrl());
        Assert.assertNotNull(connector.getJdbcUser());
        Assert.assertNotNull(connector.getJdbcPassword());

        try (Connection conn = connector.getConnection()) {
            Assert.assertNotNull(conn);
            Assert.assertTrue(!conn.isClosed());
        }

        Assert.assertNotNull(connector.convertSql("select 1"));
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertFalse(connector.listDatabases().contains("EDW"));
        Assert.assertTrue(connector.listDatabases().contains("DEFAULT"));
        Assert.assertFalse(connector.listTables("DEFAULT").isEmpty());
        Assert.assertTrue(connector.listColumns("DEFAULT", "TEST_KYLIN_FACT").next());
        Assert.assertNotNull(connector.buildSqlToCreateSchema("NEW_SCHEMA"));
        Assert.assertNotNull(connector.buildSqlToCreateTable("NEW_TABLE", new LinkedHashMap<String, String>()));
        Assert.assertNotNull(connector.buildSqlToCreateView("NEW_VIEW", "select 1"));
        Assert.assertNotNull(connector.buildSqlToLoadDataFromLocal("TABLE", "/tmp"));

        connector.executeUpdate("select 1"); // expected no exceptions

        connector.executeUpdate(new String[] { "select 1" });// expected no exceptions

        SqlConverter.IConfigurer configurer = connector.getSqlConverter().getConfigurer();
        Assert.assertTrue(configurer.allowFetchNoRows());
        Assert.assertTrue(configurer.allowNoOffset());
        Assert.assertTrue(configurer.allowNoOrderByWithFetch());
        Assert.assertFalse(configurer.skipHandleDefault());
        Assert.assertFalse(configurer.skipDefaultConvert());

        Assert.assertEquals(Types.DOUBLE, connector.toKylinTypeId("DOUBLE PRECISION", 0));
        Assert.assertEquals(Types.DOUBLE, connector.toKylinTypeId("double PRECISION", 0));
        Assert.assertEquals(Types.VARCHAR, connector.toKylinTypeId("CHARACTER VARYING", 0));
    }

    @Test(expected = SQLException.class)
    public void testRetryAndClose() throws IOException, SQLException {
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "testing");
        getTestConfig().setProperty("kylin.source.jdbc.connect-retry-times", "2");
        getTestConfig().setProperty("kylin.source.jdbc.connect-retry-sleep-time", "100ms");
        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        connector.close();
        connector.getConnection();
    }

    @Test
    public void testWithCache() throws SQLException, IOException {
        getTestConfig().setProperty("kylin.source.jdbc.dialect", "testing_withcache");

        connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        Assert.assertFalse(connector.listDatabases().isEmpty());
        Assert.assertTrue(connector.listDatabases().contains("DEFAULT"));
        Assert.assertFalse(connector.listTables("DEFAULT").isEmpty());
        Assert.assertFalse(connector.listTables("DEFAULT").isEmpty());

        connector.close();

    }
}
