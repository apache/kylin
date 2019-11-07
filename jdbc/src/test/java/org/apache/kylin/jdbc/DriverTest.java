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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.avatica.DriverVersion;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for Driver.
 */
public class DriverTest {

    @Test
    public void testVersion() {
        Driver driver = new DummyDriver();
        DriverVersion version = driver.getDriverVersion();
        Assert.assertNotEquals("unknown version", version.productVersion);
    }

    @Test
    public void testStatementWithMockData() throws SQLException {
        Driver driver = new DummyDriver();

        Connection conn = driver.connect("jdbc:kylin://test_url/test_db", null);

        ResultSet tables = conn.getMetaData().getTables(null, null, null, null);
        while (tables.next()) {
            for (int i = 0; i < 4; i++) {
                assertEquals("dummy", tables.getString(i + 1));
            }
            for (int i = 4; i < 10; i++) {
                assertEquals(null, tables.getString(i + 1));
            }
        }

        Statement state = conn.createStatement();
        ResultSet resultSet = state.executeQuery("select * from test_table");

        ResultSetMetaData metadata = resultSet.getMetaData();
        assertEquals(12, metadata.getColumnType(1));
        assertEquals("varchar", metadata.getColumnTypeName(1));
        assertEquals(1, metadata.isNullable(1));

        while (resultSet.next()) {
            assertEquals("foo", resultSet.getString(1));
            assertEquals("bar", resultSet.getString(2));
            assertEquals("tool", resultSet.getString(3));
        }

        resultSet.close();
        state.close();
        conn.close();
    }

    @Test
    public void testStatementWithQuestionMask() throws SQLException {
        Driver driver = new DummyDriver();

        Connection conn = driver.connect("jdbc:kylin://test_url/test_db", null);
        Statement state = conn.createStatement();
        ResultSet resultSet = state.executeQuery("select * from test_table where url not in ('http://a.b.com/?a=b')");
        ResultSetMetaData metadata = resultSet.getMetaData();
        assertEquals(12, metadata.getColumnType(1));
        assertEquals("varchar", metadata.getColumnTypeName(1));
        assertEquals(1, metadata.isNullable(1));

        while (resultSet.next()) {
            assertEquals("foo", resultSet.getString(1));
            assertEquals("bar", resultSet.getString(2));
            assertEquals("tool", resultSet.getString(3));
        }

        resultSet.close();
        state.close();
        conn.close();
    }

    @Test
    public void testDateAndTimeStampWithMockData() throws SQLException {
        Driver driver = new DummyDriver();

        Connection conn = driver.connect("jdbc:kylin://test_url/test_db", null);
        PreparedStatement state = conn.prepareStatement("select * from test_table where id=?");
        state.setInt(1, 10);
        ResultSet resultSet = state.executeQuery();

        ResultSetMetaData metadata = resultSet.getMetaData();
        assertEquals("date", metadata.getColumnTypeName(4));
        assertEquals("timestamp", metadata.getColumnTypeName(5));

        while (resultSet.next()) {
            assertEquals("2019-04-27", resultSet.getString(4));
            assertEquals("2019-04-27 17:30:03", resultSet.getString(5));
        }

        resultSet.close();
        state.close();
        conn.close();
    }

    @Test
    public void testMultipathOfDomainForConnection() throws SQLException {
        Driver driver = new DummyDriver();

        Connection conn = driver.connect("jdbc:kylin://test_url/kylin/test_db/", null);
        Statement state = conn.createStatement();
        ResultSet resultSet = state.executeQuery("select * from test_table where url not in ('http://a.b.com/?a=b') limit 1");
        ResultSetMetaData metadata = resultSet.getMetaData();
        assertEquals(12, metadata.getColumnType(1));
        assertEquals("varchar", metadata.getColumnTypeName(1));
        assertEquals(1, metadata.isNullable(1));

        while (resultSet.next()) {
            assertEquals("foo", resultSet.getString(1));
            assertEquals("bar", resultSet.getString(2));
            assertEquals("tool", resultSet.getString(3));
        }

        resultSet.close();
        state.close();
        conn.close();
    }

    @Test
    public void testPreparedStatementWithMockData() throws SQLException {
        Driver driver = new DummyDriver();

        Connection conn = driver.connect("jdbc:kylin://test_url/test_db", null);
        PreparedStatement state = conn.prepareStatement("select * from test_table where id=?");
        state.setInt(1, 10);
        ResultSet resultSet = state.executeQuery();

        ResultSetMetaData metadata = resultSet.getMetaData();
        assertEquals(12, metadata.getColumnType(1));
        assertEquals("varchar", metadata.getColumnTypeName(1));
        assertEquals(1, metadata.isNullable(1));

        while (resultSet.next()) {
            assertEquals("foo", resultSet.getString(1));
            assertEquals("bar", resultSet.getString(2));
            assertEquals("tool", resultSet.getString(3));
        }

        resultSet.close();
        state.close();
        conn.close();
    }

    @Ignore("require dev sandbox")
    @Test
    public void testWithCubeData() throws Exception {
        Driver driver = new Driver();
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://localhost:7070/default", info);

        ResultSet catalogs = conn.getMetaData().getCatalogs();
        System.out.println("CATALOGS");
        printResultSetMetaData(catalogs);
        printResultSet(catalogs);

        ResultSet schemas = conn.getMetaData().getSchemas();
        System.out.println("SCHEMAS");
        printResultSetMetaData(schemas);
        printResultSet(schemas);

        ResultSet tables = conn.getMetaData().getTables(null, null, null, null);
        System.out.println("TABLES");
        printResultSetMetaData(tables);
        printResultSet(tables);

        for (int j = 0; j < 3; j++) {
            Statement state = conn.createStatement();
            ResultSet resultSet = state.executeQuery("select * from test_kylin_fact");

            printResultSetMetaData(resultSet);
            printResultSet(resultSet);

            resultSet.close();
        }

        catalogs.close();
        schemas.close();
        tables.close();
        conn.close();
    }

    @Ignore("require dev sandbox")
    @Test
    public void testPreparedStatementWithCubeData() throws SQLException {
        Driver driver = new Driver();
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://localhost:7070/default", info);

        PreparedStatement state = conn
                .prepareStatement("select cal_dt, count(*) from test_kylin_fact where seller_id=? group by cal_dt");
        state.setLong(1, 10000001);
        ResultSet resultSet = state.executeQuery();

        printResultSetMetaData(resultSet);
        printResultSet(resultSet);

        resultSet.close();
        state.close();
        conn.close();
    }

    @Test
    public void testSSLFromURL() throws SQLException {
        Driver driver = new DummyDriver();
        Connection conn = driver.connect("jdbc:kylin:ssl=True;//test_url/test_db", null);
        assertEquals("test_url", ((KylinConnection) conn).getBaseUrl());
        assertEquals("test_db", ((KylinConnection) conn).getProject());
        assertTrue(Boolean.parseBoolean((String) ((KylinConnection) conn).getConnectionProperties().get("ssl")));
        conn.close();
    }

    @Test
    public void testCalciteProps() throws SQLException {
        Driver driver = new DummyDriver();
        Properties props = new Properties();
        props.setProperty("kylin.query.calcite.extras-props.caseSensitive", "true");
        props.setProperty("kylin.query.calcite.extras-props.unquotedCasing", "TO_LOWER");
        props.setProperty("kylin.query.calcite.extras-props.quoting", "BRACKET");
        KylinConnection conn = (KylinConnection) driver.connect("jdbc:kylin:test_url/test_db", props);
        Properties connProps = conn.getConnectionProperties();
        assertEquals("true", connProps.getProperty("kylin.query.calcite.extras-props.caseSensitive"));
        assertEquals("TO_LOWER", connProps.getProperty("kylin.query.calcite.extras-props.unquotedCasing"));
        assertEquals("BRACKET", connProps.getProperty("kylin.query.calcite.extras-props.quoting"));

        // parameters in url is prior to props parameter
        KylinConnection conn2 = (KylinConnection) driver.connect("jdbc:kylin:kylin.query.calcite.extras-props.caseSensitive=false;" +
                "kylin.query.calcite.extras-props.unquotedCasing=UNCHANGED;" +
                "kylin.query.calcite.extras-props.quoting=BACK_TICK;" +
                "test_url/test_db", props);
        Properties connProps2 = conn2.getConnectionProperties();
        assertEquals("false", connProps2.getProperty("kylin.query.calcite.extras-props.caseSensitive"));
        assertEquals("UNCHANGED", connProps2.getProperty("kylin.query.calcite.extras-props.unquotedCasing"));
        assertEquals("BACK_TICK", connProps2.getProperty("kylin.query.calcite.extras-props.quoting"));
        conn.close();
        conn2.close();
    }

    private void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        System.out.println("Data:");

        while (rs.next()) {
            StringBuilder buf = new StringBuilder();
            buf.append("[");
            for (int i = 0; i < meta.getColumnCount(); i++) {
                if (i > 0)
                    buf.append(", ");
                buf.append(rs.getString(i + 1));
            }
            buf.append("]");
            System.out.println(buf);
        }
    }

    private void printResultSetMetaData(ResultSet rs) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        System.out.println("Metadata:");

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            String metaStr = metadata.getCatalogName(i + 1) + " " + metadata.getColumnClassName(i + 1) + " "
                    + metadata.getColumnDisplaySize(i + 1) + " " + metadata.getColumnLabel(i + 1) + " "
                    + metadata.getColumnName(i + 1) + " " + metadata.getColumnType(i + 1) + " "
                    + metadata.getColumnTypeName(i + 1) + " " + metadata.getPrecision(i + 1) + " "
                    + metadata.getScale(i + 1) + " " + metadata.getSchemaName(i + 1) + " "
                    + metadata.getTableName(i + 1);
            System.out.println(metaStr);
        }
    }
}
