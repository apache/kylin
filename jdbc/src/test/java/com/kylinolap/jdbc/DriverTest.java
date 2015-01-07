package com.kylinolap.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import com.kylinolap.jdbc.Driver;

/**
 * Unit test for Driver.
 * 
 * @author xduo
 * 
 */
public class DriverTest {

    @Test
    public void testStatementWithMockData() throws SQLException {
        Driver driver = new DummyDriver();

        Connection conn = driver.connect("jdbc:kylin://test_url/test_db", null);

        ResultSet tables = conn.getMetaData().getTables(null, null, null, null);
        while (tables.next()) {
            for (int i = 0; i < 10; i++) {
                assertEquals("dummy", tables.getString(i + 1));
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
    }

    @Test
    public void testPreStatementWithMockData() throws SQLException {
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
    }

    @Ignore
    @Test
    public void testWithCubeData() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        Driver driver = (Driver) Class.forName("com.kylinolap.kylin.jdbc.Driver").newInstance();
        Properties info = new Properties();
        info.put("user", "");
        info.put("password", "");
        Connection conn = driver.connect("jdbc:kylin://localhost/default", info);

        ResultSet catalogs = conn.getMetaData().getCatalogs();
        while (catalogs.next()) {
            System.out.println(catalogs.getString("TABLE_CAT"));
        }

        ResultSet schemas = conn.getMetaData().getSchemas();
        while (schemas.next()) {
            System.out.println(schemas.getString(1));
            System.out.println(schemas.getString(2));
        }

        ResultSet tables = conn.getMetaData().getTables(null, null, null, null);
        while (tables.next()) {
            String tableName = tables.getString(3);
            assertEquals(tables.getString("TABLE_NAME"), tableName);
            ResultSet columns = conn.getMetaData().getColumns(null, null, tableName, null);

            while (columns.next()) {
                System.out.println(columns.getString("COLUMN_NAME"));
                String column = "";
                for (int i = 0; i < 23; i++) {
                    column += columns.getString(i + 1) + ", ";
                }

                System.out.println("Column in table " + tableName + ": " + column);
            }
        }

        for (int j = 0; j < 3; j++) {
            Statement state = conn.createStatement();
            ResultSet resultSet = state.executeQuery("select * from test_kylin_fact");

            ResultSetMetaData metadata = resultSet.getMetaData();
            System.out.println("Metadata:");

            for (int i = 0; i < metadata.getColumnCount(); i++) {
                String metaStr = metadata.getCatalogName(i + 1) + " " + metadata.getColumnClassName(i + 1) + " " + metadata.getColumnDisplaySize(i + 1) + " " + metadata.getColumnLabel(i + 1) + " " + metadata.getColumnName(i + 1) + " " + metadata.getColumnType(i + 1) + " " + metadata.getColumnTypeName(i + 1) + " " + metadata.getPrecision(i + 1) + " " + metadata.getScale(i + 1) + " " + metadata.getSchemaName(i + 1) + " " + metadata.getTableName(i + 1);
                System.out.println(metaStr);
            }

            System.out.println("Data:");
            while (resultSet.next()) {
                String dataStr = resultSet.getFloat(1) + " " + resultSet.getInt(2) + " " + resultSet.getInt(3) + " " + resultSet.getLong(4) + " " + resultSet.getDate(5) + " " + resultSet.getString(6);
                System.out.println(dataStr);
            }
        }
    }

    @Ignore
    @Test
    public void testPreStatementWithCubeData() throws SQLException {
        Driver driver = new Driver();
        Properties info = new Properties();
        info.put("user", "");
        info.put("password", "");
        Connection conn = driver.connect("jdbc:kylin://localhost/default", info);
        PreparedStatement state = conn.prepareStatement("select * from test_kylin_fact where seller_id=?");
        state.setLong(1, 10000001);
        ResultSet resultSet = state.executeQuery();

        ResultSetMetaData metadata = resultSet.getMetaData();
        System.out.println("Metadata:");

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            String metaStr = metadata.getCatalogName(i + 1) + " " + metadata.getColumnClassName(i + 1) + " " + metadata.getColumnDisplaySize(i + 1) + " " + metadata.getColumnLabel(i + 1) + " " + metadata.getColumnName(i + 1) + " " + metadata.getColumnType(i + 1) + " " + metadata.getColumnTypeName(i + 1) + " " + metadata.getPrecision(i + 1) + " " + metadata.getScale(i + 1) + " " + metadata.getSchemaName(i + 1) + " " + metadata.getTableName(i + 1);
            System.out.println(metaStr);
        }

        System.out.println("Data:");
        while (resultSet.next()) {
            String dataStr = resultSet.getFloat(1) + " " + resultSet.getInt(2) + " " + resultSet.getInt(3) + " " + resultSet.getLong(4) + " " + resultSet.getDate(5) + " " + resultSet.getString(6);
            System.out.println(dataStr);
        }
    }
}
