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
package org.apache.kylin.source.jdbc;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.hive.DBConnConf;
import org.apache.kylin.source.jdbc.metadata.DefaultJdbcMetadata;
import org.apache.kylin.source.jdbc.metadata.IJdbcMetadata;
import org.apache.kylin.source.jdbc.metadata.JdbcMetadataFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ JdbcMetadataFactory.class, SqlUtil.class })

public class JdbcExplorerTest extends LocalFileMetadataTestCase {
    private JdbcExplorer jdbcExplorer;
    private static Connection connection;
    private static DatabaseMetaData dbmd;
    private IJdbcMetadata jdbcMetadata;

    @BeforeClass
    public static void setupClass() throws SQLException {
        staticCreateTestMetadata();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.source.jdbc.connection-url", "jdbc:vertica://fakehost:1433/database");
        kylinConfig.setProperty("kylin.source.jdbc.driver", "com.vertica.jdbc.Driver");
        kylinConfig.setProperty("kylin.source.jdbc.user", "user");
        kylinConfig.setProperty("kylin.source.jdbc.pass", "");
        kylinConfig.setProperty("kylin.source.jdbc.dialect", "vertica");
    }

    @Before
    public void setup() throws SQLException {
        connection = mock(Connection.class);
        dbmd = mock(DatabaseMetaData.class);
        jdbcMetadata = mock(DefaultJdbcMetadata.class);

        PowerMockito.stub(PowerMockito.method(SqlUtil.class, "getConnection")).toReturn(connection);
        PowerMockito.mockStatic(JdbcMetadataFactory.class);

        when(JdbcMetadataFactory.getJdbcMetadata(anyString(), any(DBConnConf.class))).thenReturn(jdbcMetadata);
        when(connection.getMetaData()).thenReturn(dbmd);

        jdbcExplorer = spy(JdbcExplorer.class);
    }

    @Test
    public void testListDatabases() throws SQLException {
        List<String> databases = new ArrayList<>();
        databases.add("DB1");
        databases.add("DB2");
        when(jdbcMetadata.listDatabases()).thenReturn(databases);

        List<String> result = jdbcExplorer.listDatabases();

        verify(jdbcMetadata, times(1)).listDatabases();
        Assert.assertEquals(databases, result);
    }

    @Test
    public void testListTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        tables.add("T1");
        tables.add("T2");
        String databaseName = "testDb";
        when(jdbcMetadata.listTables(databaseName)).thenReturn(tables);

        List<String> result = jdbcExplorer.listTables(databaseName);
        verify(jdbcMetadata, times(1)).listTables(databaseName);
        Assert.assertEquals(tables, result);
    }

    @Test
    public void testLoadTableMetadata() throws SQLException {
        String tableName = "tb1";
        String databaseName = "testdb";
        ResultSet rs1 = mock(ResultSet.class);
        when(rs1.next()).thenReturn(true).thenReturn(false);
        when(rs1.getString("TABLE_TYPE")).thenReturn("TABLE");

        ResultSet rs2 = mock(ResultSet.class);
        when(rs2.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(rs2.getString("COLUMN_NAME")).thenReturn("COL1").thenReturn("COL2").thenReturn("COL3");
        when(rs2.getInt("DATA_TYPE")).thenReturn(Types.VARCHAR).thenReturn(Types.INTEGER).thenReturn(Types.DECIMAL);
        when(rs2.getInt("COLUMN_SIZE")).thenReturn(128).thenReturn(10).thenReturn(19);
        when(rs2.getInt("DECIMAL_DIGITS")).thenReturn(0).thenReturn(0).thenReturn(4);
        when(rs2.getInt("ORDINAL_POSITION")).thenReturn(1).thenReturn(3).thenReturn(2);
        when(rs2.getString("REMARKS")).thenReturn("comment1").thenReturn("comment2").thenReturn("comment3");

        when(jdbcMetadata.getTable(dbmd, databaseName, tableName)).thenReturn(rs1);
        when(jdbcMetadata.listColumns(dbmd, databaseName, tableName)).thenReturn(rs2);

        Pair<TableDesc, TableExtDesc> result = jdbcExplorer.loadTableMetadata(databaseName, tableName, "proj");
        TableDesc tableDesc = result.getFirst();
        ColumnDesc columnDesc = tableDesc.getColumns()[1];

        Assert.assertEquals(databaseName.toUpperCase(), tableDesc.getDatabase());
        Assert.assertEquals(3, tableDesc.getColumnCount());
        Assert.assertEquals("TABLE", tableDesc.getTableType());
        Assert.assertEquals("COL2", columnDesc.getName());
        Assert.assertEquals("integer", columnDesc.getTypeName());
        Assert.assertEquals("comment2", columnDesc.getComment());
        Assert.assertEquals(databaseName.toUpperCase() + "." + tableName.toUpperCase(),
                result.getSecond().getIdentity());
    }

    @AfterClass
    public static void clenup() {
        staticCleanupTestMetadata();
    }

}
