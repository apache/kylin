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
package org.apache.kylin.source.jdbc.metadata;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.kylin.source.hive.DBConnConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MySQLJdbcMetadataTest extends DefaultJdbcMetadataTest {

    @Before
    public void setup() {
        dbConnConf = new DBConnConf();
        dbConnConf.setUrl("jdbc:mysql://fakehost:1433/database");
        dbConnConf.setDriver("com.mysql.jdbc.Driver");
        dbConnConf.setUser("user");
        dbConnConf.setPass("pass");
        jdbcMetadata = new MySQLJdbcMetadata(dbConnConf);

        setupProperties();
    }

    @Test
    public void testListDatabases() throws SQLException {
        when(connection.getCatalog()).thenReturn("catalog1");

        List<String> dbs = jdbcMetadata.listDatabases();

        Assert.assertEquals(1, dbs.size());
        Assert.assertEquals("catalog1", dbs.get(0));
    }

    @Test
    public void testListTables() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        when(rs.getString("TABLE_NAME")).thenReturn("KYLIN_SALES").thenReturn("CAT_DT").thenReturn("KYLIN_CAT");

        String catalog = "testCatalog";
        when(connection.getMetaData()).thenReturn(dbmd);
        when(dbmd.getTables(catalog, null, null, null)).thenReturn(rs);

        List<String> tables = jdbcMetadata.listTables(catalog);

        Assert.assertEquals(3, tables.size());
        Assert.assertEquals("CAT_DT", tables.get(1));
    }

    @Test
    public void testGetTable() throws SQLException {
        String catalog = "testSchema";
        String table = "testTable";
        ResultSet rs = mock(ResultSet.class);
        when(dbmd.getTables(catalog, null, table, null)).thenReturn(rs);

        ResultSet result = jdbcMetadata.getTable(dbmd, catalog, table);

        verify(dbmd, times(1)).getTables(catalog, null, table, null);
        Assert.assertEquals(rs, result);
    }

    @Test
    public void testListColumns() throws SQLException {
        String catalog = "testSchema";
        String table = "testTable";
        ResultSet rs = mock(ResultSet.class);
        when(dbmd.getColumns(catalog, null, table, null)).thenReturn(rs);

        ResultSet result = jdbcMetadata.listColumns(dbmd, catalog, table);

        verify(dbmd, times(1)).getColumns(catalog, null, table, null);
        Assert.assertEquals(rs, result);
    }
}
