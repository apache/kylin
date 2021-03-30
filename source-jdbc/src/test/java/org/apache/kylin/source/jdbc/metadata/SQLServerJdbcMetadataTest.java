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
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.kylin.source.hive.DBConnConf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SQLServerJdbcMetadataTest extends DefaultJdbcMetadataTest {

    @Before
    public void setup() {
        dbConnConf = new DBConnConf();
        dbConnConf.setUrl("jdbc:sqlserver://fakehost:1433;database=testdb");
        dbConnConf.setDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dbConnConf.setUser("user");
        dbConnConf.setPass("pass");
        jdbcMetadata = new SQLServerJdbcMetadata(dbConnConf);

        setupProperties();
    }

    @Test
    public void testListDatabases() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(rs.getString("TABLE_SCHEM")).thenReturn("schema1").thenReturn("schema2");
        when(rs.getString("TABLE_CAT")).thenReturn("catalog1").thenReturn("testdb");

        when(connection.getCatalog()).thenReturn("testdb");
        when(connection.getMetaData()).thenReturn(dbmd);
        when(dbmd.getTables("testdb", null, null, null)).thenReturn(rs);

        List<String> dbs = jdbcMetadata.listDatabases();

        Assert.assertEquals(1, dbs.size());
        Assert.assertEquals("schema2", dbs.get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testListDatabasesWithoutSpecificDB() throws SQLException {
        when(connection.getCatalog()).thenReturn("");
        jdbcMetadata.listDatabases();
    }
}
