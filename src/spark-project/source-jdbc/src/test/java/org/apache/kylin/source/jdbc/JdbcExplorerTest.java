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

import java.sql.SQLException;
import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcExplorerTest extends JdbcTestBase {
    private static JdbcExplorer explorer;
    private static JdbcSource jdbcSource;

    @BeforeClass
    public static void setUp() throws SQLException {
        JdbcTestBase.setUp();
        jdbcSource = new JdbcSource(getTestConfig());
        explorer = (JdbcExplorer) jdbcSource.getSourceMetadataExplorer();
    }

    @Test
    public void testListDatabases() throws Exception {
        List<String> databases = explorer.listDatabases();
        Assert.assertTrue(databases != null && databases.size() > 0);
    }

    @Test
    public void testListTables() throws Exception {
        List<String> tables = explorer.listTables("SSB");
        Assert.assertTrue(tables != null && tables.size() > 0);
    }

    @Test
    public void testListTablesInDatabase() throws Exception {
        String testDataBase = "SSB";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        explorer.createSampleDatabase(testDataBase);
        Assert.assertTrue(explorer.listDatabases().contains(testDataBase));

        connector.executeUpdate("drop table if exists SSB.PART");
        Assert.assertFalse(explorer.listTables(testDataBase).contains("PART"));

        TableDesc tableDesc = tableMgr.getTableDesc("SSB.PART");
        explorer.createSampleTable(tableDesc);
        Assert.assertTrue(explorer.listTables(testDataBase).contains("PART"));
    }

    @Test
    public void testGetTableDesc() throws Exception {
        Pair<TableDesc, TableExtDesc> tableDescTableExtDescPair = explorer.loadTableMetadata("SSB", "LINEORDER", "ssb");
        Assert.assertTrue(tableDescTableExtDescPair != null && tableDescTableExtDescPair.getFirst() != null);
    }

    @Test
    public void testCreateSampleDatabase() throws Exception {
        explorer.createSampleDatabase("TEST");
        List<String> databases = explorer.listDatabases();
        Assert.assertTrue(databases != null && databases.contains("TEST"));
    }

}
