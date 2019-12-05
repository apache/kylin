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
package io.kyligence.kap.engine.spark.source;

import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class NSparkMetadataExplorerTest extends NLocalWithSparkSessionTest {

    @Test
    public void testListDatabases() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        List<String> databases = sparkMetadataExplorer.listDatabases();
        Assert.assertTrue(databases != null && databases.size() > 0);
    }

    @Test
    public void testListTables() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        List<String> tables = sparkMetadataExplorer.listTables("");
        Assert.assertTrue(tables != null && tables.size() > 0);
    }

    @Test
    public void testListTablesInDatabase() throws Exception {
        String testDataBase = "SSB";
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "ssb");
        sparkMetadataExplorer.createSampleDatabase(testDataBase);
        Assert.assertTrue(ss.catalog().databaseExists(testDataBase));

        TableDesc tableDesc = tableMgr.getTableDesc("SSB.PART");
        sparkMetadataExplorer.createSampleTable(tableDesc);

        List<String> tables = sparkMetadataExplorer.listTables(testDataBase);

        Assert.assertTrue(tables != null && tables.size() > 0);
        Assert.assertEquals(tables.get(0), "part");
    }

    @Test
    public void testGetTableDesc() throws Exception {
        populateSSWithCSVData(getTestConfig(), "ssb", ss);

        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        Pair<TableDesc, TableExtDesc> tableDescTableExtDescPair = sparkMetadataExplorer.loadTableMetadata("",
                "p_lineorder", "ssb");
        Assert.assertTrue(tableDescTableExtDescPair != null && tableDescTableExtDescPair.getFirst() != null);
    }

    @Test
    public void testCreateSampleDatabase() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        sparkMetadataExplorer.createSampleDatabase("test");
        List<String> databases = sparkMetadataExplorer.listDatabases();
        Assert.assertTrue(databases != null && databases.contains("test"));
    }

    @Test
    public void testCreateSampleTable() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "default");
        TableDesc fact = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        sparkMetadataExplorer.createSampleTable(fact);
        List<String> tables = sparkMetadataExplorer.listTables("default");
        Assert.assertTrue(tables != null && tables.contains("test_kylin_fact"));
    }

    @Test
    public void testLoadData() throws Exception {
        NSparkMetadataExplorer sparkMetadataExplorer = new NSparkMetadataExplorer();
        sparkMetadataExplorer.loadSampleData("SSB.PART", "../examples/test_metadata/data/");
        List<Row> rows = ss.sql("select * from part").collectAsList();
        Assert.assertTrue(rows != null && rows.size() > 0);
    }
}
