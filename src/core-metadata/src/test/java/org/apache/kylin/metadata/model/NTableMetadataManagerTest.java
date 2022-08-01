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

package org.apache.kylin.metadata.model;

import static org.apache.kylin.metadata.model.NTableMetadataManager.getInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class NTableMetadataManagerTest extends NLocalFileMetadataTestCase {
    private final String projectDefault = "default";
    private final String tableKylinFact = "DEFAULT.TEST_KYLIN_FACT";
    private NTableMetadataManager mgrDefault;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        mgrDefault = getInstance(getTestConfig(), projectDefault);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testListAllTables() {
        List<TableDesc> tables = mgrDefault.listAllTables();
        Assert.assertNotNull(tables);
        Assert.assertTrue(tables.size() > 0);
    }

    @Test
    public void testGetAllTablesMap() {
        Map<String, TableDesc> tm = mgrDefault.getAllTablesMap();
        Assert.assertTrue(tm.size() > 0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", tm.get("DEFAULT.TEST_KYLIN_FACT").getIdentity());
    }

    @Test
    public void testGetTableDesc() {
        TableDesc tbl = mgrDefault.getTableDesc(tableKylinFact);
        Assert.assertEquals(tableKylinFact, tbl.getIdentity());
    }

    @Test
    public void testFindTableByName() {
        TableDesc table = mgrDefault.getTableDesc("EDW.TEST_CAL_DT");
        Assert.assertNotNull(table);
        Assert.assertEquals("EDW.TEST_CAL_DT", table.getIdentity());
    }

    @Test
    public void testGetInstance() {
        Assert.assertNotNull(mgrDefault);
        Assert.assertNotNull(mgrDefault.listAllTables());
        Assert.assertTrue(mgrDefault.listAllTables().size() > 0);
    }

    @Test
    public void testTableSample() {
        TableExtDesc tableExtDesc = mgrDefault.getOrCreateTableExt(tableKylinFact);
        Assert.assertNotNull(tableExtDesc);

        List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>();
        TableExtDesc.ColumnStats columnStats = new TableExtDesc.ColumnStats();
        columnStats.setColumnName("test_col");
        columnStats.setColumnSamples("Max", "Min", "dfadsfdsfdsafds", "d");
        columnStatsList.add(columnStats);
        tableExtDesc.setColumnStats(columnStatsList);
        mgrDefault.saveTableExt(tableExtDesc);

        TableExtDesc tableExtDesc1 = mgrDefault.getOrCreateTableExt(tableKylinFact);
        Assert.assertNotNull(tableExtDesc1);
        Assert.assertEquals(1, tableExtDesc1.getAllColumnStats().size());
        mgrDefault.removeTableExt(tableKylinFact);
    }

    @Test
    public void testGetTableExt() {
        TableDesc tableDesc = mgrDefault.getTableDesc(tableKylinFact);

        final TableExtDesc t1 = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNull(t1);

        final TableExtDesc t2 = mgrDefault.getOrCreateTableExt(tableDesc);
        Assert.assertNotNull(t2);
        Assert.assertEquals(0, t2.getAllColumnStats().size());
        Assert.assertEquals(0, t2.getTotalRows());

        final TableExtDesc t3 = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNull(t3);

        t2.setTotalRows(100);
        mgrDefault.saveTableExt(t2);

        final TableExtDesc t4 = mgrDefault.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(t4);
        Assert.assertEquals(100, t4.getTotalRows());

        final TableExtDesc t5 = mgrDefault.getOrCreateTableExt(tableDesc);
        Assert.assertNotNull(t5);
        Assert.assertEquals(100, t5.getTotalRows());

    }

    @Test
    public void testGetIncrementalLoadTables() {
        TableDesc tableDesc = mgrDefault.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        tableDesc.setIncrementLoading(true);
        List<TableDesc> tables = mgrDefault.getAllIncrementalLoadTables();
        Assert.assertEquals(1, tables.size());
        Assert.assertTrue(tables.get(0).isIncrementLoading());
    }

}
