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

package org.apache.kylin.metadata;

import static org.apache.kylin.metadata.TableMetadataManager.getInstance;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class TableMetadataManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testListAllTables() throws Exception {
        List<TableDesc> tables = getInstance(getTestConfig()).listAllTables(null);
        Assert.assertNotNull(tables);
        Assert.assertTrue(tables.size() > 0);
    }

    @Test
    public void testFindTableByName() throws Exception {
        TableDesc table = getInstance(getTestConfig()).getTableDesc("EDW.TEST_CAL_DT", "default");
        Assert.assertNotNull(table);
        Assert.assertEquals("EDW.TEST_CAL_DT", table.getIdentity());
    }

    @Test
    public void testGetInstance() throws Exception {
        Assert.assertNotNull(getInstance(getTestConfig()));
        Assert.assertNotNull(getInstance(getTestConfig()).listAllTables(null));
        Assert.assertTrue(getInstance(getTestConfig()).listAllTables(null).size() > 0);
    }

    @Test
    public void testTableSample() throws IOException {
        TableExtDesc tableExtDesc = getInstance(getTestConfig()).getTableExt("DEFAULT.WIDE_TABLE", "default");
        Assert.assertNotNull(tableExtDesc);

        List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>();
        TableExtDesc.ColumnStats columnStats = new TableExtDesc.ColumnStats();
        columnStats.setColumnSamples("Max", "Min", "dfadsfdsfdsafds", "d");
        columnStatsList.add(columnStats);
        tableExtDesc.setColumnStats(columnStatsList);
        getInstance(getTestConfig()).saveTableExt(tableExtDesc, "default");

        TableExtDesc tableExtDesc1 = getInstance(getTestConfig()).getTableExt("DEFAULT.WIDE_TABLE", "default");
        Assert.assertNotNull(tableExtDesc1);

        List<TableExtDesc.ColumnStats> columnStatsList1 = tableExtDesc1.getColumnStats();
        Assert.assertEquals(1, columnStatsList1.size());

        getInstance(getTestConfig()).removeTableExt("DEFAULT.WIDE_TABLE", "default");
    }

    @Test
    public void testTableExtCompatibility() throws IOException {
        String tableName = "DEFAULT.WIDE_TABLE";
        Map<String, String> oldTableExt = new HashMap<>();
        oldTableExt.put(MetadataConstants.TABLE_EXD_CARDINALITY, "1,2,3,4");
        mockUpOldTableExtJson(tableName, oldTableExt);
        TableExtDesc tableExtDesc = getInstance(getTestConfig()).getTableExt(tableName, "default");
        Assert.assertEquals("1,2,3,4,", tableExtDesc.getCardinality());
        getInstance(getTestConfig()).removeTableExt(tableName, "default");
    }

    private void mockUpOldTableExtJson(String tableId, Map<String, String> tableExdProperties) throws IOException {
        String path = TableExtDesc.concatResourcePath(tableId, null);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        JsonUtil.writeValueIndent(os, tableExdProperties);
        os.flush();
        InputStream is = new ByteArrayInputStream(os.toByteArray());
        getStore().putResource(path, is, System.currentTimeMillis());
        os.close();
        is.close();
    }
}
