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

import static org.apache.kylin.metadata.MetadataManager.getInstance;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class MetadataManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }
    
    @Test
    public void testCiModel() {
        MetadataManager mgr = getInstance(getTestConfig());
        DataModelDesc lm = mgr.getDataModelDesc("ci_left_join_model");
        DataModelDesc im = mgr.getDataModelDesc("ci_inner_join_model");
        assertSnowflakeQuality(lm);
        assertSnowflakeQuality(im);
        
        // check inner/left models are identical apart from the left/inner difference
        assertEquals(lm.getJoinTables().length, im.getJoinTables().length);
        for (int i = 0, n = im.getJoinTables().length; i < n; i++) {
            JoinTableDesc lt = lm.getJoinTables()[i];
            JoinTableDesc it = im.getJoinTables()[i];
            assertEquals(lt.getAlias(), it.getAlias());
            assertEquals(lt.getKind(), it.getKind());
            assertEquals(lt.getTable(), it.getTable());
            assertArrayEquals(lt.getJoin().getForeignKey(), it.getJoin().getForeignKey());
            assertArrayEquals(lt.getJoin().getPrimaryKey(), it.getJoin().getPrimaryKey());
            assertTrue(lt.getJoin().isLeftJoin());
            assertTrue(it.getJoin().isInnerJoin());
        }
        
        assertEquals(lm.getDimensions().size(), im.getDimensions().size());
        for (int i = 0, n = im.getDimensions().size(); i < n; i++) {
            ModelDimensionDesc ld = lm.getDimensions().get(i);
            ModelDimensionDesc id = im.getDimensions().get(i);
            assertEquals(ld.getTable(), id.getTable());
            assertArrayEquals(ld.getColumns(), id.getColumns());
        }
        
        assertArrayEquals(lm.getMetrics(), im.getMetrics());
    }
    
    private void assertSnowflakeQuality(DataModelDesc model) {
        Assert.assertNotNull(model);
        try {
            model.findTable("TEST_COUNTRY");
            Assert.fail();
        } catch (IllegalArgumentException ex) {
            // excepted
        }
        
        Assert.assertNotNull(model.findTable("BUYER_COUNTRY"));
        Assert.assertNotNull(model.findTable("SELLER_COUNTRY"));
        Assert.assertNotNull(model.findColumn("BUYER_COUNTRY.NAME"));
        Assert.assertNotNull(model.findColumn("BUYER_ID"));

    }

    @Test
    public void testListAllTables() throws Exception {
        List<TableDesc> tables = getInstance(getTestConfig()).listAllTables();
        Assert.assertNotNull(tables);
        Assert.assertTrue(tables.size() > 0);
    }

    @Test
    public void testFindTableByName() throws Exception {
        TableDesc table = getInstance(getTestConfig()).getTableDesc("EDW.TEST_CAL_DT");
        Assert.assertNotNull(table);
        Assert.assertEquals("EDW.TEST_CAL_DT", table.getIdentity());
    }

    @Test
    public void testGetInstance() throws Exception {
        Assert.assertNotNull(getInstance(getTestConfig()));
        Assert.assertNotNull(getInstance(getTestConfig()).listAllTables());
        Assert.assertTrue(getInstance(getTestConfig()).listAllTables().size() > 0);
    }

    @Test
    public void testDataModel() throws Exception {
        DataModelDesc modelDesc = getInstance(getTestConfig()).getDataModelDesc("test_kylin_left_join_model_desc");
        Assert.assertTrue(modelDesc.getDimensions().size() > 0);
    }

    @Test
    public void testTableSample() throws IOException {
        TableExtDesc tableExtDesc = getInstance(getTestConfig()).getTableExt("TEST.TEST_TABLE");
        Assert.assertNotNull(tableExtDesc);

        List<TableExtDesc.ColumnStats> columnStatsList = new ArrayList<>();
        TableExtDesc.ColumnStats columnStats = new TableExtDesc.ColumnStats();
        columnStats.setColumnSamples("Max", "Min", "dfadsfdsfdsafds", "d");
        columnStatsList.add(columnStats);
        tableExtDesc.setColumnStats(columnStatsList);
        getInstance(getTestConfig()).saveTableExt(tableExtDesc);

        TableExtDesc tableExtDesc1 = getInstance(getTestConfig()).getTableExt("TEST.TEST_TABLE");
        Assert.assertNotNull(tableExtDesc1);

        List<TableExtDesc.ColumnStats> columnStatsList1 = tableExtDesc1.getColumnStats();
        Assert.assertEquals(1, columnStatsList1.size());

        getInstance(getTestConfig()).removeTableExt("TEST.TEST_TABLE");
    }
}
