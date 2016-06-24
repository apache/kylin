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

import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
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
}
