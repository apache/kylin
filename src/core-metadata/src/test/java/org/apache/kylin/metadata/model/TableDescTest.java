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

import java.util.Locale;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TableDescTest extends NLocalFileMetadataTestCase {
    private final String project = "default";
    private NTableMetadataManager tableMetadataManager;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        tableMetadataManager = getInstance(getTestConfig(), project);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testTransactional() {
        final String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        Assert.assertFalse(tableDesc.isTransactional());
        Assert.assertTrue(
                tableDesc.getTransactionalTableIdentity().endsWith("_hive_tx_intermediate".toUpperCase(Locale.ROOT)));
        Assert.assertEquals("`DEFAULT`.`TEST_KYLIN_FACT_HIVE_TX_INTERMEDIATE_suffix`",
                tableDesc.getBackTickTransactionalTableIdentity("_suffix"));
    }

    @Test
    public void testGetIdentityWithBacktick() {
        final String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        Assert.assertEquals("`DEFAULT`.`TEST_KYLIN_FACT`", tableDesc.getBackTickIdentity());
    }

    @Test
    public void testRangePartition() {
        final String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
        Assert.assertFalse(tableDesc.isRangePartition());
    }
}
