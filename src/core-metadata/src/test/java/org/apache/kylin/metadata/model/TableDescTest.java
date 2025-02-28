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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    @Test
    public void testGetBackTickTransactionalTableIdentity() {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setDatabase("TESTDB");
        tableDesc.setName("TEST_KYLIN_FACT");

        // Test returned identity keeping original db
        Assert.assertEquals("`TESTDB`.`TEST_KYLIN_FACT_HIVE_TX_INTERMEDIATE123`", tableDesc.getBackTickTransactionalTableIdentity(null, "123"));
        // Test returned identity using config db
        Assert.assertEquals("`ANOTHER_DB`.`TEST_KYLIN_FACT_HIVE_TX_INTERMEDIATE123`", tableDesc.getBackTickTransactionalTableIdentity("another_db", "123"));
    }

    @Test
    public void testGetTransactionalTableIdentity() {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setDatabase("TESTDB");
        tableDesc.setName("TEST_KYLIN_FACT");

        // Test returned identity keeping original db
        Assert.assertEquals("TESTDB.TEST_KYLIN_FACT_HIVE_TX_INTERMEDIATE", tableDesc.getTransactionalTableIdentity(null));
        // Test returned identity using config db
        Assert.assertEquals("ANOTHER_DB.TEST_KYLIN_FACT_HIVE_TX_INTERMEDIATE", tableDesc.getTransactionalTableIdentity("another_db"));
    }

    @Test
    public void testFieldRenameSerialize() throws JsonProcessingException {
        testSerDes("{\"has_Internal\":true,\"rangePartition\":false}", true, false);

        // test default value
        testSerDes("{}", false, false);

        testSerDes("{\"has_Internal\":true,\"rangePartition\":true, \"has_internal\":false,\"range_partition\":false}",
                false, false);
    }

    @Test
    public void testConstructor() {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setHasInternal(true);
        tableDesc.setRangePartition(false);
        // avoid NPE
        tableDesc.setColumns(new ColumnDesc[0]);
        TableDesc tableDesc1 = new TableDesc(tableDesc);
        Assert.assertTrue(tableDesc1.isHasInternal());
        Assert.assertFalse(tableDesc1.isRangePartition());
    }

    private static void testSerDes(String json, boolean isInternal, boolean isRangePartition)
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        TableDesc tableDesc = mapper.readValue(json, TableDesc.class);
        Assert.assertEquals(isInternal, tableDesc.isHasInternal());
        Assert.assertEquals(isRangePartition, tableDesc.isRangePartition());
        String serializeJson = mapper.writeValueAsString(tableDesc);
        JsonNode jsonNode = mapper.readTree(serializeJson);
        Assert.assertEquals(isInternal, jsonNode.get("has_internal").asBoolean());
        Assert.assertEquals(isRangePartition, jsonNode.get("range_partition").asBoolean());
        Assert.assertNull(jsonNode.get("has_Internal"));
        Assert.assertNull(jsonNode.get("rangePartition"));
    }
}
