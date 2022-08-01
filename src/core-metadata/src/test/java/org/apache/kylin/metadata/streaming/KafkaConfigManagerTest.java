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
package org.apache.kylin.metadata.streaming;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class KafkaConfigManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private KafkaConfigManager mgr;
    private static String PROJECT = "streaming_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgr = KafkaConfigManager.getInstance(getTestConfig(), PROJECT);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetKafkaConfig() {
        val emptyId = "";
        Assert.assertNull(mgr.getKafkaConfig(emptyId));

        val id = "DEFAULT.SSB_TOPIC";
        val kafkaConf = mgr.getKafkaConfig(id);
        Assert.assertNotNull(kafkaConf);

        val id_not_existed = "DEFAULT.SSB_TOPIC_not_existed";
        val kafkaConfNotExisted = mgr.getKafkaConfig(id_not_existed);
        Assert.assertNull(kafkaConfNotExisted);
    }

    @Test
    public void testCreateKafkaConfig() {
        val kafkaConf = new KafkaConfig();
        kafkaConf.setProject(PROJECT);
        kafkaConf.setDatabase("DEFAULT");
        kafkaConf.setName("TPCH_TOPIC");
        kafkaConf.setKafkaBootstrapServers("10.1.2.210:9094");
        kafkaConf.setStartingOffsets("earliest");
        mgr.createKafkaConfig(kafkaConf);
        val kafkaConfig = KafkaConfigManager.getInstance(getTestConfig(), PROJECT).getKafkaConfig("DEFAULT.TPCH_TOPIC");
        Assert.assertEquals("10.1.2.210:9094", kafkaConfig.getKafkaBootstrapServers());
        Assert.assertEquals("earliest", kafkaConfig.getStartingOffsets());
    }

    @Test
    public void testUpdateKafkaConfig() {
        val kafkaConf = new KafkaConfig();
        kafkaConf.setDatabase("default");
        kafkaConf.setName("empty");
        try {
            mgr.updateKafkaConfig(kafkaConf);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Kafka Config 'empty' does not exist.", e.getMessage());
        }
        val kafkaConfig = KafkaConfigManager.getInstance(getTestConfig(), PROJECT).getKafkaConfig("DEFAULT.SSB_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9094");
        kafkaConfig.setStartingOffsets("earliest");
        mgr.updateKafkaConfig(kafkaConfig);
        Assert.assertEquals("10.1.2.210:9094", kafkaConfig.getKafkaBootstrapServers());
        Assert.assertEquals("earliest", kafkaConfig.getStartingOffsets());
    }

    @Test
    public void testRemoveKafkaConfig() {
        val id = "DEFAULT.SSB_TOPIC";
        val kafkaConf = mgr.getKafkaConfig(id);
        Assert.assertNotNull(kafkaConf);

        val id_not_existed = "DEFAULT.SSB_TOPIC_not_existed";
        mgr.removeKafkaConfig(id);
        val kafkaConf1 = mgr.getKafkaConfig(id);
        Assert.assertNull(kafkaConf1);

    }

    @Test
    public void testListAllKafkaConfigs() {
        val id = "SSB.LINEORDER_HIVE";
        val kafkaConfs = mgr.listAllKafkaConfigs();
        Assert.assertEquals(5, kafkaConfs.size());

        val tables = mgr.getKafkaTablesUsingTable(id);
        Assert.assertEquals(2, tables.size());
    }

    @Test
    public void testBatchTableAlias() {
        val id = "SSB.P_LINEORDER_STREAMING";
        val kafkaConf = mgr.getKafkaConfig(id);
        Assert.assertNotNull(kafkaConf);
        Assert.assertTrue(kafkaConf.hasBatchTable());
        Assert.assertEquals("LINEORDER_HIVE", kafkaConf.getBatchTableAlias());
        Assert.assertEquals(id, kafkaConf.getIdentity());
    }
}
