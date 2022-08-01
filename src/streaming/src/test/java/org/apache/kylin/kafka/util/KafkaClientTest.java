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
package org.apache.kylin.kafka.util;

import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.streaming.jobs.StreamingJobUtils;
import org.apache.kylin.streaming.util.ReflectionUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaClientTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testConstructMethod() {
        val constructors = KafkaClient.class.getDeclaredConstructors();
        Assert.assertTrue(constructors.length == 1);

        try {
            constructors[0].setAccessible(true);
            constructors[0].newInstance();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void testConstructDefaultKafkaConsumerProperties() {
        val prop = KafkaClient.constructDefaultKafkaConsumerProperties("localhost:9092", "client1", new Properties());
        Assert.assertNotNull(prop);
        val propNull = KafkaClient.constructDefaultKafkaConsumerProperties("localhost:9092", "client1", null);
        Assert.assertNotNull(propNull);
        Assert.assertEquals("localhost:9092", prop.getProperty("bootstrap.servers"));
        Assert.assertEquals("client1", prop.getProperty("group.id"));
        Assert.assertEquals("false", prop.getProperty("enable.auto.commit"));
    }

    @Test
    public void testGetKafkaConsumer() {
        val prop = KafkaClient.constructDefaultKafkaConsumerProperties("localhost:9092", "client1", new Properties());
        val consumer = KafkaClient.getKafkaConsumer("localhost:9092", "client2");

        ConsumerCoordinator coordinator = (ConsumerCoordinator) ReflectionUtils.getField(consumer, "coordinator");
        String groupId = (String) ReflectionUtils.getField(coordinator, "groupId");
        Assert.assertEquals("client2", groupId);

        Metadata meta = (Metadata) ReflectionUtils.getField(consumer, "metadata");
        Cluster cluster = (Cluster) ReflectionUtils.getField(meta, "cluster");
        List<Node> nodes = cluster.nodes();
        Assert.assertEquals("localhost", nodes.get(0).host());
        Assert.assertEquals(9092, nodes.get(0).port());
    }

    @Test
    public void testKafkaAdminClient() {
        try (AdminClient client = KafkaClient.getKafkaAdminClient("localhost:9092", "group1")) {
            AdminMetadataManager metadataManager = (AdminMetadataManager) ReflectionUtils.getField(client,
                    "metadataManager");
            Cluster cluster = (Cluster) ReflectionUtils.getField(metadataManager, "cluster");
            List<Node> nodes = cluster.nodes();
            Assert.assertEquals("localhost", nodes.get(0).host());
            Assert.assertEquals(9092, nodes.get(0).port());
        } catch (Exception e) {
            log.error("failed to close AdminClient.", e);
        }
    }

    @Test
    public void testConstructDefaultKafkaAdminClientProperties() throws Exception {
        val prop = KafkaClient.constructDefaultKafkaAdminClientProperties("localhost:9092", "group1", new Properties());
        Assert.assertNotNull(prop);
        val prop1 = KafkaClient.constructDefaultKafkaAdminClientProperties("localhost:9092", "group1", null);
        Assert.assertNotNull(prop1);
        Assert.assertEquals("localhost:9092", prop.getProperty("bootstrap.servers"));
        Assert.assertEquals("group1", prop.getProperty("group.id"));

        val kapConfig = KapConfig.getInstanceFromEnv();

        FileUtils.write(new File(kapConfig.getKafkaJaasConfPath()),
                "KafkaClient{ org.apache.kafka.common.security.scram.ScramLoginModule required}");
        val text = StreamingJobUtils.extractKafkaSaslJaasConf();
        Assert.assertNull(text);
        Pair<Boolean, String> kafkaJaasTextPair = (Pair<Boolean, String>) ReflectionUtils.getField(KafkaClient.class,
                "kafkaJaasTextPair");
        kafkaJaasTextPair.setFirst(false);
        getTestConfig().setProperty("kylin.kafka-jaas.enabled", "true");
        val prop2 = KafkaClient.constructDefaultKafkaAdminClientProperties("localhost:9092", "group1", null);
        Assert.assertNotNull(prop2.get(SASL_JAAS_CONFIG));
        getTestConfig().setProperty("kylin.kafka-jaas.enabled", "false");
    }
}
