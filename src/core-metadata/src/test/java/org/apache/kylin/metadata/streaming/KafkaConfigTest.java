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

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class KafkaConfigTest {
    @Test
    public void testCreate() {
        val kafkaConfig1 = new KafkaConfig();

        ReflectionUtils.setField(kafkaConfig1, "name", "config_name");
        ReflectionUtils.setField(kafkaConfig1, "kafkaBootstrapServers", "localhost:9092");

        val kafkaConfig2 = new KafkaConfig(kafkaConfig1);

        String name = kafkaConfig2.getName();
        String bootstrapServers = kafkaConfig2.getKafkaBootstrapServers();

        Assert.assertEquals("config_name", name);
        Assert.assertEquals("localhost:9092", bootstrapServers);
    }

    @Test
    public void testGetKafkaParam() {
        val kafkaConfig1 = new KafkaConfig();

        ReflectionUtils.setField(kafkaConfig1, "database", "DEFAULT");
        ReflectionUtils.setField(kafkaConfig1, "kafkaBootstrapServers", "localhost:9092");
        ReflectionUtils.setField(kafkaConfig1, "name", "config_name");
        ReflectionUtils.setField(kafkaConfig1, "subscribe", "subscribe_item");
        ReflectionUtils.setField(kafkaConfig1, "startingOffsets", "startingOffset_item");

        Map<String, String> params = kafkaConfig1.getKafkaParam();

        Assert.assertEquals("localhost:9092", params.get("kafka.bootstrap.servers"));
        Assert.assertEquals("subscribe_item", params.get("subscribe"));
        Assert.assertEquals("startingOffset_item", params.get("startingOffsets"));
        Assert.assertEquals("false", params.get("failOnDataLoss"));
    }
}
