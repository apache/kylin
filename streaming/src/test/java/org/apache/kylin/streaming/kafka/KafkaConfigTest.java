/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.streaming.kafka;

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by qianzhou on 3/2/15.
 */
public class KafkaConfigTest {

    @Test
    public void test() throws IOException {
        final Properties properties = new Properties();
        properties.load(ClassLoader.getSystemResourceAsStream("kafka_streaming_test/kafka.properties"));
        KafkaConfig config = KafkaConfig.load(properties);
        assertEquals(1000, config.getMaxReadCount());
        assertEquals(65536, config.getBufferSize());
        assertEquals(60000, config.getTimeout());
        assertEquals("sandbox.hortonworks.com:2181", config.getZookeeper());
        assertEquals("kafka_stream_test", config.getTopic());
        assertEquals(0, config.getPartitionId());
        assertEquals(1, config.getBrokers().size());
        assertEquals("sandbox.hortonworks.com:6667", config.getBrokers().get(0).getConnectionString());

    }
}
