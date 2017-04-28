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

package org.apache.kylin.source.kafka.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

public class KafkaConsumerPropertiesTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testLoadKafkaProperties() {
        KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties.getInstanceFromEnv();
        assertFalse(kafkaConsumerProperties.extractKafkaConfigToProperties().containsKey("acks"));
        assertTrue(kafkaConsumerProperties.extractKafkaConfigToProperties().containsKey("session.timeout.ms"));
        assertEquals("30000", kafkaConsumerProperties.extractKafkaConfigToProperties().getProperty("session.timeout.ms"));
    }

    @Test
    public void testLoadKafkaPropertiesAsHadoopJobConf() throws IOException, ParserConfigurationException, SAXException {
        KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties.getInstanceFromEnv();
        Configuration conf = new Configuration(false);
        conf.addResource(new FileInputStream(new File(kafkaConsumerProperties.getKafkaConsumerHadoopJobConf())), KafkaConsumerProperties.KAFKA_CONSUMER_FILE);
        assertEquals("30000", conf.get("session.timeout.ms"));

        Properties prop = KafkaConsumerProperties.extractKafkaConfigToProperties(conf);
        assertEquals("30000", prop.getProperty("session.timeout.ms"));
    }
}
