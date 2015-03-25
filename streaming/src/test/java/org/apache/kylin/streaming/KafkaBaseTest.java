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

package org.apache.kylin.streaming;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.streaming.KafkaConfig;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by qianzhou on 2/16/15.
 */
public abstract class KafkaBaseTest {

    protected static final Logger logger = LoggerFactory.getLogger("kafka test");

    protected static ZkClient zkClient;

    protected static KafkaConfig kafkaConfig;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaConfig = StreamManager.getInstance(KylinConfig.getInstanceFromEnv()).getKafkaConfig("kafka");

        zkClient = new ZkClient(kafkaConfig.getZookeeper());
    }


    public static void createTopic(String topic, int partition, int replica) {
        try {
            AdminUtils.createTopic(zkClient, topic, partition, replica, new Properties());
        } catch (TopicExistsException e) {
            logger.info(e.getMessage());
        }
    }


}
