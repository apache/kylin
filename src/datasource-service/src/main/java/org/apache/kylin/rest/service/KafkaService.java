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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.BROKER_TIMEOUT_MESSAGE;
import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_TIMEOUT_MESSAGE;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.kafka.KafkaTableUtil;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;

@Component("kafkaService")
public class KafkaService extends BasicService {

    @Autowired
    private AclEvaluate aclEvaluate;

    public void checkBrokerStatus(KafkaConfig kafkaConfig) {
        List<String> brokenBrokers = KafkaTableUtil.getBrokenBrokers(kafkaConfig);
        if (CollectionUtils.isEmpty(brokenBrokers)) {
            return;
        }

        Map<String, Object> brokenBrokersMap = Maps.newHashMap();
        brokenBrokersMap.put("failed_servers", brokenBrokers);
        throw new KylinException(BROKER_TIMEOUT_MESSAGE, MsgPicker.getMsg().getBrokerTimeoutMessage())
                .withData(brokenBrokersMap);
    }

    public Map<String, List<String>> getTopics(KafkaConfig kafkaConfig, String project, final String fuzzyTopic) {
        aclEvaluate.checkProjectWritePermission(project);
        checkBrokerStatus(kafkaConfig);
        return KafkaTableUtil.getTopics(kafkaConfig, fuzzyTopic);
    }

    public List<ByteBuffer> getMessages(KafkaConfig kafkaConfig, String project, int clusterIndex) {
        aclEvaluate.checkProjectWritePermission(project);
        try {
            return KafkaTableUtil.getMessages(kafkaConfig, clusterIndex);
        } catch (TimeoutException e) {
            throw new KylinException(STREAMING_TIMEOUT_MESSAGE, MsgPicker.getMsg().getStreamingTimeoutMessage());
        }
    }

    public Map<String, Object> getMessageTypeAndDecodedMessages(List<ByteBuffer> messages) {
        return KafkaTableUtil.getMessageTypeAndDecodedMessages(messages);
    }

    public Map<String, Object> convertSampleMessageToFlatMap(KafkaConfig kafkaConfig, String messageType,
            String message) {
        return KafkaTableUtil.convertMessageToFlatMap(kafkaConfig, messageType, message);
    }

}
