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

package org.apache.kylin.kafka;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_BROKER_DEFINITION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_STREAMING_MESSAGE;
import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_PARSER_ERROR;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.parser.StreamingParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTableUtil {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTableUtil.class);

    private KafkaTableUtil() {
    }

    // =========get message from topic==========
    public static List<ByteBuffer> getMessages(KafkaConfig kafkaConfig, int clusterIndex) {
        if (!KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers())) {
            throw new KylinException(INVALID_BROKER_DEFINITION, MsgPicker.getMsg().getInvalidBrokerDefinition());
        }
        return CollectKafkaStats.getMessages(kafkaConfig, clusterIndex);
    }

    public static Map<String, Object> getMessageTypeAndDecodedMessages(List<ByteBuffer> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalStateException("There is no message in this topic");
        }
        boolean isJson = JsonUtil.isJson(StandardCharsets.UTF_8.decode(messages.get(0)).toString());

        List<String> samples = new ArrayList<>();
        for (ByteBuffer buffer : messages) {
            if (isJson) {
                // message is in JSON format, return json
                String str = StandardCharsets.UTF_8.decode(buffer).toString();
                if (StringUtils.isNotBlank(str))
                    samples.add(str);
            } else {
                // message is not in JSON format, use base64 encode to a string and return it
                String str = Base64.encodeBase64String(buffer.array());
                if (StringUtils.isNotBlank(str))
                    samples.add(str);
            }
        }

        Map<String, Object> resp = new HashMap<>();
        resp.put("message_type", isJson ? CollectKafkaStats.JSON_MESSAGE : CollectKafkaStats.BINARY_MESSAGE);
        resp.put("message", samples);
        return resp;
    }

    public static boolean validateKafkaConfig(String kafkaBootstrapServers) {
        if (StringUtils.isEmpty(kafkaBootstrapServers)) {
            return false;
        }
        return true;
    }

    public static List<String> getBrokenBrokers(KafkaConfig kafkaConfig) {
        if (!KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers())) {
            throw new KylinException(INVALID_BROKER_DEFINITION, MsgPicker.getMsg().getInvalidBrokerDefinition());
        }
        return CollectKafkaStats.getBrokenBrokers(kafkaConfig);
    }

    public static Map<String, List<String>> getTopics(KafkaConfig kafkaConfig, final String fuzzyTopic) {
        if (!KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers())) {
            throw new KylinException(INVALID_BROKER_DEFINITION, MsgPicker.getMsg().getInvalidBrokerDefinition());
        }
        return CollectKafkaStats.getTopics(kafkaConfig, fuzzyTopic);
    }

    // =========convert Message To Flat Map=========
    public static Map<String, Object> convertMessageToFlatMap(KafkaConfig kafkaConfig, String messageType,
            String message) {
        if (StringUtils.isBlank(message)) {
            throw new KylinException(INVALID_STREAMING_MESSAGE, MsgPicker.getMsg().getEmptyStreamingMessage());
        }

        KafkaTableUtil.validateStreamMessageType(messageType);
        Map<String, Object> result;
        try {
            result = flattenMessage(kafkaConfig, messageType, message);
        } catch (KylinException e) {
            throw new KylinException(STREAMING_PARSER_ERROR, e);
        } catch (Exception e) {
            logger.error("Failed to convert streaming message to flat key value.", e);
            throw new KylinException(STREAMING_PARSER_ERROR, MsgPicker.getMsg().getParseStreamingMessageError(), e);
        }
        return result;
    }

    public static void validateStreamMessageType(String messageType) {
        if (StringUtils.isBlank(messageType)) {
            throw new KylinException(INVALID_STREAMING_MESSAGE, MsgPicker.getMsg().getInvalidStreamingMessageType());
        }
        if (!StringUtils.equals(messageType, CollectKafkaStats.JSON_MESSAGE)
                && !StringUtils.equals(messageType, CollectKafkaStats.BINARY_MESSAGE)) {
            throw new KylinException(INVALID_STREAMING_MESSAGE, MsgPicker.getMsg().getInvalidStreamingMessageType());
        }
    }

    private static Map<String, Object> flattenMessage(KafkaConfig kafkaConfig, String messageType, String message) {
        ByteBuffer byteBuf = deserializeSampleMessage(messageType, message);
        StreamingParser streamingParser;
        try {
            streamingParser = StreamingParser.getStreamingParser(kafkaConfig.getParserName(), null, null);
        } catch (ReflectiveOperationException e) {
            throw new KylinException(STREAMING_PARSER_ERROR, e);
        }
        return streamingParser.flattenMessage(byteBuf);
    }

    public static ByteBuffer deserializeSampleMessage(String messageType, String message) {
        if (StringUtils.equals(messageType, CollectKafkaStats.JSON_MESSAGE)) {
            if (JsonUtil.isJson(message)) {
                return StandardCharsets.UTF_8.encode(message);
            } else {
                throw new KylinException(STREAMING_PARSER_ERROR, "Json message is not valid: " + message);
            }
        }

        if (StringUtils.equals(messageType, CollectKafkaStats.BINARY_MESSAGE)) {
            return ByteBuffer.wrap(Base64.decodeBase64(message));
        }
        throw new KylinException(STREAMING_PARSER_ERROR, "Message type is not valid: " + messageType);
    }
}
