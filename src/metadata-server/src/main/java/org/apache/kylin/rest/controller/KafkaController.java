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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/api/kafka", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class KafkaController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    @Qualifier("kafkaService")
    private KafkaService kafkaService;

    @PostMapping(value = "topics", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse getTopics(@RequestBody StreamingRequest streamingRequest) throws IOException {
        checkStreamingEnabled();
        KafkaConfig kafkaConfig = streamingRequest.getKafkaConfig();
        return new EnvelopeResponse(KylinException.CODE_SUCCESS,
                kafkaService.getTopics(kafkaConfig, streamingRequest.getProject(), streamingRequest.getFuzzyKey()), "");
    }

    @PostMapping(value = "messages", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse getMessages(@RequestBody StreamingRequest streamingRequest) throws IOException {
        checkStreamingEnabled();
        KafkaConfig kafkaConfig = streamingRequest.getKafkaConfig();
        List<ByteBuffer> messages = kafkaService.getMessages(kafkaConfig, streamingRequest.getProject(),
                streamingRequest.getClusterIndex());
        if (messages == null || messages.isEmpty()) {
            return new EnvelopeResponse(KylinException.CODE_SUCCESS, "", "There is no message in this topic");
        }
        Map<String, Object> resp = kafkaService.getMessageTypeAndDecodedMessages(messages);
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, resp, "");
    }

    @PostMapping(value = "convert", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse convertMessageToFlatMap(@RequestBody StreamingRequest streamingRequest) throws IOException {
        checkStreamingEnabled();
        KafkaConfig kafkaConfig = streamingRequest.getKafkaConfig();
        String message = streamingRequest.getMessage();
        String messageType = streamingRequest.getMessageType();
        Map<String, Object> result = kafkaService.convertSampleMessageToFlatMap(kafkaConfig, messageType, message);
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, result, "");
    }
}
