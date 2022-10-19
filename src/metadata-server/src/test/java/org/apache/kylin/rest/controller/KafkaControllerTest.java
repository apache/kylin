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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.service.KafkaService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import lombok.val;

public class KafkaControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private KafkaService kafkaService = Mockito.spy(KafkaService.class);

    @InjectMocks
    private KafkaController kafkaController = Mockito.spy(new KafkaController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static String PROJECT = "streaming_test";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(kafkaController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(kafkaController, "kafkaService", kafkaService);
    }

    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetTopics() throws Exception {
        val request = mockStreamingRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).getTopics(Mockito.any(StreamingRequest.class));
    }

    @Test
    public void testGetMessages() throws Exception {
        val request = mockStreamingRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/kafka/messages").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).getMessages(Mockito.any(StreamingRequest.class));
    }

    @Test
    public void testGetMessages1() throws Exception {
        val request = mockStreamingRequest();
        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setKafkaBootstrapServers("127.0.0.1:9092");
        kafkaConfig.setSubscribe("ssb_topic");
        kafkaConfig.setStartingOffsets("latest");
        request.setKafkaConfig(kafkaConfig);
        val messages = Arrays.asList(ByteBuffer.allocate(10));
        Mockito.when(
                kafkaService.getMessages(request.getKafkaConfig(), request.getProject(), request.getClusterIndex()))
                .thenReturn(messages);
        Mockito.when(kafkaService.getMessageTypeAndDecodedMessages(messages)).thenReturn(new HashMap<String, Object>());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/kafka/messages").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).getMessages(request);
    }

    @Test
    public void testConvertMessageToFlatMap() throws Exception {
        val request = mockStreamingRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/kafka/convert").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(kafkaController).convertMessageToFlatMap(Mockito.any(StreamingRequest.class));
    }

    private StreamingRequest mockStreamingRequest() {
        val request = new StreamingRequest();
        request.setProject(PROJECT);
        request.setKafkaConfig(Mockito.mock(KafkaConfig.class));
        return request;
    }
}
