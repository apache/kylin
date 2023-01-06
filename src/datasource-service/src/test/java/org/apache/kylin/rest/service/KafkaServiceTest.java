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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED;
import static org.apache.kylin.streaming.constants.StreamingConstants.DEFAULT_PARSER_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.jar.JarInfoManager;
import org.apache.kylin.metadata.jar.JarTypeEnum;
import org.apache.kylin.metadata.streaming.DataParserInfo;
import org.apache.kylin.metadata.streaming.DataParserManager;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class KafkaServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private final KafkaService kafkaService = Mockito.spy(KafkaService.class);

    @Mock
    private final CustomFileService customFileService = Mockito.spy(CustomFileService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    private static final String brokerServer = "localhost:19093";
    private static final String PROJECT = "streaming_test";
    private static final String JAR_NAME = "custom_parser.jar";
    private static final String PARSER_NAME1 = "org.apache.kylin.parser.JsonDataParser1";
    private static final String PARSER_NAME2 = "org.apache.kylin.parser.CsvDataParser2";
    private static final String PARSER_NAME3 = "org.apache.kylin.parser.CustomDataParser2";
    private static String JAR_ABS_PATH;

    KafkaConfig kafkaConfig = new KafkaConfig();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(kafkaService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(customFileService, "aclEvaluate", aclEvaluate);
        init();
    }

    public void init() {
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setName("P_LINEORDER");
        kafkaConfig.setProject("streaming_test");
        kafkaConfig.setKafkaBootstrapServers(brokerServer);
        kafkaConfig.setSubscribe("ssb-topic1");
        kafkaConfig.setStartingOffsets("latest");
        kafkaConfig.setBatchTable("");
        kafkaConfig.setParserName(DEFAULT_PARSER_NAME);
    }

    public void initJar() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // ../examples/test_data/21767/metadata
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());
        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        JAR_ABS_PATH = new File(jarPath.toString()).toString();
    }

    @Test
    public void testCheckBrokerStatus() {
        try {
            kafkaService.checkBrokerStatus(kafkaConfig);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            KylinException kylinException = (KylinException) e;
            val dataMap = (Map<String, Object>) kylinException.getData();
            Assert.assertNotNull(dataMap);
            Assert.assertEquals(1, dataMap.size());
            Assert.assertEquals(Collections.singletonList(brokerServer), dataMap.get("failed_servers"));
        }
    }

    @Test
    public void testGetTopics() {
        Message msg = MsgPicker.getMsg();
        Assert.assertThrows(msg.getBrokerTimeoutMessage(), KylinException.class,
                () -> kafkaService.getTopics(kafkaConfig, PROJECT, "test"));

        kafkaConfig.setKafkaBootstrapServers("");
        Assert.assertThrows(msg.getInvalidBrokerDefinition(), KylinException.class,
                () -> kafkaService.getTopics(kafkaConfig, PROJECT, "test"));
    }

    @Test
    public void testGetMessage() {
        Assert.assertThrows(MsgPicker.getMsg().getStreamingTimeoutMessage(), KylinException.class,
                () -> kafkaService.getMessages(kafkaConfig, PROJECT));

        kafkaConfig.setKafkaBootstrapServers("");
        Assert.assertThrows(MsgPicker.getMsg().getInvalidBrokerDefinition(), KylinException.class,
                () -> kafkaService.getMessages(kafkaConfig, PROJECT));
    }

    @Test
    public void testGetMessageTypeAndDecodedMessages() {
        val value = ByteBuffer.allocate(10);
        value.put("msg-1".getBytes());
        value.flip();
        val messages = Collections.singletonList(value);
        val decoded = kafkaService.decodeMessage(messages);
        val decodedMessages = (List) decoded.get("message");
        Assert.assertEquals(1, decodedMessages.size());
    }

    @Test
    public void testConvertSampleMessageToFlatMap() {
        val result = kafkaService.parserMessage("streaming_test", kafkaConfig,
                "{\"a\": 2, \"b\": 2, \"timestamp\": \"2000-01-01 05:06:12\"}");
        Assert.assertEquals(3, result.size());
        Assert.assertThrows(CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED.getMsg(), KylinException.class,
                () -> kafkaService.parserMessage("streaming_test", kafkaConfig,
                        "{\"_a\": 2, \"b\": 2, \"timestamp\": \"2000-01-01 05:06:12\"}"));
        Assert.assertThrows(MsgPicker.getMsg().getEmptyStreamingMessage(), KylinException.class,
                () -> kafkaService.parserMessage("streaming_test", kafkaConfig, ""));
    }

    @Test
    public void testGetParsers() {
        Assert.assertFalse(kafkaService.getParsers(PROJECT).isEmpty());
    }

    @Test
    public void testRemoveParser() throws IOException {
        DataParserManager manager = DataParserManager.getInstance(getTestConfig(), PROJECT);
        JarInfoManager infoManager = JarInfoManager.getInstance(getTestConfig(), PROJECT);

        initJar();
        String jarType = "STREAMING_CUSTOM_PARSER";
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        String jarHdfsPath = customFileService.uploadCustomJar(jarFile, PROJECT, jarType);
        customFileService.loadParserJar(JAR_NAME, jarHdfsPath, PROJECT);
        Assert.assertNotNull(infoManager.getJarInfo(JarTypeEnum.valueOf(jarType), JAR_NAME));

        Assert.assertNotNull(manager.getDataParserInfo(PARSER_NAME1));
        kafkaService.removeParser(PROJECT, PARSER_NAME1);
        Assert.assertNull(manager.getDataParserInfo(PARSER_NAME1));

        Assert.assertNotNull(manager.getDataParserInfo(PARSER_NAME2));
        kafkaService.removeParser(PROJECT, PARSER_NAME2);
        Assert.assertNull(manager.getDataParserInfo(PARSER_NAME2));

        Assert.assertNotNull(manager.getDataParserInfo(PARSER_NAME3));
        kafkaService.removeParser(PROJECT, PARSER_NAME3);
        Assert.assertNull(manager.getDataParserInfo(PARSER_NAME3));
        Assert.assertNull(infoManager.getJarInfo(JarTypeEnum.valueOf(jarType), JAR_NAME));
    }

    @Test
    public void testInitDefaultParser() {
        DataParserManager manager = DataParserManager.getInstance(getTestConfig(), PROJECT);
        val defaultParser = manager.getDataParserInfo(DEFAULT_PARSER_NAME);
        val crud = (CachedCrudAssist<DataParserInfo>) ReflectionTestUtils.getField(manager, "crud");
        Assert.assertNotNull(crud);
        crud.delete(defaultParser);
        Assert.assertNull(manager.getDataParserInfo(DEFAULT_PARSER_NAME));
        kafkaService.initDefaultParser(PROJECT);
        Assert.assertNotNull(manager.getDataParserInfo(DEFAULT_PARSER_NAME));
    }

}
