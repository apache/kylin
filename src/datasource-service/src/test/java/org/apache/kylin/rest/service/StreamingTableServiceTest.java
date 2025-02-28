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

import static org.apache.kylin.streaming.constants.StreamingConstants.DEFAULT_PARSER_NAME;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.junit.rule.TransactionExceptedException;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class StreamingTableServiceTest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private UserService userService = Mockito.spy(UserService.class);

    @Mock
    private UserAclService userAclService = Mockito.spy(UserAclService.class);

    @InjectMocks
    private AccessService accessService = Mockito.spy(new AccessService());

    @InjectMocks
    private StreamingTableService streamingTableService = Mockito.spy(new StreamingTableService());

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    private static final String PROJECT = "streaming_test";

    @Before
    public void setup() {
        SparkJobFactoryUtils.initJobFactory();
        createTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.forceDropProject("broken_test");
        projectManager.forceDropProject("bad_query_test");

        System.setProperty("HADOOP_USER_NAME", "root");

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(streamingTableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(tableService, "accessService", accessService);
        ReflectionTestUtils.setField(userAclService, "userService", userService);
        ReflectionTestUtils.setField(accessService, "userAclService", userAclService);
        ReflectionTestUtils.setField(accessService, "userService", userService);

        Mockito.when(userService.listSuperAdminUsers()).thenReturn(Arrays.asList("admin"));
        Mockito.when(userAclService.hasUserAclPermissionInProject(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(false);

        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        EventBusFactory.getInstance().restart();
        cleanupTestMetadata();
    }

    @Test
    public void testInnerReloadTable() {
        val database = "SSB";

        val config = getTestConfig();
        try {
            val tableDescList = tableService
                    .getTableDesc(PROJECT, true, "P_LINEORDER_STR", database, false, Collections.emptyList(), 10)
                    .getFirst();
            Assert.assertEquals(1, tableDescList.size());
            val tableDesc = tableDescList.get(0);
            val tableExtDesc = tableService.getOrCreateTableExt(PROJECT, tableDesc);
            val list = streamingTableService.innerReloadTable(PROJECT, tableDesc, tableExtDesc);
            Assert.assertEquals(0, list.size());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testReloadTable() {
        val database = "DEFAULT";

        try {
            val tableDescList = tableService
                    .getTableDesc(PROJECT, true, "", database, true, Collections.emptyList(), 10).getFirst();
            Assert.assertEquals(2, tableDescList.size());
            val tableDesc = tableDescList.get(0);
            val tableExtDesc = tableService.getOrCreateTableExt(PROJECT, tableDesc);
            streamingTableService.reloadTable(PROJECT, tableDesc, tableExtDesc);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testCreateKafkaConfig() {
        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setDatabase("DEFAULT");
        kafkaConfig.setName("TPCH_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9092");
        kafkaConfig.setSubscribe("tpch_topic");
        kafkaConfig.setStartingOffsets("latest");
        kafkaConfig.setParserName(DEFAULT_PARSER_NAME);
        streamingTableService.createKafkaConfig(PROJECT, kafkaConfig);

        val kafkaConf = KafkaConfigManager.getInstance(getTestConfig(), PROJECT).getKafkaConfig("DEFAULT.TPCH_TOPIC");
        Assert.assertEquals("DEFAULT", kafkaConf.getDatabase());
        Assert.assertEquals("TPCH_TOPIC", kafkaConf.getName());
        Assert.assertEquals("10.1.2.210:9092", kafkaConf.getKafkaBootstrapServers());
        Assert.assertEquals("tpch_topic", kafkaConf.getSubscribe());
        Assert.assertEquals("latest", kafkaConf.getStartingOffsets());
    }

    @Test
    public void testUpdateKafkaConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        val kafkaConfig = KafkaConfigManager.getInstance(kylinConfig, PROJECT).getKafkaConfig("DEFAULT.SSB_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9093");
        streamingTableService.updateKafkaConfig(PROJECT, kafkaConfig);
        val kafkaConf = KafkaConfigManager.getInstance(getTestConfig(), PROJECT).getKafkaConfig("DEFAULT.SSB_TOPIC");
        Assert.assertEquals("10.1.2.210:9093", kafkaConf.getKafkaBootstrapServers());
    }

    @Test
    public void testDecimalConvertToDouble() {
        StreamingRequest streamingRequest = new StreamingRequest();
        TableDesc tableDesc = new TableDesc();
        tableDesc.setColumns(new ColumnDesc[] { new ColumnDesc("1", "name1", "DECIMAL", "", "", "", ""),
                new ColumnDesc("2", "name2", "double", "", "", "", ""),
                new ColumnDesc("3", "name3", "int", "", "", "", "") });
        streamingRequest.setTableDesc(tableDesc);

        streamingTableService.decimalConvertToDouble(PROJECT, streamingRequest);

        Assert.assertEquals(2L, Arrays.stream(streamingRequest.getTableDesc().getColumns())
                .filter(column -> StringUtils.equalsIgnoreCase(column.getDatatype(), DataType.DOUBLE)).count());
    }

    @Test
    public void testCheckColumnsNotMatch() {
        StreamingRequest streamingRequest = new StreamingRequest();
        streamingRequest.setProject(PROJECT);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setColumns(new ColumnDesc[] { new ColumnDesc("1", "name1", "DECIMAL", "", "", "", ""),
                new ColumnDesc("2", "name2", "double", "", "", "", ""),
                new ColumnDesc("3", "name3", "int", "", "", "", "") });
        streamingRequest.setTableDesc(tableDesc);
        val kafkaConfig = new KafkaConfig();
        val batchTableName = "SSB.P_LINEORDER";
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setBatchTable(batchTableName);
        kafkaConfig.setName("TPCH_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9092");
        kafkaConfig.setSubscribe("tpch_topic");
        kafkaConfig.setStartingOffsets("latest");
        streamingRequest.setKafkaConfig(kafkaConfig);
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                String.format(Locale.ROOT, MsgPicker.getMsg().getBatchStreamTableNotMatch(), batchTableName));
        streamingTableService.checkColumns(streamingRequest);
    }

    /**
     * fusion model check
     */
    @Test
    public void testCheckColumnsNoTimestampPartition() {
        StreamingRequest streamingRequest = new StreamingRequest();
        streamingRequest.setProject(PROJECT);
        TableDesc streamingTableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getTableDesc("SSB.LINEORDER");
        streamingRequest.setTableDesc(streamingTableDesc);

        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setBatchTable("SSB.LINEORDER");
        kafkaConfig.setName("TPCH_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9092");
        kafkaConfig.setSubscribe("tpch_topic");
        kafkaConfig.setStartingOffsets("latest");
        streamingRequest.setKafkaConfig(kafkaConfig);
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getTimestampColumnNotExist());
        streamingTableService.checkColumns(streamingRequest);
    }

    /**
     * streaming model check
     */
    @Test
    public void testCheckColumnsNoTimestampPartition1() {
        StreamingRequest streamingRequest = new StreamingRequest();
        streamingRequest.setProject(PROJECT);
        TableDesc streamingTableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getTableDesc("SSB.LINEORDER");
        streamingRequest.setTableDesc(streamingTableDesc);

        val kafkaConfig = new KafkaConfig();
        kafkaConfig.setDatabase("SSB");
        kafkaConfig.setName("TPCH_TOPIC");
        kafkaConfig.setKafkaBootstrapServers("10.1.2.210:9092");
        kafkaConfig.setSubscribe("tpch_topic");
        kafkaConfig.setStartingOffsets("latest");
        streamingRequest.setKafkaConfig(kafkaConfig);
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getTimestampColumnNotExist());
        streamingTableService.checkColumns(streamingRequest);
    }
}
