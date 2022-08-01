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

package io.kyligence.kap.secondstorage;

import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.configClickhouseWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.common.transaction.TransactionException;
import org.apache.kylin.common.util.Unsafe;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.toolchain.test.SimpleRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.newten.clickhouse.ClickHouseSimpleITTestUtils;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.newten.clickhouse.EmbeddedHttpServer;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.controller.SegmentController;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.JobService;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.SegmentHelper;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.management.OpenSecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageScheduleService;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.ProjectLockOperateRequest;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.test.EnableScheduler;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import io.kyligence.kap.secondstorage.test.utils.SecondStorageMetadataHelperTest;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

/**
 * Second storage lock operator unit tests
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.management.*", "org.apache.hadoop.*", "javax.security.*", "javax.crypto.*", "javax.script.*"})
@PrepareForTest({SpringContext.class, InsertInto.class})
public class SecondStorageLockOperatorTest extends SecondStorageMetadataHelperTest implements JobWaiter {
    private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    private final String userName = "ADMIN";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "org.apache.kylin.query.SQLPushDownExtensions",
                    "spark.sql.broadcastTimeout", "900")
    );

    public EnableTestUser enableTestUser = new EnableTestUser();

    public EnableScheduler enableScheduler = new EnableScheduler("table_index_incremental", "src/test/resources/ut_meta");

    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(enableScheduler);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final JobService jobService = Mockito.spy(JobService.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @InjectMocks
    private SecondStorageService secondStorageService = Mockito.spy(new SecondStorageService());

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    @Mock
    private OpenSecondStorageEndpoint openSecondStorageEndpoint = new OpenSecondStorageEndpoint();

    @Mock
    private SecondStorageScheduleService secondStorageScheduleService = new SecondStorageScheduleService();

    @Mock
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final ModelSemanticHelper modelSemanticHelper = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    private final ModelBuildService modelBuildService = Mockito.spy(ModelBuildService.class);

    @Mock
    private final SegmentHelper segmentHelper = Mockito.spy(new SegmentHelper());

    @Mock
    private final SegmentController segmentController = new SegmentController();

    @Mock
    private final NModelController nModelController = new NModelController();

    @Mock
    private final FusionModelService fusionModelService = new FusionModelService();


    private EmbeddedHttpServer _httpServer = null;
    protected IndexDataConstructor indexDataConstructor;
    private final SparkSession ss = sharedSpark.getSpark();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.when(SpringContext.getBean(SecondStorageUpdater.class)).thenAnswer((Answer<SecondStorageUpdater>) invocation -> secondStorageService);

        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageEndpoint.setModelService(modelService);
        openSecondStorageEndpoint.setSecondStorageService(secondStorageService);
        openSecondStorageEndpoint.setModelService(modelService);
        openSecondStorageEndpoint.setSecondStorageEndpoint(secondStorageEndpoint);

        secondStorageService.setAclEvaluate(aclEvaluate);

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);

        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "indexPlanService", indexPlanService);
        ReflectionTestUtils.setField(modelService, "semanticUpdater", modelSemanticHelper);
        ReflectionTestUtils.setField(modelService, "modelBuildService", modelBuildService);

        ReflectionTestUtils.setField(modelBuildService, "modelService", modelService);
        ReflectionTestUtils.setField(modelBuildService, "segmentHelper", segmentHelper);
        ReflectionTestUtils.setField(modelBuildService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(segmentController, "modelService", modelService);

        ReflectionTestUtils.setField(nModelController, "modelService", modelService);
        ReflectionTestUtils.setField(nModelController, "fusionModelService", fusionModelService);

        ReflectionTestUtils.setField(fusionModelService, "modelService", modelService);

        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());

        _httpServer = EmbeddedHttpServer.startServer(getLocalWorkingDirectory());

        indexDataConstructor = new IndexDataConstructor(getProject());
    }


    @Test
    public void testLockOperate() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[]{clickhouse1};
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                val lockOperateRequest1 = new ProjectLockOperateRequest();

                lockOperateRequest1.setProject(getProject());
                lockOperateRequest1.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
                lockOperateRequest1.setOperateType(LockOperateTypeEnum.LOCK.name());

                EnvelopeResponse<Void> envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest1);

                ClickHouseSimpleITTestUtils.checkLockOperateResult(envelopeResponse,
                        Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()), getProject());

                val lockOperateRequest2 = new ProjectLockOperateRequest();

                lockOperateRequest2.setProject(getProject());
                lockOperateRequest2.setLockTypes(Collections.singletonList(LockTypeEnum.ALL.name()));
                lockOperateRequest2.setOperateType(LockOperateTypeEnum.LOCK.name());

                Assert.assertThrows(KylinException.class, () -> secondStorageEndpoint.lockOperate(lockOperateRequest2));

                val lockOperateRequest3 = new ProjectLockOperateRequest();

                lockOperateRequest3.setProject(getProject());
                lockOperateRequest3.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
                lockOperateRequest3.setOperateType(LockOperateTypeEnum.UNLOCK.name());

                envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest3);

                Assertions.assertEquals("000", envelopeResponse.getCode());

                val lockOperateRequest4 = new ProjectLockOperateRequest();

                lockOperateRequest4.setProject(getProject());
                lockOperateRequest4.setLockTypes(Collections.singletonList(LockTypeEnum.ALL.name()));
                lockOperateRequest4.setOperateType(LockOperateTypeEnum.LOCK.name());

                envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest4);
                ClickHouseSimpleITTestUtils.checkLockOperateResult(envelopeResponse,
                        Collections.singletonList(LockTypeEnum.ALL.name()), getProject());

                val lockOperateRequest5 = new ProjectLockOperateRequest();

                lockOperateRequest5.setProject(getProject());
                lockOperateRequest5.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
                lockOperateRequest5.setOperateType(LockOperateTypeEnum.LOCK.name());

                Assert.assertThrows(KylinException.class, () -> secondStorageEndpoint.lockOperate(lockOperateRequest5));

                val lockOperateRequest6 = new ProjectLockOperateRequest();

                lockOperateRequest6.setProject(getProject());
                lockOperateRequest6.setLockTypes(Collections.singletonList(LockTypeEnum.ALL.name()));
                lockOperateRequest6.setOperateType(LockOperateTypeEnum.UNLOCK.name());

                envelopeResponse = secondStorageEndpoint.lockOperate(lockOperateRequest6);

                Assertions.assertEquals("000", envelopeResponse.getCode());

                return null;
            });
        }
    }

    @Test
    public void testOpenModelRecovery() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[]{clickhouse1};
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                return lockAndUnlock(() -> {
                    RecoverRequest req = new RecoverRequest();
                    req.setProject(getProject());
                    req.setModelName(getNDataModelManager().getDataModelDesc(getModelId()).getAlias());
                    assertThrows(MsgPicker.getMsg().getProjectLocked(), KylinException.class, () -> this.openSecondStorageEndpoint.recoverModel(req));

                    return null;
                });
            });
        }
    }

    @Test
    public void testDeleteSegments() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[]{clickhouse1};
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                return lockAndUnlock(() -> {
                    String project = getProject();
                    assertThrows(MsgPicker.getMsg().getProjectLocked(), KylinException.class, () -> segmentController.deleteSegments(modelId, project, true, false, null, null));
                    return null;
                });
            });
        }
    }

    @Test
    public void testDeleteModel() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[]{clickhouse1};
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                return lockAndUnlock(() -> {
                    String project = getProject();
                    assertThrows(TransactionException.class, () -> nModelController.deleteModel(modelId, project));
                    return null;
                });
            });
        }
    }

    @Test
    public void testDeleteIndexesFromSegments() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[]{clickhouse1};
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                long layout = updateIndex("TRANS_ID");
                buildIncrementalLoadQuery(); // build table index
                checkHttpServer(); // check http server
                ClickHouseUtils.triggerClickHouseJob(getDataFlow()); //load into clickhouse

                IndexesToSegmentsRequest request = new IndexesToSegmentsRequest();
                request.setProject(getProject());
                request.setSegmentIds(getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
                request.setIndexIds(ImmutableList.of(layout));

                return lockAndUnlock(() -> {
                    assertThrows(TransactionException.class,
                            () -> segmentController.deleteIndexesFromSegments(modelId, request));
                    return null;
                });
            });
        }
    }

    @Test
    public void testUpdateSemantic() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[]{clickhouse1}, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                val partitionDesc = getNDataModel().getPartitionDesc();
                partitionDesc.setPartitionDateFormat("yyyy-MM-dd");

                getNDataModelManager().updateDataModel(modelId, copier -> copier.setManagementType(ManagementType.MODEL_BASED));

                ModelRequest request1 = getChangedModelRequestWithNoPartition("TRANS_ID");
                EnvelopeResponse<BuildBaseIndexResponse> res1 = nModelController.updateSemantic(request1);
                assertEquals("000", res1.getCode());

                ModelRequest request2 = getChangedModelRequestWithPartition("LEAF_CATEG_ID", partitionDesc);
                return lockAndUnlock(() -> {
                    assertThrows(KylinException.class, () -> nModelController.updateSemantic(request2));
                    return null;
                });
            });
        }
    }

    @Override
    public String getProject() {
        return "table_index_incremental";
    }

    @Override
    public String getModelId() {
        return modelId;
    }

    public <T> T lockAndUnlock(final Callable<T> lambda) throws Exception {
        secondStorageService.lockOperate(getProject(), Collections.singletonList(LockTypeEnum.LOAD.name()), LockOperateTypeEnum.LOCK.name());

        try {
            return lambda.call();
        } finally {
            secondStorageService.lockOperate(getProject(), Collections.singletonList(LockTypeEnum.LOAD.name()), LockOperateTypeEnum.UNLOCK.name());
        }
    }

    private String getSourceUrl() {
        return _httpServer.uriAccessedByDocker.toString();
    }

    @SneakyThrows
    private void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        assertTrue(content.length() > 0);
    }

    private static String getLocalWorkingDirectory() {
        String dir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        if (dir.startsWith("file://"))
            dir = dir.substring("file://".length());
        try {
            return new File(dir).getCanonicalPath();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private long updateIndex(String columnName) throws IOException {
        val indexResponse = modelService.updateDataModelSemantic(getProject(), getChangedModelRequest(columnName));
        val layoutId = JsonUtil.readValue(JsonUtil.writeValueAsString(indexResponse), BuildBaseIndexUT.class).tableIndex.layoutId;

        getNExecutableManager().getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
        return layoutId;
    }

    private ModelRequest getChangedModelRequest(String columnName) throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel();

        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val columnDesc = model.getRootFactTable().getColumn(columnName).getColumnDesc(); // TRANS_ID
        request.getSimplifiedDimensions().add(getNamedColumn(columnDesc));
        val partitionDesc = model.getPartitionDesc();
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        request.setPartitionDesc(model.getPartitionDesc());
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    private NDataModel.NamedColumn getNamedColumn(ColumnDesc columnDesc) {
        NDataModel.NamedColumn transIdColumn = new NDataModel.NamedColumn();
        transIdColumn.setId(Integer.parseInt(columnDesc.getId()));
        transIdColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        transIdColumn.setName(columnDesc.getTable().getName() + "_" + columnDesc.getName());
        transIdColumn.setAliasDotColumn(columnDesc.getTable().getName() + "." + columnDesc.getName());
        return transIdColumn;
    }

    @Data
    public static class BuildBaseIndexUT {
        @JsonProperty("base_table_index")
        public IndexInfo tableIndex;


        @Data
        public static class IndexInfo {
            @JsonProperty("layout_id")
            private long layoutId;
        }
    }

    private void buildIncrementalLoadQuery() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        buildIncrementalLoadQuery("2012-01-03", "2012-01-04");

        waitAllJobFinish(getProject());
    }

    private void buildIncrementalLoadQuery(String start, String end) throws Exception {
        buildIncrementalLoadQuery(start, end, new HashSet<>(getIndexPlan().getAllLayouts()));
    }

    private void buildIncrementalLoadQuery(String start, String end, Set<LayoutEntity> layoutIds) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = modelId;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val timeRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        indexDataConstructor.buildIndex(dfName, timeRange, layoutIds, true);
    }

    private ModelRequest getChangedModelRequestWithNoPartition(String columnName) throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel();

        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val columnDesc = model.getRootFactTable().getColumn(columnName).getColumnDesc(); // TRANS_ID
        request.getSimplifiedDimensions().add(getNamedColumn(columnDesc));
        request.setPartitionDesc(null);
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    private ModelRequest getChangedModelRequestWithPartition(String columnName, PartitionDesc partitionDesc) throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel();

        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val columnDesc = model.getRootFactTable().getColumn(columnName).getColumnDesc(); // TRANS_ID
        request.getSimplifiedDimensions().add(getNamedColumn(columnDesc));
        request.setPartitionDesc(partitionDesc);
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

}
