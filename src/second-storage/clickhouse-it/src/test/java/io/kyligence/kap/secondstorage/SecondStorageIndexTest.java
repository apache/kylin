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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.controller.NAdminController;
import org.apache.kylin.rest.controller.NModelController;
import org.apache.kylin.rest.controller.NQueryController;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ParameterResponse;
import org.apache.kylin.rest.response.SimplifiedMeasure;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.FusionModelService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.IndexPlanService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ModelBuildService;
import org.apache.kylin.rest.service.ModelQueryService;
import org.apache.kylin.rest.service.ModelSemanticHelper;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.QueryHistoryService;
import org.apache.kylin.rest.service.SegmentHelper;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
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

import com.amazonaws.util.EC2MetadataUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.job.ClickHouseRefreshSecondaryIndexJob;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableSet;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.newten.clickhouse.EmbeddedHttpServer;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.SkippingIndexChooser;
import io.kyligence.kap.secondstorage.enums.SkippingIndexType;
import io.kyligence.kap.secondstorage.management.OpenSecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageScheduleService;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.SecondStorageIndexLoadStatus;
import io.kyligence.kap.secondstorage.management.request.SecondStorageIndexResponse;
import io.kyligence.kap.secondstorage.management.request.UpdateIndexRequest;
import io.kyligence.kap.secondstorage.management.request.UpdateIndexResponse;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.test.EnableScheduler;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PowerMockIgnore({ "javax.net.ssl.*", "javax.management.*", "org.apache.hadoop.*", "javax.security.*", "javax.crypto.*",
        "javax.script.*" })
@PrepareForTest({ SpringContext.class, InsertInto.class, EC2MetadataUtils.class })
public class SecondStorageIndexTest implements JobWaiter {
    private final String USER_NAME = "ADMIN";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(ImmutableMap.of("spark.sql.extensions",
            "org.apache.kylin.query.SQLPushDownExtensions", "spark.sql.broadcastTimeout", "900"));

    public EnableTestUser enableTestUser = new EnableTestUser();

    public EnableScheduler enableScheduler = new EnableScheduler("table_index_incremental",
            "src/test/resources/ut_meta");

    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(enableScheduler);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final JobService jobService = Mockito.spy(JobService.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @InjectMocks
    private final SecondStorageService secondStorageService = Mockito.spy(new SecondStorageService());

    @InjectMocks
    private final ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private final SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    @Mock
    private final SecondStorageScheduleService secondStorageScheduleService = new SecondStorageScheduleService();

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
    private final FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @Mock
    private final NModelController nModelController = Mockito.spy(new NModelController());

    @Mock
    private final ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());

    @Mock
    private final NQueryController nQueryController = Mockito.spy(new NQueryController());

    @Mock
    private final QueryHistoryService queryHistoryService = Mockito.spy(new QueryHistoryService());

    @Mock
    private final NAdminController nAdminController = Mockito.spy(new NAdminController());

    @Mock
    private final OpenSecondStorageEndpoint openSecondStorageEndpoint = Mockito.spy(new OpenSecondStorageEndpoint());

    private EmbeddedHttpServer _httpServer = null;
    protected IndexDataConstructor indexDataConstructor;
    private final SparkSession ss = sharedSpark.getSpark();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.when(SpringContext.getBean(SecondStorageUpdater.class))
                .thenAnswer((Answer<SecondStorageUpdater>) invocation -> secondStorageService);

        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageEndpoint.setModelService(modelService);

        secondStorageService.setAclEvaluate(aclEvaluate);

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);

        ReflectionTestUtils.setField(modelQueryService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "indexPlanService", indexPlanService);
        ReflectionTestUtils.setField(modelService, "semanticUpdater", modelSemanticHelper);
        ReflectionTestUtils.setField(modelService, "modelBuildService", modelBuildService);
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);

        ReflectionTestUtils.setField(modelBuildService, "modelService", modelService);
        ReflectionTestUtils.setField(modelBuildService, "segmentHelper", segmentHelper);
        ReflectionTestUtils.setField(modelBuildService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(nModelController, "modelService", modelService);
        ReflectionTestUtils.setField(nModelController, "fusionModelService", fusionModelService);

        ReflectionTestUtils.setField(fusionModelService, "modelService", modelService);

        ReflectionTestUtils.setField(queryHistoryService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(nQueryController, "queryHistoryService", queryHistoryService);

        openSecondStorageEndpoint.setModelService(modelService);
        openSecondStorageEndpoint.setSecondStorageEndpoint(secondStorageEndpoint);
        openSecondStorageEndpoint.setSecondStorageService(secondStorageService);

        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        System.setProperty("kylin.second-storage.wait-index-build-second", "1");

        _httpServer = EmbeddedHttpServer.startServer(getLocalWorkingDirectory());

        indexDataConstructor = new IndexDataConstructor(getProject());
    }

    @Test
    public void testSkippingIndexType() {
        assertEquals(SkippingIndexType.MINMAX,
                SkippingIndexChooser.getSkippingIndexType(new DataType(DataType.INTEGER, 0, 0)));
        assertEquals(SkippingIndexType.MINMAX,
                SkippingIndexChooser.getSkippingIndexType(new DataType(DataType.TINY_INT, 0, 0)));
        assertEquals(SkippingIndexType.MINMAX,
                SkippingIndexChooser.getSkippingIndexType(new DataType(DataType.BIGINT, 0, 0)));
        assertEquals(SkippingIndexType.MINMAX,
                SkippingIndexChooser.getSkippingIndexType(new DataType(DataType.SMALL_INT, 0, 0)));

        assertEquals(SkippingIndexType.SET,
                SkippingIndexChooser.getSkippingIndexType(new DataType(DataType.BOOLEAN, 0, 0)));
        assertEquals(SkippingIndexType.BLOOM_FILTER,
                SkippingIndexChooser.getSkippingIndexType(new DataType(DataType.VARCHAR, 0, 0)));
        assertEquals(SkippingIndexType.BLOOM_FILTER,
                SkippingIndexChooser.getSkippingIndexType(new DataType(DataType.STRING, 0, 0)));

        DataType real = new DataType(DataType.REAL, 0, 0);
        assertThrows(KylinException.class, () -> SkippingIndexChooser.getSkippingIndexType(real));

        System.setProperty("kylin.second-storage.skipping-index.set", "101");
        assertEquals("bloom_filter(0.025)", SkippingIndexType.BLOOM_FILTER.toSql(getConfig()));
        assertEquals("set(101)", SkippingIndexType.SET.toSql(getConfig()));
        System.setProperty("kylin.second-storage.skipping-index.bloom-filter", "0.5");
        assertEquals("bloom_filter(0.5)", SkippingIndexType.BLOOM_FILTER.toSql(getConfig()));
        assertEquals("minmax()", SkippingIndexType.MINMAX.toSql(getConfig()));
    }

    @Test
    public void testOrderByAndSkippingIndex() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";
            System.setProperty("kylin.second-storage.allow-nullable-skipping-index", "true");
            System.setProperty("kylin.second-storage.skipping-index.granularity", "-3");

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());

                String modelId = createModel();

                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                testNoData(modelId);
                testHasData(modelId);
                testLockedKeIndex(modelId);
                testClickhouseDown(clickhouse, modelId, replica);
                return true;
            });
        }
    }

    @SneakyThrows
    private void testClickhouseDown(JdbcDatabaseContainer<?>[] clickhouse, String modelId, int replica) {
        String modelName = getNDataModel(modelId).getAlias();
        val secondaryIndex = Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID");
        clickhouse[0].stop();

        List<String> primaryIndexError = Lists.newArrayList("TEST_KYLIN_FACT.LEAF_CATEG_ID123");
        assertThrows(KylinException.class,
                () -> updatePrimaryIndexAndSecondaryIndex(modelName, primaryIndexError, null));

        clickhouse[0].start();
        ClickHouseUtils.internalConfigClickHouse(clickhouse, replica);

        updatePrimaryIndexAndSecondaryIndex(modelName, null, secondaryIndex);
        // load secondstorage
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02", new HashSet<>(getIndexPlan(modelId).getAllLayouts()),
                modelId);
        waitAllJoEnd();

        clickhouse[0].stop();
        String jobId = updatePrimaryIndexAndSecondaryIndex(modelName, null, Sets.newHashSet());
        waitJobEnd(getProject(), jobId);

        assertThrows(String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectJobExists(), getProject()),
                KylinException.class, () -> updatePrimaryIndexAndSecondaryIndex(modelName, null, secondaryIndex));
        clickhouse[0].start();
        ClickHouseUtils.internalConfigClickHouse(clickhouse, replica);

        secondStorageService.cleanModel(getProject(), modelId);
        waitAllJoEnd();
        getNExecutableManager().resumeJob(jobId);
        waitJobEnd(getProject(), jobId);
        assertEquals(ExecutableState.SUCCEED, getNExecutableManager().getJob(jobId).getStatus());

        checkEmpty(modelId);
    }

    private void testNoData(String modelId) {
        String modelName = getNDataModel(modelId).getAlias();

        List<String> primaryIndexError = Lists.newArrayList("TEST_KYLIN_FACT.LEAF_CATEG_ID123");
        assertThrows(KylinException.class,
                () -> updatePrimaryIndexAndSecondaryIndex(modelName, primaryIndexError, null));

        System.setProperty("kylin.second-storage.allow-nullable-skipping-index", "false");
        List<String> primaryIndex = Lists.newArrayList("TEST_KYLIN_FACT.LEAF_CATEG_ID");
        Set<String> secondaryIndex = Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID");
        assertThrows(TransactionException.class,
                () -> updatePrimaryIndexAndSecondaryIndex(modelName, primaryIndex, null));
        assertThrows(TransactionException.class,
                () -> updatePrimaryIndexAndSecondaryIndex(modelName, null, secondaryIndex));

        primaryIndex.add("TEST_KYLIN_FACT.CAL_DT");
        secondaryIndex.add("TEST_KYLIN_FACT.CAL_DT");
        assertThrows(TransactionException.class,
                () -> updatePrimaryIndexAndSecondaryIndex(modelName, primaryIndex, null));
        assertThrows(TransactionException.class,
                () -> updatePrimaryIndexAndSecondaryIndex(modelName, null, secondaryIndex));

        System.setProperty("kylin.second-storage.allow-nullable-skipping-index", "true");
        updatePrimaryIndexAndSecondaryIndex(modelName, Lists.newArrayList("TEST_KYLIN_FACT.LEAF_CATEG_ID"),
                Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID"));

        assertTrue(getTableFlow(modelId).getTableDataList().isEmpty());
        TableEntity tableEntity = getTablePlan(modelId).getTableMetas().get(0);
        assertTrue(tableEntity.getSecondaryIndexColumns().contains(0));
        assertEquals(1, tableEntity.getPrimaryIndexColumns().size());
        assertEquals(1, tableEntity.getPrimaryIndexColumns().get(0).intValue());
        assertEquals(1, tableEntity.getSecondaryIndexColumns().size());
        assertEquals(0, tableEntity.getSecondaryIndexColumns().stream().findFirst().get().intValue());

        long jobCnt = getNExecutableManager().getAllExecutables().stream()
                .filter(job -> job instanceof ClickHouseRefreshSecondaryIndexJob).count();
        updatePrimaryIndexAndSecondaryIndex(modelName, null, Sets.newHashSet("TEST_KYLIN_FACT.LEAF_CATEG_ID"));
        assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream()
                .filter(job -> job instanceof ClickHouseRefreshSecondaryIndexJob).count());

        tableEntity = getTablePlan(modelId).getTableMetas().get(0);
        assertEquals(1, tableEntity.getPrimaryIndexColumns().size());
        assertEquals(1, tableEntity.getPrimaryIndexColumns().get(0).intValue());
        assertEquals(1, tableEntity.getSecondaryIndexColumns().size());
        assertEquals(1, tableEntity.getSecondaryIndexColumns().stream().findFirst().get().intValue());

        updatePrimaryIndexAndSecondaryIndex(modelName, Lists.newArrayList(), Sets.newHashSet());
        checkEmpty(modelId);
        updatePrimaryIndexAndSecondaryIndex(modelName, Lists.newArrayList("TEST_KYLIN_FACT.LEAF_CATEG_ID"),
                Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID"));
        updatePrimaryIndexAndSecondaryIndex(modelName, Lists.newArrayList("TEST_KYLIN_FACT.TRANS_ID"),
                Sets.newHashSet("TEST_KYLIN_FACT.LEAF_CATEG_ID"));
        long layoutId = getTablePlan(modelId).getTableMetas().get(0).getLayoutID();
        val res1 = secondStorageEndpoint.deletePrimaryIndex(getProject(), modelName, layoutId);
        val res2 = secondStorageEndpoint.deleteSecondaryIndex(getProject(), modelName, layoutId);
        assertEquals(KylinException.CODE_SUCCESS, res1.getCode());
        assertEquals(KylinException.CODE_SUCCESS, res2.getCode());

        checkEmpty(modelId);
    }

    private void checkEmpty(String modelId) {
        for (TableEntity tableEntity : getTablePlan(modelId).getTableMetas()) {
            assertTrue(tableEntity.getPrimaryIndexColumns().isEmpty());
            assertTrue(tableEntity.getSecondaryIndexColumns().isEmpty());
        }

        for (TableData tableData : getTableFlow(modelId).getTableDataList()) {
            for (TablePartition partition : tableData.getPartitions()) {
                assertTrue(partition.getSecondaryIndexColumns().isEmpty());
            }
        }
    }

    private void testHasData(String modelId) {
        String modelName = getNDataModel(modelId).getAlias();
        updatePrimaryIndexAndSecondaryIndex(modelName, Lists.newArrayList("TEST_KYLIN_FACT.LEAF_CATEG_ID"),
                Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID"));
        // load secondstorage
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02",
                new HashSet<>(
                        NIndexPlanManager.getInstance(getConfig(), getProject()).getIndexPlan(modelId).getAllLayouts()),
                modelId);
        waitAllJoEnd();

        TableData tableData = getTableFlow(modelId).getTableDataList().get(0);
        TablePartition partition = tableData.getPartitions().get(0);
        assertTrue(partition.getSecondaryIndexColumns().contains(0));
        assertEquals(1, partition.getSecondaryIndexColumns().size());

        val primaryIndexList = Lists.<String> newArrayList();
        assertThrows(TransactionException.class,
                () -> updatePrimaryIndexAndSecondaryIndex(modelName, primaryIndexList, null));
        long jobCnt = getNExecutableManager().getAllExecutables().stream()
                .filter(job -> job instanceof ClickHouseRefreshSecondaryIndexJob).count();
        String jobId = updatePrimaryIndexAndSecondaryIndex(modelName, null, Sets.newHashSet());
        checkJobOperation(jobId);
        waitAllJoEnd();
        jobCnt++;
        assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream()
                .filter(job -> job instanceof ClickHouseRefreshSecondaryIndexJob).count());

        // test range lock
        val lockSecondaryIndex = Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID");
        SegmentRange<Long> range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        assertThrows(MsgPicker.getMsg().getSecondStorageConcurrentOperate(), KylinException.class,
                () -> updatePrimaryIndexAndSecondaryIndex(modelName, null, lockSecondaryIndex));
        SecondStorageLockUtils.unlock(modelId, range);

        tableData = getTableFlow(modelId).getTableDataList().get(0);
        partition = tableData.getPartitions().get(0);
        assertTrue(partition.getSecondaryIndexColumns().isEmpty());

        updatePrimaryIndexAndSecondaryIndex(modelName, null, Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID"));
        waitAllJoEnd();
        jobCnt++;
        assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream()
                .filter(job -> job instanceof ClickHouseRefreshSecondaryIndexJob).count());
        tableData = getTableFlow(modelId).getTableDataList().get(0);
        partition = tableData.getPartitions().get(0);
        assertTrue(partition.getSecondaryIndexColumns().contains(0));
        assertEquals(1, partition.getSecondaryIndexColumns().size());

        EnvelopeResponse<List<SecondStorageIndexResponse>> res1 = secondStorageEndpoint.listIndex(getProject(),
                modelName);
        assertEquals(KylinException.CODE_SUCCESS, res1.getCode());
        assertEquals(1, res1.getData().size());
        res1.getData().forEach(r -> {
            assertEquals(SecondStorageIndexLoadStatus.ALL, r.getPrimaryIndexStatus());
            assertEquals(SecondStorageIndexLoadStatus.ALL, r.getSecondaryIndexStatus());
        });

        secondStorageService.sizeInNode(getProject());
        EnvelopeResponse<List<SecondStorageIndexResponse>> res3 = secondStorageEndpoint.listIndex(getProject(),
                modelName);
        assertEquals(KylinException.CODE_SUCCESS, res3.getCode());
        assertEquals(1, res3.getData().size());
        res3.getData().forEach(r -> {
            assertEquals(SecondStorageIndexLoadStatus.ALL, r.getPrimaryIndexStatus());
            assertEquals(SecondStorageIndexLoadStatus.ALL, r.getSecondaryIndexStatus());
        });

        secondStorageService.triggerSegmentsClean(getProject(), modelId,
                getDataFlow(modelId).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet()));
        waitAllJoEnd();

        EnvelopeResponse<List<SecondStorageIndexResponse>> res2 = secondStorageEndpoint.listIndex(getProject(),
                modelName);
        assertEquals(KylinException.CODE_SUCCESS, res2.getCode());
        assertEquals(1, res2.getData().size());
        res2.getData().forEach(r -> {
            assertEquals(SecondStorageIndexLoadStatus.NONE, r.getPrimaryIndexStatus());
            assertEquals(SecondStorageIndexLoadStatus.NONE, r.getSecondaryIndexStatus());
        });

        updatePrimaryIndexAndSecondaryIndex(modelName, Lists.newArrayList(), Sets.newHashSet());
        assertEquals(jobCnt, NExecutableManager.getInstance(getConfig(), getProject()).getAllExecutables().stream()
                .filter(job -> job instanceof ClickHouseRefreshSecondaryIndexJob).count());
        checkEmpty(modelId);

        modelService.deleteSegmentById(modelId, getProject(), getDataFlow(modelId).getSegments().stream()
                .map(NDataSegment::getId).collect(Collectors.toList()).toArray(new String[] {}), true);
        waitAllJoEnd();
    }

    private void testLockedKeIndex(String modelId) {
        String modelName = getNDataModel(modelId).getAlias();
        updatePrimaryIndexAndSecondaryIndex(modelName,
                Lists.newArrayList("TEST_KYLIN_FACT.LEAF_CATEG_ID", "TEST_KYLIN_FACT.TRANS_ID"),
                Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID"));
        // load secondstorage
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02",
                new HashSet<>(
                        NIndexPlanManager.getInstance(getConfig(), getProject()).getIndexPlan(modelId).getAllLayouts()),
                modelId);
        waitAllJoEnd();

        modelService.updateDataModelSemantic(getProject(), getChangedModelRequest(modelId, "IS_EFFECTUAL"));
        waitAllJoEnd();

        LayoutEntity layout = getIndexPlan(modelId).getBaseTableLayout();
        TableEntity tableEntity = getTablePlan(modelId).getEntity(layout).get();
        assertTrue(getTablePlan(modelId).getEntity(layout).isPresent());
        assertEquals(2, tableEntity.getPrimaryIndexColumns().size());
        assertEquals(1, tableEntity.getPrimaryIndexColumns().get(0).intValue());
        assertEquals(0, tableEntity.getPrimaryIndexColumns().get(1).intValue());
        assertEquals(1, tableEntity.getSecondaryIndexColumns().size());
        assertTrue(tableEntity.getSecondaryIndexColumns().contains(0));

        updatePrimaryIndexAndSecondaryIndex(modelName, Lists.newArrayList("TEST_KYLIN_FACT.LEAF_CATEG_ID"), null);

        layout = getIndexPlan(modelId).getBaseTableLayout();
        tableEntity = getTablePlan(modelId).getEntity(layout).get();
        assertTrue(getTablePlan(modelId).getEntity(layout).isPresent());
        assertEquals(1, tableEntity.getPrimaryIndexColumns().size());
        assertEquals(1, tableEntity.getPrimaryIndexColumns().get(0).intValue());
        assertEquals(1, tableEntity.getSecondaryIndexColumns().size());
        assertTrue(tableEntity.getSecondaryIndexColumns().contains(0));

        buildIncrementalLoadQuery("2012-01-02", "2012-01-03", new HashSet<>(getIndexPlan(modelId).getAllLayouts()),
                modelId);
        waitAllJoEnd();

        for (TableData tableData : getTableFlow(modelId).getTableDataList()) {
            for (TablePartition partition : tableData.getPartitions()) {
                assertEquals(1, partition.getSecondaryIndexColumns().size());
                assertEquals(0, partition.getSecondaryIndexColumns().stream().findFirst().get().intValue());
            }
        }

        updatePrimaryIndexAndSecondaryIndex(modelName, null,
                Sets.newHashSet("TEST_KYLIN_FACT.TRANS_ID", "TEST_KYLIN_FACT.LEAF_CATEG_ID"));
        EnvelopeResponse<List<SecondStorageIndexResponse>> res1 = secondStorageEndpoint.listIndex(getProject(),
                modelName);
        assertEquals(KylinException.CODE_SUCCESS, res1.getCode());
        assertEquals(2, res1.getData().size());
        waitAllJoEnd();

        UpdateIndexRequest materialize = new UpdateIndexRequest();
        materialize.setProject(getProject());
        materialize.setModelName(modelName);
        materialize.setLayoutId(getIndexPlan(modelId).getBaseTableLayoutId());
        EnvelopeResponse<UpdateIndexResponse> res2 = secondStorageEndpoint.materializeSecondaryIndex(materialize);
        assertEquals(KylinException.CODE_SUCCESS, res2.getCode());
        assertNotNull(res2.getData().getTieredStorageIndexJobId());

        indexPlanService.removeIndexes(getProject(), modelId, getIndexPlan(modelId).getAllToBeDeleteLayoutId());
        modelService.updateDataModelSemantic(getProject(), getModelNoPartitionRequest(modelId));
        waitAllJoEnd();
        checkEmpty(modelId);

        modelService.updateDataModelSemantic(getProject(), getModelRequest(modelId));
        waitAllJoEnd();
        checkEmpty(modelId);
    }

    @SneakyThrows
    private ModelRequest getChangedModelRequest(String modelId, String columnName) {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel(modelId);

        val request = JsonUtil.readValue(writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val columnDesc = model.getRootFactTable().getColumn(columnName).getColumnDesc();
        request.getSimplifiedDimensions().add(getNamedColumn(columnDesc));
        val partitionDesc = model.getPartitionDesc();
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        request.setPartitionDesc(model.getPartitionDesc());
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(writeValueAsString(request), ModelRequest.class);
    }

    @SneakyThrows
    private ModelRequest getModelNoPartitionRequest(String modelId) {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel(modelId);
        val request = JsonUtil.readValue(writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);
        request.setPartitionDesc(null);
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(writeValueAsString(request), ModelRequest.class);
    }

    @SneakyThrows
    private ModelRequest getModelRequest(String modelId) {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel(modelId);
        val request = JsonUtil.readValue(writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val partition = new PartitionDesc();
        partition.setPartitionDateFormat("yyyy-MM-dd");
        partition.setPartitionDateColumn("TEST_KYLIN_FACT.CAL_DT");
        partition.setCubePartitionType(PartitionDesc.PartitionType.APPEND);
        request.setPartitionDesc(partition);
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(writeValueAsString(request), ModelRequest.class);
    }

    @SneakyThrows
    private String writeValueAsString(Object obj) {
        return JsonUtil.writeValueAsString(obj);
    }

    private String updatePrimaryIndexAndSecondaryIndex(String modelName, List<String> primaryIndex,
            Set<String> secondaryIndex) {
        UpdateIndexRequest request = new UpdateIndexRequest();
        request.setProject(getProject());
        request.setModelName(modelName);
        request.setPrimaryIndexes(primaryIndex);
        request.setSecondaryIndexes(secondaryIndex);
        EnvelopeResponse<UpdateIndexResponse> result = secondStorageEndpoint.updateIndex(request);
        assertEquals(KylinException.CODE_SUCCESS, result.getCode());
        return result.getData().getTieredStorageIndexJobId();
    }

    private String createModel() {
        val modelRequest = newModelRequest();
        modelRequest.setWithSecondStorage(true);
        EnvelopeResponse<BuildBaseIndexResponse> result = nModelController.createModel(modelRequest);
        assertEquals(KylinException.CODE_SUCCESS, result.getCode());
        return modelRequest.getId();
    }

    private ModelRequest newModelRequest() {
        ModelRequest modelRequest = new ModelRequest();
        modelRequest.setAlias("SecondStorageModel");
        modelRequest.setProject(getProject());
        modelRequest.setSaveOnly(true);
        modelRequest.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");
        modelRequest.setManagementType(ManagementType.MODEL_BASED);
        modelRequest.setJoinTables(Lists.newArrayList());
        modelRequest.setSimplifiedJoinTableDescs(Lists.newArrayList());

        val partition = new PartitionDesc();
        partition.setPartitionDateFormat("yyyy-MM-dd");
        partition.setPartitionDateColumn("TEST_KYLIN_FACT.CAL_DT");
        partition.setCubePartitionType(PartitionDesc.PartitionType.APPEND);
        modelRequest.setPartitionDesc(partition);
        modelRequest.setWithSecondStorage(false);

        SimplifiedMeasure measure = new SimplifiedMeasure();
        measure.setName("COUNT_ALL");
        measure.setExpression("COUNT");
        measure.setParameterValue(Lists.newArrayList(new ParameterResponse("constant", "1")));
        modelRequest.setSimplifiedMeasures(Lists.newArrayList(measure));

        NDataModel.NamedColumn d0 = new NDataModel.NamedColumn();
        d0.setId(0);
        d0.setName("TEST_KYLIN_FACT_TRANS_ID");
        d0.setAliasDotColumn("TEST_KYLIN_FACT.TRANS_ID");
        d0.setStatus(NDataModel.ColumnStatus.DIMENSION);

        NDataModel.NamedColumn d1 = new NDataModel.NamedColumn();
        d1.setId(1);
        d1.setName("TEST_KYLIN_FACT_LEAF_CATEG_ID");
        d1.setAliasDotColumn("TEST_KYLIN_FACT.LEAF_CATEG_ID");
        d1.setStatus(NDataModel.ColumnStatus.DIMENSION);

        NDataModel.NamedColumn d5 = new NDataModel.NamedColumn();
        d5.setId(5);
        d5.setName("TEST_KYLIN_FACT_CAL_DT");
        d5.setAliasDotColumn("TEST_KYLIN_FACT.CAL_DT");
        d5.setStatus(NDataModel.ColumnStatus.DIMENSION);

        modelRequest.setSimplifiedDimensions(Lists.newArrayList(d0, d1, d5));
        return modelRequest;
    }

    private void buildIncrementalLoadQuery(String start, String end, String modelId) throws Exception {
        getIndexPlan(modelId).getAllLayouts().forEach(layout -> {
            if (!layout.isBaseIndex() || !layout.getIndex().isTableIndex()) {
                indexPlanService.removeIndexes(getProject(), modelId, ImmutableSet.of(layout.getId()));
            }
        });
        buildIncrementalLoadQuery(start, end, new HashSet<>(getIndexPlan(modelId).getAllLayouts()), modelId);
    }

    @SneakyThrows
    private void buildIncrementalLoadQuery(String start, String end, Set<LayoutEntity> layoutIds, String modelId) {
        val timeRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        indexDataConstructor.buildIndex(modelId, timeRange, layoutIds, true);
    }

    private void checkJobOperation(String jobId) {
        String project = getProject();
        assertThrows(MsgPicker.getMsg().getJobPauseFailed(), KylinException.class,
                () -> SecondStorageUtil.checkJobPause(project, jobId));
        assertThrows(KylinException.class, () -> SecondStorageUtil.checkJobRestart(project, jobId));
    }

    private TableFlow getTableFlow(String modelId) {
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(
                SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(modelId).isPresent());
        return SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(modelId).get();
    }

    private NDataModel.NamedColumn getNamedColumn(ColumnDesc columnDesc) {
        NDataModel.NamedColumn transIdColumn = new NDataModel.NamedColumn();
        transIdColumn.setId(Integer.parseInt(columnDesc.getId()));
        transIdColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        transIdColumn.setName(columnDesc.getTable().getName() + "_" + columnDesc.getName());
        transIdColumn.setAliasDotColumn(columnDesc.getTable().getName() + "." + columnDesc.getName());
        return transIdColumn;
    }

    private NDataModel getNDataModel(String modelId) {
        return getNDataModelManager().getDataModelDesc(modelId);
    }

    private NDataModelManager getNDataModelManager() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    private NExecutableManager getNExecutableManager() {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    private NDataflow getDataFlow(String modelId) {
        return NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getDataflow(modelId);
    }

    private KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    private void setQuerySession(String catalog, String jdbcUrl, String driverClassName) {
        System.setProperty("kylin.query.use-tableindex-answer-non-raw-query", "true");
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".url", jdbcUrl);
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".driver", driverClassName);
    }

    private void waitAllJoEnd() {
        getNExecutableManager().getAllExecutables().forEach(exec -> waitJobEnd(getProject(), exec.getId()));
    }

    private TablePlan getTablePlan(String modelId) {
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(
                SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(modelId).isPresent());
        return SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(modelId).get();
    }

    public String getProject() {
        return "table_index_incremental";
    }

    private String getSourceUrl() {
        return _httpServer.uriAccessedByDocker.toString();
    }

    private IndexPlan getIndexPlan(String modelId) {
        return NIndexPlanManager.getInstance(getConfig(), getProject()).getIndexPlan(modelId);
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
}
