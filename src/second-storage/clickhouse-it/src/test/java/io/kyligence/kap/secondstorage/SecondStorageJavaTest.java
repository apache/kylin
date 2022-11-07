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

import static io.kyligence.kap.clickhouse.ClickHouseConstants.CONFIG_CLICKHOUSE_QUERY_CATALOG;
import static io.kyligence.kap.secondstorage.SecondStorageConcurrentTestUtil.registerWaitPoint;
import static org.apache.kylin.common.exception.JobErrorCode.SECOND_STORAGE_JOB_EXISTS;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_NODE_NOT_AVAILABLE;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobSchedulerModeEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.response.NDataSegmentResponse;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.kyligence.kap.clickhouse.database.ClickHouseOperator;
import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseSegmentCleanJob;
import io.kyligence.kap.clickhouse.job.Engine;
import io.kyligence.kap.clickhouse.job.LoadContext;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import org.apache.kylin.engine.spark.job.NResourceDetectStep;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.management.OpenSecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageScheduleService;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.val;

public class SecondStorageJavaTest implements JobWaiter {
    private static final String modelName = "test_table_index";
    static private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    static private final String project = "table_index";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "org.apache.kylin.query.SQLPushDownExtensions"));

    public EnableTestUser enableTestUser = new EnableTestUser();
    @ClassRule
    public static ClickHouseClassRule clickHouseClassRule = new ClickHouseClassRule(1);

    public EnableClickHouseJob test = new EnableClickHouseJob(clickHouseClassRule.getClickhouse(), 1, project,
            Collections.singletonList(modelId), "src/test/resources/ut_meta");
    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(test);
    private ModelService modelService = new ModelService();
    private SecondStorageScheduleService secondStorageScheduleService = new SecondStorageScheduleService();
    private SecondStorageService secondStorageService = new SecondStorageService();
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();
    private OpenSecondStorageEndpoint openSecondStorageEndpoint = new OpenSecondStorageEndpoint();
    private AclEvaluate aclEvaluate = Mockito.mock(AclEvaluate.class);

    private final SparkSession sparkSession = sharedSpark.getSpark();

    @Before
    public void setUp() {
        System.setProperty("kylin.second-storage.wait-index-build-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageService.setAclEvaluate(aclEvaluate);
        openSecondStorageEndpoint.setSecondStorageService(secondStorageService);
        openSecondStorageEndpoint.setSecondStorageEndpoint(secondStorageEndpoint);
        openSecondStorageEndpoint.setModelService(modelService);
    }

    @Test
    public void testCleanSegment() throws Exception {
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertTrue(tableFlowManager.isPresent());
        buildModel();
        int segmentNum = tableFlowManager.get().get(modelId)
                .orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0)
                .getPartitions().size();
        Assert.assertEquals(1, segmentNum);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val segmentResponse = new NDataSegmentResponse(dataflow, dataflow.getFirstSegment());
        Assert.assertTrue(segmentResponse.isHasBaseTableIndexData());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        request.setSegmentIds(segs);
        secondStorageEndpoint.cleanStorage(request, segs);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job = manager.getAllExecutables().stream().filter(ClickHouseSegmentCleanJob.class::isInstance).findFirst();
        Assert.assertTrue(job.isPresent());
        waitJobFinish(project, job.get().getId());
        int partitionNum = tableFlowManager.get().get(modelId)
                .orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0)
                .getPartitions().size();
        Assert.assertEquals(0, partitionNum);
    }

    @Test
    public void testOpenCleanSegment() throws Exception {
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertTrue(tableFlowManager.isPresent());
        buildModel();
        int segmentNum = tableFlowManager.get().get(modelId)
                .orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0)
                .getPartitions().size();
        Assert.assertEquals(1, segmentNum);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val segmentResponse = new NDataSegmentResponse(dataflow, dataflow.getFirstSegment());
        Assert.assertTrue(segmentResponse.isHasBaseTableIndexData());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModelName(modelName);
        request.setSegmentIds(segs);
        openSecondStorageEndpoint.cleanStorage(request);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job1 = manager.getAllExecutables().stream().filter(ClickHouseSegmentCleanJob.class::isInstance).findFirst();
        int partitionNum = tableFlowManager.get().get(modelId)
                .orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0)
                .getPartitions().size();
        Assert.assertEquals(0, partitionNum);
    }

    @Test
    public void testDoubleTriggerSegmentLoad() throws Exception {
        buildModel();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val request = new StorageRequest();
        val jobId = triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        try {
            SecondStorageUtil.checkJobResume(project, jobId);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.JOB_RESUME_FAILED.toErrorCode(), e.getErrorCode());
        }
        try {
            triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        } catch (KylinException e) {
            Assert.assertEquals(FAILED_CREATE_JOB.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testRecoverModelNotEnableSecondStorage() {
        val request = new RecoverRequest();
        request.setProject(project);
        request.setModelName(modelName);
        val jobId = secondStorageService.disableModel(project, modelId);
        waitJobFinish(project, jobId);
        openSecondStorageEndpoint.recoverModel(request);
        Assert.fail();
    }

    @Test
    public void testRecoverModelWhenHasLoadTask() throws Exception {
        buildModel();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        val request = new RecoverRequest();
        request.setProject(project);
        request.setModelName(modelName);
        try {
            openSecondStorageEndpoint.recoverModel(request);
        } catch (KylinException e) {
            Assert.assertEquals(SECOND_STORAGE_JOB_EXISTS.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test
    public void testCleanSegmentWhenHasLoadTask() throws Exception {
        buildModel();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        try {
            secondStorageEndpoint.cleanStorage(request, segs);
        } catch (KylinException e) {
            Assert.assertEquals(SECOND_STORAGE_JOB_EXISTS.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test(expected = KylinException.class)
    public void testRecoverModelNotExist() {
        val request = new RecoverRequest();
        request.setProject(project);
        request.setModelName(modelName + "123");
        openSecondStorageEndpoint.recoverModel(request);
        Assert.fail();
    }

    @Test
    public void testModelCleanJobWithoutSegments() {
        val jobId = triggerModelCleanJob(project, modelId, enableTestUser.getUser());
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job = manager.getJob(jobId);
        Assert.assertTrue(job.getDataRangeStart() < job.getDataRangeEnd());
    }

    @Test
    public void testEnableModelWithoutBaseLayout() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager manager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            manager.updateIndexPlan(modelId,
                    copy -> copy.removeLayouts(Sets.newHashSet(copy.getBaseTableLayout().getId()), true, true));
            return null;
        }, project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val segmentResponse = new NDataSegmentResponse(dataflow, dataflow.getFirstSegment());
        Assert.assertFalse(segmentResponse.isHasBaseTableIndexData());
        secondStorageService.updateIndex(project, modelId);

        secondStorageService.disableModel(project, modelId);
        secondStorageService.enableModelSecondStorage(project, modelId);
        secondStorageService.updateIndex(project, modelId);
        secondStorageService.enableModelSecondStorage(project, modelId);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, modelId));
    }

    @Test
    public void testEnableProjectNodeNotAvailable() {
        try {
            secondStorageService.changeProjectSecondStorageState("table_index_incremental",
                    SecondStorageNodeHelper.getAllNames(), true);
        } catch (KylinException e) {
            Assert.assertEquals(SECOND_STORAGE_NODE_NOT_AVAILABLE.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test
    public void testResetStorage() {
        Assert.assertTrue(SecondStorageUtil.isProjectEnable(project));
        secondStorageEndpoint.resetStorage();
        Assert.assertFalse(SecondStorageUtil.isProjectEnable(project));
    }

    @Test
    public void testQueryWithClickHouseSuccess() throws Exception {
        final String queryCatalog = "testQueryWithClickHouseSuccess";
        Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);
        secondStorageEndpoint.refreshConf();
        Mockito.verify(aclEvaluate).checkIsGlobalAdmin();
        secondStorageService.sizeInNode(project);

        //build
        buildModel();
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertEquals(3, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());

        // check
        test.checkHttpServer();
        test.overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

        JdbcDatabaseContainer<?> clickhouse1 = clickHouseClassRule.getClickhouse(0);
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".url",
                clickhouse1.getJdbcUrl());
        sparkSession.sessionState().conf().setConfString("spark.sql.catalog." + queryCatalog + ".driver",
                clickhouse1.getDriverClassName());

        String sql = "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE";
        ExecAndComp.queryModel(project, sql);
        Assert.assertTrue(
                OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));
    }

    @Test
    public void testClickHouseOperator() throws Exception {
        val jdbcUrl = SecondStorageNodeHelper.resolve(SecondStorageNodeHelper.getAllNames().get(0));
        ClickHouseOperator operator = new ClickHouseOperator(
                SecondStorageNodeHelper.resolve(SecondStorageNodeHelper.getAllNames().get(0)));
        val databases = operator.listDatabases();
        Assert.assertEquals(4, databases.size());
        ClickHouse clickHouse = new ClickHouse(jdbcUrl);
        clickHouse.apply("CREATE TABLE test(a int) engine=Memory()");
        val tables = operator.listTables("default");
        Assert.assertEquals(1, tables.size());
        operator.dropTable("default", "test");
        val remainingTables = operator.listTables("default");
        Assert.assertEquals(0, remainingTables.size());
        operator.close();
        clickHouse.close();
    }

    @Test
    public void testSchedulerService() throws Exception {
        buildModel();
        val jdbcUrl = SecondStorageNodeHelper.resolve(SecondStorageNodeHelper.getAllNames().get(0));
        ClickHouse clickHouse = new ClickHouse(jdbcUrl);
        ClickHouseOperator operator = new ClickHouseOperator(
                SecondStorageNodeHelper.resolve(SecondStorageNodeHelper.getAllNames().get(0)));
        val render = new ClickHouseRender();
        val fakeJobId = RandomUtil.randomUUIDStr().replace("-", "_");
        val tempTable = fakeJobId + "@" + "test_temp";
        val database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply(
                ClickHouseCreateTable.createCKTable(database, tempTable).columns(new ColumnWithType("i1", "Int32"))
                        .columns(new ColumnWithType("i2", "Nullable(Int64)")).engine(Engine.DEFAULT).toSql(render));

        val srcTempTable = fakeJobId + "@" + "test_src_0";
        clickHouse.apply(
                ClickHouseCreateTable.createCKTable(database, srcTempTable).columns(new ColumnWithType("i1", "Int32"))
                        .columns(new ColumnWithType("i2", "Nullable(Int64)")).engine(Engine.DEFAULT).toSql(render));

        secondStorageScheduleService.secondStorageTempTableCleanTask();
        val tables = operator.listTables(database);
        Assert.assertFalse(tables.contains(tempTable));
        Assert.assertFalse(tables.contains(srcTempTable));
    }

    private void cleanSegments(List<String> segs) {
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        request.setSegmentIds(segs);
        secondStorageEndpoint.cleanStorage(request, segs);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job = manager.getAllExecutables().stream().filter(ClickHouseSegmentCleanJob.class::isInstance).findFirst();
        Assert.assertTrue(job.isPresent());
        waitJobFinish(project, job.get().getId());
    }

    @Test
    public void testJobPaused() throws Exception {
        buildModel();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        cleanSegments(segs);
        registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT, 10000);
        val jobId = triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            return ((DefaultExecutable) executableManager.getJob(jobId)).getTasks().stream()
                    .anyMatch(t -> t.getStatus() == ExecutableState.RUNNING);
        });
        await().pollDelay(5, TimeUnit.SECONDS).until(() -> {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                executableManager.pauseJob(jobId);
                return null;
            }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
            return true;
        });
        waitJobEnd(project, jobId);
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        await().atMost(15, TimeUnit.SECONDS).until(() -> scheduler.getContext().getRunningJobs().values().size() == 0);
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        int partitionNum = tableFlowManager.get().get(modelId)
                .orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0)
                .getPartitions().size();
        Assert.assertEquals(0, partitionNum);
        Assert.assertFalse(
                SecondStorageLockUtils.containsKey(modelId, SegmentRange.TimePartitionedSegmentRange.createInfinite()));

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val job = manager.getJob(jobId);
            Assert.assertEquals(ExecutableState.PAUSED, job.getStatus());
        });

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            executableManager.resumeJob(jobId);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            return executableManager.getJob(jobId).getStatus() == ExecutableState.RUNNING;
        });
        waitJobFinish(project, jobId);
        Assert.assertEquals(10000, IncrementalWithIntPartitionTest.getModelRowCount(project, modelId));
        SecondStorageUtil.checkSecondStorageData(project);
    }

    @Test
    public void testJobResume() throws Exception {
        buildModel();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT, 10000);
        val jobId = triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            return ((DefaultExecutable) executableManager.getJob(jobId)).getTasks().stream()
                    .anyMatch(t -> t.getStatus() == ExecutableState.RUNNING);
        });
        try {
            SecondStorageUtil.checkJobResume(project, jobId);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
        }

        waitJobFinish(project, jobId);
    }

    @Test
    public void testJobPausedWithAny() throws Exception {
        wrapWithOnlineMode("ANY", () -> {
            testJobPaused();
            return null;
        });
    }

    @Test
    public void testJobPausedDeserialize() throws Exception {
        LoadContext c1 = new LoadContext(null);
        LoadContext c2 = new LoadContext(null);
        c2.deserializeToString(c1.serializeToString());

        c2.finishSingleFile(new LoadContext.CompletedFileKeyUtil("test", 20000010001L), "file1");
        c2.finishSegment("segment1", new LoadContext.CompletedSegmentKeyUtil(20000010001L));
        LoadContext c3 = new LoadContext(null);
        c3.deserializeToString(c2.serializeToString());
        Assert.assertTrue(c3.getHistory(new LoadContext.CompletedFileKeyUtil("test", 20000010001L)).contains("file1"));
        Assert.assertTrue(
                c3.getHistorySegments(new LoadContext.CompletedSegmentKeyUtil(20000010001L)).contains("segment1"));
    }

    @Test(expected = KylinException.class)
    public void testCheckJobRestart() throws Exception {
        buildModel();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        String jobId = triggerClickHouseLoadJob(project, modelId, "ADMIN", dataflowManager.getDataflow(modelId)
                .getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        waitJobFinish(project, jobId);
        SecondStorageUtil.checkJobRestart(project, jobId);
    }

    @Test
    public void testCheckJobRestartMock() {
        DefaultExecutable exportCh = new DefaultExecutable();
        exportCh.setName(SecondStorageConstants.STEP_REFRESH_SECOND_STORAGE);
        exportCh.setProject(project);

        NResourceDetectStep detectResource = new NResourceDetectStep();
        detectResource.setName(ExecutableConstants.STEP_NAME_DETECT_RESOURCE);
        detectResource.setProject(project);

        DefaultExecutable updateMetadata = new DefaultExecutable();
        updateMetadata.setName(ExecutableConstants.STEP_UPDATE_METADATA);
        updateMetadata.setProject(project);

        DefaultExecutable executable1 = new DefaultExecutable();
        executable1.setProject(project);
        executable1.setJobType(JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        executable1.addTask(exportCh);
        Assert.assertThrows(KylinException.class, () -> SecondStorageUtil.checkJobRestart(executable1));

        DefaultExecutable executable2 = new DefaultExecutable();
        executable2.setProject(project);
        executable2.setJobType(JobTypeEnum.INDEX_BUILD);
        executable2.addTask(updateMetadata);
        SecondStorageUtil.checkJobRestart(executable2);

        executable2.addTask(exportCh);
        SecondStorageUtil.checkJobRestart(executable2);

        executable2.setJobSchedulerMode(JobSchedulerModeEnum.DAG);
        Assert.assertThrows(KylinException.class, () -> SecondStorageUtil.checkJobRestart(executable2));

        executable2.getTasks().add(0, detectResource);
        SecondStorageUtil.checkJobRestart(executable2);
    }

    @Test
    public void testCheckJobResumeMock() {
        DefaultExecutable exportCh = new DefaultExecutable();
        exportCh.setName(SecondStorageConstants.STEP_REFRESH_SECOND_STORAGE);
        exportCh.setProject(project);

        NResourceDetectStep detectResource = new NResourceDetectStep();
        detectResource.setName(ExecutableConstants.STEP_NAME_DETECT_RESOURCE);
        detectResource.setProject(project);

        DefaultExecutable updateMetadata = new DefaultExecutable();
        updateMetadata.setName(ExecutableConstants.STEP_UPDATE_METADATA);
        updateMetadata.setProject(project);

        DefaultExecutable executable = new DefaultExecutable();
        executable.setProject(project);
        executable.setJobType(JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        executable.addTask(detectResource);
        executable.addTask(updateMetadata);
        SecondStorageUtil.checkJobResume(executable);

        executable.addTask(exportCh);
        SecondStorageUtil.checkJobResume(executable);
    }

    @Test
    public void testCleanModelWhenTableNotExists() throws Exception {
        buildModel();
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        val jdbc = SecondStorageNodeHelper.resolve(node);
        ClickHouse clickHouse = new ClickHouse(jdbc);
        val table = NameUtil.getTable(modelId, 20000000001L);
        String database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply("DROP TABLE " + database + "." + table);
        String jobId = triggerModelCleanJob(project, modelId, enableTestUser.getUser());
        waitJobFinish(project, jobId);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        waitJobFinish(project, triggerClickHouseLoadJob(project, modelId, "ADMIN", dataflowManager.getDataflow(modelId)
                .getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList())));
        database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply("DROP DATABASE " + database);
        jobId = triggerModelCleanJob(project, modelId, enableTestUser.getUser());
        waitJobFinish(project, jobId);
        SecondStorageUtil.checkJobResume(project, jobId);
        SecondStorageUtil.checkJobRestart(project, jobId);
    }

    @Test
    public void testSshPort() throws Exception {
        final String queryCatalog = "testQueryWithClickHouseHASuccess";
        Unsafe.setProperty(CONFIG_CLICKHOUSE_QUERY_CATALOG, queryCatalog);

        JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
        JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();

        internalConfigClickHouse(2, 22, clickhouse1, clickhouse2);

        ClusterInfo cluster = ClickHouseConfigLoader.getInstance().getCluster();
        List<Node> nodes = cluster.getNodes();
        nodes.forEach(node -> {
            Assert.assertEquals(22, node.getSSHPort());
        });

        Node node = nodes.get(0);

        val cliCommandExecutor = new CliCommandExecutor(node.getIp(), cluster.getUserName(), cluster.getPassword(),
                KylinConfig.getInstanceFromEnv().getSecondStorageSshIdentityPath(), node.getSSHPort());
        cliCommandExecutor.getSshClient().toString();
    }

    public static void internalConfigClickHouse(int replica, int sshPort, JdbcDatabaseContainer<?>... clickhouse)
            throws IOException {
        ClickHouseUtils.internalConfigClickHouse(clickhouse, replica, sshPort);
    }

    public void buildModel() throws Exception {
        new IndexDataConstructor(project).buildDataflow(modelId);
        waitAllJobFinish(project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        waitJobFinish(project, triggerClickHouseLoadJob(project, modelId, "ADMIN", dataflowManager.getDataflow(modelId)
                .getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList())));
    }

    private static <T> T wrapWithOnlineMode(String onlineModel, final Callable<T> lambda) throws Exception {
        String defaultOnlineModel = KylinConfig.getInstanceFromEnv().getKylinEngineSegmentOnlineMode();
        System.setProperty("kylin.engine.segment-online-mode", onlineModel);

        try {
            return lambda.call();
        } finally {
            System.setProperty("kylin.engine.segment-online-mode", defaultOnlineModel);
        }
    }
}
