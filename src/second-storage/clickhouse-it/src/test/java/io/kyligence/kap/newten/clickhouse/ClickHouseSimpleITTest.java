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
package io.kyligence.kap.newten.clickhouse;

import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.columnMapping;
import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.configClickhouseWith;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.execution.datasources.jdbc.ClickHouseDialect$;
import org.apache.spark.sql.execution.datasources.v2.PostV2ScanRelationPushDown$;
import org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown$;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan;
import org.apache.spark.sql.jdbc.JdbcDialects$;
import org.eclipse.jetty.toolchain.test.SimpleRequest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.clearspring.analytics.util.Preconditions;

import io.kyligence.kap.clickhouse.ClickHouseNameUtil;
import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.clickhouse.tool.ClickHouseSanityCheckTool;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.OpenSecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseSimpleITTest extends NLocalWithSparkSessionTest implements JobWaiter {
    public final String cubeName = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    public final String userName = "ADMIN";
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final JobService jobService = Mockito.spy(JobService.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    private OpenSecondStorageEndpoint openSecondStorageEndpoint = new OpenSecondStorageEndpoint();
    private SecondStorageService secondStorageService = new SecondStorageService();
    private ModelService modelService = Mockito.mock(ModelService.class);
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    /**
     * According to JUnit's mechanism, the super class's method will be hidden by the child class for the same
     * Method signature. So we use {@link #beforeClass()} to hide {@link NLocalWithSparkSessionTest#beforeClass()}
     */
    @BeforeClass
    public static void beforeClass() {
        JdbcDialects$.MODULE$.registerDialect(ClickHouseDialect$.MODULE$);
        NLocalWithSparkSessionTest.ensureSparkConf();
        ClickHouseUtils.InjectNewPushDownRule(sparkConf);
        NLocalWithSparkSessionTest.beforeClass();
        Assert.assertTrue(SparderEnv.getSparkSession().sessionState().optimizer().preCBORules()
                .contains(V2ScanRelationPushDown$.MODULE$));
        Assert.assertTrue(SparderEnv.getSparkSession().sessionState().optimizer().preCBORules()
                .contains(PostV2ScanRelationPushDown$.MODULE$));
        System.setProperty("kylin.second-storage.wait-index-build-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
    }

    @AfterClass
    public static void afterClass() {
        NLocalWithSparkSessionTest.afterClass();
        JdbcDialects$.MODULE$.unregisterDialect(ClickHouseDialect$.MODULE$);
    }

    private EmbeddedHttpServer _httpServer = null;
    @Rule
    public TestName testName = new TestName();

    protected boolean needHttpServer() {
        return true;
    }

    protected void doSetup() throws Exception {

    }

    protected void prepareMeta() throws IOException {
        this.createTestMetadata("src/test/resources/ut_meta");
        Assert.assertTrue(tempMetadataDirectory.exists());
        final File testProjectMetaDir = new File(tempMetadataDirectory.getPath() + "/metadata/" + getProject());
        final String message = String.format(Locale.ROOT, "%s's meta (%s) doesn't exist, please check!", getProject(),
                testProjectMetaDir.getCanonicalPath());
        Assert.assertTrue(message, testProjectMetaDir.exists());
    }

    /**
     * It will hidden  {@link NLocalWithSparkSessionTest#setUp()} }
     */
    @Before
    public void setUp() throws Exception {
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);

        secondStorageService.setAclEvaluate(aclEvaluate);
        secondStorageService.setJobService(jobService);
        secondStorageService.setModelService(modelService);
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageEndpoint.setModelService(modelService);
        openSecondStorageEndpoint.setSecondStorageEndpoint(secondStorageEndpoint);
        prepareMeta();
        SparkJobFactoryUtils.initJobFactory();

        doSetup();

        if (needHttpServer()) {
            _httpServer = EmbeddedHttpServer.startServer(getLocalWorkingDirectory());
        }

        overwriteSystemProp("kylin.second-storage.query-pushdown-limit", "0");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        SecurityContextHolder.getContext().setAuthentication(authentication);
        populateSSWithCSVData(getTestConfig(), getProject(), ss);
        indexDataConstructor = new IndexDataConstructor(getProject());
    }

    @After
    public void tearDown() throws Exception {
        if (_httpServer != null) {
            _httpServer.stopServer();
            _httpServer = null;
        }
        QueryContext.reset();
        ClickHouseConfigLoader.clean();
        NDefaultScheduler.destroyInstance();
        ResourceStore.clearCache();
        FileUtils.deleteDirectory(new File("../clickhouse-it/metastore_db"));
        super.tearDown();
    }

    @Override
    public String getProject() {
        String project;
        if (testName.getMethodName().toLowerCase(Locale.ROOT).contains("incremental")) {
            project = "table_index_incremental";
        } else {
            project = "table_index";
        }
        return project;
    }

    @Test
    public void testSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShard", false, clickhouse);
            EnvelopeResponse response = secondStorageEndpoint.tableSync(getProject());
            Assertions.assertEquals(response.getCode(), "000");

            TableFlow flow = SecondStorage.tableFlowManager(getTestConfig(), getProject()).get(cubeName).orElse(null);
            Assert.assertNotNull(flow);
            checkLoadTempTableName(
                    NExecutableManager.getInstance(getTestConfig(), getProject()).getAllExecutables().get(0).getId(),
                    NDataflowManager.getInstance(getTestConfig(), getProject()).getDataflow(cubeName).getFirstSegment()
                            .getId(),
                    flow.getTableDataList().get(0).getLayoutID());
        }
    }

    @Test
    public void testTwoShardDoubleReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testTwoShardDoubleReplica", false, 2, null, clickhouse1, clickhouse2, clickhouse3,
                    clickhouse4);
        }
    }

    @Test
    public void testTwoShards() throws Exception {
        // TODO: make sure splitting data into two shards
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testTwoShards", false, clickhouse1, clickhouse2);
        }
    }

    @SneakyThrows
    protected void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        Assert.assertTrue(content.length() > 0);
    }

    @Test
    public void testIncrementalTwoShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalTwoShard", true, clickhouse1, clickhouse2);
        }
    }

    @Test
    public void testIncrementalTwoShardDoubleReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalTwoShardDoubleReplica", true, 2, null, clickhouse1, clickhouse2,
                    clickhouse3, clickhouse4);
        }
    }

    @Test
    public void testDisableSSModel() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalCleanModel", false, clickhouse);
            val request = new ModelEnableRequest();
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            val modelName = modelManager.getDataModelDesc(cubeName).getAlias();
            request.setModelName(modelName);
            request.setEnabled(false);
            request.setProject(getProject().toUpperCase());
            Assert.assertEquals(cubeName, modelManager.getDataModelDesc(cubeName).getUuid());
            val jobInfo = openSecondStorageEndpoint.enableStorage(request);
            Assert.assertEquals(1, jobInfo.getData().getJobs().size());
            waitJobFinish(jobInfo.getData().getJobs().get(0).getJobId());
            val tablePlanManager = SecondStorageUtil.tablePlanManager(KylinConfig.getInstanceFromEnv(), getProject());
            val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), getProject());
            val nodeGroupManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(), getProject());
            Preconditions.checkState(
                    tableFlowManager.isPresent() && tablePlanManager.isPresent() && nodeGroupManager.isPresent());
            Assert.assertEquals(0, tablePlanManager.get().listAll().size());
            Assert.assertEquals(0, tableFlowManager.get().listAll().size());
            Assert.assertEquals(1, nodeGroupManager.get().listAll().size());
        }
    }

    @Test
    public void testCheckUtil() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse }, 1, "testCheckUtil", () -> {
                ClickHouseSanityCheckTool.execute(new String[] { "1" });
                return null;
            });
        }
    }

    @Test
    public void testHAJobPaused() throws Exception {
        final String modelId = cubeName;
        final String project = getProject(); //"table_index";

        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1, clickhouse2 }, 2, "testHAJobPaused", () -> {
                buildFullLoadQuery();
                val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                val dataflow = dataflowManager.getDataflow(modelId);
                val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId)
                        .collect(Collectors.toList());

                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(2, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                val jobId = triggerClickHouseLoadJob(project, modelId, userName, segs);

                await().atMost(30, TimeUnit.SECONDS).until(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                    return executableManager.getJob(jobId).getStatus() == ExecutableState.RUNNING;
                });

                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                    executableManager.pauseJob(jobId);
                    return null;
                }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId);

                waitJobEnd(project, jobId);

                NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
                await().atMost(30, TimeUnit.SECONDS)
                        .until(() -> scheduler.getContext().getRunningJobs().values().size() == 0);

                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                    executableManager.resumeJob(jobId);
                    return null;
                }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
                await().atMost(30, TimeUnit.SECONDS).until(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                    return executableManager.getJob(jobId).getStatus() == ExecutableState.RUNNING;
                });
                waitJobFinish(project, jobId);

                List<Integer> rowsList = getHAModelRowCount(project, modelId);

                for (Integer rows : rowsList) {
                    Assert.assertEquals(10000, rows.intValue());
                }
                return null;
            });
        }
    }

    protected void buildIncrementalLoadQuery() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = cubeName;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(dfName);

        val timeRange1 = new SegmentRange.TimePartitionedSegmentRange("2012-01-01", "2012-01-02");
        val indexes = new HashSet<>(df.getIndexPlan().getAllLayouts());
        indexDataConstructor.buildIndex(dfName, timeRange1, indexes, true);
        val timeRange2 = new SegmentRange.TimePartitionedSegmentRange("2012-01-02", "2012-01-03");
        indexDataConstructor.buildIndex(dfName, timeRange2, indexes, true);
    }

    protected void buildFullLoadQuery() throws Exception {
        fullBuild(cubeName);
    }

    protected void mergeSegments(List<String> segIds) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(cubeName);
        val jobManager = JobManager.getInstance(config, getProject());
        long start = Long.MAX_VALUE;
        long end = -1;
        for (String id : segIds) {
            val segment = df.getSegment(id);
            val segmentStart = segment.getTSRange().getStart();
            val segmentEnd = segment.getTSRange().getEnd();
            if (segmentStart < start)
                start = segmentStart;
            if (segmentEnd > end)
                end = segmentEnd;
        }

        val mergeSeg = dfMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(start, end), true);
        val jobParam = new JobParam(mergeSeg, cubeName, userName);
        val jobId = jobManager.mergeSegmentJob(jobParam);
        waitJobFinish(jobId);
        waitJobFinish(triggerClickHouseLoadJob(getProject(), cubeName, userName,
                Collections.singletonList(mergeSeg.getId())));
    }

    private void waitJobFinish(String jobId, boolean isAllowFailed) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, getProject());
        DefaultExecutable job = (DefaultExecutable) executableManager.getJob(jobId);
        await().atMost(300, TimeUnit.SECONDS).until(() -> !job.getStatus().isProgressing());
        Assert.assertFalse(job.getStatus().isProgressing());
        if (!isAllowFailed) {
            val firstErrorMsg = IndexDataConstructor.firstFailedJobErrorMessage(executableManager, job);
            Assert.assertEquals(firstErrorMsg, ExecutableState.SUCCEED, executableManager.getJob(jobId).getStatus());
        }
    }

    private void waitJobFinish(String jobId) {
        waitJobFinish(jobId, false);
    }

    protected void refreshSegment(String segId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(cubeName);
        val jobManager = JobManager.getInstance(config, getProject());
        NDataSegment newSeg = dfMgr.refreshSegment(df, df.getSegment(segId).getSegRange());
        val jobParam = new JobParam(newSeg, df.getModel().getId(), userName);
        val jobId = jobManager.refreshSegmentJob(jobParam);
        waitJobFinish(jobId);
    }

    protected String getSourceUrl() {
        return _httpServer.uriAccessedByDocker.toString();
    }

    private String simulateJobMangerAddJob(JobParam jobParam, AbstractJobHandler handler) {
        ExecutableUtil.computeParams(jobParam);
        handler.handle(jobParam);
        return jobParam.getJobId();
    }

    protected void build_load_query(String catalog, boolean incremental, JdbcDatabaseContainer<?>... clickhouse)
            throws Exception {
        build_load_query(catalog, incremental, 1, null, clickhouse);
    }

    private JobParam triggerClickHouseJob(NDataflow df, KylinConfig config) {
        val segments = new HashSet<>(df.getSegments());
        AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
        JobParam jobParam = SecondStorageJobParamUtil.of(getProject(), cubeName, userName,
                segments.stream().map(NDataSegment::getId));
        String jobId = simulateJobMangerAddJob(jobParam, localHandler);
        waitJobFinish(jobId);
        return jobParam;
    }

    private void changeProjectSecondStorageState(String catalog, boolean incremental, int replica,
            JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            return true;
        });
    }

    protected void build_load_query(String catalog, boolean incremental, int replica, Callable<Void> beforeQuery,
            JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        build_load_query(catalog, incremental, true, replica, beforeQuery, null, clickhouse);
    }

    protected void build_load_query(String catalog, boolean incremental, boolean isMergeSegment, int replica,
            Callable<Void> beforeQuery, Callable<Void> checkQuery, JdbcDatabaseContainer<?>... clickhouse)
            throws Exception {
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), cubeName, true);
            // build table index
            if (incremental) {
                buildIncrementalLoadQuery();
            } else {
                buildFullLoadQuery();
            }

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    getProject());
            if ("table_index_incremental".equals(getProject())) {
                Assert.assertEquals(1, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());
            } else if ("table_index".equals(getProject())) {
                Assert.assertEquals(3, SecondStorageUtil.setSecondStorageSizeInfo(modelManager.listAllModels()).size());
            }

            // check http server
            checkHttpServer();

            //load into clickhouse
            //load into clickhouse
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
            NDataflow df = dsMgr.getDataflow(cubeName);
            triggerClickHouseJob(df, config);

            // test refresh segment
            val needRefresh = dsMgr.getDataflow(cubeName).getSegments().get(0);
            refreshSegment(needRefresh.getId());

            // test merge segment
            if (incremental && isMergeSegment) {
                mergeSegments(dsMgr.getDataflow(cubeName).getQueryableSegments().stream().map(NDataSegment::getId)
                        .collect(Collectors.toList()));
            }

            // check TableFlow
            TablePlan plan = SecondStorage.tablePlanManager(config, getProject()).get(cubeName).orElse(null);
            Assert.assertNotNull(plan);
            TableFlow flow = SecondStorage.tableFlowManager(config, getProject()).get(cubeName).orElse(null);
            Assert.assertNotNull(flow);

            Set<LayoutEntity> allLayouts = df.getIndexPlan().getAllLayouts().stream()
                    .filter(SecondStorageUtil::isBaseTableIndex).collect(Collectors.toSet());
            Assert.assertEquals(allLayouts.size(), flow.getTableDataList().size());
            for (LayoutEntity layoutEntity : allLayouts) {
                TableEntity tableEntity = plan.getEntity(layoutEntity).orElse(null);
                Assert.assertNotNull(tableEntity);
                TableData data = flow.getEntity(layoutEntity).orElse(null);
                Assert.assertNotNull(data);
                Assert.assertEquals(incremental ? PartitionType.INCREMENTAL : PartitionType.FULL,
                        data.getPartitionType());
                Assert.assertEquals(dsMgr.getDataflow(cubeName).getQueryableSegments().size(),
                        data.getPartitions().size() / replica);
                TablePartition partition = data.getPartitions().get(0);
                int shards = Math.min(clickhouse.length / replica, tableEntity.getShardNumbers());
                Assert.assertEquals(shards, partition.getShardNodes().size());
                Assert.assertEquals(shards, partition.getSizeInNode().size());
                Assert.assertTrue(partition.getSizeInNode().values().stream().reduce(Long::sum).orElse(0L) > 0L);
            }

            // check
            overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");

            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog,
                    "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".url", clickhouse[0].getJdbcUrl());
            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".driver",
                    clickhouse[0].getDriverClassName());
            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".pushDownAggregate", "true");
            ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".numPartitions",
                    String.valueOf(clickhouse.length / replica));

            // check ClickHouse
            if (beforeQuery != null)
                beforeQuery.call();

            if (checkQuery != null) {
                checkQuery.call();
            } else {
                checkQueryResult(incremental, clickhouse, replica);
            }

            return true;
        });
    }

    private void checkQueryResult(boolean incremental, JdbcDatabaseContainer<?>[] clickhouse, int replica)
            throws Exception {
        Dataset<Row> dataset = ExecAndComp.queryModelWithoutCompute(getProject(),
                "select PRICE from TEST_KYLIN_FACT group by PRICE");
        Assert.assertTrue(ClickHouseUtils.findShardJDBCTable(dataset.queryExecution().optimizedPlan()));
        QueryContext.current().setRetrySecondStorage(true);
        // check Aggregate push-down
        Dataset<Row> groupPlan = ExecAndComp.queryModelWithoutCompute(getProject(),
                "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE");
        JDBCScan jdbcScan = ClickHouseUtils.findJDBCScan(groupPlan.queryExecution().optimizedPlan());
        Assert.assertEquals(clickhouse.length / replica, jdbcScan.relation().parts().length);
        if (clickhouse.length == 1) {
            ClickHouseUtils.checkAggregateRemoved(groupPlan);
        }
        String[] expectedPlanFragment = new String[] { "PushedAggregates: [SUM(" + columnMapping.get("PRICE") + ")], ",
                "PushedFilters: [], ", "PushedGroupByExpressions: [" + columnMapping.get("PRICE") + "], " };
        ClickHouseUtils.checkPushedInfo(groupPlan, expectedPlanFragment);

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> query = new ArrayList<>();
        Pair<String, String> pair = null;
        if (incremental) {
            val result = SparderEnv.getSparkSession()
                    .sql("select * from TEST_KYLIN_FACT where CAL_DT >= '2012-01-01' and CAL_DT < '2012-01-03'");
            result.createOrReplaceTempView("TEST_KYLIN_FACT1");
            pair = new Pair<>("TEST_KYLIN_FACT", "TEST_KYLIN_FACT1");
        }
        query.add(Pair.newPair("query_table_index1", "select PRICE from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index2", "select sum(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index3", "select max(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index4", "select min(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(Pair.newPair("query_table_index5", "select count(PRICE) from TEST_KYLIN_FACT group by PRICE"));
        query.add(
                Pair.newPair("query_table_index6", "select count(distinct PRICE) from TEST_KYLIN_FACT group by PRICE"));

        query.add(Pair.newPair("query_table_index7", "select sum(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index8", "select max(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index9", "select min(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index10", "select count(PRICE) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_table_index11", "select count(distinct PRICE) from TEST_KYLIN_FACT"));

        query.add(Pair.newPair("query_table_index12",
                "select sum(PRICE),sum(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index13",
                "select max(PRICE),max(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index14",
                "select min(PRICE),min(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index15",
                "select count(PRICE),count(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index16",
                "select count(distinct PRICE),count(distinct ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));
        query.add(Pair.newPair("query_table_index17",
                "select min(PRICE) from TEST_KYLIN_FACT where ORDER_ID=2 group by PRICE "));

        query.add(Pair.newPair("query_agg_index1", "select sum(ORDER_ID) from TEST_KYLIN_FACT"));
        query.add(Pair.newPair("query_agg_index2",
                "select sum(ORDER_ID),LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME"));

        query.add(Pair.newPair("query_agg_inner_col_index1",
                "select \n" + "  sum(ORDER_ID + 1), \n" + "  count( distinct \n"
                        + "    case when LSTG_FORMAT_NAME <> '' then LSTG_FORMAT_NAME else 'unknown' end\n"
                        + "  ) from TEST_KYLIN_FACT \n" + "group by \n" + "  LSTG_FORMAT_NAME\n"));
        query.add(Pair.newPair("query_agg_inner_col_index2",
                "select \n" + "  sum(ORDER_ID + 1), \n" + "  count( distinct \n"
                        + "    case when LSTG_FORMAT_NAME <> '' then LSTG_FORMAT_NAME else 'unknown' end\n" + "  ) \n"
                        + "from \n" + "  (\n" + "    select \n" + "      a1.ORDER_ID - 10 as ORDER_ID, \n"
                        + "      a1.LSTG_FORMAT_NAME\n" + "    from \n" + "      TEST_KYLIN_FACT a1\n" + "  ) \n"
                        + "where \n" + "  order_id > 10 \n" + "group by \n" + "  LSTG_FORMAT_NAME\n"));

        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left", pair);
    }

    public List<Integer> getHAModelRowCount(String project, String modelId) throws SQLException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val database = NameUtil.getDatabase(config, project);
        val table = NameUtil.getTable(modelId, 20000000001L);
        return SecondStorageNodeHelper.getAllNames().stream().map(SecondStorageNodeHelper::resolve).map(url -> {
            try (ClickHouse clickHouse = new ClickHouse(url)) {
                List<Integer> count = clickHouse.query("select count(*) from `" + database + "`.`" + table + "`",
                        rs -> {
                            try {
                                return rs.getInt(1);
                            } catch (SQLException e) {
                                return ExceptionUtils.rethrow(e);
                            }
                        });
                Assert.assertFalse(count.isEmpty());
                return count.get(0);
            } catch (Exception e) {
                return ExceptionUtils.rethrow(e);
            }
        }).collect(Collectors.toList());
    }

    @Test
    public void testSecondStorage() throws Exception {
        try {
            SecondStorage.init(false);
        } catch (Exception e) {
            Assert.assertFalse(SecondStorage.enabled());
        }
    }

    @Test
    public void testCheckBaseTableIndex() {
        LayoutEntity layout1 = null;
        Assert.assertFalse(SecondStorageUtil.isBaseTableIndex(layout1));

        LayoutEntity layout2 = new LayoutEntity();
        layout2.setId(20_000_001L);
        Assert.assertFalse(SecondStorageUtil.isBaseTableIndex(layout2));

        LayoutEntity layout3 = new LayoutEntity();
        layout3.setId(20_000_000_001L);
        Assert.assertFalse(SecondStorageUtil.isBaseTableIndex(layout3));

        layout3.setBase(true);
        Assert.assertTrue(SecondStorageUtil.isBaseTableIndex(layout3));
    }

    public void checkLoadTempTableName(String jobId, String segmentId, long layoutId) {
        Assert.assertEquals(94, ClickHouseNameUtil.getDestTempTableName(jobId, segmentId, layoutId).length());
        Assert.assertEquals(90, ClickHouseNameUtil.getInsertTempTableName(jobId, segmentId, layoutId).length());
        Assert.assertEquals(98, ClickHouseNameUtil.getLikeTempTableName(jobId, segmentId, layoutId).length());
        Assert.assertEquals(100, ClickHouseNameUtil
                .getFileSourceTableName(ClickHouseNameUtil.getInsertTempTableName(jobId, segmentId, layoutId), 1)
                .length());

        jobId = "aa8b5a7b_53ec_a044_d1f3_d3b260f7fad4";
        Assert.assertEquals(94, ClickHouseNameUtil.getDestTempTableName(jobId, segmentId, layoutId).length());
        Assert.assertEquals(90, ClickHouseNameUtil.getInsertTempTableName(jobId, segmentId, layoutId).length());
        Assert.assertEquals(98, ClickHouseNameUtil.getLikeTempTableName(jobId, segmentId, layoutId).length());
    }
}
