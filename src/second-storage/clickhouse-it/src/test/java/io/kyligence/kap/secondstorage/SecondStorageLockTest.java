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
import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.triggerClickHouseJob;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_STATUS_ERROR;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.ForceToTieredStorage;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.SegmentOnlineMode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageIndexCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.epoch.EpochOrchestrator;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentSecondStorageStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDaoTest;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.controller.NAdminController;
import org.apache.kylin.rest.controller.NModelController;
import org.apache.kylin.rest.controller.NQueryController;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ExecutableStepResponse;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.NDataSegmentResponse;
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
import org.apache.kylin.rest.service.QueryHistoryScheduler;
import org.apache.kylin.rest.service.QueryHistoryService;
import org.apache.kylin.rest.service.SegmentHelper;
import org.apache.kylin.rest.service.params.MergeSegmentParams;
import org.apache.kylin.rest.service.params.ModelQueryParams;
import org.apache.kylin.rest.service.params.RefreshSegmentParams;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.awaitility.Duration;
import org.eclipse.jetty.toolchain.test.SimpleRequest;
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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.AzureBlobClient;
import io.kyligence.kap.clickhouse.job.BlobUrl;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseIndexCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.job.ClickHouseModelCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseProjectCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseSegmentCleanJob;
import io.kyligence.kap.clickhouse.job.ClickhouseLoadFileLoad;
import io.kyligence.kap.clickhouse.job.DataLoader;
import io.kyligence.kap.clickhouse.job.LoadContext;
import io.kyligence.kap.clickhouse.job.S3TableSource;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.clickhouse.parser.ShowDatabasesParser;
import io.kyligence.kap.newten.clickhouse.ClickHouseSimpleITTestUtils;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.newten.clickhouse.EmbeddedHttpServer;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.database.QueryOperator;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.ShowDatabases;
import io.kyligence.kap.secondstorage.ddl.ShowTables;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.factory.SecondStorageFactoryUtils;
import io.kyligence.kap.secondstorage.management.OpenSecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageScheduleService;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectLoadRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectLockOperateRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectNodeRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.ProjectTableSyncResponse;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.SecondStorageMetadataRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.test.EnableScheduler;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PowerMockIgnore({ "javax.net.ssl.*", "javax.management.*", "org.apache.hadoop.*", "javax.security.*", "javax.crypto.*",
        "javax.script.*" })
@PrepareForTest({ SpringContext.class, InsertInto.class, EC2MetadataUtils.class })
public class SecondStorageLockTest implements JobWaiter {
    private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    private final String userName = "ADMIN";
    private static final String NULLABLE_STRING = "Nullable(String)";
    private static final String LOW_CARDINALITY_STRING = "LowCardinality(Nullable(String))";

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
    private SecondStorageService secondStorageService = Mockito.spy(new SecondStorageService());

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

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
        ReflectionTestUtils.setField(modelBuildService, "accessService", accessService);
        ReflectionTestUtils.setField(modelBuildService, "userGroupService", userGroupService);

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
    public void testIsAzure() throws URISyntaxException, StorageException {
        System.setProperty("fs.azure.account.key.devstoreaccount1.localhost:10000",
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        System.setProperty("fs.azure.skip.metrics", "true");
        // resolve dns problem
        System.setProperty("fs.azure.storage.emulator.account.name", "devstoreaccount1.localhost:10000");
        System.setProperty("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
        System.setProperty("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");

        ClickHouseLoad clickHouseLoad = new ClickHouseLoad();

        String v = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
        System.setProperty("kylin.env.hdfs-working-dir", "abfs://test@devstoreaccount1.localhost:10000/kylin");
        Assert.assertTrue(clickHouseLoad.isAzurePlatform());
        KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
        System.setProperty("kylin.env.hdfs-working-dir", "abfss://test@devstoreaccount1.localhost:10000/kylin");
        Assert.assertTrue(clickHouseLoad.isAzurePlatform());
        KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
        System.setProperty("kylin.env.hdfs-working-dir", "file:///test@devstoreaccount1.localhost:10000/kylin");
        Assert.assertFalse(clickHouseLoad.isAzurePlatform());
        KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
        System.setProperty("kylin.env.hdfs-working-dir", v);

        Assert.assertEquals("http", BlobUrl.blob2httpSchema("wasb"));
        Assert.assertEquals("https", BlobUrl.blob2httpSchema("wasbs"));
        Assert.assertEquals("https", BlobUrl.blob2httpSchema("abfs"));
        Assert.assertEquals("https", BlobUrl.blob2httpSchema("abfss"));

        CloudBlobClient client = Mockito.mock(CloudBlobClient.class);
        CloudBlobContainer container = Mockito.mock(CloudBlobContainer.class);
        Mockito.when(client.getContainerReference(Mockito.anyString())).thenReturn(container);
        AzureBlobClient azureBlobClient = new AzureBlobClient(client, new BlobUrl());
        Assert.assertThrows(NullPointerException.class, () -> azureBlobClient.generateSasKey("", 1));
    }

    @Test
    public void s3TransformFileUrl() {
        PowerMockito.mockStatic(EC2MetadataUtils.class, (t) -> {
            if ("getEC2InstanceRegion".equalsIgnoreCase(t.getMethod().getName())) {
                return "cn-northwest-1";
            }
            return t.callRealMethod();
        });
        String result = new S3TableSource().transformFileUrl(
                "hdfs://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet",
                "host.docker.internal:9000&test&test123", URI.create("/test"));
        Assert.assertEquals(
                "S3('http://host.docker.internal:9000/liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet', 'test','test123', Parquet)",
                result);
        PowerMockito.mockStatic(EC2MetadataUtils.class, (t) -> {
            if ("getEC2InstanceRegion".equalsIgnoreCase(t.getMethod().getName())) {
                return "northwest-1";
            }
            return t.callRealMethod();
        });
        result = new S3TableSource().transformFileUrl(
                "hdfs://liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet",
                "host.docker.internal:9000&test&test123", URI.create("/test"));
        Assert.assertEquals(
                "S3('http://host.docker.internal:9000/liunengdev/kylin_clickhouse/ke_metadata/test/parquet/3070ef88-fa57-4ad6-9a6b-9587cfcd4140/62fea0e8-40ef-4baa-a59f-29822fc17321/20000040001/part-00000-b1920799-ef6f-4c77-b0bf-2e72edb558dd-c000.snappy.parquet', 'test','test123', Parquet)",
                result);
    }

    @Test
    public void testCleanClickhouseTempTable() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                waitAllJobFinish();
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
                val dataflow = dataflowManager.getDataflow(modelId);
                val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId)
                        .collect(Collectors.toList());
                SecondStorageConcurrentTestUtil.registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT,
                        10000);
                int rows = 0;
                String database = NameUtil.getDatabase(dataflow);
                val jobId2 = triggerClickHouseLoadJob(getProject(), modelId, enableTestUser.getUser(), segs);
                await().pollDelay(Duration.FIVE_SECONDS).until(() -> true);

                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            getProject());
                    executableManager.pauseJob(jobId2);
                    return null;
                }, getProject(), 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId2);
                waitJobEnd(getProject(), jobId2);

                secondStorageScheduleService.secondStorageTempTableCleanTask();

                try (Connection connection = DriverManager.getConnection(clickhouse1.getJdbcUrl());
                        val stmt = connection.createStatement()) {
                    val rs = stmt.executeQuery(String.format(Locale.ROOT,
                            "select count() cnt from system.tables where database='%s'", database));

                    if (rs.next()) {
                        rows = rs.getInt("cnt");
                    }
                }
                assertNotEquals(0, rows);
                return true;
            });
        }
    }

    @Test
    public void testDoubleAcquireLock() throws Exception {
        String modelId = RandomUtil.randomUUIDStr();
        SegmentRange<Long> range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        Assert.assertTrue(SecondStorageLockUtils.containsKey(modelId, range));
    }

    @Test(expected = ExecutionException.class)
    public void acquireLock() throws Exception {
        System.setProperty("kylin.second-storage.wait-lock-timeout", "60");
        String modelId = RandomUtil.randomUUIDStr();
        SegmentRange<Long> range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        SegmentRange<Long> range2 = new SegmentRange.TimePartitionedSegmentRange(1L, 2L);
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        Future<Boolean> future = executorService.submit(() -> {
            SecondStorageLockUtils.acquireLock(modelId, range2).lock();
            return true;
        });
        future.get();
        executorService.shutdown();
    }

    @Test
    public void testBuildAllIndexDeleteLockedIndex() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());
                updateIndex("TRANS_ID");
                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
                waitAllJobFinish();

                long layout = updateIndex("LSTG_SITE_ID");
                val jobs = getNExecutableManager().getAllExecutables().stream().map(AbstractExecutable::getId)
                        .collect(Collectors.toSet());
                modelBuildService.addIndexesToSegments(getProject(), modelId,
                        Collections.singletonList(getDataFlow().getSegments().getFirstSegment().getId()), null, false,
                        3);
                waitAllJobFinish();
                val existLayouts = getTableFlow().getTableDataList().stream().map(TableData::getLayoutID).distinct()
                        .collect(Collectors.toList());

                assertEquals(1, existLayouts.size());
                assertEquals(layout, existLayouts.get(0).longValue());

                val job = getNExecutableManager().getAllExecutables().stream().filter(e -> !jobs.contains(e.getId()))
                        .findFirst();
                assertTrue(job.isPresent());
                assertTrue(NExecutableManager.toPO(job.get(), getProject()).getTasks().stream()
                        .anyMatch(e0 -> SecondStorageConstants.STEP_SECOND_STORAGE_INDEX_CLEAN.equals(e0.getName())));
                return true;
            });
        }
    }

    @Test
    public void testLoadErrorAndCloseSecondStorage() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());

                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());
                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                waitAllJobFinish();
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                clickhouse1.stop();
                String jobId = triggerClickHouseLoadJob(getProject(), modelId, userName,
                        new ArrayList<>(getAllSegmentIds()));
                waitJobEnd(getProject(), jobId);
                assertEquals(ExecutableState.ERROR, NExecutableManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getJob(jobId).getStatus());
                Optional<JobInfoResponse.JobInfo> jobInfo = secondStorageService
                        .changeModelSecondStorageState(getProject(), modelId, false);
                jobInfo.ifPresent(job -> waitJobEnd(getProject(), job.getJobId()));
                NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).resumeJob(jobId);
                waitJobEnd(getProject(), jobId);
                assertEquals(ExecutableState.SUCCEED, NExecutableManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getJob(jobId).getStatus());
                return true;
            });
        }
    }

    @Test
    public void testCleanClickhouseDiscardTable() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                waitAllJobFinish();
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
                val dataflow = dataflowManager.getDataflow(modelId);
                val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId)
                        .collect(Collectors.toList());
                SecondStorageConcurrentTestUtil.registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT,
                        10000);

                val jobId1 = triggerClickHouseLoadJob(getProject(), modelId, enableTestUser.getUser(), segs);
                await().pollDelay(Duration.FIVE_SECONDS).until(() -> true);

                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            getProject());
                    executableManager.pauseJob(jobId1);
                    return null;
                }, getProject(), 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId1);
                waitJobEnd(getProject(), jobId1);
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            getProject());
                    executableManager.discardJob(jobId1);
                    return null;
                }, getProject(), 1, UnitOfWork.DEFAULT_EPOCH_ID);
                waitJobEnd(getProject(), jobId1);

                secondStorageScheduleService.secondStorageTempTableCleanTask();

                String database = NameUtil.getDatabase(dataflow);
                int rows = 0;

                try (Connection connection = DriverManager.getConnection(clickhouse1.getJdbcUrl());
                        val stmt = connection.createStatement()) {
                    val rs = stmt.executeQuery(String.format(Locale.ROOT,
                            "select count() cnt from system.tables where database='%s'", database));

                    if (rs.next()) {
                        rows = rs.getInt("cnt");

                    }
                }
                assertEquals(0, rows);
                return true;
            });
        }
    }

    @Test
    public void testOneNodeIsDownBeforeCommit() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                val jobs = getNExecutableManager().getAllExecutables().stream().map(AbstractExecutable::getId)
                        .collect(Collectors.toSet());
                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                waitAllJobFinish();
                val buildJob = getNExecutableManager().getAllExecutables().stream()
                        .filter(j -> !jobs.contains(j.getId())).findAny().get();
                try {
                    SecondStorageUtil.checkJobResume(getProject(), buildJob.getId());
                } catch (Exception ignore) {
                }

                assertTrue(SecondStorageUtil.checkMergeFlatTableIsSuccess(buildJob));
                SecondStorageConcurrentTestUtil.registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT,
                        10000);
                val jobId = triggerClickHouseLoadJob(getProject(), modelId, enableTestUser.getUser(),
                        getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
                await().pollDelay(Duration.FIVE_SECONDS).until(() -> true);
                clickhouse1.stop();
                waitJobEnd(getProject(), jobId);
                assertTrue(getNExecutableManager().getAllExecutables().stream()
                        .anyMatch(job -> job.getStatus() == ExecutableState.ERROR));
                return true;
            });
        }
    }

    @Test
    public void testIncrementBuildLockedLayout() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            wrapWithKylinConfigMode("ANY", "DAG", () -> {
                testIncrementBuildLockedLayout(1, clickhouse1);
                return null;
            });
        }
    }

    @Test
    public void testIncrementBuildLockedLayoutChain() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            wrapWithKylinConfigMode("DFS", "CHAIN", () -> {
                testIncrementBuildLockedLayout(1, clickhouse1);
                return null;
            });
        }
    }

    private static <T> T wrapWithKylinConfigMode(String onlineMode, String scheduleMode, final Callable<T> lambda)
            throws Exception {
        String defaultOnlineMode = KylinConfig.getInstanceFromEnv().getKylinEngineSegmentOnlineMode();
        String defaultScheduleMode = KylinConfig.getInstanceFromEnv().getJobSchedulerMode();
        System.setProperty("kylin.engine.segment-online-mode", onlineMode);
        System.setProperty("kylin.engine.job-scheduler-mode", scheduleMode);

        try {
            return lambda.call();
        } finally {
            System.setProperty("kylin.engine.segment-online-mode", defaultOnlineMode);
            System.setProperty("kylin.engine.job-scheduler-mode", defaultScheduleMode);
        }
    }

    @Test
    public void testStatic() {
        JobParam jobParam = new JobParam();
        jobParam.setSecondStorageDeleteLayoutIds(null);
        assertNull(jobParam.getSecondStorageDeleteLayoutIds());

        SecondStorageCleanJobBuildParams params = new SecondStorageCleanJobBuildParams(null, jobParam, null);
        params.setSecondStorageDeleteLayoutIds(jobParam.getSecondStorageDeleteLayoutIds());
        assertNull(params.getSecondStorageDeleteLayoutIds());
    }

    @Test
    public void testSegmentLoadWithRetry() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testSegmentLoadWithRetry(1, clickhouse1);
        }
    }

    @Test
    public void testModelCleanJobWithAddColumn() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1 }, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());

                long jobCnt = getNExecutableManager().getAllExecutables().stream()
                        .filter(ClickHouseModelCleanJob.class::isInstance).count();

                ModelRequest request = getChangedModelRequest("TRANS_ID");
                EnvelopeResponse<BuildBaseIndexResponse> res1 = nModelController.updateSemantic(request);
                assertEquals("000", res1.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream()
                        .filter(ClickHouseModelCleanJob.class::isInstance).count());

                request = getChangedModelRequest("LEAF_CATEG_ID");
                request.setWithSecondStorage(false);
                EnvelopeResponse<BuildBaseIndexResponse> res2 = nModelController.updateSemantic(request);
                assertEquals("000", res2.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream()
                        .filter(ClickHouseModelCleanJob.class::isInstance).count());

                return null;
            });
        }
    }

    @Test
    public void testModelCleanJobWithChangePartition() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1 }, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());

                val partitionDesc = getNDataModel().getPartitionDesc();
                partitionDesc.setPartitionDateFormat("yyyy-MM-dd");

                long jobCnt = getNExecutableManager().getAllExecutables().stream()
                        .filter(ClickHouseModelCleanJob.class::isInstance).count();

                getNDataModelManager().updateDataModel(modelId,
                        copier -> copier.setManagementType(ManagementType.MODEL_BASED));

                ModelRequest request = getChangedModelRequestWithNoPartition("TRANS_ID");
                EnvelopeResponse<BuildBaseIndexResponse> res1 = nModelController.updateSemantic(request);
                assertEquals("000", res1.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream()
                        .filter(ClickHouseModelCleanJob.class::isInstance).count());

                request = getChangedModelRequestWithPartition("LEAF_CATEG_ID", partitionDesc);
                EnvelopeResponse<BuildBaseIndexResponse> res2 = nModelController.updateSemantic(request);
                assertEquals("000", res2.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream()
                        .filter(ClickHouseModelCleanJob.class::isInstance).count());

                request = getChangedModelRequestWithNoPartition("TEST_COUNT_DISTINCT_BITMAP");
                request.setWithSecondStorage(false);
                EnvelopeResponse<BuildBaseIndexResponse> res3 = nModelController.updateSemantic(request);
                assertEquals("000", res3.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream()
                        .filter(ClickHouseModelCleanJob.class::isInstance).count());

                return null;
            });
        }
    }

    @Test
    public void testDropModelWithSecondStorage() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1 }, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                Assert.assertTrue(getIndexPlan().containBaseTableLayout());
                ModelRequest request = new ModelRequest();
                request.setWithSecondStorage(true);
                request.setUuid(modelId);
                BuildBaseIndexResponse changedResponse = mock(BuildBaseIndexResponse.class);
                Mockito.doCallRealMethod().when(modelService).changeSecondStorageIfNeeded(eq("default"), eq(request),
                        eq(() -> true));
                when(changedResponse.hasTableIndexChange()).thenReturn(true);

                modelService.dropModel(modelId, getProject());

                val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(),
                        getProject());
                val tableFlow = tableFlowManager.get().get(modelId);
                Assert.assertFalse(tableFlow.isPresent());

                return null;
            });
        }
    }

    @Test
    public void testDeleteIndexWithSegmentNotDelete() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val dataflow = dataflowManager.getDataflow(modelId);
        val oldLayouts = dataflow.getFirstSegment().getSegDetails().getLayouts();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager manager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            manager.updateIndexPlan(modelId, copy -> copy.removeLayouts(Sets.newHashSet(1L), true, true));
            return null;
        }, getProject());

        val segment = dataflow.copy().getFirstSegment();
        segment.getSegDetails().setLayouts(oldLayouts);
        val segmentResponse = new NDataSegmentResponse(dataflow, segment);
        assertTrue(segmentResponse.isHasBaseTableIndexData());
    }

    @Test
    public void changeSecondStorageIfNeeded() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1 }, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                Assert.assertTrue(getIndexPlan().containBaseTableLayout());
                ModelRequest request = new ModelRequest();
                request.setWithSecondStorage(false);
                request.setUuid(modelId);

                modelService.changeSecondStorageIfNeeded(getProject(), request, () -> true);
                Assert.assertFalse(SecondStorageUtil.isModelEnable(getProject(), modelId));

                request.setWithSecondStorage(true);
                modelService.changeSecondStorageIfNeeded(getProject(), request, () -> true);
                Assert.assertTrue(SecondStorageUtil.isModelEnable(getProject(), modelId));

                request.setWithSecondStorage(true);
                modelService.changeSecondStorageIfNeeded(getProject(), request, () -> true);
                Assert.assertTrue(SecondStorageUtil.isModelEnable(getProject(), modelId));

                request.setWithSecondStorage(false);
                modelService.changeSecondStorageIfNeeded(getProject(), request, () -> true);
                Assert.assertFalse(SecondStorageUtil.isModelEnable(getProject(), modelId));

                return null;
            });
        }
    }

    @Test
    public void testPurgeModelWithSecondStorage() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1 }, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                Assert.assertTrue(getIndexPlan().containBaseTableLayout());

                modelService.purgeModel(modelId, getProject());

                val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(),
                        getProject());
                val tableFlow = tableFlowManager.get().get(modelId);
                Assert.assertFalse(tableFlow.isPresent());
                return null;
            });
        }
    }

    @Test
    public void testSecondStorageMetricsTwoShardTwoReplica() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            testSecondStorageMetricsWithRetry(2, clickhouse1, clickhouse2, clickhouse3, clickhouse4);
        }
    }

    @Test
    public void testSecondStorageMetricsWithEmpty() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testSecondStorageMetricsWithEmpty(1, clickhouse1);
        }
    }

    @Test
    public void testIncrementalCleanProject() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse }, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse.getJdbcUrl(), clickhouse.getDriverClassName());

                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                val request = new ProjectEnableRequest();
                request.setProject(getProject().toUpperCase(Locale.ROOT));
                request.setEnabled(false);
                request.setNewNodes(null);
                val jobInfo = secondStorageEndpoint.enableProjectStorage(request);
                Assert.assertEquals(1, jobInfo.getData().getJobs().size());
                waitAllJobFinish();
                val manager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), getProject());
                val nodeGroupManager = SecondStorageUtil.nodeGroupManager(KylinConfig.getInstanceFromEnv(),
                        getProject());
                assertTrue(manager.isPresent());
                assertTrue(nodeGroupManager.isPresent());
                Assert.assertEquals(0, manager.get().listAll().size());
                Assert.assertEquals(0, nodeGroupManager.get().listAll().size());
                return true;
            });
        }
    }

    @Test
    public void testSecondStorageMetricsEndpoint() throws Exception {
        EnvelopeResponse<Map<String, Long>> r1 = nQueryController.queryHistoryTiredStorageMetrics(getProject(), "12 3");
        assertEquals(0L, r1.getData().get("total_scan_count").longValue());
        assertEquals(0L, r1.getData().get("total_scan_bytes").longValue());
        assertEquals(0L, r1.getData().get("source_result_count").longValue());

        EnvelopeResponse<Map<String, Long>> r2 = nQueryController.queryHistoryTiredStorageMetrics(getProject(), "123");
        assertEquals(0L, r2.getData().get("total_scan_count").longValue());
        assertEquals(0L, r2.getData().get("total_scan_bytes").longValue());
        assertEquals(0L, r2.getData().get("source_result_count").longValue());

        val dao = RDBMSQueryHistoryDAO.getInstance();
        QueryMetrics queryMetrics = RDBMSQueryHistoryDaoTest.createQueryMetrics(1580311512000L, 1L, true, getProject(),
                true);
        dao.insert(queryMetrics);
        EnvelopeResponse<Map<String, Long>> r3 = nQueryController.queryHistoryTiredStorageMetrics(getProject(),
                queryMetrics.getQueryId());
        assertEquals(queryMetrics.getTotalScanCount(), r3.getData().get("total_scan_count").longValue());
        assertEquals(queryMetrics.getTotalScanBytes(), r3.getData().get("total_scan_bytes").longValue());
        assertEquals(queryMetrics.getQueryHistoryInfo().getSourceResultCount(),
                r3.getData().get("source_result_count").longValue());
    }

    public void testSecondStorageQueryRandom() throws Exception {
        List<Set<String>> shardNames = new ArrayList<>();
        shardNames.add(ImmutableSet.of("node00", "node01"));
        Map<String, AtomicInteger> cnt = new HashMap<>();

        IntStream.range(0, 100).forEach(i -> {
            val result = SecondStorageNodeHelper.resolveShardToJDBC(shardNames, QueryContext.current());
            result.forEach(r -> cnt.computeIfAbsent(r, v -> new AtomicInteger()).incrementAndGet());
        });
        assertEquals(2, cnt.keySet().size());
    }

    private void testSecondStorageMetricsWithRetry(int replica, JdbcDatabaseContainer<?>... clickhouse)
            throws Exception {
        final String catalog = "default";
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            val r1 = secondStorageService.getQueryMetric(getProject(), "");
            assertEquals(-1L, r1.get(QueryMetrics.TOTAL_SCAN_COUNT));

            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            val r2 = secondStorageService.getQueryMetric(getProject(), "");
            assertEquals(-1L, r2.get(QueryMetrics.TOTAL_SCAN_COUNT));

            Map<String, Map<String, Boolean>> nodeStatusMap = ImmutableMap.of("pair0",
                    ImmutableMap.of("node00", false, "node02", false));
            secondStorageEndpoint.updateNodeStatus(nodeStatusMap);

            QueryHistoryScheduler q1 = new QueryHistoryScheduler();
            val rm1 = new QueryMetrics.RealizationMetrics();
            rm1.setSecondStorage(true);

            val h1 = new QueryHistoryInfo();
            h1.setRealizationMetrics(ImmutableList.of(rm1));
            val m1 = new QueryMetrics("123");
            m1.setProjectName(getProject());

            m1.setQueryHistoryInfo(h1);

            System.setProperty("kylin.second-storage.query-metric-collect", "false");
            q1.init();
            q1.collectSecondStorageMetric(ImmutableList.of(m1));
            assertEquals(0L, m1.getTotalScanCount());
            assertEquals(0L, m1.getTotalScanBytes());
            assertEquals(0L, m1.getQueryHistoryInfo().getSourceResultCount());

            EnvelopeResponse<String> ers = nAdminController.getPublicConfig();
            assertEquals("000", ers.getCode());
            assertTrue(ers.getData().contains("kylin.second-storage.query-metric-collect"));

            System.setProperty("kylin.second-storage.query-metric-collect", "true");
            q1.init();
            q1.collectSecondStorageMetric(ImmutableList.of(m1));

            assertEquals(-1L, m1.getTotalScanCount());
            assertEquals(-1L, m1.getTotalScanBytes());
            assertEquals(-1L, m1.getQueryHistoryInfo().getSourceResultCount());

            nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true, "node02", true));
            secondStorageEndpoint.updateNodeStatus(nodeStatusMap);

            // Test
            // Step1: create model and load to second storage
            val layout01 = updateIndex("TRANS_ID");

            buildIncrementalLoadQuery("2012-01-01", "2012-01-02"); // build table index
            checkHttpServer(); // check http server
            triggerClickHouseJob(getDataFlow()); //load into clickhouse

            String sql1 = "select TRANS_ID from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-02'";
            assertQueryResult(sql1, layout01);
            String queryId = QueryContext.current().getQueryId();
            QueryHistoryScheduler queryHistoryScheduler = new QueryHistoryScheduler();
            queryHistoryScheduler.init();
            val realizationMetrics = new QueryMetrics.RealizationMetrics();
            realizationMetrics.setSecondStorage(true);

            val history = new QueryHistoryInfo();
            history.setRealizationMetrics(ImmutableList.of(realizationMetrics));
            val metric = new QueryMetrics(queryId);
            metric.setProjectName(getProject());

            metric.setQueryHistoryInfo(history);
            queryHistoryScheduler.collectSecondStorageMetric(ImmutableList.of(metric));

            long scanRows = metric.getTotalScanCount();
            long scanBytes = metric.getTotalScanBytes();
            long resultRows = metric.getQueryHistoryInfo().getSourceResultCount();

            String metricSql = "SELECT\n" + "    sum(read_rows) AS readRows,\n" + "    sum(read_bytes) AS readBytes,\n"
                    + "    client_name AS clientName,\n" + "    sum(result_rows) AS resultRows\n"
                    + "FROM system.query_log\n" + "PREWHERE type = 'QueryFinish'\n"
                    + "WHERE (type = 'QueryFinish') AND (event_time >= addHours(now(), -1)) AND (event_date >= addDays(now(), -1)) AND (position(client_name, '%s') = 1)\n"
                    + "GROUP BY client_name order by client_name desc";

            Map<String, Long> chRowsMetric = new HashMap<>(2);
            Map<String, Long> chBytesMetric = new HashMap<>(2);
            Map<String, Long> chResultMetric = new HashMap<>(2);

            for (JdbcDatabaseContainer jdbcDatabaseContainer : clickhouse) {
                try (Connection connection = DriverManager.getConnection(jdbcDatabaseContainer.getJdbcUrl());
                        val stmt = connection.createStatement()) {
                    stmt.execute("SYSTEM FLUSH LOGS");
                    val rs = stmt.executeQuery(String.format(Locale.ROOT, metricSql, queryId));

                    if (rs.next()) {
                        long rowsCH = rs.getLong("readRows");
                        long bytesCH = rs.getLong("readBytes");
                        long resultCH = rs.getLong("resultRows");
                        String clientName = rs.getString("clientName");
                        chRowsMetric.computeIfAbsent(clientName, k -> 0L);
                        chBytesMetric.computeIfAbsent(clientName, k -> 0L);
                        chResultMetric.computeIfAbsent(clientName, k -> 0L);

                        chRowsMetric.computeIfPresent(clientName, (k, v) -> v + rowsCH);
                        chBytesMetric.computeIfPresent(clientName, (k, v) -> v + bytesCH);
                        chResultMetric.computeIfPresent(clientName, (k, v) -> v + resultCH);
                    }
                }
            }

            assertEquals(scanRows, chRowsMetric.get(queryId + "_1").longValue());
            assertEquals(scanBytes, chBytesMetric.get(queryId + "_1").longValue());
            assertEquals(resultRows, chResultMetric.get(queryId + "_1").longValue());

            clickhouse[0].stop();
            clickhouse[3].stop();
            queryHistoryScheduler.collectSecondStorageMetric(ImmutableList.of(metric));

            scanRows = metric.getTotalScanCount();
            scanBytes = metric.getTotalScanBytes();
            resultRows = metric.getQueryHistoryInfo().getSourceResultCount();
            assertEquals(-1L, scanRows);
            assertEquals(-1L, scanBytes);
            assertEquals(-1L, resultRows);
            return true;
        });
    }

    private void testSecondStorageMetricsWithEmpty(int replica, JdbcDatabaseContainer<?>... clickhouse)
            throws Exception {
        final String catalog = "default";
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            // Test
            // Step1: create model and load to second storage
            val layout01 = updateIndex("TRANS_ID");

            buildIncrementalLoadQuery("2012-01-01", "2012-01-02"); // build table index
            checkHttpServer(); // check http server
            triggerClickHouseJob(getDataFlow()); //load into clickhouse

            String sql1 = "select TRANS_ID from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-02'";
            assertQueryResult(sql1, layout01);
            String queryId = QueryContext.current().getQueryId();

            for (JdbcDatabaseContainer jdbcDatabaseContainer : clickhouse) {
                try (Connection connection = DriverManager.getConnection(jdbcDatabaseContainer.getJdbcUrl());
                        val stmt = connection.createStatement()) {
                    stmt.execute("SYSTEM FLUSH LOGS");
                    stmt.execute("truncate table system.query_log");
                }
            }

            QueryHistoryScheduler queryHistoryScheduler = new QueryHistoryScheduler();
            queryHistoryScheduler.init();
            val realizationMetrics = new QueryMetrics.RealizationMetrics();
            realizationMetrics.setSecondStorage(true);

            val history = new QueryHistoryInfo();
            history.setRealizationMetrics(ImmutableList.of(realizationMetrics));
            val metric = new QueryMetrics(queryId);
            metric.setProjectName(getProject());

            metric.setQueryHistoryInfo(history);
            queryHistoryScheduler.collectSecondStorageMetric(ImmutableList.of(metric));

            assertEquals(-1, metric.getTotalScanCount());
            assertEquals(-1, metric.getTotalScanBytes());
            return true;
        });
    }

    public void testSegmentLoadWithRetry(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        PowerMockito.mockStatic(InsertInto.class);
        PowerMockito.when(InsertInto.insertInto(Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new SQLException("broken pipe HTTPSession"));

        final String catalog = "default";
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            buildIncrementalLoadQuery("2012-01-01", "2012-01-02"); // build table index
            checkHttpServer(); // check http server

            val segments = new HashSet<>(getDataFlow().getSegments());
            AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
            JobParam jobParam = SecondStorageJobParamUtil.of(getProject(), getDataFlow().getModel().getUuid(), "ADMIN",
                    segments.stream().map(NDataSegment::getId));
            String jobId = ClickHouseUtils.simulateJobMangerAddJob(jobParam, localHandler);
            await().atMost(20, TimeUnit.SECONDS);
            PowerMockito.doCallRealMethod().when(InsertInto.class);
            InsertInto.insertInto(Mockito.anyString(), Mockito.anyString());
            waitJobFinish(getProject(), jobId);
            return true;
        });
    }

    @Test
    public void testDeleteShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {

            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                ProjectNodeRequest request = new ProjectNodeRequest();
                request.setProject("wrong");
                Assert.assertThrows(KylinException.class,
                        () -> this.secondStorageEndpoint.deleteProjectNodes(request, null));
                request.setProject(getProject());
                Assert.assertThrows(KylinException.class,
                        () -> this.secondStorageEndpoint.deleteProjectNodes(request, null));

                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                getBuildBaseLayout(new HashSet<>(), new HashSet<>(), clickhouse, replica);

                val clickhouseNew = new JdbcDatabaseContainer[] { clickhouse1, clickhouse2 };
                ClickHouseUtils.internalConfigClickHouse(clickhouseNew, replica);
                secondStorageService.changeProjectSecondStorageState(getProject(), ImmutableList.of("pair1"), true);
                assertEquals(clickhouseNew.length, SecondStorageUtil.listProjectNodes(getProject()).size());

                checkSegmentDisplay(replica, clickhouse.length / replica);

                EnvelopeResponse<ProjectTableSyncResponse> response = secondStorageEndpoint.tableSync(getProject());
                assertEquals("000", response.getCode());

                deleteShardParamsCheck(request);

                request.setForce(false);
                val shardNames2 = ImmutableList.of("pair0");
                assertThrows(TransactionException.class,
                        () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames2));

                EnvelopeResponse<List<String>> res1 = this.secondStorageEndpoint.deleteProjectNodes(request,
                        ImmutableList.of("pair1"));
                assertEquals("000", res1.getCode());
                assertTrue(res1.getData().isEmpty());
                checkDeletedStatus(Collections.singletonList("pair0"), Collections.singletonList("pair1"));

                assertFalse(
                        LockTypeEnum.locked(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(getProject())));

                assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeProjectSecondStorageState(getProject(), ImmutableList.of("pair1"), true);
                assertEquals(clickhouseNew.length, SecondStorageUtil.listProjectNodes(getProject()).size());

                EnvelopeResponse<ProjectTableSyncResponse> response2 = secondStorageEndpoint.tableSync(getProject());
                assertEquals("000", response2.getCode());

                secondStorageService.lockOperate(getProject(), Collections.singletonList(LockTypeEnum.LOAD.name()),
                        LockOperateTypeEnum.LOCK.name());

                request.setForce(true);
                List<String> shardNames = ImmutableList.of("pair0");
                EnvelopeResponse<List<String>> res2 = this.secondStorageEndpoint.deleteProjectNodes(request,
                        shardNames);
                assertEquals("000", res2.getCode());
                assertFalse(res2.getData().isEmpty());
                checkDeletedStatus(Collections.singletonList("pair1"), Collections.singletonList("pair0"));
                assertTrue(getTableFlow().getTableDataList().isEmpty());

                assertEquals(1,
                        getNExecutableManager().getAllExecutables().stream()
                                .filter(ClickHouseProjectCleanJob.class::isInstance)
                                .filter(s -> s.getId().equals(res2.getData().get(0))).count());

                assertTrue(
                        LockTypeEnum.locked(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(getProject())));
                secondStorageService.lockOperate(getProject(), Collections.singletonList(LockTypeEnum.LOAD.name()),
                        LockOperateTypeEnum.UNLOCK.name());

                return true;
            });
        }
    }

    @Test
    public void testMultiProcessLoad() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
                buildIncrementalLoadQuery("2012-01-03", "2012-01-04");
                buildIncrementalLoadQuery("2012-01-04", "2012-01-05");
                buildIncrementalLoadQuery("2012-01-05", "2012-01-06");
                buildIncrementalLoadQuery("2012-01-06", "2012-01-07");
                buildIncrementalLoadQuery("2012-01-07", "2012-01-08");
                buildIncrementalLoadQuery("2012-01-08", "2012-01-09");
                buildIncrementalLoadQuery("2012-01-09", "2012-01-10");

                waitAllJobFinish();
                String sql = "select CAL_DT from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-08'";
                OLAPContext.clearThreadLocalContexts();
                QueryContext.current().close();
                QueryContext.current().setRetrySecondStorage(true);
                val result1 = ExecAndComp.queryModel(getProject(), sql).count();

                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                String jobId = triggerClickHouseLoadJob(getProject(), modelId, "ADMIN",
                        getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));

                SecondStorageConcurrentTestUtil.registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT,
                        5000);
                clickhouse1.stop();

                waitJobEnd(getProject(), jobId);
                assertEquals(ExecutableState.ERROR,
                        NExecutableManager.getInstance(getConfig(), getProject()).getJob(jobId).getStatus());
                clickhouse1.start();
                ClickHouseUtils.internalConfigClickHouse(new JdbcDatabaseContainer[] { clickhouse1 }, 1);

                SecondStorageConcurrentTestUtil.registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_PAUSED, 5000);
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            getProject());
                    executableManager.resumeJob(jobId);
                    return null;
                }, getProject(), 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId);

                await().atMost(30, TimeUnit.SECONDS)
                        .until(() -> NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                                .getJob(jobId).getStatus() == ExecutableState.RUNNING);
                await().pollDelay(5, TimeUnit.SECONDS).until(() -> true);
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            getProject());
                    executableManager.pauseJob(jobId);
                    return null;
                }, getProject(), 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
                waitJobEnd(getProject(), jobId);
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                            getProject());
                    executableManager.resumeJob(jobId);
                    return null;
                }, getProject(), 1, UnitOfWork.DEFAULT_EPOCH_ID, jobId);
                waitAllJobFinish();

                OLAPContext.clearThreadLocalContexts();
                QueryContext.current().close();
                QueryContext.current().setRetrySecondStorage(true);
                val result2 = ExecAndComp.queryModel(getProject(), sql).count();
                assertEquals(result1, result2);
                return true;
            });
        }
    }

    @Test
    public void testChangeScheduleMode() throws Exception {
        ClickHouseLoad load = new ClickHouseLoad();
        LoadContext context = new LoadContext(null);
        context.deserializeToString("{}");
        List<DataLoader> dataLoaders = new ArrayList<>();
        load.checkResumeFileCorrect(dataLoaders, context);

        context.finishSingleFile(new LoadContext.CompletedFileKeyUtil("test", 20000010001L), "file1");
        context.deserializeToString(context.serializeToString());
        load.checkResumeFileCorrect(dataLoaders, context);
        DataLoader dataLoader = mock(DataLoader.class);
        ClickhouseLoadFileLoad fileLoader = Mockito.mock(ClickhouseLoadFileLoad.class);
        Mockito.when(dataLoader.getSingleFileLoaderPerNode())
                .thenReturn(ImmutableMap.of("node", Collections.singletonList(fileLoader)));
        dataLoaders.add(dataLoader);
        assertThrows(IllegalStateException.class, () -> load.checkResumeFileCorrect(dataLoaders, context));

        Mockito.when(fileLoader.getParquetFile()).thenReturn("file1");
        load.checkResumeFileCorrect(dataLoaders, context);
    }

    @Test
    public void testSizeInNode() throws Exception {
        SecondStorageMetadataRequest request = new SecondStorageMetadataRequest();
        request.setProject("");
        Assert.assertThrows(MsgPicker.getMsg().getEmptyProjectName(), KylinException.class,
                () -> this.secondStorageEndpoint.sizeInNode(request));
        request.setProject("123");
        Assert.assertThrows("123", KylinException.class, () -> this.secondStorageEndpoint.sizeInNode(request));
        request.setProject(getProject());
        Assert.assertThrows(
                String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectEnabled(), getProject()),
                KylinException.class, () -> this.secondStorageEndpoint.sizeInNode(request));

        Assert.assertThrows(MsgPicker.getMsg().getEmptyProjectName(), KylinException.class,
                () -> this.secondStorageEndpoint.tableSync(""));
        Assert.assertThrows("123", KylinException.class, () -> this.secondStorageEndpoint.tableSync("123"));
        String project = getProject();
        Assert.assertThrows(
                String.format(Locale.ROOT, MsgPicker.getMsg().getSecondStorageProjectEnabled(), getProject()),
                KylinException.class, () -> this.secondStorageEndpoint.tableSync(project));
    }

    @Test
    public void testDeleteShardHA() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {

            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1, clickhouse2, clickhouse3, clickhouse4 };
            int replica = 2;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                getBuildBaseLayout(new HashSet<>(), new HashSet<>(), clickhouse, replica);
                checkSegmentDisplay(replica, clickhouse.length / replica);

                ProjectNodeRequest request = new ProjectNodeRequest();
                request.setProject(getProject());
                request.setForce(true);
                EnvelopeResponse<List<String>> res2 = this.secondStorageEndpoint.deleteProjectNodes(request,
                        ImmutableList.of("pair1"));
                assertEquals("000", res2.getCode());
                assertFalse(res2.getData().isEmpty());
                checkDeletedStatus(Collections.singletonList("pair0"), Collections.singletonList("pair1"));
                assertTrue(getTableFlow().getTableDataList().isEmpty());

                assertFalse(
                        LockTypeEnum.locked(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(getProject())));
                assertEquals(1,
                        getNExecutableManager().getAllExecutables().stream()
                                .filter(ClickHouseProjectCleanJob.class::isInstance)
                                .filter(s -> s.getId().equals(res2.getData().get(0))).count());
                return true;
            });
        }
    }

    private void deleteShardParamsCheck(ProjectNodeRequest request) {
        request.setProject(getProject());
        Assert.assertThrows(KylinException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, null));

        List<String> shardNames1 = Collections.emptyList();
        Assert.assertThrows(KylinException.class,
                () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames1));

        List<String> shardNames2 = ImmutableList.of("test");
        Assert.assertThrows(KylinException.class,
                () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames2));

        List<String> shardNames3 = ImmutableList.of("pair0", "test");
        Assert.assertThrows(KylinException.class,
                () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames3));

        List<String> shardNames4 = ImmutableList.of("pair0", "pair1");
        Assert.assertThrows(KylinException.class,
                () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames4));

        List<String> shardNames5 = ImmutableList.of("pair0");
        Assert.assertThrows(TransactionException.class,
                () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames5));
    }

    private void checkDeletedStatus(List<String> shards, List<String> deletedShards) {
        List<NodeGroup> nodeGroups = getNodeGroups();
        Map<String, List<Node>> clusters = ClickHouseConfigLoader.getInstance().getCluster().getCluster();
        assertEquals(clusters.get(shards.get(0)).size(), nodeGroups.size());
        assertEquals(shards.size(), nodeGroups.get(0).getNodeNames().size());

        shards.forEach(
                shard -> nodeGroups
                        .forEach(
                                nodeGroup -> assertFalse(
                                        CollectionUtils
                                                .intersection(clusters.get(shard).stream().map(Node::getName)
                                                        .collect(Collectors.toList()), nodeGroup.getNodeNames())
                                                .isEmpty())));

        deletedShards
                .forEach(
                        shard -> nodeGroups
                                .forEach(
                                        nodeGroup -> assertTrue(CollectionUtils
                                                .intersection(clusters.get(shard).stream().map(Node::getName)
                                                        .collect(Collectors.toList()), nodeGroup.getNodeNames())
                                                .isEmpty())));

        getTableFlow().getTableDataList().forEach(tableData -> tableData.getPartitions().forEach(p -> {
            assertEquals(shards.size(), p.getShardNodes().size());

            shards.stream().flatMap(shard -> clusters.get(shard).stream()).map(Node::getName).forEach(nodeName -> {
                assertTrue(p.getSizeInNode().containsKey(nodeName));
                assertTrue(p.getNodeFileMap().containsKey(nodeName));
            });

            deletedShards.stream().flatMap(shard -> clusters.get(shard).stream()).map(Node::getName)
                    .forEach(nodeName -> {
                        assertFalse(p.getSizeInNode().containsKey(nodeName));
                        assertFalse(p.getNodeFileMap().containsKey(nodeName));
                    });
        }));
    }

    @Test
    public void testProjectSecondStorageJobs() {
        try {
            secondStorageEndpoint.getProjectSecondStorageJobs("error");
        } catch (KylinException e) {
            assertEquals(SECOND_STORAGE_PROJECT_STATUS_ERROR.toErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testProjectLoadWithOneNodeDown() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {

            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                getBuildBaseLayout(new HashSet<>(), new HashSet<>(), clickhouse, replica);

                Map<String, Map<String, Boolean>> nodeStatusMap = ImmutableMap.of("pair0",
                        ImmutableMap.of("node00", false));
                secondStorageEndpoint.updateNodeStatus(nodeStatusMap);

                ProjectLoadRequest request = new ProjectLoadRequest();
                request.setProjects(ImmutableList.of(getProject()));
                EnvelopeResponse<List<ProjectRecoveryResponse>> response = this.secondStorageEndpoint
                        .projectLoad(request);
                assertEquals("000", response.getCode());
                waitAllJobFinish();

                getTableFlow().getTableDataList().forEach(tableData -> tableData.getPartitions()
                        .forEach(p -> assertNotEquals(0, (long) p.getSizeInNode().getOrDefault("node00", 0L))));

                nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true));
                secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                return true;
            });
        }
    }

    public void testSegmentLoadWithoutRetry() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testSegmentLoadWithoutRetry(1, clickhouse1);
        }
    }

    public void testSegmentLoadWithoutRetry(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        PowerMockito.mockStatic(InsertInto.class);
        PowerMockito.when(InsertInto.insertInto(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer((Answer<InsertInto>) invocation -> new InsertInto(TableIdentifier.table("def", "tab")));

        final String catalog = "default";
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                    true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            buildIncrementalLoadQuery("2012-01-01", "2012-01-02"); // build table index
            checkHttpServer(); // check http server

            val segments = new HashSet<>(getDataFlow().getSegments());
            AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
            JobParam jobParam = SecondStorageJobParamUtil.of(getProject(), getDataFlow().getModel().getUuid(), "ADMIN",
                    segments.stream().map(NDataSegment::getId));
            String jobId = ClickHouseUtils.simulateJobMangerAddJob(jobParam, localHandler);
            await().atMost(20, TimeUnit.SECONDS);
            PowerMockito.doCallRealMethod().when(InsertInto.class);
            InsertInto.insertInto(Mockito.anyString(), Mockito.anyString());
            waitJobEnd(getProject(), jobId);
            return true;
        });
    }

    private void testIncrementBuildLockedLayout(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        final String catalog = "default";

        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
            waitAllJobFinish();
            checkSecondStorageLayoutStatus(Collections.emptySet());

            val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            val dataflow = dataflowManager.getDataflow(modelId);
            String sid1 = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).findAny().get();

            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());
            assertTrue(SecondStorageUtil.listEnableLayoutBySegment(getProject(), modelId, "123").isEmpty());
            assertTrue(SecondStorageUtil.listEnableLayoutBySegment(getProject(), modelId, sid1).isEmpty());

            checkSecondStorageLayoutNullWithStatus();

            triggerClickHouseJob(dataflow);
            assertFalse(SecondStorageUtil.listEnableLayoutBySegment(getProject(), modelId, sid1).isEmpty());

            AtomicInteger indexDeleteJobCnt = new AtomicInteger(0);
            Set<Long> existTablePlanLayoutIds = new HashSet<>();
            Set<Long> existTableDataLayoutIds = new HashSet<>();
            checkSecondStorageLayoutStatus(existTableDataLayoutIds);

            // Test
            // Step1: create model and load to second storage
            val layout01 = getBuildBaseLayout(existTablePlanLayoutIds, existTableDataLayoutIds, clickhouse, replica);

            {
                List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelId, getProject(), "0", "" + (Long.MAX_VALUE - 1), null, null, null, false, null, false, null, null);
                Assert.assertEquals(3, segments.size());
                Assert.assertEquals(SegmentStatusEnumToDisplay.ONLINE, segments.get(0).getStatusToDisplay());
                Assert.assertEquals(SegmentSecondStorageStatusEnum.LOADED, segments.get(0).getStatusSecondStorageToDisplay());
            }

            {
                List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelId, getProject(), "0",
                        "" + (Long.MAX_VALUE - 1), null, null, null, false, null, false, Lists.newArrayList("ONLINE"),
                        Lists.newArrayList("LOADED"));
                Assert.assertEquals(3, segments.size());
                Assert.assertEquals(SegmentStatusEnumToDisplay.ONLINE, segments.get(0).getStatusToDisplay());
                Assert.assertEquals(SegmentSecondStorageStatusEnum.LOADED,
                        segments.get(0).getStatusSecondStorageToDisplay());
            }

            {
                List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelId, getProject(), "0",
                        "" + (Long.MAX_VALUE - 1), null, null, null, false, null, false, Lists.newArrayList("LOADING"),
                        Lists.newArrayList("LOADED"));
                Assert.assertEquals(0, segments.size());
            }

            checkSecondStorageLayoutStatus(existTableDataLayoutIds);

            // Step2: change model and removed locked index
            val layout02 = updateIndex("LSTG_SITE_ID");

            buildSegmentAndLoadCH(layout02); // build new index segment
            existTablePlanLayoutIds.add(layout02);
            existTableDataLayoutIds.add(layout02);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout01);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout02);
            checkSegmentDisplay(replica, clickhouse.length / replica);

            // removed old index
            indexPlanService.removeIndexes(getProject(), modelId, ImmutableSet.of(layout01));
            waitAllJobFinish();
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream()
                    .filter(ClickHouseIndexCleanJob.class::isInstance).count());

            existTablePlanLayoutIds.remove(layout01);
            existTableDataLayoutIds.remove(layout01);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout02);

            // check load second storage button is enable
            assertTrue(checkNDataSegmentResponse());

            // Step3: change model and test remove segment of locked index
            val layout03 = updateIndex("IS_EFFECTUAL"); // update model to new
            existTablePlanLayoutIds.add(layout03);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout02);

            buildSegmentAndLoadCH(layout03); // build new index segment
            existTableDataLayoutIds.add(layout03);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout02);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout03);

            NDataSegment segmentTmp01 = getDataFlow().getSegments().getFirstSegment();
            removeIndexesFromSegments(segmentTmp01.getId(), layout02);
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream()
                    .filter(ClickHouseIndexCleanJob.class::isInstance).count());

            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(
                    getDataFlow().getSegments().stream().map(NDataSegment::getId)
                            .filter(segmentId -> !segmentId.equals(segmentTmp01.getId())).collect(Collectors.toSet()),
                    layout02);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout03);

            // test remove all segment of locked index
            removeIndexesFromSegments(
                    getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()),
                    layout02);
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream()
                    .filter(ClickHouseIndexCleanJob.class::isInstance).count());
            existTablePlanLayoutIds.remove(layout02);
            existTableDataLayoutIds.remove(layout02);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout03);

            // Step4: change model and test refresh segment
            long layout04 = testRefreshSegment(existTablePlanLayoutIds, existTableDataLayoutIds, layout03);
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream()
                    .filter(ClickHouseIndexCleanJob.class::isInstance).count());

            long layout05 = testCleanSegment(existTablePlanLayoutIds, existTableDataLayoutIds, layout04);
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream()
                    .filter(ClickHouseIndexCleanJob.class::isInstance).count());

            // test merge
            testMerge(existTablePlanLayoutIds, existTableDataLayoutIds, layout04, layout05);

            // Step7: test build new segment and load all to second storage
            Set<String> layout04Segments = getAllSegmentIds();
            buildIncrementalLoadQuery("2012-01-04", "2012-01-05",
                    ImmutableSet.of(getIndexPlan().getLayoutEntity(layout05)));
            triggerClickHouseJob(getDataFlow());

            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(layout04Segments, layout04);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout05);

            // Step8: close second storage
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, false);
            return true;
        });
    }

    private void checkSecondStorageLayoutStatus(Set<Long> existTableDataLayoutIds) {
        if (!SegmentOnlineMode.ANY.toString().equalsIgnoreCase(NProjectManager.getInstance(getConfig())
                .getProject(getProject()).getConfig().getKylinEngineSegmentOnlineMode())) {
            return;
        }
        Map<Long, Boolean> exist = indexPlanService.getSecondStorageLayoutStatus(getProject(), existTableDataLayoutIds,
                getIndexPlan());
        if (existTableDataLayoutIds.isEmpty()) {
            assertTrue(exist.isEmpty());
            assertFalse(indexPlanService.isSecondStorageLayoutReady(getProject(), modelId, 0));
        }
        for (Long existTableDataLayoutId : existTableDataLayoutIds) {
            assertTrue(indexPlanService.isSecondStorageLayoutReady(getProject(), modelId, existTableDataLayoutId));
            assertTrue(exist.get(existTableDataLayoutId));
        }
    }

    private void checkSecondStorageLayoutNullWithStatus() {
        Map<Long, Boolean> exist = indexPlanService.getSecondStorageLayoutStatus(getProject(), ImmutableSet.of(123L),
                getIndexPlan());
        assertTrue(exist.isEmpty());
    }

    private long getBuildBaseLayout(Set<Long> existTablePlanLayoutIds, Set<Long> existTableDataLayoutIds,
            JdbcDatabaseContainer<?>[] clickhouse, int replica) throws Exception {
        // Test
        // Step1: create model and load to second storage
        val layout01 = updateIndex("TRANS_ID");

        buildIncrementalLoadQuery(); // build table index
        checkHttpServer(); // check http server
        triggerClickHouseJob(getDataFlow()); //load into clickhouse
        existTablePlanLayoutIds.add(layout01);
        existTableDataLayoutIds.add(layout01);

        checkSecondStorageBaseMetadata(true, clickhouse.length, replica);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout01);

        String sql1 = "select TRANS_ID from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-02' limit 1";
        assertQueryResult(sql1, layout01);

        return layout01;
    }

    private long testRefreshSegment(Set<Long> existTablePlanLayoutIds, Set<Long> existTableDataLayoutIds,
            long lockedLayoutId) throws IOException, InterruptedException {
        val layout04 = updateIndex("LEAF_CATEG_ID");
        existTablePlanLayoutIds.add(layout04);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);

        buildSegmentAndLoadCH(layout04); // build new index segment
        existTableDataLayoutIds.add(layout04);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout04);

        Set<String> notRefreshedSegments = getAllSegmentIds();

        for (NDataSegment segment : getDataFlow().getSegments()) {
            notRefreshedSegments.remove(segment.getId());
            List<JobInfoResponse.JobInfo> jobInfos = modelBuildService.refreshSegmentById(
                    new RefreshSegmentParams(getProject(), modelId, new String[] { segment.getId() }));

            jobInfos.forEach(j -> waitJobFinish(getProject(), j.getJobId()));

            if (notRefreshedSegments.size() == 0) {
                existTablePlanLayoutIds.remove(lockedLayoutId);
                existTableDataLayoutIds.remove(lockedLayoutId);
            }

            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(notRefreshedSegments, lockedLayoutId);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout04);
        }

        return layout04;
    }

    private long testCleanSegment(Set<Long> existTablePlanLayoutIds, Set<Long> existTableDataLayoutIds,
            long lockedLayoutId) throws IOException, InterruptedException {
        // Step5: change model and test clean second storage segment
        val layout05 = updateIndex("TEST_COUNT_DISTINCT_BITMAP");
        existTablePlanLayoutIds.add(layout05);
        buildSegmentAndLoadCH(layout05); // build new index segment
        existTableDataLayoutIds.add(layout05);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout05);

        // fix ut coverage, it not work
        //            cleanSegments(modelId + "1", null, null); // fix ut coverage, it not work
        getTableFlow().cleanTableData(null); // fix ut coverage, it not work
        getTablePlan().cleanTable(null); // fix ut coverage, it not work

        cleanSegments(getAllSegmentIds(), ImmutableSet.of(lockedLayoutId, layout05));

        triggerClickHouseJob(getDataFlow());
        existTableDataLayoutIds.add(lockedLayoutId);
        existTableDataLayoutIds.add(layout05);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout05);

        return layout05;
    }

    private void testMerge(Set<Long> existTablePlanLayoutIds, Set<Long> existTableDataLayoutIds, long lockedLayoutId,
            long layoutId) {
        // Step6: test merge
        assertEquals(3, getDataFlow().getSegments().size());
        Collections.sort(getDataFlow().getSegments());
        mergeSegment(ImmutableSet.of(getDataFlow().getSegments().get(0).getId(),
                getDataFlow().getSegments().get(1).getId()));
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layoutId);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getTableFlow().update(TableFlow::cleanTableData);
            return null;
        }, getProject(), 1, UnitOfWork.DEFAULT_EPOCH_ID);

        Collections.sort(getDataFlow().getSegments());
        mergeSegment(ImmutableSet.of(getDataFlow().getSegments().get(0).getId(),
                getDataFlow().getSegments().get(1).getId()));
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layoutId);
    }

    public String getProject() {
        return "table_index_incremental";
    }

    private void buildIncrementalLoadQuery() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        buildIncrementalLoadQuery("2012-01-03", "2012-01-04");

        waitAllJobFinish();
    }

    private void buildIncrementalLoadQuery(String start, String end) throws Exception {
        getIndexPlan().getAllLayouts().forEach(layout -> {
            if (!layout.isBaseIndex() || !layout.getIndex().isTableIndex()) {
                indexPlanService.removeIndexes(getProject(), modelId, ImmutableSet.of(layout.getId()));
            }
        });
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

    @SneakyThrows
    private void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        assertTrue(content.length() > 0);
    }

    private void checkSecondStorageBaseMetadata(boolean isIncremental, int clickhouseNodeSize, int replicaSize) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(modelId);
        // check TableFlow
        TablePlan plan = SecondStorage.tablePlanManager(config, getProject()).get(modelId).orElse(null);
        Assert.assertNotNull(plan);
        TableFlow flow = SecondStorage.tableFlowManager(config, getProject()).get(modelId).orElse(null);
        Assert.assertNotNull(flow);

        Set<LayoutEntity> allLayouts = df.getIndexPlan().getAllLayouts().stream()
                .filter(SecondStorageUtil::isBaseTableIndex).collect(Collectors.toSet());
        Assert.assertEquals(allLayouts.size(), flow.getTableDataList().size());

        for (LayoutEntity layoutEntity : allLayouts) {
            TableEntity tableEntity = plan.getEntity(layoutEntity).orElse(null);
            Assert.assertNotNull(tableEntity);
            TableData data = flow.getEntity(layoutEntity).orElse(null);
            Assert.assertNotNull(data);
            Assert.assertEquals(isIncremental ? PartitionType.INCREMENTAL : PartitionType.FULL,
                    data.getPartitionType());
            Assert.assertEquals(dsMgr.getDataflow(modelId).getQueryableSegments().size(),
                    data.getPartitions().size() / replicaSize);
            TablePartition partition = data.getPartitions().get(0);
            int shards = Math.min(clickhouseNodeSize / replicaSize, tableEntity.getShardNumbers());
            Assert.assertEquals(shards, partition.getShardNodes().size());
            Assert.assertEquals(shards, partition.getSizeInNode().size());
            Assert.assertTrue(partition.getSizeInNode().values().stream().reduce(Long::sum).orElse(0L) > 0L);
        }
    }

    private void checkSecondStorageMetadata(Set<Long> tablePlanLayoutIds, Set<Long> tableDataLayoutIds) {
        Set<Long> existTablePlanLayoutId = getTablePlan().getTableMetas().stream().map(TableEntity::getLayoutID)
                .collect(Collectors.toSet());
        assertEquals(existTablePlanLayoutId.size(), tablePlanLayoutIds.size());
        assertTrue(existTablePlanLayoutId.containsAll(tablePlanLayoutIds));

        Set<Long> existTableDataLayoutId = getTableFlow().getTableDataList().stream().map(TableData::getLayoutID)
                .collect(Collectors.toSet());
        assertEquals(existTableDataLayoutId.size(), tableDataLayoutIds.size());
        assertTrue(existTableDataLayoutId.containsAll(tableDataLayoutIds));
    }

    private void checkSecondStorageSegmentMetadata(Set<String> segmentIds, long layoutId) {
        if (segmentIds.isEmpty()) {
            return;
        }

        assertTrue(getTableFlow().getEntity(layoutId).isPresent());
        Set<String> existSegmentIds = getTableFlow().getEntity(layoutId).get().getAllSegments();
        assertEquals(existSegmentIds.size(), segmentIds.size());
        assertTrue(existSegmentIds.containsAll(segmentIds));
    }

    private void mergeSegment(Set<String> segmentIds) {
        JobInfoResponse.JobInfo jobInfo = modelBuildService.mergeSegmentsManually(
                new MergeSegmentParams(getProject(), modelId, segmentIds.toArray(new String[] {})));

        waitJobFinish(getProject(), jobInfo.getJobId());
        assertTrue(SecondStorageUtil.checkMergeFlatTableIsSuccess(getNExecutableManager().getJob(jobInfo.getJobId())));

        try {
            SecondStorageUtil.checkJobRestart(getProject(), jobInfo.getJobId());
        } catch (Exception ignore) {
        }

        JobParam chJob = triggerClickHouseJob(getDataFlow());
        waitAllJobFinish(getProject());
        String project = getProject();
        String jobId = chJob.getJobId();
        assertThrows(KylinException.class, () -> SecondStorageUtil.checkJobRestart(project, jobId));
    }

    private void checkSegmentDisplay(int replica, int shardCnt) {
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelId, getProject(), "0",
                "" + (Long.MAX_VALUE - 1), null, null, null, false, null, false, null, null);
        segments.forEach(segment -> {
            assertEquals(shardCnt, segment.getSecondStorageNodes().size());
            assertNotNull(segment.getSecondStorageNodes().values());
            assertTrue(segment.getSecondStorageNodes().values().stream().findFirst().isPresent());
            assertEquals(replica, segment.getSecondStorageNodes().values().stream().findFirst().get().size());
        });

        val sum = segments.stream().mapToLong(NDataSegmentResponse::getSecondStorageSize).sum();

        ModelQueryParams request = new ModelQueryParams(modelId, null, true, getProject(), null, null, null, 0, 10,
                "last_modify", false, null, null, null, null, true, false);
        DataResult<List<NDataModel>> result = modelService.getModels(request);

        result.getValue().stream().filter(nDataModel -> modelId.equals(nDataModel.getId())).forEach(nDataModel -> {
            val nDataModelRes = (NDataModelResponse) nDataModel;
            assertEquals(sum, nDataModelRes.getSecondStorageSize());
            assertEquals(shardCnt, nDataModelRes.getSecondStorageNodes().size());
            assertEquals(replica, nDataModelRes.getSecondStorageNodes().get("pair0").size());
        });
    }

    @Data
    public static class BuildBaseIndexUT {
        @JsonProperty("base_table_index")
        public IndexInfo tableIndex;

        @Data
        public static class IndexInfo {
            @JsonProperty("layout_id")
            public long layoutId;
        }
    }

    private KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    private TableFlow getTableFlow() {
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(
                SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(modelId).isPresent());
        return SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(modelId).get();
    }

    private TablePlan getTablePlan() {
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(
                SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(modelId).isPresent());
        return SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(modelId).get();
    }

    private IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(getConfig(), getProject()).getIndexPlan(modelId);
    }

    private NDataflow getDataFlow() {
        return NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getDataflow(modelId);
    }

    private NDataModelManager getNDataModelManager() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    private NDataModel getNDataModel() {
        return getNDataModelManager().getDataModelDesc(modelId);
    }

    private NExecutableManager getNExecutableManager() {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    private List<NodeGroup> getNodeGroups() {
        Preconditions.checkState(SecondStorageUtil.nodeGroupManager(getConfig(), getProject()).isPresent());
        return SecondStorageUtil.nodeGroupManager(getConfig(), getProject()).get().listAll();
    }

    private Set<String> getAllSegmentIds() {
        return getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet());
    }

    private String getSourceUrl() {
        return _httpServer.uriAccessedByDocker.toString();
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

    private ModelRequest getChangedModelRequestWithPartition(String columnName, PartitionDesc partitionDesc)
            throws IOException {
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

    private NDataModel.NamedColumn getNamedColumn(ColumnDesc columnDesc) {
        NDataModel.NamedColumn transIdColumn = new NDataModel.NamedColumn();
        transIdColumn.setId(Integer.parseInt(columnDesc.getId()));
        transIdColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        transIdColumn.setName(columnDesc.getTable().getName() + "_" + columnDesc.getName());
        transIdColumn.setAliasDotColumn(columnDesc.getTable().getName() + "." + columnDesc.getName());
        return transIdColumn;
    }

    private void buildSegmentAndLoadCH(long layoutId) throws InterruptedException {
        // build new index segment
        val segments = getDataFlow().getSegments().stream()
                .map(s -> new IndexDataConstructor.BuildInfo(modelId, s,
                        ImmutableSet.of(getDataFlow().getIndexPlan().getLayoutEntity(layoutId)), true, null))
                .collect(Collectors.toList());
        indexDataConstructor.buildSegments(segments);
        waitAllJobFinish();
        triggerClickHouseJob(getDataFlow());
    }

    private long updateIndex(String columnName) throws IOException {
        val indexResponse = modelService.updateDataModelSemantic(getProject(), getChangedModelRequest(columnName));
        val layoutId = JsonUtil.readValue(JsonUtil.writeValueAsString(indexResponse),
                BuildBaseIndexUT.class).tableIndex.layoutId;

        getNExecutableManager().getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
        return layoutId;
    }

    private boolean checkNDataSegmentResponse() {
        NDataSegmentResponse res = new NDataSegmentResponse(getDataFlow(), getDataFlow().getFirstSegment());
        return res.isHasBaseTableIndexData();
    }

    private void removeIndexesFromSegments(String segmentId, long indexId) {
        removeIndexesFromSegments(ImmutableList.of(segmentId), indexId);
    }

    private void removeIndexesFromSegments(List<String> segmentIds, long indexId) {
        modelService.removeIndexesFromSegments(getProject(), modelId, segmentIds, ImmutableList.of(indexId));
        waitAllJobFinish();
    }

    private void setQuerySession(String catalog, String jdbcUrl, String driverClassName) {
        System.setProperty("kylin.query.use-tableindex-answer-non-raw-query", "true");
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".url", jdbcUrl);
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".driver", driverClassName);
    }

    private void assertQueryResult(String sql, long hitLayoutId) throws SQLException {
        OLAPContext.clearThreadLocalContexts();
        QueryContext.current().close();
        QueryContext.current().setRetrySecondStorage(true);
        ExecAndComp.queryModel(getProject(), sql);
        assertTrue(OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));
        assertTrue(OLAPContext.getNativeRealizations().stream().findFirst().isPresent());
        assertEquals(OLAPContext.getNativeRealizations().stream().findFirst().get().getLayoutId().longValue(),
                hitLayoutId);
    }

    public void cleanSegments(Set<String> segments, Set<Long> layoutIds) {
        SecondStorageUtil.cleanSegments(getProject(), modelId, segments, layoutIds);

        val jobHandler = new SecondStorageIndexCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.layoutCleanParam(getProject(), modelId, "ADMIN", layoutIds,
                segments);
        ClickHouseUtils.simulateJobMangerAddJob(param, jobHandler);
        waitAllJobFinish();
    }

    private void waitAllJobFinish() {
        NExecutableManager.getInstance(getConfig(), getProject()).getAllExecutables()
                .forEach(exec -> waitJobFinish(getProject(), exec.getId()));
    }

    private void deleteSegmentById(String segmentId) {
        modelService.deleteSegmentById(modelId, getProject(), new String[] { segmentId }, true);
    }

    private void triggerSegmentLoad(List<String> segments) {
        val request = new StorageRequest();
        request.setProject(getProject());
        request.setModel(modelId);
        request.setSegmentIds(segments);
        EnvelopeResponse<List<String>> jobs = secondStorageEndpoint.getAllSecondStorageJobs();
        assertEquals("000", jobs.getCode());
        assertEquals(0, jobs.getData().size());
        EnvelopeResponse<JobInfoResponse> res = secondStorageEndpoint.loadStorage(request);
        assertEquals("000", res.getCode());
        EnvelopeResponse<List<String>> jobs1 = secondStorageEndpoint.getProjectSecondStorageJobs(getProject());
        assertEquals("000", jobs1.getCode());
        assertEquals(1, jobs1.getData().size());
        for (JobInfoResponse.JobInfo job : res.getData().getJobs()) {
            waitJobFinish(getProject(), job.getJobId());
        }
    }

    private void triggerSegmentClean(List<String> segments) {
        val request = new StorageRequest();
        request.setProject(getProject());
        request.setModel(modelId);
        request.setSegmentIds(segments);
        secondStorageEndpoint.cleanStorage(request, segments);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val job = manager.getAllExecutables().stream().filter(ClickHouseSegmentCleanJob.class::isInstance).findFirst();
        Assert.assertTrue(job.isPresent());
        waitJobEnd(getProject(), job.get().getId());
    }

    @Test
    public void test2Replica2ShardHA() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1, clickhouse2, clickhouse3, clickhouse4 }, 2,
                    catalog, () -> {
                        secondStorageService.changeProjectSecondStorageState(getProject(),
                                SecondStorageNodeHelper.getAllPairs(), true);
                        Assert.assertEquals(4, SecondStorageUtil.listProjectNodes(getProject()).size());
                        secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                        setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());

                        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                        waitAllJobFinish();
                        triggerClickHouseJob(getDataFlow());

                        {
                            long layoutId = getDataFlow().getIndexPlan().getBaseTableLayoutId();
                            List<Integer> rowsList = getHAModelRowCount(getProject(), modelId, layoutId,
                                    new JdbcDatabaseContainer[] { clickhouse1, clickhouse3 });
                            Assert.assertEquals(1, rowsList.stream().distinct().count());
                            rowsList = getHAModelRowCount(getProject(), modelId, layoutId,
                                    new JdbcDatabaseContainer[] { clickhouse2, clickhouse4 });
                            Assert.assertEquals(1, rowsList.stream().distinct().count());
                        }

                        String sql = "select CAL_DT from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-02'";
                        Map<String, Map<String, Boolean>> nodeStatusMap;

                        {
                            // all live
                            clearQueryContext();
                            ExecAndComp.queryModel(getProject(), sql);
                            Assert.assertTrue(OLAPContext.getNativeRealizations().stream()
                                    .allMatch(NativeQueryRealization::isSecondStorage));
                        }

                        {
                            // all group down
                            clearQueryContext();
                            nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false, "node02", false),
                                    "pair1", ImmutableMap.of("node01", false, "node03", false));
                            secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                            ExecAndComp.queryModel(getProject(), sql);
                            Assert.assertTrue(OLAPContext.getNativeRealizations().stream()
                                    .noneMatch(NativeQueryRealization::isSecondStorage));
                        }

                        {
                            // different group down
                            clearQueryContext();
                            nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false, "node02", true),
                                    "pair1", ImmutableMap.of("node01", false, "node03", true));
                            secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                            clearQueryContext();
                            ExecAndComp.queryModel(getProject(), sql);
                            Assert.assertTrue(OLAPContext.getNativeRealizations().stream()
                                    .allMatch(NativeQueryRealization::isSecondStorage));
                        }

                        {
                            // same group down
                            clearQueryContext();
                            nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true, "node02", false),
                                    "pair1", ImmutableMap.of("node01", false, "node03", true));
                            secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                            ExecAndComp.queryModel(getProject(), sql);
                            Assert.assertTrue(OLAPContext.getNativeRealizations().stream()
                                    .anyMatch(NativeQueryRealization::isSecondStorage));
                        }

                        {
                            // down one shard
                            clearQueryContext();
                            nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false, "node02", false),
                                    "pair1", ImmutableMap.of("node01", true, "node03", true));
                            secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                            ExecAndComp.queryModel(getProject(), sql);
                            Assert.assertTrue(OLAPContext.getNativeRealizations().stream()
                                    .noneMatch(NativeQueryRealization::isSecondStorage));

                        }

                        {
                            nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false, "node02", true),
                                    "pair1", ImmutableMap.of("node01", true, "node03", true));
                            secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                            assertEquals("000", secondStorageEndpoint.tableSync(getProject()).getCode());
                            getTableFlow().getTableDataList()
                                    .forEach(tableData -> tableData.getPartitions().forEach(partition -> {
                                        assertEquals(2, partition.getShardNodes().size());
                                        if (partition.getShardNodes().contains("node00")) {
                                            assertEquals(0L, partition.getSizeInNode().get("node00").longValue());
                                        }
                                    }));
                        }

                        {
                            val lock1 = new ProjectLockOperateRequest();
                            lock1.setProject(getProject());
                            lock1.setLockTypes(Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()));
                            lock1.setOperateType(LockOperateTypeEnum.LOCK.name());
                            ClickHouseSimpleITTestUtils.checkLockOperateResult(secondStorageEndpoint.lockOperate(lock1),
                                    Arrays.asList(LockTypeEnum.LOAD.name(), LockTypeEnum.QUERY.name()), getProject());

                            val lock2 = new ProjectLockOperateRequest();
                            lock2.setProject(getProject());
                            lock2.setLockTypes(Collections.singletonList(LockTypeEnum.LOAD.name()));
                            lock2.setOperateType(LockOperateTypeEnum.UNLOCK.name());
                            ClickHouseSimpleITTestUtils.checkLockOperateResult(secondStorageEndpoint.lockOperate(lock2),
                                    Collections.singletonList(LockTypeEnum.QUERY.name()), getProject());

                            secondStorageService.sizeInNode(getProject());

                            val lock3 = new ProjectLockOperateRequest();
                            lock3.setProject(getProject());
                            lock3.setLockTypes(Collections.singletonList(LockTypeEnum.QUERY.name()));
                            lock3.setOperateType(LockOperateTypeEnum.UNLOCK.name());
                            ClickHouseSimpleITTestUtils.checkLockOperateResult(secondStorageEndpoint.lockOperate(lock3),
                                    Collections.emptyList(), getProject());
                        }

                        {
                            // test recover model
                            val request = new RecoverRequest();
                            request.setProject(getProject());
                            val response = openSecondStorageEndpoint.recoverProject(request);
                            Assert.assertEquals(1, response.getData().getSubmittedModels().size());
                            Assert.assertEquals(0, response.getData().getFailedModels().size());
                        }

                        //reset status
                        nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true, "node02", true),
                                "pair1", ImmutableMap.of("node01", true, "node03", true));
                        secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                        return true;
                    });
        }
    }

    @Test
    public void test1Replica2ShardHA() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val container = new JdbcDatabaseContainer[] { clickhouse1, clickhouse2 };
            int replica = 1;
            configClickhouseWith(container, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(2, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());

                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                waitAllJobFinish();
                triggerClickHouseJob(getDataFlow());

                String sql = "select CAL_DT from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-02'";
                Map<String, Map<String, Boolean>> nodeStatusMap;

                testForceToTSAndChDown(sql, container, replica);
                        
                {
                    // testGroupNodeDownForceToTierStorageOK
                    clearQueryContext();
                    nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false), "pair1",
                            ImmutableMap.of("node01", false));
                    secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                    ExecAndComp.queryModel(getProject(), sql, null);
                    Assert.assertFalse(OLAPContext.getNativeRealizations().stream()
                            .allMatch(NativeQueryRealization::isSecondStorage));
                }

                {
                    // testForceToTierStoragePushDown
                    clearQueryContext();
                    nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false), "pair1",
                            ImmutableMap.of("node01", false));
                    secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                    QueryContext queryContext = QueryContext.current();
                    queryContext.setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN);
                    queryContext.setForceTableIndex(false);
                    assertThrows(SQLException.class, () -> ExecAndComp.queryModel(getProject(), sql));
                }

                {
                    // testForceToTierStorageInvalidParameters
                    clearQueryContext();
                    nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false), "pair1",
                            ImmutableMap.of("node01", false));
                    secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                    QueryContext queryContext = QueryContext.current();
                    queryContext.setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN);
                    queryContext.setForceTableIndex(true);
                    assertThrows(SQLException.class, () -> ExecAndComp.queryModel(getProject(), sql));
                }

                {
                    // testeDownForceToTierStorageReturnError
                    clearQueryContext();
                    nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false), "pair1",
                            ImmutableMap.of("node01", false));
                    secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                    QueryContext queryContext = QueryContext.current();
                    queryContext.setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_RETURN);
                    assertThrows(SQLException.class, () -> ExecAndComp.queryModel(getProject(), sql));
                }

                {
                    // testForceToTierStorageOtherValue
                    clearQueryContext();
                    nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false), "pair1",
                            ImmutableMap.of("node01", false));
                    secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                    QueryContext queryContext = QueryContext.current();
                    queryContext.setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TAIL);
                    assertThrows(SQLException.class, () -> ExecAndComp.queryModel(getProject(), sql));
                }

                {
                    // testReverseForceToTierStorageWhenCHDataNotComplete
                    nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true), "pair1",
                            ImmutableMap.of("node01", true));
                    secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                    triggerSegmentClean(
                            getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
                    val tablePartitions = getTableFlow().getTableDataList().get(0).getPartitions();
                    assertTrue(tablePartitions.isEmpty());

                    for (ForceToTieredStorage f : ForceToTieredStorage.values()) {
                        clearQueryContext();
                        QueryContext.current().setForcedToTieredStorage(f);

                        try {
                            ExecAndComp.queryModel(getProject(), sql);
                            Assert.assertFalse(OLAPContext.getNativeRealizations().stream()
                                    .allMatch(NativeQueryRealization::isSecondStorage));
                        } catch (Exception e) {
                            assertTrue(e instanceof SQLException);
                        }
                    }
                    triggerClickHouseJob(getDataFlow());
                }
                
                testReverseForceToTierStorageWhenCHUnavailable(sql);
                testReverseForceToTierStorageWhenCHOK(sql);
                
                //reset status
                nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true), "pair1",
                        ImmutableMap.of("node01", true));
                secondStorageEndpoint.updateNodeStatus(nodeStatusMap);

                testForceToTierStorageShutTierStorage(sql);
                return true;
            });
        }
    }
    
    @SneakyThrows
    private void testForceToTSAndChDown(String sql, JdbcDatabaseContainer<?>[] container, int replica) {
        ExecAndComp.queryModel(getProject(), sql);
        OLAPContext.getNativeRealizations().stream().findFirst().ifPresent(r -> assertTrue(r.isSecondStorage()));

        for (JdbcDatabaseContainer<?> clickhouse : container) {
            clickhouse.stop();
        }
        clearQueryContext();
        QueryContext queryContext = QueryContext.current();
        queryContext.setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_RETURN);
        queryContext.setForceTableIndex(true);
        assertThrows(SQLException.class, () -> ExecAndComp.queryModel(getProject(), sql));

        clearQueryContext();
        queryContext = QueryContext.current();
        queryContext.setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_DFS);
        queryContext.setForceTableIndex(false);
        ExecAndComp.queryModel(getProject(), sql);

        for (JdbcDatabaseContainer<?> clickhouse : container) {
            clickhouse.start();
        }
        ClickHouseUtils.internalConfigClickHouse(container, replica);
    }

    private void testReverseForceToTierStorageWhenCHUnavailable(String sql) {
        // testReverseForceToTierStorageWhenCHUnavailable
        clearQueryContext();
        Map<String, Map<String, Boolean>> nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false),
                "pair1", ImmutableMap.of("node01", false));
        secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
        List<Boolean> reverseForceTableIndex = Arrays.asList(true, false);
        for (ForceToTieredStorage f : ForceToTieredStorage.values()) {
            for (Boolean forceTableIndex : reverseForceTableIndex) {
                clearQueryContext();
                QueryContext.current().setForceTableIndex(forceTableIndex);
                QueryContext.current().setForcedToTieredStorage(f);
                try {
                    ExecAndComp.queryModel(getProject(), sql);
                    Assert.assertFalse(OLAPContext.getNativeRealizations().stream()
                            .allMatch(NativeQueryRealization::isSecondStorage));
                } catch (Exception e) {
                    assertTrue(e instanceof SQLException);
                }
            }
        }
    }

    private void testReverseForceToTierStorageWhenCHOK(String sql) throws SQLException {
        // testReverseForceToTierStorageWhenCHOK
        Map<String, Map<String, Boolean>> nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true),
                "pair1", ImmutableMap.of("node01", true));
        secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
        for (ForceToTieredStorage f : ForceToTieredStorage.values()) {
            clearQueryContext();
            QueryContext.current().setForcedToTieredStorage(f);
            ExecAndComp.queryModel(getProject(), sql);
        }
    }

    private void testForceToTierStorageShutTierStorage(String sql) throws SQLException {
        // testForceToTierStorageShutTierStorage
        secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(),
                false);
        clearQueryContext();
        QueryContext queryContext = QueryContext.current();
        queryContext.setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN);
        Dataset<Row> rows = ExecAndComp.queryModel(getProject(), sql, null);
        Assert.assertFalse(
                OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));
    }

    @Test
    public void test1Replica3Shard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1, clickhouse2 }, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(2, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());

                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                waitAllJobFinish();
                triggerClickHouseJob(getDataFlow());

                {
                    getTableFlow().getTableDataList().forEach(tableData -> tableData.getPartitions()
                            .forEach(partition -> assertEquals(2, partition.getShardNodes().size())));
                }

                ClickHouseUtils.internalConfigClickHouse(
                        new JdbcDatabaseContainer[] { clickhouse1, clickhouse2, clickhouse3 }, 1);
                secondStorageService.changeProjectSecondStorageState(getProject(), ImmutableList.of("pair2"), true);
                Assert.assertEquals(3, SecondStorageUtil.listProjectNodes(getProject()).size());

                Map<String, Map<String, Boolean>> nodeStatusMap;

                {
                    nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true), "pair1",
                            ImmutableMap.of("node01", false), "pair2", ImmutableMap.of("node02", true));
                    secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                    EnvelopeResponse<ProjectTableSyncResponse> response = secondStorageEndpoint.tableSync(getProject());
                    assertEquals("000", response.getCode());
                    getTableFlow().getTableDataList()
                            .forEach(tableData -> tableData.getPartitions().forEach(partition -> {
                                assertEquals(3, partition.getShardNodes().size());
                                assertEquals(0, partition.getSizeInNode().get("node01").longValue());
                                assertEquals(0, partition.getSizeInNode().get("node02").longValue());
                            }));
                }

                //reset status
                nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true), "pair1",
                        ImmutableMap.of("node01", true), "pair2", ImmutableMap.of("node02", true));
                secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                return true;
            });
        }
    }

    @Test
    public void test1Replica1Shard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1 }, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());
                String sql = "select CAL_DT from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-08'";
                List<Long> resultList = new ArrayList<>();
                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                waitAllJobFinish();
                clearQueryContext();
                resultList.add(ExecAndComp.queryModel(getProject(), sql).count());
                triggerClickHouseJob(getDataFlow());
                clearQueryContext();
                resultList.add(ExecAndComp.queryModel(getProject(), sql).count());

                wrapWithKylinConfigMode("ANY", "CHAIN", () -> {
                    modelBuildService.refreshSegmentById(
                            new RefreshSegmentParams(getProject(), modelId, getDataFlow().getSegments().stream()
                                    .map(NDataSegment::getId).collect(Collectors.toList()).toArray(new String[] {})));
                    waitAllJobFinish();
                    clearQueryContext();
                    resultList.add(ExecAndComp.queryModel(getProject(), sql).count());
                    triggerClickHouseJob(getDataFlow());
                    clearQueryContext();
                    resultList.add(ExecAndComp.queryModel(getProject(), sql).count());
                    return true;
                });

                assertEquals(1, resultList.stream().distinct().count());
                return true;
            });
        }
    }

    @Test
    public void test2Replica1Shard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";
            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            configClickhouseWith(new JdbcDatabaseContainer[] { clickhouse1, clickhouse2 }, 2, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(2, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());

                buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
                waitAllJobFinish();
                testWait();
                triggerClickHouseJob(getDataFlow());
                Map<String, Map<String, Boolean>> nodeStatusMap;

                {
                    testSecondStorageQueryRandom();
                }

                {
                    Date now = new Date();
                    List<NDataSegmentResponse> mockSegments = Lists.newArrayList();
                    NDataSegmentResponse segmentResponse1 = new NDataSegmentResponse();
                    segmentResponse1.setId("1");
                    segmentResponse1.setRowCount(1);
                    segmentResponse1.setCreateTime(DateUtils.addHours(now, -1).getTime());

                    NDataSegmentResponse segmentResponse2 = new NDataSegmentResponse();
                    segmentResponse2.setId("2");
                    segmentResponse2.setRowCount(2);
                    segmentResponse2.setCreateTime(now.getTime());

                    NDataSegmentResponse segmentResponse3 = new NDataSegmentResponse();
                    segmentResponse3.setId("3");
                    segmentResponse3.setRowCount(3);
                    segmentResponse3.setCreateTime(DateUtils.addHours(now, 1).getTime());

                    String id = getTableFlow().getTableDataList().get(0).getPartitions().get(0).getSegmentId();
                    segmentResponse1.setId(id);
                    segmentResponse2.setId(id);
                    segmentResponse3.setId(id);

                    mockSegments.add(segmentResponse1);
                    mockSegments.add(segmentResponse3);
                    mockSegments.add(segmentResponse2);

                    modelService.addSecondStorageResponse(modelId, getProject(), mockSegments, getDataFlow());
                    Assert.assertEquals(2, (mockSegments.get(0)).getSecondStorageNodes().get("pair0").size());
                }

                {
                    try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve("node01"))) {
                        ShowDatabases showDatabases = new ShowDatabases();
                        List<String> databases = clickHouse.query(showDatabases.toSql(),
                                ShowDatabasesParser.SHOW_DATABASES);
                        databases = databases.stream()
                                .filter(database -> !database.equals("default") && !database.equals("system"))
                                .collect(Collectors.toList());
                        assertTrue(new HashSet<>(databases).contains("UT_table_index_incremental"));
                        List<String> tables = clickHouse.query(
                                ShowTables.createShowTables("UT_table_index_incremental").toSql(),
                                ShowDatabasesParser.SHOW_DATABASES);
                        assertTrue(new HashSet<>(tables).contains("acfde546_2cc9_4eec_bc92_e3bd46d4e2ee_20000000001"));
                    }

                    assertEquals("000", secondStorageEndpoint.tableSync(getProject()).getCode());
                    SecondStorageMetadataRequest request = new SecondStorageMetadataRequest();
                    request.setProject(getProject());
                    assertEquals("000", secondStorageEndpoint.sizeInNode(request).getCode());
                }

                //reset status
                nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", true, "node02", true));
                secondStorageEndpoint.updateNodeStatus(nodeStatusMap);
                return true;
            });
        }
    }

    @Test
    public void testClickhouseLowCardinality() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            val clickhouse = new JdbcDatabaseContainer[] { clickhouse1 };
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                QueryOperator queryOperator = SecondStorageFactoryUtils.createQueryMetricOperator(getProject());
                queryOperator.modifyColumnByCardinality("default", "table", Sets.newHashSet());

                buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
                waitAllJobFinish();
                secondStorageService.changeProjectSecondStorageState(getProject(),
                        SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                buildIncrementalLoadQuery("2012-01-04", "2012-01-05");
                waitAllJobFinish();
                secondStorageScheduleService.secondStorageLowCardinality();
                EpochManager epochManager = EpochManager.getInstance();
                String ownerIdentity = EpochOrchestrator.getOwnerIdentity();
                Epoch epoch = epochManager.getEpoch(EpochManager.GLOBAL);
                if (epoch == null) {
                    epoch = new Epoch(1L, EpochManager.GLOBAL, ownerIdentity, System.currentTimeMillis(), "all", null,
                            0L);
                }
                ReflectionTestUtils.invokeMethod(epochManager, "insertOrUpdateEpoch", epoch);
                secondStorageScheduleService.secondStorageLowCardinality();

                val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
                val dataflow = dataflowManager.getDataflow(modelId);

                String rows = "";
                String database = NameUtil.getDatabase(dataflow);
                val destTableName = NameUtil.getTable(dataflow, SecondStorageUtil.getBaseIndex(dataflow).getId());

                try (Connection connection = DriverManager.getConnection(clickhouse1.getJdbcUrl());
                        val stmt = connection.createStatement()) {
                    val rs = stmt.executeQuery(String.format(Locale.ROOT, "desc %s.%s", database, destTableName));
                    while (rs.next()) {
                        if ("c4".equals(rs.getString(1))) {
                            rows = rs.getString(2);
                        }
                    }
                }
                assertEquals(LOW_CARDINALITY_STRING, rows);

                getConfig().setProperty("kylin.second-storage.high-cardinality-number", "2");
                secondStorageScheduleService.secondStorageLowCardinality();
                try (Connection connection = DriverManager.getConnection(clickhouse1.getJdbcUrl());
                        val stmt = connection.createStatement()) {
                    val rs = stmt.executeQuery(String.format(Locale.ROOT, "desc %s.%s", database, destTableName));
                    while (rs.next()) {
                        if ("c4".equals(rs.getString(1))) {
                            rows = rs.getString(2);
                        }
                    }
                }
                assertEquals(NULLABLE_STRING, rows);
                try {
                    secondStorageService.modifyColumn(getProject(), modelId, "TEST_KYLIN_FACT.LSTG_FORMAT",
                            LOW_CARDINALITY_STRING);
                } catch (KylinException e) {
                    Assert.assertEquals(INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
                }
                secondStorageService.modifyColumn(getProject(), modelId, "TEST_KYLIN_FACT.LSTG_FORMAT_NAME",
                        LOW_CARDINALITY_STRING);
                try (Connection connection = DriverManager.getConnection(clickhouse1.getJdbcUrl());
                        val stmt = connection.createStatement()) {
                    val rs = stmt.executeQuery(String.format(Locale.ROOT, "desc %s.%s", database, destTableName));
                    while (rs.next()) {
                        if ("c4".equals(rs.getString(1))) {
                            rows = rs.getString(2);
                        }
                    }
                }
                assertEquals(LOW_CARDINALITY_STRING, rows);

                queryOperator.modifyColumnByCardinality(database, destTableName, Sets.newHashSet(4));
                try (Connection connection = DriverManager.getConnection(clickhouse1.getJdbcUrl());
                     val stmt = connection.createStatement()) {
                    val rs = stmt.executeQuery(String.format(Locale.ROOT, "desc %s.%s", database, destTableName));
                    while (rs.next()) {
                        if ("c4".equals(rs.getString(1))) {
                            rows = rs.getString(2);
                        }
                    }
                }
                assertEquals(LOW_CARDINALITY_STRING, rows);
                return true;
            });
        }
    }

    public List<Integer> getHAModelRowCount(String project, String modelId, long layoutId,
            JdbcDatabaseContainer<?>[] clickhouse) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val database = NameUtil.getDatabase(config, project);
        val table = NameUtil.getTable(modelId, layoutId);

        return Arrays.stream(clickhouse).map(JdbcDatabaseContainer::getJdbcUrl).map(url -> {
            try (ClickHouse clickHouse = new ClickHouse(url)) {
                List<Integer> count = clickHouse.query("select count(*) from `" + database + "`.`" + table + "`",
                        rs -> {
                            try {
                                return rs.getInt(1);
                            } catch (SQLException e) {
                                assertNull(e);
                                return 0;
                            }
                        });
                Assert.assertFalse(count.isEmpty());
                return count.get(0);
            } catch (Exception e) {
                assertNull(e);
                return 0;
            }
        }).collect(Collectors.toList());
    }

    private void testWait() {
        val executableManager = NExecutableManager.getInstance(getConfig(), getProject());
        val job = executableManager.getAllExecutables().stream().filter(j -> j instanceof NSparkCubingJob).findFirst();
        Map<String, String> waiteTimeMap = new HashMap<>();
        job.ifPresent(j -> ((NSparkCubingJob) j).getTasks().forEach(task -> {
            final ExecutableStepResponse executableStepResponse = jobService.parseToExecutableStep(task,
                    executableManager.getOutput(task.getId()), waiteTimeMap, j.getOutput().getState());
            assertTrue(executableStepResponse.getWaitTime() >= 0L);
        }));
    }

    private void clearQueryContext() {
        OLAPContext.clearThreadLocalContexts();
        QueryContext.current().close();
        QueryContext.current().setRetrySecondStorage(true);
    }
}
