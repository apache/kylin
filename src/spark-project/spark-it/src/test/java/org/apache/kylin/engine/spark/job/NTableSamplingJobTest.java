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

package org.apache.kylin.engine.spark.job;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.scheduler.ResourceAcquirer;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;
import lombok.var;

public class NTableSamplingJobTest extends NLocalWithSparkSessionTestBase {
    private static final String PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        super.init();
    }

    @After
    public void after() throws IOException {
        JobContextUtil.cleanUp();
        super.cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    @Test
    public void testTableSamplingJob() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        JobContext jobContext = JobContextUtil.getJobContext(config);
        ResourceAcquirer resourceAcquirer = jobContext.getResourceAcquirer();

        val currMem = resourceAcquirer.currentAvailableMem();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        final TableExtDesc tableExtBefore = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(tableDesc);
        Assert.assertNull(tableExtBefore);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, PROJECT);
        val samplingJob = NTableSamplingJob.internalCreate(tableDesc, PROJECT, "ADMIN", 20_000_000);
        execMgr.addJob(samplingJob);
        Assert.assertEquals(ExecutableState.READY, samplingJob.getStatus());
        val tableSamplingMem = config.getSparkEngineDriverMemoryTableSampling();
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(currMem - tableSamplingMem, resourceAcquirer.currentAvailableMem(), 0.1);
        });
        final String jobId = samplingJob.getId();
        await().atMost(3, TimeUnit.MINUTES).until(() -> !execMgr.getJob(jobId).getStatus().isProgressing());
        Assert.assertEquals(ExecutableState.SUCCEED, samplingJob.getStatus());

        final TableExtDesc tableExtAfter = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(tableExtAfter);
        Assert.assertEquals(12, tableExtAfter.getAllColumnStats().size());
        Assert.assertEquals(10, tableExtAfter.getSampleRows().size());
        Assert.assertEquals(10_000, tableExtAfter.getTotalRows());
        Assert.assertEquals(tableName, tableExtAfter.getIdentity());

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(currMem, resourceAcquirer.currentAvailableMem(), 0.1);
        });

        // assert table ext
        final String metadataPath = config.getMetadataUrl().toString();
        val buildConfig = KylinConfig.createKylinConfig(config);
        buildConfig.setMetadataUrl(metadataPath);
        final TableExtDesc tableExt = NTableMetadataManager.getInstance(buildConfig, PROJECT)
                .getTableExtIfExists(tableDesc);
        Assert.assertNotNull(tableExt);
        Assert.assertEquals(12, tableExt.getAllColumnStats().size());
        Assert.assertEquals(10, tableExt.getSampleRows().size());
        Assert.assertEquals(10_000, tableExt.getTotalRows());
        Assert.assertEquals(samplingJob.getCreateTime(), tableExt.getCreateTime());
    }

    @Test
    public void testTableSamplingJobFailed_withCheckColumnsError() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String TABLE_SAMPLING_TEST_TMP_DB = "TABLE_SAMPLING_TEST_TMP_DB";
        overwriteSystemProp("kylin.source.ddl.logical-view.database", TABLE_SAMPLING_TEST_TMP_DB);

        final String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        Assert.assertNotNull(tableDesc);

        ColumnDesc[] columns = tableDesc.getColumns();
        ColumnDesc[] columnsModified = Arrays.copyOfRange(columns, 0, columns.length + 2);
        columnsModified[columnsModified.length - 2] = new ColumnDesc("13", "A_CC_COL", "boolean", "",
                "true|false|TRUE|FALSE|True|False", null, "non-empty expr");
        columnsModified[columnsModified.length - 1] = new ColumnDesc("14", "A_NON_EXIST_COL", "boolean", "",
                "true|false|TRUE|FALSE|True|False", null, null);
        TableDesc tableDescModified = tableMgr.copyForWrite(tableDesc);
        tableDescModified.setDatabase(null);
        tableDescModified.setColumns(columnsModified);
        tableDescModified.setMvcc(-1L);
        tableMgr.saveSourceTable(tableDescModified);

        Map<String, String> params = NProjectManager.getInstance(config).getProject(PROJECT)
                .getLegalOverrideKylinProps();
        Dataset<Row> dataFrame = SourceFactory
                .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(tableDesc, ss, params).coalesce(1);
        dataFrame.createOrReplaceTempView(tableDescModified.getName());

        ExecutableManager execMgr = ExecutableManager.getInstance(config, PROJECT);
        val samplingJob = NTableSamplingJob.internalCreate(tableDescModified, PROJECT, "ADMIN", 20_000_000);
        execMgr.addJob(samplingJob);
        Assert.assertEquals(ExecutableState.READY, samplingJob.getStatus());

        await().atMost(60000, TimeUnit.MINUTES)
                .until(() -> !execMgr.getJob(samplingJob.getId()).getStatus().isProgressing());
        Assert.assertEquals(ExecutableState.ERROR, samplingJob.getStatus());
    }

    @Test
    public void testTableSamplingJobWithS3Role() {
        getTestConfig().setProperty("kylin.env.use-dynamic-role-credential-in-table", "true");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        JobContext jobContext = JobContextUtil.getJobContext(config);
        ResourceAcquirer resourceAcquirer = jobContext.getResourceAcquirer();
        val currMem = resourceAcquirer.currentAvailableMem();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        final TableExtDesc tableExtBefore = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(tableDesc);
        Assert.assertNull(tableExtBefore);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateTableExt(tableName,
                    copyForWrite -> {
                        copyForWrite.addDataSourceProp(TableExtDesc.LOCATION_PROPERTY_KEY, "s3://test/a");
                        copyForWrite.addDataSourceProp(TableExtDesc.S3_ROLE_PROPERTY_KEY, "s3Role");
                        copyForWrite.addDataSourceProp(TableExtDesc.S3_ENDPOINT_KEY, "us-west-1.amazonaws.com");
                    });
            return null;
        }, PROJECT);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, PROJECT);
        val samplingJob = NTableSamplingJob.internalCreate(tableDesc, PROJECT, "ADMIN", 20_000_000);
        execMgr.addJob(samplingJob);
        Assert.assertEquals(ExecutableState.READY, samplingJob.getStatus());
        val tableSamplingMem = config.getSparkEngineDriverMemoryTableSampling();
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(resourceAcquirer.currentAvailableMem(), currMem - tableSamplingMem, 0.1);
        });
        final String jobId = samplingJob.getId();
        await().atMost(3, TimeUnit.MINUTES).until(() -> !execMgr.getJob(jobId).getStatus().isProgressing());
        Assert.assertEquals(ExecutableState.SUCCEED, samplingJob.getStatus());
        assert SparderEnv.getSparkSession().conf().get("fs.s3a.bucket.test.assumed.role.arn").equals("s3Role");
        assert SparderEnv.getSparkSession().conf().get("fs.s3a.bucket.test.endpoint").equals("us-west-1.amazonaws.com");

    }

    @Test
    public void testSamplingUpdateJobStatistics() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        ExecutableManager executableManager = ExecutableManager.getInstance(config, PROJECT);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(config, PROJECT);

        long endTime = System.currentTimeMillis() + 302400000L;
        long startTime = endTime - 604800000L;

        var stats = jobStatisticsManager.getOverallJobStats(startTime, endTime);
        Assert.assertEquals(0, (int) stats.getFirst());

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        val samplingJob = NTableSamplingJob.internalCreate(tableDesc, PROJECT, "ADMIN", 20_000_000);
        executableManager.addJob(samplingJob);
        final String jobId = samplingJob.getId();
        await().atMost(60, TimeUnit.MINUTES).until(() -> executableManager.getJob(jobId).getStatus().isFinalState());
        Assert.assertEquals(ExecutableState.SUCCEED, samplingJob.getStatus());

        await().atMost(Duration.FIVE_SECONDS)
                .until(() -> jobStatisticsManager.getOverallJobStats(startTime, endTime).getFirst() == 1);
    }

    @Test
    public void testSamplingUpdateJobStatisticsByPartitionTable() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        ExecutableManager executableManager = ExecutableManager.getInstance(config, PROJECT);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(config, PROJECT);

        long endTime = System.currentTimeMillis() + 302400000L;
        long startTime = endTime - 604800000L;

        var stats = jobStatisticsManager.getOverallJobStats(startTime, endTime);
        Assert.assertEquals(0, (int) stats.getFirst());

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        tableDesc.setRangePartition(true);
        val samplingJob = NTableSamplingJob.internalCreate(tableDesc, PROJECT, "ADMIN", 20_000_000);
        executableManager.addJob(samplingJob);
        final String jobId = samplingJob.getId();
        await().atMost(60, TimeUnit.MINUTES).until(() -> executableManager.getJob(jobId).getStatus().isFinalState());
        Assert.assertEquals(ExecutableState.SUCCEED, samplingJob.getStatus());

        await().atMost(Duration.FIVE_SECONDS)
                .until(() -> jobStatisticsManager.getOverallJobStats(startTime, endTime).getFirst() == 1);
    }

    @Test
    public void testPauseTableSamplingJob() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        JobContext jobContext = JobContextUtil.getJobContext(config);
        ResourceAcquirer resourceAcquirer = jobContext.getResourceAcquirer();
        val currMem = resourceAcquirer.currentAvailableMem();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        ExecutableManager execMgr = ExecutableManager.getInstance(config, PROJECT);
        var samplingJob = NTableSamplingJob.internalCreate(tableDesc, PROJECT, "ADMIN", 20000);
        execMgr.addJob(samplingJob);
        String jobId = samplingJob.getJobId();
        Assert.assertEquals(ExecutableState.READY, execMgr.getJob(samplingJob.getId()).getStatus());
        await().atMost(1, TimeUnit.MINUTES).until(() -> execMgr.getJob(jobId).getStatus() != ExecutableState.READY);
        samplingJob = (NTableSamplingJob) execMgr.getJob(samplingJob.getId());
        execMgr.pauseJob(samplingJob.getId(), ExecutableManager.toPO(samplingJob, PROJECT), samplingJob);
        Assert.assertEquals(ExecutableState.PAUSED, execMgr.getJob(samplingJob.getId()).getStatus());
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(currMem, resourceAcquirer.currentAvailableMem(), 0.1);
        });
    }
}
