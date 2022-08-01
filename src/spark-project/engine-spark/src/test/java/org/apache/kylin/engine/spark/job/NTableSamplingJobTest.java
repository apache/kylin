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
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;
import lombok.var;

public class NTableSamplingJobTest extends NLocalWithSparkSessionTest {
    private static final String PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        super.init();
    }

    @After
    public void after() throws IOException {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    @Test
    public void testTableSamplingJob() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        val currMem = NDefaultScheduler.currentAvailableMem();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        final TableExtDesc tableExtBefore = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(tableDesc);
        Assert.assertNull(tableExtBefore);

        NExecutableManager execMgr = NExecutableManager.getInstance(config, PROJECT);
        val samplingJob = NTableSamplingJob.create(tableDesc, PROJECT, "ADMIN", 20_000_000);
        execMgr.addJob(samplingJob);
        Assert.assertEquals(ExecutableState.READY, samplingJob.getStatus());
        val tableSamplingMem = config.getSparkEngineDriverMemoryTableSampling();
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(currMem - tableSamplingMem, NDefaultScheduler.currentAvailableMem(), 0.1);
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
            Assert.assertEquals(currMem, NDefaultScheduler.currentAvailableMem(), 0.1);
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
    public void testTableSamplingJobWithS3Role() {
        getTestConfig().setProperty("kylin.env.use-dynamic-S3-role-credential-in-table", "true");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val currMem = NDefaultScheduler.currentAvailableMem();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        final TableExtDesc tableExtBefore = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertNotNull(tableDesc);
        Assert.assertNull(tableExtBefore);
        TableExtDesc tableExtWithS3Role = tableMgr.getOrCreateTableExt(tableDesc);
        tableExtWithS3Role.addDataSourceProp(TableExtDesc.LOCATION_PROPERTY_KEY, "s3://test/a");
        tableExtWithS3Role.addDataSourceProp(TableExtDesc.S3_ROLE_PROPERTY_KEY, "s3Role");
        tableExtWithS3Role.addDataSourceProp(TableExtDesc.S3_ENDPOINT_KEY, "us-west-1.amazonaws.com");

        tableMgr.saveTableExt(tableExtWithS3Role);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, PROJECT);
        val samplingJob = NTableSamplingJob.create(tableDesc, PROJECT, "ADMIN", 20_000_000);
        execMgr.addJob(samplingJob);
        Assert.assertEquals(ExecutableState.READY, samplingJob.getStatus());
        val tableSamplingMem = config.getSparkEngineDriverMemoryTableSampling();
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(NDefaultScheduler.currentAvailableMem(), currMem - tableSamplingMem, 0.1);
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
        NExecutableManager executableManager = NExecutableManager.getInstance(config, PROJECT);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(config, PROJECT);

        long endTime = System.currentTimeMillis() + 302400000L;
        long startTime = endTime - 604800000L;

        var stats = jobStatisticsManager.getOverallJobStats(startTime, endTime);
        Assert.assertEquals(0, (int) stats.getFirst());

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        val samplingJob = NTableSamplingJob.create(tableDesc, PROJECT, "ADMIN", 20_000_000);
        executableManager.addJob(samplingJob);
        final String jobId = samplingJob.getId();
        await().atMost(60, TimeUnit.MINUTES).until(() -> executableManager.getJob(jobId).getStatus().isFinalState());
        Assert.assertEquals(ExecutableState.SUCCEED, samplingJob.getStatus());

        stats = jobStatisticsManager.getOverallJobStats(startTime, endTime);
        Assert.assertEquals(1, (int) stats.getFirst());

    }

    @Test
    public void testSamplingUpdateJobStatisticsByPartitionTable() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        NExecutableManager executableManager = NExecutableManager.getInstance(config, PROJECT);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(config, PROJECT);

        long endTime = System.currentTimeMillis() + 302400000L;
        long startTime = endTime - 604800000L;

        var stats = jobStatisticsManager.getOverallJobStats(startTime, endTime);
        Assert.assertEquals(0, (int) stats.getFirst());

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        tableDesc.setRangePartition(true);
        val samplingJob = NTableSamplingJob.create(tableDesc, PROJECT, "ADMIN", 20_000_000);
        executableManager.addJob(samplingJob);
        final String jobId = samplingJob.getId();
        await().atMost(60, TimeUnit.MINUTES).until(() -> executableManager.getJob(jobId).getStatus().isFinalState());
        Assert.assertEquals(ExecutableState.SUCCEED, samplingJob.getStatus());

        stats = jobStatisticsManager.getOverallJobStats(startTime, endTime);
        Assert.assertEquals(1, (int) stats.getFirst());

    }

    @Test
    public void testPauseTableSamplingJob() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val currMem = NDefaultScheduler.currentAvailableMem();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(config, PROJECT);
        final TableDesc tableDesc = tableMgr.getTableDesc(tableName);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, PROJECT);
        val samplingJob = NTableSamplingJob.create(tableDesc, PROJECT, "ADMIN", 20000);
        execMgr.addJob(samplingJob);
        Assert.assertEquals(ExecutableState.READY, execMgr.getJob(samplingJob.getId()).getStatus());

        execMgr.pauseJob(samplingJob.getId());
        Assert.assertEquals(ExecutableState.PAUSED, execMgr.getJob(samplingJob.getId()).getStatus());
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(currMem, NDefaultScheduler.currentAvailableMem(), 0.1);
        });
    }
}
