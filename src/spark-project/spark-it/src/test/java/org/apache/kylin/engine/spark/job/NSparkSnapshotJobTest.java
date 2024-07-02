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

import java.io.IOException;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class NSparkSnapshotJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        config = getTestConfig();
        JobContextUtil.getJobContext(config);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Test
    public void testBuildSnapshotByPartitionJob() throws Exception {
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String partitionCol = "CAL_DT";
        Set<String> partitions = ImmutableSet.of("2012-01-01", "2012-01-02");
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());
        TableDesc table = tableManager.getTableDesc(tableName);
        table.setSelectedSnapshotPartitionCol(partitionCol);
        table.setPartitionColumn(partitionCol);
        tableManager.updateTableDesc(table);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        Assert.assertNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());

        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN",
                JobTypeEnum.SNAPSHOT_BUILD, RandomUtil.randomUUIDStr(), partitionCol, "false", null);
        setPartitions(job, partitions);
        execMgr.addJob(job);

        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        ResourceStore remoteResource = ExecutableUtils.getRemoteStore(config, job.getSnapshotBuildingStep());
        NTableMetadataManager remoteTableManager = NTableMetadataManager.getInstance(remoteResource.getConfig(),
                getProject());
        String snapshotPath = tableManager.getTableDesc(tableName).getLastSnapshotPath();
        Assert.assertNotNull(snapshotPath);
        Assert.assertEquals(2, list(snapshotPath).length);
        val fs = HadoopUtil.getWorkingFileSystem();
        Assert.assertEquals(Stream.of(list(snapshotPath)).mapToLong(s -> {
            try {
                return HadoopUtil.getContentSummary(fs, s.getPath()).getLength();
            } catch (IOException e) {
                return 0L;
            }
        }).sum(), tableManager.getTableDesc(tableName).getLastSnapshotSize());
        Assert.assertNotNull(remoteTableManager.getTableDesc(tableName).getTempSnapshotPath());
        Assert.assertEquals(partitionCol, tableManager.getTableDesc(tableName).getSnapshotPartitionCol());
        Assert.assertTrue(
                tableManager.getTableDesc(tableName).getSnapshotLastModified() > table.getSnapshotLastModified());
    }

    @Test
    public void testBuildSnapshotByPartitionRefreshPart() throws Exception {
        testBuildSnapshotByPartitionJob();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String partitionCol = "CAL_DT";
        Set<String> partitions = ImmutableSet.of("2012-01-03", "2012-01-04");
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());
        TableDesc table = tableManager.getTableDesc(tableName);
        table.setSelectedSnapshotPartitionCol(partitionCol);
        table.setPartitionColumn(partitionCol);
        tableManager.updateTableDesc(table);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN",
                JobTypeEnum.SNAPSHOT_BUILD, RandomUtil.randomUUIDStr(), partitionCol, "true", null);
        setPartitions(job, partitions);
        execMgr.addJob(job);
        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        String snapshotPath = tableManager.getTableDesc(tableName).getLastSnapshotPath();
        Assert.assertNotNull(snapshotPath);
        Assert.assertEquals(4, list(snapshotPath).length);

        ResourceStore remoteResource = ExecutableUtils.getRemoteStore(config, job.getSnapshotBuildingStep());
        NTableMetadataManager remoteTableManager = NTableMetadataManager.getInstance(remoteResource.getConfig(),
                getProject());
        Assert.assertNotNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertNotNull(remoteTableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertEquals(partitionCol, tableManager.getTableDesc(tableName).getSnapshotPartitionCol());
        Assert.assertTrue(
                tableManager.getTableDesc(tableName).getSnapshotLastModified() > table.getSnapshotLastModified());
    }

    @Test
    public void testBuildSnapshotByPartitionRefreshChoosePartition() throws Exception {
        testBuildSnapshotByPartitionJob();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String partitionCol = "CAL_DT";
        Set<String> partitions = ImmutableSet.of("2012-01-03", "2012-01-04");
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());
        TableDesc table = tableManager.getTableDesc(tableName);
        table.setSelectedSnapshotPartitionCol(partitionCol);
        table.setPartitionColumn(partitionCol);
        tableManager.updateTableDesc(table);

        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        Set<String> partitionToBuild = ImmutableSet.of("2012-01-03");
        TableDesc tableDesc = tableManager.getTableDesc(tableName);
        tableDesc.setRangePartition(true);
        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN",
                JobTypeEnum.SNAPSHOT_BUILD, RandomUtil.randomUUIDStr(), partitionCol, "true",
                JsonUtil.writeValueAsString(partitionToBuild));
        setPartitions(job, partitions);
        execMgr.addJob(job);
        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        String snapshotPath = tableManager.getTableDesc(tableName).getLastSnapshotPath();
        Assert.assertNotNull(snapshotPath);
        Assert.assertEquals(3, list(snapshotPath).length);

    }

    private FileStatus[] list(String path) {

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String baseDir = KapConfig.getInstanceFromEnv().getMetadataWorkingDirectory();
        String resourcePath = baseDir + "/" + path;
        try {
            return fs.listStatus(new Path(resourcePath));
        } catch (IOException e) {
            return null; // on IOException, skip the checking
        }
    }

    private void setPartitions(NSparkSnapshotJob job, Set<String> partitions) {
        job.setParam("partitions", String.join(",", partitions));
        job.getSnapshotBuildingStep().setParam("partitions", String.join(",", partitions));
    }

    @Test
    public void testBuildSnapshotJob() throws Exception {
        String tableName = "SSB.PART";
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        Assert.assertNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());

        NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.getTableDesc(tableName), "ADMIN", false, null);
        execMgr.addJob(job);
        StorageURL distMetaUrl = StorageURL.valueOf(job.getSnapshotBuildingStep().getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        ResourceStore remoteResource = ExecutableUtils.getRemoteStore(config, job.getSnapshotBuildingStep());
        NTableMetadataManager remoteTableManager = NTableMetadataManager.getInstance(remoteResource.getConfig(),
                getProject());
        Assert.assertNotNull(tableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertNotEquals(0L, tableManager.getTableDesc(tableName).getLastSnapshotSize());
        Assert.assertNotNull(remoteTableManager.getTableDesc(tableName).getLastSnapshotPath());
        Assert.assertNotEquals(0L, tableManager.getTableDesc(tableName).getSnapshotTotalRows());
    }

}
