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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.sparkproject.guava.collect.Sets;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

@Ignore("disable this suite, it is error in ci")
@Slf4j
@RunWith(JUnit4.class)
public class ClickHouseSimpleITTestWithBlob extends ClickHouseSimpleITTest {
    public static final String TEST_BLOB_META = "src/test/resources/blob_metadata";
    private AzuriteContainer azuriteContainer;

    @Override
    protected void doSetup() throws Exception {
        System.setProperty("kylin.second-storage.wait-index-build-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        File tempRoot = FileUtils.getTempDirectory();
        File tempDir = new File(tempRoot, RandomUtil.randomUUIDStr());
        if (!tempDir.mkdir()) {
            throw new IllegalStateException("temp blob directory create failed");
        }
        log.info("use {} as temp blob dir", tempDir.getAbsolutePath());
        FileUtils.copyDirectory(new File(TEST_BLOB_META), tempDir);

        azuriteContainer = new AzuriteContainer(10000, tempDir.getAbsolutePath());
        azuriteContainer.start();

        Map<String, String> overrideConf = Maps.newHashMap();
        overrideConf.put("fs.azure.account.key.devstoreaccount1.localhost:10000",
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
        overrideConf.put("fs.azure.skip.metrics", "true");
        // resolve dns problem
        overrideConf.put("fs.azure.storage.emulator.account.name", "devstoreaccount1.localhost:10000");
        overrideConf.put("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
        overrideConf.put("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.env.hdfs-working-dir", "wasb://test@devstoreaccount1.localhost:10000/kylin");
        SparkContext sc = ss.sparkContext();
        overrideConf.forEach(sc.hadoopConfiguration()::set);
    }

    @After
    public void tearDown() throws Exception {
        if (azuriteContainer != null) {
            azuriteContainer.stop();
            azuriteContainer = null;
        }
        super.tearDown();
    }

    @Test
    public void testSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testSingleShardBlob", false, clickhouse);
        }
    }

    @Test
    public void testTwoShards() throws Exception {
        // TODO: make sure splitting data into two shards
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testTwoShardsBlob", false, clickhouse1, clickhouse2);
        }
    }

    @Test
    public void testIncrementalSingleShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalSingleShardBlob", true, clickhouse);
        }
    }

    @Test
    @Ignore
    public void testIncrementalTwoShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
                JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {
            build_load_query("testIncrementalTwoShardBlob", true, clickhouse1, clickhouse2);
        }
    }

    @Override
    protected String getSourceUrl() {
        return "host.docker.internal";
    }

    protected void fullBuild(String dfName) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("wasb"));
        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow(dfName);
        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);
        indexDataConstructor.buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(round1), true);
    }

    @Test
    public void testCheckBaseTableIndex() {
        // overwrite
    }

    @Test
    public void testIncrementalTwoShardDoubleReplica() throws Exception {
        // overwrite
    }

    @Test
    public void testHAJobPaused() throws Exception {
        // overwrite
    }

    @Test
    public void testTwoShardDoubleReplica() throws Exception {
        // overwrite
    }

    @Test
    public void testCheckUtil() throws Exception {
        // overwrite
    }

    @Test
    public void testSecondStorage() throws Exception {
        // overwrite
    }

    @Test
    public void testDisableSSModel() throws Exception {
        // overwrite
    }

    @Override
    protected void checkHttpServer() throws IOException {
    }
}
