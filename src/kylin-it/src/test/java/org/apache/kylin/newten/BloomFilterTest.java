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


package org.apache.kylin.newten;

import static org.apache.kylin.engine.spark.filter.QueryFiltersCollector.SERVER_HOST;
import static org.apache.kylin.engine.spark.filter.QueryFiltersCollector.getProjectFiltersFile;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.filter.BloomFilterSkipCollector;
import org.apache.kylin.engine.spark.filter.ParquetBloomFilter;
import org.apache.kylin.engine.spark.filter.QueryFiltersCollector;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sparkproject.guava.collect.Sets;

@RunWith(TimeZoneTestRunner.class)
public class BloomFilterTest extends NLocalWithSparkSessionTest implements AdaptiveSparkPlanHelper {

    private NDataflowManager dfMgr = null;

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC) {
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        }
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[2]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.bloom.collect-filter.enabled", "true");
        overwriteSystemProp("kylin.bloom.build.enabled", "true");
        overwriteSystemProp("kylin.query.filter.collect-interval", "10");
        this.createTestMetadata("src/test/resources/ut_meta/bloomfilter");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        QueryFiltersCollector.initScheduler();
        dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        QueryFiltersCollector.destoryScheduler();
        cleanupTestMetadata();
    }

    public String getProject() {
        return "bloomfilter";
    }

    @Test
    public void testBuildBloomFilter() throws Exception {
        Path projectFilterPath = getProjectFiltersFile(SERVER_HOST, getProject());
        FileSystem fs = HadoopUtil.getFileSystem(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory());
        if (fs.exists(projectFilterPath)) {
            fs.delete(projectFilterPath, true);
        }
        String dfID = "c41390c5-b93d-4db3-b167-029874b85a2c";
        NDataflow dataflow = dfMgr.getDataflow(dfID);
        LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(20000000001L);
        Assert.assertNotNull(layout);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        overwriteSystemProp("kylin.bloom.build.column-ids", "0#1");
        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(
                        dataflow.getIndexPlan().getLayoutEntity(20000000001L)), true);
        // In our PR UT environment, building job may not be executed when there has cache,
        // so we will ignore it if job is skipped
        if (ParquetBloomFilter.isLoaded()) {
            // set BloomFilter to build manually, see "kylin.bloom.build.column"
            Assert.assertTrue(ParquetBloomFilter.getBuildBloomColumns().contains("0"));
            Assert.assertTrue(ParquetBloomFilter.getBuildBloomColumns().contains("1"));
        }

        List<Pair<String, String>> query = new ArrayList<>();
        String sql1 = "select * from SSB.P_LINEORDER where LO_CUSTKEY in (13,8) and LO_SHIPPRIOTITY = 0 ";
        query.add(Pair.newPair("bloomfilter", sql1));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.NONE, "inner");
        // wait until `QueryFiltersCollector` record filter info
        await().atMost(120, TimeUnit.SECONDS).until(() -> {
            try {
                if (!fs.exists(projectFilterPath)) {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
            return true;
        });
        Map<String, Map<String, Integer>> history = JsonUtil.readValue(
                HadoopUtil.readStringFromHdfs(fs, projectFilterPath), Map.class);
        Assert.assertTrue(history.get(dfID).keySet().contains("8"));
        Assert.assertTrue(history.get(dfID).keySet().contains("9"));
        Integer hitNum = history.get(dfID).get("8");
        Assert.assertTrue(fs.exists(projectFilterPath));
        String sql2 = "select * from SSB.P_LINEORDER where LO_CUSTKEY in (13,8)";
        query.add(Pair.newPair("bloomfilter", sql2));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.NONE, "inner");

        await().atMost(120, TimeUnit.SECONDS).until(() -> {
            try {
                Map<String, Map<String, Integer>> newHistory = JsonUtil.readValue(
                        HadoopUtil.readStringFromHdfs(fs, projectFilterPath), Map.class);
                Integer newHitNum = newHistory.get(dfID).get("8");
                if (newHitNum <= hitNum) {
                    return false;
                }
            } catch (Exception e) {
                return false;
            }
            return true;
        });

        overwriteSystemProp("kylin.bloom.build.column-ids", "");
        overwriteSystemProp("kylin.bloom.build.column.max-size", "1");
        ParquetBloomFilter.resetParquetBloomFilter();
        indexDataConstructor.buildIndex(dfID, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newHashSet(dataflow.getIndexPlan().getLayoutEntity(20000000001L)), true);
        // In our PR UT environment, building job may not be executed when there has cache,
        // so we will ignore it if job is skipped
        if (ParquetBloomFilter.isLoaded()) {
            // build BloomFilter according to query statics
            Assert.assertTrue(ParquetBloomFilter.getBuildBloomColumns().contains("8"));
            // `kylin.bloom.build.column.max-size=1`, only build one column
            Assert.assertFalse(ParquetBloomFilter.getBuildBloomColumns().contains("9"));
        }
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "inner");

        testBloomFilterSkipCollector();
    }

    private void testBloomFilterSkipCollector() {
        String queryId1 = "query-id1";
        BloomFilterSkipCollector.addQueryMetrics(queryId1, 3,
                2, 20, 100, 1);
        BloomFilterSkipCollector.addQueryMetrics(queryId1, 1,
                1, 10, 100, 1);
        Assert.assertEquals(4L, BloomFilterSkipCollector.queryTotalBloomBlocks.getIfPresent(queryId1).get());
        Assert.assertEquals(3L, BloomFilterSkipCollector.querySkipBloomBlocks.getIfPresent(queryId1).get());
        Assert.assertEquals(30L, BloomFilterSkipCollector.querySkipBloomRows.getIfPresent(queryId1).get());
        Assert.assertEquals(200L, BloomFilterSkipCollector.queryFooterReadTime.getIfPresent(queryId1).get());
        Assert.assertEquals(2L, BloomFilterSkipCollector.queryFooterReadNumber.getIfPresent(queryId1).get());
        BloomFilterSkipCollector.logAndCleanStatus(queryId1);
        BloomFilterSkipCollector.logAndCleanStatus("query-id2");
        Assert.assertNull(BloomFilterSkipCollector.queryTotalBloomBlocks.getIfPresent(queryId1));
        Assert.assertNull(BloomFilterSkipCollector.querySkipBloomBlocks.getIfPresent(queryId1));
        Assert.assertNull(BloomFilterSkipCollector.querySkipBloomRows.getIfPresent(queryId1));
        Assert.assertNull(BloomFilterSkipCollector.queryFooterReadTime.getIfPresent(queryId1));
        Assert.assertNull(BloomFilterSkipCollector.queryFooterReadNumber.getIfPresent(queryId1));
        for (int i = 0; i < 200; i++) {
            BloomFilterSkipCollector.logAndCleanStatus("query-id2");
        }
        Assert.assertTrue(BloomFilterSkipCollector.logCounter.get() <= 100);
    }
}
