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

import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_LAYOUT_IDS;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.builder.SnapshotBuilder;
import org.apache.kylin.engine.spark.merger.AfterBuildResourceMerger;
import org.apache.kylin.engine.spark.storage.ParquetStorage;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.cuboid.NCuboidLayoutChooser;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTree;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.sparkproject.guava.collect.Sets;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;
import scala.Option;
import scala.runtime.AbstractFunction1;

public class NSparkCubingJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Before
    public void setup() {
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        NDefaultScheduler.destroyInstance();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(getTestConfig()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

        config = getTestConfig();
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    @Test
    public void testMergeBasics() throws IOException {
        final String dataJson1 = "0,1,3,1000\n0,2,2,1000";
        final String dataJson2 = "0,1,2,2000";

        File dataFile1 = File.createTempFile("tmp1", ".csv");
        FileUtils.writeStringToFile(dataFile1, dataJson1, Charset.defaultCharset());
        Dataset<Row> dataset1 = ss.read().csv(dataFile1.getAbsolutePath());
        Assert.assertEquals(2, dataset1.count());
        dataset1.show();

        File dataFile2 = File.createTempFile("tmp2", ".csv");
        FileUtils.writeStringToFile(dataFile2, dataJson2, Charset.defaultCharset());
        Dataset<Row> dataset2 = ss.read().csv(dataFile2.getAbsolutePath());
        Assert.assertEquals(1, dataset2.count());
        dataset2.show();

        Dataset<Row> dataset3 = dataset2.union(dataset1);
        Assert.assertEquals(3, dataset3.count());
        dataset3.show();
        FileUtils.deleteQuietly(dataFile1);
        FileUtils.deleteQuietly(dataFile2);
    }

    @Test
    public void testBuildSnapshot() throws Exception {
        KylinConfig config = getTestConfig();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        new SnapshotBuilder().buildSnapshot(ss, getLookTables(df));
        getLookTables(df).forEach(table -> Assert.assertNotNull(table.getLastSnapshotPath()));
    }

    private Set<TableDesc> getLookTables(NDataflow df) {
        return df.getModel().getLookupTables().stream().map(TableRef::getTableDesc).collect(Collectors.toSet());
    }

    @Test
    public void testBuildSnapshotIgnored_SnapshotIsNull() throws Exception {
        final Set<String> ignoredSnapshotTableSet = new HashSet<>(
                Arrays.asList("DEFAULT.TEST_COUNTRY", "EDW.TEST_CAL_DT"));
        KylinConfig config = getTestConfig();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        //snapshot building cannot be skip when it is null

        new SnapshotBuilder().buildSnapshot(ss, df.getModel(), ignoredSnapshotTableSet);
        getLookTables(df).forEach(table -> Assert.assertNotNull(table.getLastSnapshotPath()));
    }

    @Test
    public void testBuildSnapshotIgnored_SnapshotExists() throws Exception {
        final Set<String> ignoredSnapshotTableSet = new HashSet<>(
                Arrays.asList("DEFAULT.TEST_COUNTRY", "EDW.TEST_CAL_DT"));
        KylinConfig config = getTestConfig();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        //assert snapshot already exists
        String mockPath = "default/table_snapshot/mock";
        NTableMetadataManager nTableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
        ignoredSnapshotTableSet.forEach(ignoredSnapshotTable -> {
            nTableMetadataManager.getTableDesc(ignoredSnapshotTable).setLastSnapshotPath(mockPath);
        });

        //snapshot building can be skip when it is not null
        new SnapshotBuilder().buildSnapshot(ss, df.getModel(), ignoredSnapshotTableSet);
        Assert.assertTrue(ignoredSnapshotTableSet.stream().allMatch(
                tableName -> nTableMetadataManager.getTableDesc(tableName).getLastSnapshotPath().equals(mockPath)));
        getLookTables(df).forEach(table -> Assert.assertNotNull(table.getLastSnapshotPath()));

    }

    @Test
    public void testBuildJob() throws Exception {
        String dfName = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        long startLong = System.currentTimeMillis();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        cleanupSegments(dsMgr, dfName);
        NDataflow df = dsMgr.getDataflow(dfName);

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(df.getIndexPlan().getLayoutEntity(20_000_020_001L));
        round1.add(df.getIndexPlan().getLayoutEntity(1_000_001L));
        round1.add(df.getIndexPlan().getLayoutEntity(30001L));
        round1.add(df.getIndexPlan().getLayoutEntity(10002L));

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(round1, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg, rootCuboid);
            Assert.assertNull(layout);
        }

        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN",
                null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        long buildEndTime = sparkStep.getEndTime();
        long startOfDay = TimeUtil.getDayStart(buildEndTime);

        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(config, sparkStep.getProject());
        Pair<Integer, JobStatistics> overallJobStats = jobStatisticsManager.getOverallJobStats(startOfDay,
                buildEndTime);
        JobStatistics jobStatistics = overallJobStats.getSecond();
        // assert date is recorded correctly before metadata merge
        Assert.assertEquals(startOfDay, jobStatistics.getDate());
        Assert.assertEquals(1, jobStatistics.getCount());

        val merger = new AfterBuildResourceMerger(config, getProject());
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), ExecutableUtils.getLayoutIds(sparkStep),
                ExecutableUtils.getRemoteStore(config, sparkStep));

        Pair<Integer, JobStatistics> overallJobStats2 = jobStatisticsManager.getOverallJobStats(startOfDay,
                buildEndTime);
        JobStatistics jobStatistics2 = overallJobStats2.getSecond();
        // assert job stats recorded correctly after metadata merge
        Assert.assertEquals(startOfDay, jobStatistics2.getDate());
        Assert.assertEquals(1, jobStatistics2.getCount());

        /*
         * Round2. Build new layouts, should reuse the data from already existing cuboid.
         * Notice: After round1 the segment has been updated, need to refresh the cache before use the old one.
         */
        List<LayoutEntity> round2 = new ArrayList<>();
        round2.add(df.getIndexPlan().getLayoutEntity(1L));
        round2.add(df.getIndexPlan().getLayoutEntity(20_000_000_001L));
        round2.add(df.getIndexPlan().getLayoutEntity(20001L));
        round2.add(df.getIndexPlan().getLayoutEntity(10001L));

        //update seg
        val df2 = dsMgr.getDataflow(dfName);
        oneSeg = df2.getSegment(oneSeg.getId());
        nSpanningTree = NSpanningTreeFactory.fromLayouts(round2, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg, rootCuboid);
            Assert.assertNotNull(layout);
        }

        job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round2), "ADMIN", null);
        execMgr.addJob(job);

        // wait job done
        status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        merger.mergeAfterCatchup(df2.getUuid(), Sets.newHashSet(oneSeg.getId()),
                ExecutableUtils.getLayoutIds(job.getSparkCubingStep()),
                ExecutableUtils.getRemoteStore(config, job.getSparkCubingStep()), null);

        validateCube(df2.getSegments().getFirstSegment().getId());
        validateTableIndex(df2.getSegments().getFirstSegment().getId());
        //        validateTableExt(df.getModel());
        //validate lastBuildTime
        oneSeg = dsMgr.getDataflow(dfName).getSegment(oneSeg.getId());
        Assert.assertTrue(oneSeg.getLastBuildTime() > startLong);
        getLookTables(df).forEach(table -> Assert.assertTrue(table.getSnapshotTotalRows() > 0));
    }

    @Test
    public void testBuildJobWithExcludeTable() throws Exception {
        String dfName = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        long startLong = System.currentTimeMillis();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        cleanupSegments(dsMgr, dfName);
        NDataflow df = dsMgr.getDataflow(dfName);

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(df.getIndexPlan().getLayoutEntity(20_000_020_001L));
        round1.add(df.getIndexPlan().getLayoutEntity(1_000_001L));
        round1.add(df.getIndexPlan().getLayoutEntity(30001L));
        round1.add(df.getIndexPlan().getLayoutEntity(10002L));

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(round1, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg, rootCuboid);
            Assert.assertNull(layout);
        }

        // add ExcludedTables
        FavoriteRuleManager ruleManager = FavoriteRuleManager.getInstance(config, df.getProject());
        List<FavoriteRule.AbstractCondition> conds = Lists.newArrayList();
        //        isEnabled = request.isExcludeTablesEnable();
        conds.add(new FavoriteRule.Condition(null, df.getModel().getRootFactTableName()));
        ruleManager.updateRule(conds, true, FavoriteRule.EXCLUDED_TABLES_RULE);

        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN",
                null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        long buildEndTime = sparkStep.getEndTime();
        long startOfDay = TimeUtil.getDayStart(buildEndTime);

        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(config, sparkStep.getProject());
        Pair<Integer, JobStatistics> overallJobStats = jobStatisticsManager.getOverallJobStats(startOfDay,
                buildEndTime);
        JobStatistics jobStatistics = overallJobStats.getSecond();
        // assert date is recorded correctly before metadata merge
        Assert.assertEquals(startOfDay, jobStatistics.getDate());
        Assert.assertEquals(1, jobStatistics.getCount());

        val merger = new AfterBuildResourceMerger(config, getProject());
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), ExecutableUtils.getLayoutIds(sparkStep),
                ExecutableUtils.getRemoteStore(config, sparkStep));

        Pair<Integer, JobStatistics> overallJobStats2 = jobStatisticsManager.getOverallJobStats(startOfDay,
                buildEndTime);
        JobStatistics jobStatistics2 = overallJobStats2.getSecond();
        // assert job stats recorded correctly after metadata merge
        Assert.assertEquals(startOfDay, jobStatistics2.getDate());
        Assert.assertEquals(1, jobStatistics2.getCount());

        //validate lastBuildTime
        oneSeg = dsMgr.getDataflow(dfName).getSegment(oneSeg.getId());
        Assert.assertTrue(oneSeg.getLastBuildTime() > startLong);
        getLookTables(df).forEach(table -> Assert.assertTrue(table.getSnapshotTotalRows() > 0));
    }

    @Test
    public void testBuildPartialLayouts() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        String dfName = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        cleanupSegments(dsMgr, dfName);
        NDataflow df = dsMgr.getDataflow(dfName);
        IndexPlan indexPlan = df.getIndexPlan();
        IndexEntity ie = indexPlan.getIndexEntity(10000);
        IndexEntity ie2 = indexPlan.getIndexEntity(0);
        Assert.assertEquals(2, ie.getLayouts().size());
        List<LayoutEntity> layouts = new ArrayList<>();
        layouts.add(ie.getLayouts().get(0));
        layouts.add(ie2.getLayouts().get(0));
        indexDataConstructor.buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layouts), true);
    }

    @Test
    public void testMockedDFBuildJob() throws Exception {
        overwriteSystemProp("kylin.engine.spark.build-class-name",
                "org.apache.kylin.engine.spark.job.MockedDFBuildJob");
        String dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        cleanupSegments(dsMgr, dataflowId);
        NDataflow df = dsMgr.getDataflow(dataflowId);

        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(df.getIndexPlan().getLayoutEntity(20_000_020_001L));
        round1.add(df.getIndexPlan().getLayoutEntity(1_000_001L));
        round1.add(df.getIndexPlan().getLayoutEntity(30001L));
        round1.add(df.getIndexPlan().getLayoutEntity(10002L));
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN",
                null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        execMgr.addJob(job);
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        val merger = new AfterBuildResourceMerger(config, getProject());
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), ExecutableUtils.getLayoutIds(sparkStep),
                ExecutableUtils.getRemoteStore(config, sparkStep));
        NDataSegment newSeg = dsMgr.getDataflow(dataflowId).getSegments().getFirstSegment();
        for (NDataLayout layout : newSeg.getLayoutsMap().values()) {
            Assert.assertEquals(layout.getRows(), 123);
            Assert.assertEquals(layout.getByteSize(), 123);
            Assert.assertEquals(layout.getFileCount(), 123);
            Assert.assertEquals(layout.getSourceRows(), 123);
            Assert.assertEquals(layout.getSourceByteSize(), 123);
        }
    }

    @Test
    public void testMockedDFBuildMutipleJob() throws Exception {
        overwriteSystemProp("kylin.engine.spark.build-class-name",
                "org.apache.kylin.engine.spark.job.MockedDFBuildJob");
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "true");
        String dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        cleanupSegments(dsMgr, dataflowId);
        NDataflow df = dsMgr.getDataflow(dataflowId);

        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(df.getIndexPlan().getLayoutEntity(10002L));
        NDataSegment seg1 = dsMgr.appendSegment(df,
                new SegmentRange.TimePartitionedSegmentRange("2012-01-01", "2012-02-01"), SegmentStatusEnum.READY);
        NDataSegment seg2 = dsMgr.appendSegment(df,
                new SegmentRange.TimePartitionedSegmentRange("2012-02-01", "2012-03-01"), SegmentStatusEnum.READY);

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(seg1, seg2), Sets.newLinkedHashSet(round1),
                "ADMIN", null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        execMgr.addJob(job);
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        val merger = new AfterBuildResourceMerger(config, getProject());
        merger.mergeAfterCatchup(df.getUuid(), Sets.newHashSet(seg1.getId(), seg2.getId()), Sets.newHashSet(10002L),
                ExecutableUtils.getRemoteStore(config, sparkStep), null);

        List<NDataSegment> segs = dsMgr.getDataflow(dataflowId).getSegments();
        Assert.assertEquals(2, segs.size());
        // test if segs are updated
        Assert.assertTrue(segs.get(0).isFlatTableReady());
        Assert.assertTrue(segs.get(1).isFlatTableReady());
    }

    @Test
    public void testCancelCubingJob() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        cleanupSegments(dsMgr, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());
        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(layouts.get(0));
        round1.add(layouts.get(1));
        round1.add(layouts.get(2));
        round1.add(layouts.get(3));
        round1.add(layouts.get(7));
        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN",
                null);
        execMgr.addJob(job);
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(1, df.getSegments().size());
        await().untilAsserted(() -> Assert.assertEquals(ExecutableState.RUNNING, job.getStatus()));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).discardJob(job.getId());
            return null;
        }, getProject(), UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());
        dsMgr = NDataflowManager.getInstance(config, getProject());
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());
    }

    @Test
    public void testCancelMergingJob() throws Exception {

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        cleanupSegments(dsMgr, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());
        // ready dataflow, segment, cuboid layout
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        long start = SegmentRange.dateToLong("2011-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");
        indexDataConstructor.buildIndex("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts), true);
        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        indexDataConstructor.buildIndex("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts), true);
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-02"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        execMgr.addJob(firstMergeJob);
        await().untilAsserted(() -> Assert.assertEquals(ExecutableState.RUNNING, firstMergeJob.getStatus()));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                    .discardJob(firstMergeJob.getId());
            return null;
        }, getProject(), UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, firstMergeJob.getId());
        dsMgr = NDataflowManager.getInstance(config, getProject());
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(2, df.getSegments().size());
    }

    @Test
    public void testGetJobNodeInfo() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        cleanupSegments(dsMgr, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                null);

        // launch the job
        execMgr.addJob(job);

        // wait job done
        IndexDataConstructor.wait(job);

        Assert.assertEquals(config.getServerAddress(), job.getOutput().getExtra().get("node_info"));
    }

    private void validateCube(String segmentId) {
        NDataflow df = NDataflowManager.getInstance(config, getProject())
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment seg = df.getSegment(segmentId);

        // check row count in NDataSegDetails
        Assert.assertEquals(10000, seg.getLayout(1).getRows());
        Assert.assertEquals(10000, seg.getLayout(10001).getRows());
        Assert.assertEquals(10000, seg.getLayout(10002).getRows());
    }

    private void validateTableIndex(String segmentId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment seg = df.getSegment(segmentId);
        NDataSegDetails segCuboids = seg.getSegDetails();
        NDataLayout dataCuboid = NDataLayout.newDataLayout(segCuboids, 20000000001L);
        LayoutEntity layout = dataCuboid.getLayout();
        Assert.assertEquals(10000, seg.getLayout(20000000001L).getRows());

        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getFrom(NSparkCubingUtil.getStoragePath(seg, layout.getId()), ss);
        List<Row> rows = ret.collectAsList();
        Assert.assertEquals("Ebay", rows.get(0).apply(1).toString());
        Assert.assertEquals("Ebaymotors", rows.get(1).apply(1).toString());
        Assert.assertEquals("Ebay", rows.get(9998).apply(1).toString());
        Assert.assertEquals("英国", rows.get(9999).apply(1).toString());
    }

    @Test
    public void testNSparkCubingJobUsingModelUuid() {
        String modelAlias = "nmodel_basic_alias";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());

        // set model alias
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, getProject());
        NDataModel dataModel = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setAlias(modelAlias);
        dataModelManager.updateDataModelDesc(dataModel);

        cleanupSegments(dsMgr, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                null);

        String targetSubject = job.getTargetSubject();
        Assert.assertEquals(dataModel.getUuid(), targetSubject);
    }

    @Test
    public void testSparkExecutable_WrapConfig() {
        val project = "default";
        NSparkExecutable executable = new NSparkExecutable();
        executable.setProject(project);

        NProjectManager.getInstance(getTestConfig()).updateProject(project, copyForWrite -> {
            LinkedHashMap<String, String> overrideKylinProps = copyForWrite.getOverrideKylinProps();
            overrideKylinProps.put("kylin.engine.spark-conf.spark.locality.wait", "10");
        });
        // get SparkConfigOverride from project overrideProps
        KylinConfig config = executable.getConfig();
        Assert.assertEquals(getTestConfig(), config.base());
        Assert.assertNull(getTestConfig().getSparkConfigOverride().get("spark.locality.wait"));
        Assert.assertEquals("10", config.getSparkConfigOverride().get("spark.locality.wait"));

        // get SparkConfigOverride from indexPlan overrideProps
        final String uuid = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        executable.setParam(NBatchConstants.P_DATAFLOW_ID, uuid);
        NIndexPlanManager.getInstance(getTestConfig(), project).updateIndexPlan(uuid, copyForWrite -> {
            final LinkedHashMap<String, String> overrideProps = copyForWrite.getOverrideProps();
            overrideProps.put("kylin.engine.spark-conf.spark.locality.wait", "20");
        });
        config = executable.getConfig();
        Assert.assertEquals(getTestConfig(), config.base());
        Assert.assertNull(getTestConfig().getSparkConfigOverride().get("spark.locality.wait"));
        Assert.assertEquals("20", config.getSparkConfigOverride().get("spark.locality.wait"));
    }

    @Test
    public void testLayoutIdMoreThan10000() {
        NSparkExecutable executable = Mockito.spy(NSparkExecutable.class);
        Set<Long> randomLayouts = Sets.newHashSet();
        for (int i = 0; i < 100000; i++) {
            randomLayouts.add(RandomUtils.nextLong(1, 100000));
        }
        Mockito.doReturn(executable.getParams()).when(executable).filterEmptySegments(Mockito.anyMap());
        executable.setParam(P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(randomLayouts));
        Set<Long> layouts = NSparkCubingUtil.str2Longs(executable.getParam(P_LAYOUT_IDS));
        randomLayouts.removeAll(layouts);
        Assert.assertEquals(0, randomLayouts.size());
    }

    @Test
    public void testFilterEmptySegments() {

        String project = getProject();
        String dfId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String segmentId = "ef5e0663-feba-4ed2-b71c-21958122bbff";

        NSparkExecutable executable = Mockito.spy(NSparkExecutable.class);
        Map<String, String> originParams = Maps.newHashMap();
        originParams.put(NBatchConstants.P_SEGMENT_IDS, "s1,s2," + segmentId);

        Mockito.doReturn(dfId).when(executable).getDataflowId();
        executable.setProject(project);

        Assert.assertEquals(executable.filterEmptySegments(originParams).get(NBatchConstants.P_SEGMENT_IDS), segmentId);
    }

    @Test
    public void testBuildFromFlatTable() throws Exception {
        overwriteSystemProp("kylin.storage.provider.20", MockupStorageEngine.class.getName());

        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        String dfName = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        cleanupSegments(dsMgr, dfName);
        NDataflow df = dsMgr.getDataflow(dfName);
        IndexPlan indexPlan = df.getIndexPlan();
        IndexEntity ie = indexPlan.getIndexEntity(10000);
        IndexEntity ie2 = indexPlan.getIndexEntity(30000);
        List<LayoutEntity> layouts = new ArrayList<>();
        layouts.addAll(ie.getLayouts());
        layouts.addAll(ie2.getLayouts());
        indexDataConstructor.buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layouts), true);
    }

    @Test
    public void testSafetyIfDiscard() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        cleanupSegments(dsMgr, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        NDataSegment secondSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(11L, 12L));
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(layouts.get(0));
        round1.add(layouts.get(1));
        // Round1. Build new segment
        NSparkCubingJob job1 = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN",
                JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        NSparkCubingJob job2 = NSparkCubingJob.create(Sets.newHashSet(secondSeg), Sets.newLinkedHashSet(round1),
                "ADMIN", JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        NSparkCubingJob refreshJob = NSparkCubingJob.create(Sets.newHashSet(secondSeg), Sets.newLinkedHashSet(round1),
                "ADMIN", JobTypeEnum.INDEX_REFRESH, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        execMgr.addJob(job1);
        execMgr.addJob(job2);
        execMgr.addJob(refreshJob);

        execMgr.updateJobOutput(job1.getId(), ExecutableState.READY);
        execMgr.updateJobOutput(job2.getId(), ExecutableState.READY);
        Assert.assertTrue(job1.safetyIfDiscard());
        Assert.assertTrue(job2.safetyIfDiscard());

        NDataSegment thirdSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(20L, 22L));
        NSparkCubingJob job3 = NSparkCubingJob.create(Sets.newHashSet(thirdSeg), Sets.newLinkedHashSet(round1), "ADMIN",
                JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        execMgr.addJob(job3);
        execMgr.updateJobOutput(job1.getId(), ExecutableState.RUNNING);
        Assert.assertTrue(job1.safetyIfDiscard());
        Assert.assertFalse(job2.safetyIfDiscard());
        Assert.assertTrue(job3.safetyIfDiscard());

        execMgr.updateJobOutput(job1.getId(), ExecutableState.SUCCEED);
        Assert.assertTrue(job1.safetyIfDiscard());
        Assert.assertFalse(job2.safetyIfDiscard());

        Assert.assertTrue(refreshJob.safetyIfDiscard());

        // drop data flow, and check suicide
        cleanupSegments(dsMgr, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df2 = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NDataSegment singleSeg = dsMgr.appendSegment(df2, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        List<LayoutEntity> layouts2 = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round2 = new ArrayList<>();
        round2.add(layouts2.get(0));

        NSparkCubingJob job4 = NSparkCubingJob.create(Sets.newHashSet(singleSeg), Sets.newLinkedHashSet(round2),
                "ADMIN", JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        execMgr.addJob(job4);
        execMgr.updateJobOutput(job4.getId(), ExecutableState.RUNNING);

        dsMgr.dropDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertTrue(job4.checkSuicide());
        Assert.assertTrue(job4.safetyIfDiscard());
    }

    @Ignore
    @Test
    public void testResumeBuildCheckPoints() throws Exception {
        final String project = getProject();
        final KylinConfig config = getTestConfig();
        final String dfId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        overwriteSystemProp("kylin.engine.spark.build-class-name",
                "MockResumeBuildJob");
        // prepare segment
        final NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
        final NExecutableManager execMgr = NExecutableManager.getInstance(config, project);

        // clean segments and jobs
        cleanupSegments(dfMgr, dfId);
        NDataflow df = dfMgr.getDataflow(dfId);
        Assert.assertEquals(0, df.getSegments().size());
        NDataSegment newSegment = dfMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        // available layouts: 1L, 10_001L, 10_002L, 20_001L, 30_001L, 1_000_001L
        // 20_000_000_001L, 20_000_010_001L, 20_000_020_001L, 20_000_030_001L
        List<LayoutEntity> layouts = new ArrayList<>();
        // this layout contains count_distinct
        // dict building simulation
        final long cntDstLayoutId = 1_000_001L;
        final long normalLayoutId = 20_000_010_001L;
        layouts.add(df.getIndexPlan().getLayoutEntity(cntDstLayoutId));
        layouts.add(df.getIndexPlan().getLayoutEntity(normalLayoutId));

        // prepare job
        final NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(newSegment), Sets.newLinkedHashSet(layouts),
                "test_submitter", null);
        NSparkCubingStep cubeStep = job.getSparkCubingStep();
        // set break points
        cubeStep.setParam(NBatchConstants.P_BREAK_POINT_LAYOUTS, String.valueOf(cntDstLayoutId));

        final KylinConfig metaConf = KylinConfig.createKylinConfig(config);
        metaConf.setMetadataUrl(cubeStep.getParam(NBatchConstants.P_DIST_META_URL));

        final KylinConfig metaOutConf = KylinConfig.createKylinConfig(config);
        metaOutConf.setMetadataUrl(cubeStep.getParam(NBatchConstants.P_OUTPUT_META_URL));

        TableDesc tableDesc = df.getModel().getRootFactTableRef().getTableDesc();
        final String originTableType = tableDesc.getTableType();
        try {
            // fact-view persisting simulation
            tableDesc.setTableType("VIEW");
            NTableMetadataManager.getInstance(config, project).updateTableDesc(tableDesc);

            // job scheduling simulation
            execMgr.addJob(job);
            Assert.assertFalse(execMgr.getJobOutput(cubeStep.getId()).isResumable());
            await().atMost(40, TimeUnit.SECONDS).pollDelay(5, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        final KylinConfig tempConf = KylinConfig.createKylinConfig(metaConf);
                        try {
                            // ensure that meta data were uploaded
                            Assert.assertTrue(execMgr.getJobOutput(cubeStep.getId()).isResumable());
                            NDataflow tempDf = NDataflowManager.getInstance(tempConf, project).getDataflow(dfId);
                            Assert.assertNotNull(tempDf);
                            Assert.assertFalse(tempDf.isBroken());
                            Assert.assertEquals(1, tempDf.getSegments().size());
                            NDataSegment tempSegment = tempDf.getSegments().getFirstSegment();
                            Assert.assertNotNull(tempSegment.getLayout(normalLayoutId));
                        } finally {
                            ResourceStore.clearCache(tempConf);
                        }
                    });
        } finally {
            //set back table type
            tableDesc.setTableType(originTableType);
            NTableMetadataManager.getInstance(config, project).updateTableDesc(tableDesc);
        }

        // pause job simulation
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).pauseJob(job.getId());
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());

        // job would be resumable after pause
        Assert.assertTrue(execMgr.getJobOutput(cubeStep.getId()).isResumable());

        // checkpoints
        KylinConfig tempMetaConf = KylinConfig.createKylinConfig(metaConf);
        NDataflow remoteDf = NDataflowManager.getInstance(tempMetaConf, project).getDataflow(dfId);
        Assert.assertEquals(1, remoteDf.getSegments().size());
        NDataSegment remoteSegment = remoteDf.getSegments().getFirstSegment();
        Assert.assertTrue(remoteSegment.isFlatTableReady());
        Assert.assertTrue(remoteSegment.isDictReady());
        Assert.assertTrue(remoteSegment.isFactViewReady());
        Assert.assertNotNull(remoteSegment.getLayout(normalLayoutId));
        // break points layouts wouldn't be ready
        Assert.assertNull(remoteSegment.getLayout(cntDstLayoutId));

        ResourceStore.clearCache(tempMetaConf);

        // remove break points, then resume job
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NExecutableManager tempExecMgr = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            tempExecMgr.removeBreakPoints(cubeStep.getId());
            tempExecMgr.resumeJob(job.getId());
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());

        // till job finished
        IndexDataConstructor.wait(job);

        // btw, we should also check the "skip xxx" log,
        // but the /path/to/job_tmp/job_id/01/meta/execute_output.json.xxx.log not exists in ut env.
        tempMetaConf = KylinConfig.createKylinConfig(metaConf);
        remoteDf = NDataflowManager.getInstance(tempMetaConf, project).getDataflow(dfId);
        Assert.assertEquals(1, remoteDf.getSegments().size());
        remoteSegment = remoteDf.getSegments().getFirstSegment();
        Assert.assertTrue(remoteSegment.isFlatTableReady());
        Assert.assertTrue(remoteSegment.isDictReady());
        Assert.assertTrue(remoteSegment.isFactViewReady());
        Assert.assertNotNull(remoteSegment.getLayout(normalLayoutId));
        Assert.assertNotNull(remoteSegment.getLayout(cntDstLayoutId));
        ResourceStore.clearCache(tempMetaConf);

        // sorry, at present, job restart simulation unstable
        //// restart job simulation
        // EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
        //     NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).restartJob(job.getId());
        //     return null;
        // }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID, job.getId());
        // // job wouldn't be resumable after restart
        // Assert.assertFalse(execMgr.getJobOutput(cubeStep.getId()).isResumable());
        // 
        // wait(job);

        // checkpoints should not cross building jobs
        NDataflow remoteOutDf = NDataflowManager.getInstance(metaOutConf, project).getDataflow(dfId);
        NDataSegment remoteOutSegment = remoteOutDf.getSegments().getFirstSegment();
        Assert.assertFalse(remoteOutSegment.isFlatTableReady());
        Assert.assertFalse(remoteOutSegment.isDictReady());
        Assert.assertFalse(remoteOutSegment.isFactViewReady());

        ResourceStore.clearCache(metaConf);
        ResourceStore.clearCache(metaOutConf);
    }

    private void cleanupSegments(NDataflowManager dsMgr, String dfName) {
        NDataflow df = dsMgr.getDataflow(dfName);

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
    }

    public static class MockParquetStorage extends ParquetStorage {

        @Override
        public Dataset<Row> getFrom(String path, SparkSession ss) {
            return super.getFrom(path, ss);
        }

        @Override
        public void saveTo(String path, Dataset<Row> data, SparkSession ss) {
            Option<LogicalPlan> option = data.queryExecution().optimizedPlan()
                    .find(new AbstractFunction1<LogicalPlan, Object>() {
                        @Override
                        public Object apply(LogicalPlan v1) {
                            return v1 instanceof Join;
                        }
                    });
            Assert.assertFalse(option.isDefined());
            super.saveTo(path, data, ss);
        }
    }

    public static class MockupStorageEngine implements IStorage {

        @Override
        public IStorageQuery createQuery(IRealization realization) {
            return null;
        }

        @Override
        public <I> I adaptToBuildEngine(Class<I> engineInterface) {
            Class clz;
            try {
                clz = Class.forName("org.apache.kylin.engine.spark.NSparkCubingEngine$NSparkCubingStorage");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (engineInterface == clz) {
                return (I) ClassUtil
                        .newInstance("NSparkCubingJobTest$MockParquetStorage");
            } else {
                throw new RuntimeException("Cannot adapt to " + engineInterface);
            }
        }
    }

}
