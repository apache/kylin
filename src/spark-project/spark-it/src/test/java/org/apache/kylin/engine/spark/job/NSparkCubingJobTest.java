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

import static org.apache.kylin.job.execution.JobTypeEnum.LAYOUT_DATA_OPTIMIZE;
import static org.apache.kylin.job.execution.JobTypeEnum.SUB_PARTITION_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SUB_PARTITION_REFRESH;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_LAYOUT_IDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.cuboid.NCuboidLayoutChooser;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTree;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataLayoutDetails;
import org.apache.kylin.metadata.cube.model.NDataLayoutDetailsManager;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.cube.model.SegmentPartition;
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
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.datasource.storage.StorageStoreFactory;
import org.awaitility.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.sparkproject.guava.collect.Sets;

import lombok.val;
import scala.Option;
import scala.runtime.AbstractFunction1;

public class NSparkCubingJobTest extends NLocalWithSparkSessionTest {

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
        await().timeout(Duration.ONE_MINUTE).untilAsserted(() -> Assert
                .assertFalse(JobContextUtil.getJobContext(getTestConfig()).getJobScheduler().hasRunningJob()));
        JobContextUtil.cleanUp();
        super.tearDown();
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

    @Test
    public void testCalculateTableTotalRows() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_ORDER");
        long totalRows = new SnapshotBuilder().calculateTableTotalRows(null, tableDesc, ss);
        Assert.assertEquals(5000, totalRows);
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
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        cleanupSegments(dfName);
        NDataflow df = dsMgr.getDataflow(dfName);

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = indexDataConstructor.addSegment(dfName,
                SegmentRange.TimePartitionedSegmentRange.createInfinite(), null);
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

        val segUuid = oneSeg.getId();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val conf = getTestConfig();
            val merger = new AfterBuildResourceMerger(conf, getProject());
            merger.mergeAfterIncrement(df.getUuid(), segUuid, ExecutableUtils.getLayoutIds(sparkStep),
                    ExecutableUtils.getRemoteStore(conf, sparkStep));
            return true;
        }, getProject());

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
        val newSparkStep = job.getSparkCubingStep();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val conf = getTestConfig();
            val merger = new AfterBuildResourceMerger(conf, getProject());
            merger.mergeAfterCatchup(df2.getUuid(), Sets.newHashSet(segUuid),
                    ExecutableUtils.getLayoutIds(newSparkStep), ExecutableUtils.getRemoteStore(config, newSparkStep),
                    null);
            return true;
        }, getProject());

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
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        cleanupSegments(dfName);
        NDataflow df = dsMgr.getDataflow(dfName);

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = indexDataConstructor.addSegment(dfName,
                SegmentRange.TimePartitionedSegmentRange.createInfinite(), null);
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

        // add ExcludedTable
        MetadataTestUtils.mockExcludedTable(getProject(), df.getModel().getRootFactTableName());

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
        cleanupSegments(dfName);
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
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        cleanupSegments(dataflowId);
        NDataflow df = dsMgr.getDataflow(dataflowId);

        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(df.getIndexPlan().getLayoutEntity(20_000_020_001L));
        round1.add(df.getIndexPlan().getLayoutEntity(1_000_001L));
        round1.add(df.getIndexPlan().getLayoutEntity(30001L));
        round1.add(df.getIndexPlan().getLayoutEntity(10002L));
        NDataSegment oneSeg = indexDataConstructor.addSegment(dataflowId,
                SegmentRange.TimePartitionedSegmentRange.createInfinite(), null);
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
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        cleanupSegments(dataflowId);
        NDataflow df = dsMgr.getDataflow(dataflowId);

        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(df.getIndexPlan().getLayoutEntity(10002L));
        NDataSegment seg1 = indexDataConstructor.addSegment(dataflowId,
                new SegmentRange.TimePartitionedSegmentRange("2012-01-01", "2012-02-01"), SegmentStatusEnum.READY,
                null);
        NDataSegment seg2 = indexDataConstructor.addSegment(dataflowId,
                new SegmentRange.TimePartitionedSegmentRange("2012-02-01", "2012-03-01"), SegmentStatusEnum.READY,
                null);

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(seg1, seg2), Sets.newLinkedHashSet(round1),
                "ADMIN", null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        execMgr.addJob(job);
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val conf = getTestConfig();
            val merger = new AfterBuildResourceMerger(conf, getProject());
            merger.mergeAfterCatchup(df.getUuid(), Sets.newHashSet(seg1.getId(), seg2.getId()), Sets.newHashSet(10002L),
                    ExecutableUtils.getRemoteStore(conf, sparkStep), null);
            return true;
        }, getProject());

        List<NDataSegment> segs = dsMgr.getDataflow(dataflowId).getSegments();
        Assert.assertEquals(2, segs.size());
        // test if segs are updated
        Assert.assertTrue(segs.get(0).isFlatTableReady());
        Assert.assertTrue(segs.get(1).isFlatTableReady());
    }

    @Test
    public void testCancelCubingJob() {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());
        cleanupSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());
        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = indexDataConstructor.addSegment(df.getUuid(),
                SegmentRange.TimePartitionedSegmentRange.createInfinite(), null);
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
            ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).discardJob(job.getId());
            return null;
        }, getProject(), UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID);
        dsMgr = NDataflowManager.getInstance(config, getProject());
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());
    }

    @Test
    public void testCancelMergingJob() throws Exception {

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());
        cleanupSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
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
        NDataSegment firstMergeSeg = indexDataConstructor.mergeSegment("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-02"),
                        SegmentRange.dateToLong("2013-01-01")),
                false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        execMgr.addJob(firstMergeJob);
        await().untilAsserted(() -> Assert.assertEquals(ExecutableState.RUNNING, firstMergeJob.getStatus()));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                    .discardJob(firstMergeJob.getId());
            return null;
        }, getProject(), UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID);
        dsMgr = NDataflowManager.getInstance(config, getProject());
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(2, df.getSegments().size());
    }

    @Test
    public void testUpdatePartitionOnCancelJob() {
        NDataflowManager dfManager = Mockito.mock(NDataflowManager.class);
        NDataflow df = Mockito.mock(NDataflow.class);
        NDataSegment segment = Mockito.mock(NDataSegment.class);
        NSparkCubingJob job = Mockito.spy(NSparkCubingJob.class);
        NSparkCubingStep step = Mockito.spy(NSparkCubingStep.class);

        Mockito.when(dfManager.getDataflow(any())).thenReturn(df);
        
        Mockito.when(job.getSparkCubingStep()).thenReturn(step);
        Mockito.when(job.isBucketJob()).thenReturn(true);
        
        HashSet<Long> partitions = Sets.newHashSet(1L, 2L, 3L);
        step.setParam(NBatchConstants.P_SEGMENT_IDS, "segment_01");
        step.setParam(NBatchConstants.P_DATAFLOW_ID, "dataflow_01");
        Mockito.when(step.getTargetPartitions()).thenReturn(partitions);
        
        Mockito.when(df.copy()).thenReturn(df);

        List<SegmentPartition> parts = Arrays.asList(new SegmentPartition(1L), new SegmentPartition(2L),
                new SegmentPartition(4L));
        parts.get(0).setStatus(PartitionStatusEnum.REFRESH);
        Mockito.when(segment.getMultiPartitions()).thenReturn(parts);
        Mockito.when(segment.copy()).thenReturn(segment);

        testAndCheck(dfManager, df, job, segment, LAYOUT_DATA_OPTIMIZE, false);
        testAndCheck(dfManager, df, job, segment, SUB_PARTITION_BUILD, true);
        testAndCheck(dfManager, df, job, segment, SUB_PARTITION_BUILD, false);
        testAndCheck(dfManager, df, job, segment, SUB_PARTITION_REFRESH, true);
        testAndCheck(dfManager, df, job, segment, SUB_PARTITION_REFRESH, false);
    }
    
    private void testAndCheck(NDataflowManager dfManager, NDataflow df, NSparkCubingJob job, NDataSegment segment,
            JobTypeEnum jobType, boolean nullSegment) {
        job.setJobType(jobType);
        Mockito.when(df.getSegment(any())).thenReturn(nullSegment ? null : segment);
        job.updatePartitionOnCancelJob(dfManager);

        Set<Long> partitions = job.getSparkCubingStep().getTargetPartitions();

        if(jobType == SUB_PARTITION_BUILD) {
            if (nullSegment) {
                verify(dfManager, never()).removeLayoutPartition(eq(df.getId()), eq(partitions), anySet());
                verify(dfManager, never()).removeSegmentPartition(eq(df.getId()), eq(partitions), anySet());
            } else {
                verify(dfManager).removeLayoutPartition(eq(df.getId()), eq(partitions), anySet());
                verify(dfManager).removeSegmentPartition(eq(df.getId()), eq(partitions), anySet());
            }
        } else if (jobType == SUB_PARTITION_REFRESH) {
            if (nullSegment) {
                verify(dfManager, never()).updateDataflow(any());
            } else {
                Assert.assertEquals(segment.getMultiPartitions().get(0).getStatus(), PartitionStatusEnum.READY);
                Assert.assertEquals(segment.getMultiPartitions().get(1).getStatus(), PartitionStatusEnum.NEW);
                Assert.assertEquals(segment.getMultiPartitions().get(2).getStatus(), PartitionStatusEnum.NEW);
                ArgumentCaptor<NDataflowUpdate> updateCaptor = ArgumentCaptor.forClass(NDataflowUpdate.class);
                verify(dfManager).updateDataflow(updateCaptor.capture());
            }
        } else {
            verify(dfManager, never()).removeLayoutPartition(eq(df.getId()), eq(partitions), anySet());
            verify(dfManager, never()).removeSegmentPartition(eq(df.getId()), eq(partitions), anySet());
            verify(dfManager, never()).updateDataflow(any());
        }
    }

    @Test
    public void testGetJobNodeInfo() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        cleanupSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                null);

        // launch the job
        execMgr.addJob(job);

        // wait job done
        IndexDataConstructor.wait(job);
        System.out.println("Job status" + job.getStatus().name());

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
        val storageStore = StorageStoreFactory.create(seg.getModel().getStorageType());
        Dataset<Row> ret = storage.getFrom(storageStore.getStoragePath(seg, layout.getId()), ss);
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

        cleanupSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
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
            overrideKylinProps.put("kylin.engine.spark-conf.spark.kubernetes.file.upload.path", "/tmp");
        });
        // get SparkConfigOverride from project overrideProps
        KylinConfig config = executable.getConfig();
        Assert.assertEquals("/tmp/" + executable.getId(), config.getKubernetesUploadPath());
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
        cleanupSegments(dfName);
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
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());
        cleanupSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
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

        //execMgr.updateJobOutput(job1.getId(), ExecutableState.READY);
        //execMgr.updateJobOutput(job2.getId(), ExecutableState.READY);
        Assert.assertTrue(job1.safetyIfDiscard());
        Assert.assertTrue(job2.safetyIfDiscard());

        NDataSegment thirdSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(20L, 22L));
        NSparkCubingJob job3 = NSparkCubingJob.create(Sets.newHashSet(thirdSeg), Sets.newLinkedHashSet(round1), "ADMIN",
                JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        execMgr.addJob(job3);
        await().untilAsserted(() -> Assert.assertEquals(ExecutableState.RUNNING, job1.getStatus()));
        Assert.assertTrue(job1.safetyIfDiscard());
        Assert.assertFalse(job2.safetyIfDiscard());
        Assert.assertTrue(job3.safetyIfDiscard());

        JobContextUtil.withTxAndRetry(() -> {
            execMgr.updateJobOutput(job1.getId(), ExecutableState.SUCCEED);
            return true;
        });
        Assert.assertTrue(job1.safetyIfDiscard());
        Assert.assertFalse(job2.safetyIfDiscard());

        Assert.assertTrue(refreshJob.safetyIfDiscard());

        // drop data flow, and check suicide
        cleanupSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflow df2 = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NDataSegment singleSeg = dsMgr.appendSegment(df2, new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        List<LayoutEntity> layouts2 = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round2 = new ArrayList<>();
        round2.add(layouts2.get(0));

        NSparkCubingJob job4 = NSparkCubingJob.create(Sets.newHashSet(singleSeg), Sets.newLinkedHashSet(round2),
                "ADMIN", JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), Sets.newHashSet(), null, null);
        execMgr.addJob(job4);
        await().untilAsserted(() -> Assert.assertEquals(ExecutableState.RUNNING, job4.getStatus()));

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
        overwriteSystemProp("kylin.engine.spark.build-class-name", "MockResumeBuildJob");
        // prepare segment
        final NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
        final ExecutableManager execMgr = ExecutableManager.getInstance(config, project);

        // clean segments and jobs
        cleanupSegments(dfId);
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
            ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).pauseJob(job.getId());
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID);

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
            ExecutableManager tempExecMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            tempExecMgr.removeBreakPoints(cubeStep.getId());
            tempExecMgr.resumeJob(job.getId());
            return null;
        }, project, UnitOfWork.DEFAULT_MAX_RETRY, UnitOfWork.DEFAULT_EPOCH_ID);

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
        //     ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).restartJob(job.getId());
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

    @Test
    public void testFullBuildJobV3() throws InterruptedException {
        String dfName = "53bb6ab4-8058-4696-bc06-597a5e9a9103";
        String project = "storage_v3_test";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        ExecutableManager execMgr = ExecutableManager.getInstance(config, project);

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));
        cleanupSegments(dfName, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        NDataSegment oneSeg = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager m = NDataflowManager.getInstance(getTestConfig(), project);
            return m.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        }, project);
        List<LayoutEntity> round = Lists.newArrayList();
        List<Long> layoutIdList = Lists.newArrayList();
        round.add(df.getIndexPlan().getLayoutEntity(1L));
        layoutIdList.add(1L);
        round.add(df.getIndexPlan().getLayoutEntity(30001L));
        layoutIdList.add(30001L);
        round.add(df.getIndexPlan().getLayoutEntity(20001L));
        layoutIdList.add(20001L);

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(round, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg, rootCuboid);
            Assert.assertNull(layout);
        }

        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round), "ADMIN",
                JobTypeEnum.INDEX_REFRESH, RandomUtil.randomUUIDStr(), null, null, null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        System.out.println("v3-ut-test" + job.getOutput().getShortErrMsg());
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        List<NDataLayoutDetails> nDataLayoutDetails = df.listAllLayoutDetails();

        nDataLayoutDetails.forEach(dataLayoutFragment -> {
            if (layoutIdList.contains(dataLayoutFragment.getLayoutId())) {
                Assert.assertEquals(1, dataLayoutFragment.getFragmentRangeSet().asRanges().size());
            } else {
                Assert.assertEquals(0, dataLayoutFragment.getFragmentRangeSet().asRanges().size());
            }
        });
        NDataLayoutDetails nDataLayoutDetails1 = NDataLayoutDetailsManager.getInstance(config, project)
                .getNDataLayoutDetails(df.getUuid(), 20001L);
        Assert.assertEquals(1, nDataLayoutDetails1.getNumOfFiles());
        Assert.assertEquals(1, nDataLayoutDetails1.getTableVersion());
    }

    @Test
    public void testIncBuildJobV3() throws InterruptedException {
        String dfName = "7d840904-7b34-4edd-aabd-79df992ef32e";
        String project = "storage_v3_test";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        ExecutableManager execMgr = ExecutableManager.getInstance(config, project);
        cleanupSegments(dfName, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        NDataSegment segment1 = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager m = NDataflowManager.getInstance(getTestConfig(), project);
            return m.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange("2012-01-01", "2012-02-01"));
        }, project);
        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(df.getIndexPlan().getLayoutEntity(1L));
        round1.add(df.getIndexPlan().getLayoutEntity(30001L));
        round1.add(df.getIndexPlan().getLayoutEntity(20001L));

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment1), Sets.newLinkedHashSet(round1), "ADMIN",
                JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), null, null, null);
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = IndexDataConstructor.wait(job);
        System.out.println("v3-ut-test" + job.getOutput().getShortErrMsg());
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        List<NDataLayoutDetails> nDataLayoutDetails = NDataLayoutDetailsManager.getInstance(config, project)
                .listNDataLayoutDetailsByModel(df.getUuid());
        Assert.assertEquals(3, nDataLayoutDetails.size());
        nDataLayoutDetails.forEach(dataLayoutFragment -> {
            Assert.assertEquals(1, dataLayoutFragment.getFragmentRangeSet().asRanges().size());
        });
        NDataLayoutDetails nDataLayoutDetails1 = NDataLayoutDetailsManager.getInstance(config, project)
                .getNDataLayoutDetails(df.getUuid(), 20001L);
        Assert.assertEquals(StorageStoreFactory.create(df.getModel().getStorageType())
                .getStoragePath(df.getIndexPlan().getLayoutEntity(20001L)), nDataLayoutDetails1.getLocation());
        Assert.assertEquals(1, nDataLayoutDetails1.getNumOfFiles());
        Assert.assertEquals(1, nDataLayoutDetails1.getTableVersion());
    }

    @Test
    public void testV3ConcurrentBuildJob() throws InterruptedException, ParseException {
        String dfName = "7d840904-7b34-4edd-aabd-79df992ef32e";
        int concurrentNum = 1;
        config.setProperty("kylin.job.max-concurrent-jobs", String.valueOf(concurrentNum));
        String project = "storage_v3_test";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        ExecutableManager execMgr = ExecutableManager.getInstance(config, project);
        overwriteSystemProp("kylin.engine.persist-flatview", "true");
        overwriteSystemProp("kylin.engine.spark.delta-storage-write-retry-times", "10");
        cleanupSegments(dfName, project);
        String initDate = "2012-01-01";
        long startTime = new SimpleDateFormat("yyyy-MM-dd").parse(initDate).getTime();
        long endTime;
        List<AbstractExecutable> jobList = new ArrayList<>();
        for (int offset = 0; offset < concurrentNum; offset++) {
            NDataflow df = dsMgr.getDataflow(dfName);
            startTime = startTime + offset * 1000 * 60 * 60 * 24;
            endTime = startTime + (offset + 1) * 1000 * 60 * 60 * 24;

            long finalStartTime = startTime;
            long finalEndTime = endTime;
            NDataSegment segment = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowManager m = NDataflowManager.getInstance(getTestConfig(), project);
                return m.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(finalStartTime, finalEndTime));
            }, project);
            List<LayoutEntity> round1 = new ArrayList<>();
            round1.add(df.getIndexPlan().getLayoutEntity(1L));
            round1.add(df.getIndexPlan().getLayoutEntity(30001L));
            round1.add(df.getIndexPlan().getLayoutEntity(20001L));
            NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), Sets.newLinkedHashSet(round1),
                    "ADMIN", JobTypeEnum.INC_BUILD, RandomUtil.randomUUIDStr(), null, null, null);
            execMgr.addJob(job);
            jobList.add(job);
        }
        IndexDataConstructor.wait(jobList);
        jobList.forEach(job -> {
            System.out.println("v3-ut-test" + job.getOutput().getShortErrMsg());
            Assert.assertEquals(ExecutableState.SUCCEED, job.getStatus());
        });
    }

    private void cleanupSegments(String dfName) {
        cleanupSegments(dfName, getProject());
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
                return (I) ClassUtil.newInstance("NSparkCubingJobTest$MockParquetStorage");
            } else {
                throw new RuntimeException("Cannot adapt to " + engineInterface);
            }
        }
    }

}
