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

import static io.kyligence.kap.secondstorage.SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT;
import static io.kyligence.kap.secondstorage.SecondStorageConcurrentTestUtil.registerWaitPoint;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_LOCKING;
import static org.apache.kylin.common.exception.ServerErrorCode.SEGMENT_DROP_FAILED;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseSegmentCleanJob;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.test.ClickHouseClassRule;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.val;

public class IncrementalWithIntPartitionTest implements JobWaiter {
    static private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";

    public static String getProject() {
        return project;
    }

    static private final String project = "table_index_incremental_with_int_date";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "org.apache.kylin.query.SQLPushDownExtensions"));
    private final SparkSession sparkSession = sharedSpark.getSpark();
    @ClassRule
    public static ClickHouseClassRule clickHouseClassRule = new ClickHouseClassRule(2);
    public EnableTestUser enableTestUser = new EnableTestUser();
    public EnableClickHouseJob test = new EnableClickHouseJob(clickHouseClassRule.getClickhouse(), 1, project,
            Collections.singletonList(modelId), "src/test/resources/ut_meta");
    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(test);
    private SecondStorageService secondStorageService = new SecondStorageService();
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();
    private IndexDataConstructor indexDataConstructor;

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Before
    public void setUp() {
        System.setProperty("kylin.second-storage.wait-index-build-second", "1");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        indexDataConstructor = new IndexDataConstructor(project);

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        secondStorageService.setAclEvaluate(aclEvaluate);
    }

    private void buildIncrementalLoadQuery(String start, String end) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = modelId;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, project);
        NDataflow df = dsMgr.getDataflow(dfName);
        val timeRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        val indexes = new HashSet<>(df.getIndexPlan().getAllLayouts());
        indexDataConstructor.buildIndex(dfName, timeRange, indexes, true);

        waitJobFinish(project, triggerClickHouseLoadJob(project, modelId, "ADMIN", dsMgr.getDataflow(modelId)
                .getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList())));
    }

    private void mergeSegments(List<String> segIds) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(modelId);
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
        val jobParam = new JobParam(mergeSeg, modelId, enableTestUser.getUser());
        val jobId = jobManager.mergeSegmentJob(jobParam);
        waitJobFinish(getProject(), jobId);
    }

    @Test
    public void testMergeSegmentWhenSegmentNotInSecondStorage() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        // clean first segment
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        secondStorageEndpoint.cleanStorage(request, segs.subList(0, 1));

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val jobs = executableManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        jobs.forEach(job -> waitJobFinish(getProject(), job.getId()));

        mergeSegments(segs);
        Assert.assertEquals(1, dataflowManager.getDataflow(modelId).getQueryableSegments().size());
        checkSizeInNode();
        secondStorageService.sizeInNode(project);
        checkSizeInNode();
    }

    @Test
    public void testRemoveSegmentWhenHasLoadTask() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val range = SegmentRange.TimePartitionedSegmentRange.createInfinite();
        SecondStorageLockUtils.acquireLock(modelId, range).lock();
        try {
            SecondStorageUtil.checkSegmentRemove(project, modelId, segs.toArray(new String[] {}));
        } catch (KylinException e) {
            SecondStorageLockUtils.unlock(modelId, range);
            Assert.assertEquals(SEGMENT_DROP_FAILED.toErrorCode(), e.getErrorCode());
            return;
        }
        SecondStorageLockUtils.unlock(modelId, range);
        Assert.fail();
    }

    private void checkSizeInNode() {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        val expectSegments = dataflowManager.getDataflow(modelId).getSegments().stream().map(NDataSegment::getId)
                .collect(Collectors.toSet());
        val tableData = tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0);
        Assert.assertTrue(tableData.containSegments(expectSegments));
        Long wholeSize = tableData.getPartitions().get(0).getSizeInNode().values().stream().reduce(Long::sum)
                .orElse(0L);
        Assert.assertTrue(wholeSize > 0);
    }

    @Test
    public void testRemoveSegmentFromSecondStorage() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val jobs = executableManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        jobs.forEach(job -> waitJobFinish(getProject(), job.getId()));
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        val tableData = tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0);
        Assert.assertEquals(1, tableData.getPartitions().size());
        val count = getModelRowCount(project, modelId);
        Assert.assertTrue(count > 0);
        // clean first segment
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        secondStorageEndpoint.cleanStorage(request, segs.subList(0, 1));

        val jobs2 = executableManager.listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        jobs2.forEach(job -> waitJobFinish(getProject(), job.getId()));

        val tableData2 = tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0);
        Assert.assertEquals(0, tableData2.getPartitions().size());
        val newCount = getModelRowCount(project, modelId);
        Assert.assertTrue(newCount == 0);
    }

    public static int getModelRowCount(String project, String modelId) throws SQLException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val database = NameUtil.getDatabase(config, project);
        val table = NameUtil.getTable(modelId, 20000000001L);
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        return SecondStorageNodeHelper.getAllNames().stream().map(SecondStorageNodeHelper::resolve).map(url -> {
            try (ClickHouse clickHouse = new ClickHouse(url)) {
                val count = clickHouse.query("select count(*) from `" + database + "`.`" + table + "`", rs -> {
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
        }).reduce(Integer::sum).get();
    }

    @Test
    public void testRefreshSegmentWhenLocked() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val seg = dataflowManager.getDataflow(modelId).getSegments().getFirstSegment();
        secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "LOCK");
        try {
            refreshSegment(seg.getId(), true);
        } catch (Exception e) {
            KylinException cause = (KylinException) e.getCause();
            Assert.assertEquals(SECOND_STORAGE_PROJECT_LOCKING.toErrorCode(), cause.getErrorCode());
            secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "UNLOCK");
            return;
        }
        Assert.fail();
    }

    private String refreshSegment(String segId, boolean wait) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfMgr = NDataflowManager.getInstance(config, getProject());
        val df = dfMgr.getDataflow(modelId);
        val jobManager = JobManager.getInstance(config, getProject());
        NDataSegment newSeg = dfMgr.refreshSegment(df, df.getSegment(segId).getSegRange());
        val jobParam = new JobParam(newSeg, df.getModel().getId(), enableTestUser.getUser());
        val jobId = jobManager.refreshSegmentJob(jobParam);
        if (wait) {
            waitJobFinish(project, jobId);
        }
        return jobId;
    }

    @Test
    public void testMergeSegmentsWhenLocked() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        // clean first segment
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "LOCK");
        try {
            mergeSegments(segs);
        } catch (Exception e) {
            KylinException cause = (KylinException) e.getCause();
            Assert.assertEquals(SECOND_STORAGE_PROJECT_LOCKING.toErrorCode(), cause.getErrorCode());
            secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "UNLOCK");
            return;
        }
        Assert.fail();
    }

    @Test
    public void testCheckLock() {
        secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "LOCK");
        try {
            LockTypeEnum.checkLock(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(project));
        } catch (KylinException e) {
            Assert.assertEquals(SECOND_STORAGE_PROJECT_LOCKING.toErrorCode(), e.getErrorCode());
            secondStorageService.lockOperate(project, Collections.singletonList("LOAD"), "UNLOCK");
            return;
        }
        Assert.fail();
    }

    @Test
    public void testCleanSegmentWhenDatabaseNotExists() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        val jdbc = SecondStorageNodeHelper.resolve(node);
        ClickHouse clickHouse = new ClickHouse(jdbc);
        val database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply("DROP DATABASE " + database);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val jobId = secondStorageService.triggerSegmentsClean(project, modelId, Sets.newHashSet(segs));
        waitJobFinish(project, jobId);
    }

    @Test
    public void testCleanSegmentWhenModelNotExists() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val node = SecondStorageNodeHelper.getAllNames().get(0);
        val jdbc = SecondStorageNodeHelper.resolve(node);
        ClickHouse clickHouse = new ClickHouse(jdbc);
        val table = NameUtil.getTable(modelId, 20000000001L);
        val database = NameUtil.getDatabase(KylinConfig.getInstanceFromEnv(), project);
        clickHouse.apply("DROP TABLE " + database + "." + table);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val jobId = secondStorageService.triggerSegmentsClean(project, modelId, Sets.newHashSet(segs));
        waitJobFinish(project, jobId);
    }

    @Test
    public void testCleanProjectSegments() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        val jobId = secondStorageService.projectClean(Arrays.asList(project, "default"));
        waitJobFinish(project, jobId.get(project).get(modelId));

        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        val tableData = tableFlowManager.orElseThrow(null).get(modelId).orElseThrow(null).getTableDataList().get(0);
        Assert.assertEquals(0, tableData.getPartitions().size());
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, modelId));
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

    @Ignore("TODO: mark it.")
    public void testJobPaused() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        cleanSegments(segs.subList(1, 2));
        registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_PAUSED, 10000);
        val jobId = refreshSegment(segs.get(1), false);
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            ExecutablePO jobDetail = executableManager.getAllJobs().stream().filter(job -> job.getId().equals(jobId))
                    .findFirst().orElseThrow(() -> new IllegalStateException("Job not found"));
            long finishedCount = jobDetail.getTasks().stream()
                    .filter(task -> "SUCCEED".equals(task.getOutput().getStatus())).count();
            return finishedCount >= 3;
        });
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            executableManager.pauseJob(jobId);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        waitJobEnd(project, jobId);
        try {
            SecondStorageUtil.checkJobResume(project, jobId);
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.JOB_RESUME_FAILED.toErrorCode(), e.getErrorCode());
        }
        //        Thread.sleep(15000);
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        await().atMost(15, TimeUnit.SECONDS).until(() -> scheduler.getContext().getRunningJobs().values().size() == 0);
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        int partitionNum = tableFlowManager.get().get(modelId)
                .orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0)
                .getPartitions().size();
        Assert.assertEquals(1, partitionNum);
        Assert.assertFalse(
                SecondStorageLockUtils.containsKey(modelId, SegmentRange.TimePartitionedSegmentRange.createInfinite()));

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val job = manager.getJob(jobId);
            Assert.assertEquals(ExecutableState.PAUSED, job.getStatus());
            //            Assert.assertEquals("{\"completedSegments\":[],\"completedFiles\":[]}", job.getOutput().getExtra().get(LoadContext.CLICKHOUSE_LOAD_CONTEXT));
        });

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            executableManager.resumeJob(jobId);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            return executableManager.getJob(jobId).getStatus() == ExecutableState.RUNNING;
        });
        waitJobFinish(project, jobId);
        Assert.assertEquals(24, IncrementalWithIntPartitionTest.getModelRowCount(project, modelId));
        SecondStorageUtil.checkSecondStorageData(project);
    }

    @Test
    public void testJobPausedAfterCommit() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        buildIncrementalLoadQuery("2012-01-03", "2012-01-04");
        buildIncrementalLoadQuery("2012-01-04", "2012-01-05");
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        cleanSegments(segs);
        registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_AFTER_COMMIT, 10000);
        val jobId = triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        await().atMost(15, TimeUnit.SECONDS).until(
                () -> SecondStorageConcurrentTestUtil.isWaiting(SecondStorageConcurrentTestUtil.WAIT_AFTER_COMMIT));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            executableManager.pauseJob(jobId);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
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

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            executableManager.resumeJob(jobId);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            return executableManager.getJob(jobId).getStatus() == ExecutableState.RUNNING;
        });
        waitJobFinish(project, jobId);
        int pn = tableFlowManager.get().get(modelId).orElseThrow(() -> new IllegalStateException("tableflow not found"))
                .getTableDataList().get(0).getPartitions().size();
        Assert.assertEquals(4, pn);
        Assert.assertEquals(48, IncrementalWithIntPartitionTest.getModelRowCount(project, modelId));
        SecondStorageUtil.checkSecondStorageData(project);
    }

    @Test
    public void testJobPausedBeforeCommit() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        buildIncrementalLoadQuery("2012-01-03", "2012-01-04");
        buildIncrementalLoadQuery("2012-01-04", "2012-01-05");
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        cleanSegments(segs);
        registerWaitPoint(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT, 10000);
        val jobId = triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        await().atMost(15, TimeUnit.SECONDS).until(() -> SecondStorageConcurrentTestUtil.isWaiting(WAIT_BEFORE_COMMIT));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            executableManager.pauseJob(jobId);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
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

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            executableManager.resumeJob(jobId);
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            return executableManager.getJob(jobId).getStatus() == ExecutableState.RUNNING;
        });
        waitJobFinish(project, jobId);
        int pn = tableFlowManager.get().get(modelId).orElseThrow(() -> new IllegalStateException("tableflow not found"))
                .getTableDataList().get(0).getPartitions().size();
        Assert.assertEquals(4, pn);
        Assert.assertEquals(48, IncrementalWithIntPartitionTest.getModelRowCount(project, modelId));
        SecondStorageUtil.checkSecondStorageData(project);
    }
}
