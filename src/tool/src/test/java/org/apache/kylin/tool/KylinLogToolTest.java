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
package org.apache.kylin.tool;

import static org.apache.kylin.common.util.TestUtils.writeToFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.tool.obf.KylinConfObfuscatorTest;
import org.apache.kylin.tool.util.ToolUtil;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class KylinLogToolTest extends NLocalFileMetadataTestCase {

    private static final String logs1 = "2019-09-02T02:35:19,868 INFO [dag-scheduler-event-loop] scheduler.DAGScheduler : Got job 459 (countAsync at SparkContextCanary.java:126) with 2 output partitions\n"
            + "2019-09-02T02:38:16,868 INFO  [FetchJobWorker(project:a_test)-p-9-t-9] runners.FetcherRunner : fetcher schedule d0f45b72-db2f-407b-9d6f-7cfe6f6624e8 tttt\n"
            + "2019-09-02T02:39:17,868 INFO  test1\n" + "2019-09-02 02:39:18,868 INFO  test2\n"
            + "2019-09-02T02:40:06,220 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8_00 from RUNNING to SUCCEED\n"
            + "2019-09-02T02:40:06,619 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction 1c2792bd-448e-4450-9944-32229523895d updates 1 metadata items\n"
            + "2019-09-02T02:40:07,635 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 1c2792bd-448e-4450-9944-32229523895d takes 422ms to complete";

    private static final String logs2 = "2019-09-02T02:41:19,868 INFO [dag-scheduler-event-loop] scheduler.DAGScheduler : Got job 459 (countAsync at SparkContextCanary.java:126) with 2 output partitions\n"
            + "2019-09-02T02:41:40,178 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 started on project expert_01\n"
            + "2019-09-02T02:41:40,185 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8_01 from RUNNING to SUCCEED\n"
            + "2019-09-02T02:41:40,561 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 updates 1 metadata items\n"
            + "2019-09-02T02:41:40,568 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 takes 390ms to complete\n"
            + "2019-09-02T02:41:40,568 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 02e4d49e-80f8-4c58-aa44-6be1dab98e77 started on project expert_01\n"
            + "2019-09-02T02:41:40,569 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8 from RUNNING to SUCCEED\n"
            + "2019-09-02T02:41:40,877 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction 02e4d49e-80f8-4c58-aa44-6be1dab98e77 updates 1 metadata items\n"
            + "2019-09-02T02:41:40,885 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 02e4d49e-80f8-4c58-aa44-6be1dab98e77 takes 317ms to complete\n"
            + "2019-09-02T02:41:40,890 INFO  [FetchJobWorker(project:expert_01)-p-12-t-12] threadpool.NDefaultScheduler : Job Status in project expert_01: 0 should running, 0 actual running, 0 stopped, 0 ready, 12 already succeed, 0 error, 0 discarded, 0 suicidal,  0 others\n"
            + "2019-09-02T02:41:41,886 DEBUG [EventChecker(project:expert_01)-p-4-t-4] manager.EventOrchestrator : project expert_01 contains 1 events\n"
            + "2019-09-02T02:41:41,888 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork ca43de24-b825-4414-beeb-3b966651524e started on project expert_01\n"
            + "2019-09-02T02:41:41,889 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : transaction ca43de24-b825-4414-beeb-3b966651524e updates 1 metadata items\n"
            + "2019-09-02T02:41:41,889 INFO  [FetchJobWorker(project:expert_01)-p-12-t-12] threadpool.NDefaultScheduler : Job Status in project expert_01: 0 should running, 0 actual running, 0 stopped, 0 ready, 12 already succeed, 0 error, 0 discarded, 0 suicidal,  0 others\n"
            + "2019-09-02T02:41:41,901 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork ca43de24-b825-4414-beeb-3b966651524e takes 13ms to complete\n"
            + "2019-09-02T02:41:41,901 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork 297a3ec5-ca3b-4f19-a0a3-40079c3db372 started on project expert_01\n"
            + "2019-09-02T02:41:42,903 INFO  [EventChecker(project:expert_01)-p-4-t-4] handle.AbstractEventHandler : handling event: \n"
            + " {\n" + "  \"@class\" : \"org.apache.kylin.event.model.PostAddCuboidEvent\",\n"
            + "  \"uuid\" : \"9d1cce98-29ab-4fd7-8025-11ac27c92b56\",\n" + "  \"last_modified\" : 1567392101889,\n"
            + "  \"create_time\" : 1567391988091,\n" + "  \"version\" : \"4.0.0.0\",\n"
            + "  \"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\",\n" + "  \"isGlobal\" : false,\n"
            + "  \"params\" : { },\n" + "  \"msg\" : null,\n" + "  \"sequence_id\" : 1,\n"
            + "  \"owner\" : \"ADMIN\",\n" + "  \"runTimes\" : 1,\n"
            + "  \"job_id\" : \"d0f45b72-db2f-407b-9d6f-7cfe6f6624e8\"\n" + "}";

    private static final String kgLogMsg = "2020-06-08T05:23:22,410 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: health check finished ...\n"
            + "2020-06-08T05:24:22,410 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: start to run health checkers ...\n"
            + "2020-06-08T05:24:22,410 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[KEProcessChecker] start to do check ...\n"
            + "2020-06-08T05:24:22,432 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [KEProcessChecker], do check finished! \n"
            + "2020-06-08T05:24:22,432 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [NormalStateHandler], Health Checker: [KEProcessChecker] check result is NORMAL, message: \n"
            + "2020-06-08T05:24:22,432 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [NormalStateHandler] handle the check result success ...\n"
            + "2020-06-08T05:24:22,432 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[FullGCDurationChecker] start to do check ...\n"
            + "2020-06-08T05:24:22,614 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [FullGCDurationChecker], do check finished! \n"
            + "2020-06-08T05:24:22,614 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [UpGradeStateHandler], Health Checker: [FullGCDurationChecker] check result is QUERY_UPGRADE, messa\n"
            + "ge: Full gc time duration ratio in 300 seconds is less than 20.00%\n"
            + "2020-06-08T05:24:22,617 INFO  [ke-guardian-process] handler.UpGradeStateHandler : Upgrade query service success ...\n"
            + "2020-06-08T05:24:22,617 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [UpGradeStateHandler] handle the check result success ...\n"
            + "2020-06-08T05:24:22,617 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[KEStatusChecker] start to do check ...\n"
            + "2020-06-08T05:24:22,621 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [KEStatusChecker], do check finished! \n"
            + "2020-06-08T05:24:22,621 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [NormalStateHandler], Health Checker: [KEStatusChecker] check result is NORMAL, message: \n"
            + "2020-06-08T05:24:22,621 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [NormalStateHandler] handle the check result success ...\n"
            + "2020-06-08T05:24:22,621 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: health check finished ...\n"
            + "2020-06-08T05:25:22,621 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: start to run health checkers ...";

    private static final String kgLog1Msg = "2020-06-08T05:25:22,621 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[KEProcessChecker] start to do check ...\n"
            + "2020-06-08T05:25:22,642 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [KEProcessChecker], do check finished! \n"
            + "2020-06-08T05:25:22,642 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [NormalStateHandler], Health Checker: [KEProcessChecker] check result is NORMAL, message: \n"
            + "2020-06-08T05:25:22,642 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [NormalStateHandler] handle the check result success ...\n"
            + "2020-06-08T05:25:22,642 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[FullGCDurationChecker] start to do check ...\n"
            + "2020-06-08T05:25:22,830 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [FullGCDurationChecker], do check finished! \n"
            + "2020-06-08T05:25:22,831 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [UpGradeStateHandler], Health Checker: [FullGCDurationChecker] check result is QUERY_UPGRADE, messa\n"
            + "ge: Full gc time duration ratio in 300 seconds is less than 20.00%\n"
            + "2020-06-08T05:25:22,834 INFO  [ke-guardian-process] handler.UpGradeStateHandler : Upgrade query service success ...\n"
            + "2020-06-08T05:25:22,834 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [UpGradeStateHandler] handle the check result success ...\n"
            + "2020-06-08T05:25:22,834 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[KEStatusChecker] start to do check ...\n"
            + "2020-06-08T05:25:22,838 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [KEStatusChecker], do check finished! \n"
            + "2020-06-08T05:25:22,838 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [NormalStateHandler], Health Checker: [KEStatusChecker] check result is NORMAL, message: \n"
            + "2020-06-08T05:25:22,838 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [NormalStateHandler] handle the check result success ...\n"
            + "2020-06-08T05:25:22,838 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: health check finished ...";

    private static final String queryLog1 = "2021-09-25T18:05:01,377 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] memory.MemoryStore : Block broadcast_0 stored as values in memory (estimated size 358.6 KiB, free 4.6 GiB)\n"
            + "2021-09-25T18:05:01,536 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] memory.MemoryStore : Block broadcast_0_piece0 stored as bytes in memory (estimated size 30.7 KiB, free 4.6 GiB)\n"
            + "2021-09-25T18:05:01,544 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] spark.SparkContext : Created broadcast 0 from\n"
            + "2021-09-25T18:05:02,088 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] mapred.FileInputFormat : Total input paths to process : 1\n"
            + "2021-09-25T18:05:02,146 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] spark.SparkContext : Starting job: toIterator at SparkSqlClient.scala:126\n"
            + "2021-09-25T18:05:02,148 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] scheduler.DAGScheduler : submit job : 0, executionId is 89\n"
            + "2021-09-25T18:05:05,583 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] scheduler.DAGScheduler : Job 0 finished: toIterator at SparkSqlClient.scala:126, took 3.436620 s\n"
            + "2021-09-25T18:05:05,616 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] spark.SparkContext : Starting job: toIterator at SparkSqlClient.scala:126\n"
            + "2021-09-25T18:05:05,616 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] scheduler.DAGScheduler : submit job : 1, executionId is 89\n"
            + "2021-09-25T18:05:05,760 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] scheduler.DAGScheduler : Job 1 finished: toIterator at SparkSqlClient.scala:126, took 0.143551 s\n"
            + "2021-09-25T18:05:06,603 INFO  [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] query :\n"
            + "==========================[QUERY]===============================\n"
            + "Query Id: 14f61937-e8fd-174c-c0d0-93b01f04173e\n" + "SQL: select * from CUSTOMER\n" + "User: ADMIN\n"
            + "Success: true\n" + "Duration: 11.038\n" + "Project: fanshu\n" + "Realization Names: []\n"
            + "Index Layout Ids: []\n" + "Snapshot Names: []\n" + "Is Partial Match Model: []\n" + "Scan rows: [300]\n"
            + "Total Scan rows: 300\n" + "Scan bytes: [41039]\n" + "Total Scan Bytes: 41039\n"
            + "Result Row Count: 300\n" + "Shuffle partitions: 1\n" + "Accept Partial: true\n"
            + "Is Partial Result: false\n" + "Hit Exception Cache: false\n" + "Storage Cache Used: false\n"
            + "Is Query Push-Down: true\n" + "Is Prepare: false\n" + "Is Timeout: false\n" + "Trace URL: null\n"
            + "Time Line Schema: end_http_proc,massage,end calcite parse sql,end_convert_to_relnode,end_calcite_optimize,end_plan,collect_olap_context_info\n"
            + "Time Line: 87,1842,4,444,1184,14,58\n" + "Message: null\n" + "User Defined Tag: null\n"
            + "Is forced to Push-Down: false\n"
            + "User Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36\n"
            + "Back door toggles: {DEBUG_TOGGLE_HTRACE_ENABLED=false}\n" + "Scan Segment Count: 0\n"
            + "Scan File Count: 0\n" + "==========================[QUERY]===============================\n" + "\n"
            + "2021-09-25T18:05:06,665 DEBUG [fanshu] [Query 14f61937-e8fd-174c-c0d0-93b01f04173e-65] scheduler.EventBusFactory : Post event QueryMetricsContext@1bf317b3 async\n"
            + "\n";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testExtractOtherLogs() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        TimeZone timeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        File accessLog1 = new File(ToolUtil.getLogFolder(), "access_log.2020-01-01.log");
        File accessLog2 = new File(ToolUtil.getLogFolder(), "access_log.2020-01-02.log");
        File gcLog = new File(ToolUtil.getLogFolder(), "kylin.gc.1");
        File shellLog = new File(ToolUtil.getLogFolder(), "shell.1");
        File jstackLog1 = new File(ToolUtil.getLogFolder(), "jstack.timed.log.1577894300000");
        File jstackLog2 = new File(ToolUtil.getLogFolder(), "jstack.timed.log.1577894400000");

        FileUtils.writeStringToFile(accessLog1, "111");
        FileUtils.writeStringToFile(accessLog2, "111");
        FileUtils.writeStringToFile(gcLog, "111");
        FileUtils.writeStringToFile(shellLog, "111");
        FileUtils.writeStringToFile(jstackLog1, "111");
        FileUtils.writeStringToFile(jstackLog2, "111");

        KylinLogTool.extractOtherLogs(mainDir, 1577894400000L, 1577894400000L);

        FileUtils.deleteQuietly(accessLog1);
        FileUtils.deleteQuietly(accessLog2);
        FileUtils.deleteQuietly(gcLog);
        FileUtils.deleteQuietly(shellLog);
        FileUtils.deleteQuietly(jstackLog1);
        FileUtils.deleteQuietly(jstackLog2);

        Assert.assertFalse(new File(mainDir, "logs/access_log.2020-01-01.log").exists());
        Assert.assertTrue(new File(mainDir, "logs/access_log.2020-01-02.log").exists());
        Assert.assertTrue(new File(mainDir, "logs/kylin.gc.1").exists());
        Assert.assertTrue(new File(mainDir, "logs/shell.1").exists());
        Assert.assertFalse(new File(mainDir, "logs/jstack.timed.log.1577894300000").exists());
        Assert.assertTrue(new File(mainDir, "logs/jstack.timed.log.1577894400000").exists());
        TimeZone.setDefault(timeZone);
    }

    @Test
    public void testExtractKylinLogJob() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kylinLog = new File(ToolUtil.getLogFolder(), "kylin.log");
        File kylinLog1 = new File(ToolUtil.getLogFolder(), "kylin.log.1");

        FileUtils.writeStringToFile(kylinLog, logs2);
        FileUtils.writeStringToFile(kylinLog1, logs1);

        String jobId = "d0f45b72-db2f-407b-9d6f-7cfe6f6624e8";
        KylinLogTool.extractKylinLog(mainDir, jobId);

        FileUtils.deleteQuietly(kylinLog);
        FileUtils.deleteQuietly(kylinLog1);

        Assert.assertFalse(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02 02:35:19,868"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1"))
                .contains("runners.FetcherRunner : fetcher schedule d0f45b72-db2f-407b-9d6f-7cfe6f6624e8"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("test1"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("test2"));
        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02T02:40:07,635"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log"))
                .contains("\"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\""));
    }

    @Test
    public void testExtractKylinLogFull() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kylinLog = new File(ToolUtil.getLogFolder(), "kylin.log");
        File kylinLog1 = new File(ToolUtil.getLogFolder(), "kylin.log.1");

        FileUtils.writeStringToFile(kylinLog, logs2);
        FileUtils.writeStringToFile(kylinLog1, logs1);

        long startTime = DateTime.parse("2019-09-01").withTimeAtStartOfDay().getMillis();
        long endTime = DateTime.parse("2019-09-03").withTimeAtStartOfDay().getMillis();

        KylinLogTool.extractKylinLog(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(kylinLog);
        FileUtils.deleteQuietly(kylinLog1);

        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02T02:35:19,868"));
        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02T02:40:07,635"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log"))
                .contains("\"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\""));
    }

    @Test
    public void testExtractKylinLogQuery() throws Exception {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kylinLog = new File(ToolUtil.getLogFolder(), "kylin.log");
        File kylinLog1 = new File(ToolUtil.getLogFolder(), "kylin.log.1");

        FileUtils.writeStringToFile(kylinLog, logs2);
        FileUtils.writeStringToFile(kylinLog1, logs1);

        long startTime = DateTime.parse("2019-09-01").withTimeAtStartOfDay().getMillis();
        long endTime = DateTime.parse("2019-09-03").withTimeAtStartOfDay().getMillis();
        String queryId = "6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1";

        KylinLogTool.extractKylinLog(mainDir, startTime, endTime, queryId);

        FileUtils.deleteQuietly(kylinLog);
        FileUtils.deleteQuietly(kylinLog1);

        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02T02:35:19,868"));
        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02T02:40:07,635"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log"))
                .contains("\"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\""));
    }

    @Test
    public void testExtractKylinLogAllModule() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File scheduleLog = new File(ToolUtil.getLogFolder(), "kylin.schedule.log");
        File queryLog = new File(ToolUtil.getLogFolder(), "kylin.query.log");
        File smartLog = new File(ToolUtil.getLogFolder(), "kylin.smart.log");
        File buildLog = new File(ToolUtil.getLogFolder(), "kylin.build.log");

        FileUtils.writeStringToFile(scheduleLog, logs1);
        FileUtils.writeStringToFile(queryLog, logs1);
        FileUtils.writeStringToFile(smartLog, logs1);
        FileUtils.writeStringToFile(buildLog, logs1);

        long startTime = DateTime.parse("2019-09-01").withTimeAtStartOfDay().getMillis();
        long endTime = DateTime.parse("2019-09-03").withTimeAtStartOfDay().getMillis();

        KylinLogTool.extractKylinLog(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(scheduleLog);
        FileUtils.deleteQuietly(queryLog);
        FileUtils.deleteQuietly(smartLog);
        FileUtils.deleteQuietly(buildLog);

        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.schedule.log"))
                .contains("2019-09-02T02:35:19,868"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.query.log"))
                .contains("2019-09-02T02:35:19,868"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.smart.log"))
                .contains("2019-09-02T02:35:19,868"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.build.log"))
                .contains("2019-09-02T02:35:19,868"));

    }

    @Test
    public void testExtractSparkLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String project = "expert_01";
        String jobId = "2e3be2a5-3d96-4797-a39f-3cfa88383efa";
        String sourceLogsPath = SparkLogExtractorFactory.create(getTestConfig()).getSparkLogsDir(project,
                getTestConfig());

        String normPath = sourceLogsPath.startsWith("file://") ? sourceLogsPath.substring(7) : sourceLogsPath;
        File sparkLogDir = new File(new File(normPath, DateTime.now().toString("yyyy-MM-dd")), jobId);
        FileUtils.forceMkdir(sparkLogDir);

        File tFile = new File(sparkLogDir, "a.txt");
        FileUtils.writeStringToFile(tFile, "111");

        KylinLogTool.extractSparkLog(mainDir, project, jobId);
        FileUtils.deleteQuietly(new File(sourceLogsPath));
        Assert.assertTrue(new File(mainDir, "spark_logs/" + jobId + "/a.txt").exists());
    }

    @Test
    public void testMountSparkLogExtractor() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        String project = "expert_01";
        String jobId = "2e3be2a5-3d96-4797-a39f-3cfa88383efa";
        File sparkLogDir = new File(new File(mainDir, "expert_01/spark_logs/" + DateTime.now().toString("yyyy-MM-dd")),
                jobId);
        FileUtils.forceMkdir(sparkLogDir);

        File tFile = new File(sparkLogDir, "a.txt");
        FileUtils.writeStringToFile(tFile, "111");
        overwriteSystemProp("kylin.tool.spark-log-extractor", "org.apache.kylin.tool.MountSparkLogExtractor");
        overwriteSystemProp("kylin.tool.mount-spark-log-dir", mainDir.getAbsolutePath());
        overwriteSystemProp("kylin.tool.clean-diag-tmp-file", "true");
        KylinLogTool.extractSparkLog(mainDir, project, jobId);
        Assert.assertFalse(sparkLogDir.exists());
        Assert.assertTrue(new File(mainDir, "spark_logs/" + jobId + "/a.txt").exists());
    }

    @Test
    public void testExtractJobTmp() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String project = "expert_01";
        String jobId = "2e3be2a5-3d96-4797-a39f-3cfa88383efa";
        String hdfsPath = ToolUtil.getJobTmpDir(project, jobId);
        String normPath = hdfsPath.startsWith("file://") ? hdfsPath.substring(7) : hdfsPath;

        FileUtils.forceMkdir(new File(normPath));
        File tFile = new File(normPath, "a.txt");
        FileUtils.writeStringToFile(tFile, "111");

        KylinLogTool.extractJobTmp(mainDir, project, jobId);

        FileUtils.deleteQuietly(new File(hdfsPath));
        Assert.assertTrue(new File(mainDir, "job_tmp/" + jobId + "/a.txt").exists());
    }

    @Test
    public void testExtractSparderLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String sourceLogsPath = SparkLogExtractorFactory.create(getTestConfig()).getSparderLogsDir(getTestConfig());
        String normPath = sourceLogsPath.startsWith("file://") ? sourceLogsPath.substring(7) : sourceLogsPath;

        String[] childDirs = { "2019-08-29/application_1563861406192_0139", "2019-08-30/application_1563861406192_0139",
                "2019-08-31/application_1563861406192_0144" };
        for (String childDir : childDirs) {
            File sparderLogDir = new File(normPath, childDir);
            FileUtils.forceMkdir(sparderLogDir);

            File tFile = new File(sparderLogDir, "executor-1.log");
            FileUtils.writeStringToFile(tFile, "111");
        }

        long startTime = DateTime.parse("2019-08-29").withTimeAtStartOfDay().getMillis() + 3600_000L;
        long endTime = DateTime.parse("2019-08-30").withTimeAtStartOfDay().getMillis() + 3600_000L;

        KylinLogTool.extractFullDiagSparderLog(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(new File(sourceLogsPath));

        for (String childDir : childDirs) {
            Assert.assertTrue(new File(mainDir, "spark_logs/" + childDir + "/executor-1.log").exists());
        }

        // > 31 day
        long startTime2 = DateTime.parse("2019-07-28").withTimeAtStartOfDay().getMillis() + 3600_000L;
        long endTime2 = DateTime.parse("2019-08-29").withTimeAtStartOfDay().getMillis() + 3600_000L;
        KylinLogTool.extractFullDiagSparderLog(mainDir, startTime2, endTime2);
    }

    @Test
    public void testMountSparderLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        String[] childDirs = { "2019-08-29/application_1563861406192_0139", "2019-08-30/application_1563861406192_0139",
                "2019-08-31/application_1563861406192_0144" };
        overwriteSystemProp("kylin.tool.spark-log-extractor", "org.apache.kylin.tool.MountSparkLogExtractor");
        overwriteSystemProp("kylin.tool.mount-spark-log-dir", mainDir.getAbsolutePath());
        overwriteSystemProp("kylin.tool.clean-diag-tmp-file", "true");
        File sparkLogDir = new File(mainDir, "_sparder_logs");
        for (String childDir : childDirs) {
            File sparderLogDir = new File(sparkLogDir, childDir);
            FileUtils.forceMkdir(sparderLogDir);

            File tFile = new File(sparderLogDir, "executor-1.log");
            FileUtils.writeStringToFile(tFile, "111");
        }

        long startTime = DateTime.parse("2019-08-29").withTimeAtStartOfDay().getMillis() + 3600_000L;
        long endTime = DateTime.parse("2019-08-30").withTimeAtStartOfDay().getMillis() + 3600_000L;

        KylinLogTool.extractFullDiagSparderLog(mainDir, startTime, endTime);

        for (String childDir : childDirs) {
            Assert.assertTrue(new File(mainDir, "spark_logs/" + childDir + "/executor-1.log").exists());
        }
    }

    @Test
    public void testExtractKGLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kgLog = new File(ToolUtil.getLogFolder(), "guardian.log");
        File kgLog1 = new File(ToolUtil.getLogFolder(), "guardian.log.1");

        FileUtils.writeStringToFile(kgLog, kgLogMsg);
        FileUtils.writeStringToFile(kgLog1, kgLog1Msg);

        long startTime = new DateTime(2020, 6, 8, 5, 24, 0).getMillis();
        long endTime = new DateTime(2020, 6, 8, 5, 26, 0).getMillis();

        KylinLogTool.extractKGLogs(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(kgLog);
        FileUtils.deleteQuietly(kgLog1);

        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/guardian.log")).contains("2020-06-08T05:24:22,410"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/guardian.log.1"))
                .contains("2020-06-08T05:25:22,838"));
        Assert.assertFalse(
                FileUtils.readFileToString(new File(mainDir, "logs/guardian.log")).contains("2020-06-08T05:23:22,410"));
    }

    @Test
    public void testExtractQueryLogByQueryId() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File qyLog = new File(ToolUtil.getLogFolder(), "kylin.query.log");

        FileUtils.writeStringToFile(qyLog, queryLog1);
        String queryId = "14f61937-e8fd-174c-c0d0-93b01f04173e";
        KylinLogTool.extractKylinQueryLog(mainDir, queryId);

        FileUtils.deleteQuietly(qyLog);

        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.query.log"))
                .contains("14f61937-e8fd-174c-c0d0-93b01f04173e"));
    }

    @Test
    public void testExtractQuerySparderLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String sourceLogsPath = SparkLogExtractorFactory.create(getTestConfig()).getSparderLogsDir(getTestConfig());
        String normPath = sourceLogsPath.startsWith("file://") ? sourceLogsPath.substring(7) : sourceLogsPath;

        String[] childDirs = { "2021-08-29/application_1563861406192_0139",
                "2021-08-30/application_1563861406192_0139" };
        int i = 0;
        for (String childDir : childDirs) {
            i++;
            File sparderLogDir = new File(normPath, childDir);
            FileUtils.forceMkdir(sparderLogDir);

            File tFile = new File(sparderLogDir, "executor-1.log");
            if (i == 1) {
                FileUtils.writeStringToFile(tFile,
                        "2021-08-29T14:46:31,095 INFO  [main] spark.SecurityManager : Changing view acls to: yarn,root\n"
                                + "2021-08-29T15:06:53,844 INFO  [dispatcher-Executor] executor.YarnCoarseGrainedExecutorBackend : Got assigned task 0");
            } else {
                FileUtils.writeStringToFile(tFile,
                        "2021-08-30T14:46:31,095 INFO  [main] spark.SecurityManager : Changing view acls to: yarn,root\n"
                                + "2021-08-30T15:06:53,844 INFO  [dispatcher-Executor] executor.YarnCoarseGrainedExecutorBackend : Got assigned task 0");
            }
        }

        // null
        long startTime1 = DateTime.parse("2021-08-29").getMillis();
        long endTime1 = DateTime.parse("2021-08-30").getMillis();

        KylinLogTool.extractQueryDiagSparderLog(mainDir, startTime1, endTime1);

        for (String childDir : childDirs) {
            if (childDir.equals("2021-08-29/application_1563861406192_0139")) {
                Assert.assertTrue(new File(mainDir, "spark_logs/" + childDir + "/executor-1.log").exists());
            } else {
                Assert.assertFalse(new File(mainDir, "spark_logs/" + childDir + "/executor-1.log").exists());
            }
        }

        // sparder log
        long startTime = DateTime.parse("2021-08-29").getMillis();
        long endTime = DateTime.parse("2021-08-31").getMillis();

        KylinLogTool.extractQueryDiagSparderLog(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(new File(sourceLogsPath));

        for (String childDir : childDirs) {
            Assert.assertTrue(new File(mainDir, "spark_logs/" + childDir + "/executor-1.log").exists());
        }

        // time range
        long startTime2 = DateTime.parse("2019-07-28").getMillis() + 3600_000L;
        long endTime2 = DateTime.parse("2019-03-29").getMillis() + 3600_000L;
        KylinLogTool.extractQueryDiagSparderLog(mainDir, startTime2, endTime2);
    }

    @Test
    public void testGetJobLogPatternAndJobTimeString() {
        String jobId = "d0f45b72-db2f-407b-9d6f-7cfe6f6624e8";
        String patternString = KylinLogTool.getJobLogPattern(jobId);
        String log;
        Assert.assertEquals("^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(.*JobWorker.*jobid:d0f45b72.*)"
                + "|^(traceId: ([0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}) )?([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}).*d0f45b72-db2f-407b-9d6f-7cfe6f6624e8",
                patternString);
        Pattern pattern = Pattern.compile(patternString);
        log = "2019-09-02T02:38:16,868 INFO  [FetchJobWorker(project:a_test)-p-9-t-9] runners.FetcherRunner : fetcher schedule d0f45b72-db2f-407b-9d6f-7cfe6f6624e8";
        Matcher matcher = pattern.matcher(log);
        Assert.assertTrue(matcher.find());
        Assert.assertEquals("2019-09-02T02:38:16", KylinLogTool.getJobTimeString(matcher));
        log = "2019-09-02T02:40:07,635 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 1c2792bd-448e-4450-9944-32229523895d takes 422ms to complete";
        matcher = pattern.matcher(log);
        Assert.assertTrue(matcher.find());
        Assert.assertEquals("2019-09-02T02:40:07", KylinLogTool.getJobTimeString(matcher));
        log = "traceId: ae0c0172-54b0-973f-de6c-3aab18289649 2019-09-02T02:38:16,868 INFO  [FetchJobWorker(project:a_test)-p-9-t-9] runners.FetcherRunner : fetcher schedule d0f45b72-db2f-407b-9d6f-7cfe6f6624e8";
        matcher = pattern.matcher(log);
        Assert.assertTrue(matcher.find());
        Assert.assertEquals("2019-09-02T02:38:16", KylinLogTool.getJobTimeString(matcher));
        log = "traceId: 4b8324bb-6467-d0b6-8087-9c71c28cc443 2023-05-31T13:51:57,945 INFO  [test2] [http-nio-17971-exec-5] handler.AbstractJobHandler : Job JobParam(jobId=d0f45b72-db2f-407b-9d6f-7cfe6f6624e8, "
                + "targetSegments=[edbd2366-f8b3-0f29-39c1-dcf03895a295], targetLayouts=[], owner=ADMIN, model=4b0a4399-0903-18ec-5d7f-52eb6c96df31, project=test2, jobTypeEnum=IN"
                + "C_BUILD, ignoredSnapshotTables=null, targetPartitions=[], targetBuckets=[], priority=3, yarnQueue=null, tag=null, condition={}, "
                + "processLayouts=[LayoutEntity{id=160001}, LayoutEntity{id=170001}], deleteLayouts=null) creates job NSparkCubingJob{id=d0f45b72-db2f-407b-9d6f-7cfe6f6624e8, name=INC_BUILD, state=READY}";
        matcher = pattern.matcher(log);
        Assert.assertTrue(matcher.find());
        Assert.assertEquals("2023-05-31T13:51:57", KylinLogTool.getJobTimeString(matcher));
    }

    @Test
    public void testHideLicenseString() throws Exception {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        val eventLogFile = new File(
                KylinConfObfuscatorTest.class.getClassLoader().getResource("diag/eventlog").getFile());
        File testFile = new File(mainDir, "eventlog");
        Files.copy(eventLogFile.toPath(), testFile.toPath());
        Assert.assertTrue(testFile.isFile());
        String license = "6v2pi13f19vqn10jlmo8vhf2d5iq52qwcb7s3gg9zyhgwrxko23gon5xzav8n19q7vgb"
                + "chk6xpvf8jshz3xwnqrtupvwwcydlygli2wro2i41bix91xgdhcy3g2l31hjkhef5ij0l8ok9pwsfmh"
                + "6450a6xp9ht30uq8aaxx65qt8uudcsrgyakz71wacs7hgvkefbz7q0pq194cgdewfznilavltdrazw40s"
                + "tv6s7f31j3ziy4shne3doxkbee1ko0b2etrm9zozooyu2bx0qxk0e5q7x52btvnfun38zfjzxezun7u0zj"
                + "9iq8u8n8jx8258o1qnld9pobwcm8hugdizlzg3srwbk9xejmivpipjc1kxitql5k2ag1cai7rqij2zz3hw"
                + "lln23y86yc782fwhp8kl4bdt5kizsu2sxkivxal5bi5pigknyttao2gtx4wj2gs1lmk91yu8iwo09s3ykoigj"
                + "sfgb1cht2edlimgcj5b91y3vpaczru18k8wj1ncrhx8vyibayffvbwc6v4q76cgvuvnj7g50syxszwyypvrqpr"
                + "ky7jfuby6iqylvkkfgxip1us5js0b0t8sxx6n4f6bxrbbro9yckfzn8zyxasnfv546f9kt4n04tkn2ioxvg83pe"
                + "vi7z3uxuy1t1we3nlrsndh0e4q2e3i2unhjihw11mroz9vt1qedhg1ug7e0ynbk8l7mts8xhhke1nsvzj0mecnov"
                + "pneckhdbbcksys5jnm9ym2ojgxljhloseau1c7upxfqlro31x7zmvuinnv5mq4p9ikn4q4j5jdu9chz8xpasll2z"
                + "46b1jak3m11s4z204abyxfydyp11dhm1ddydm0rmmg7uhdtwlg4pts0";
        Assert.assertTrue(FileUtils.readFileToString(testFile).contains(license));
        KylinLogTool.hideLicenseString(testFile);
        Assert.assertFalse(FileUtils.readFileToString(testFile).contains(license));
    }

    @Test
    public void testExtractJobTmpCandidateLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String project = "expert_05";
        String hdfsPath = ToolUtil.getHdfsJobTmpDir(project);
        String normPath = hdfsPath.startsWith("file://") ? hdfsPath.substring(7) : hdfsPath;

        FileUtils.forceMkdir(new File(normPath));
        String zipFile = "expert_05-2021-04-21-06-53-51-549.zip";
        File tFile = new File(normPath, zipFile);
        FileUtils.writeStringToFile(tFile, "111");

        KylinLogTool.extractJobTmpCandidateLog(mainDir, project, 0, 1619058301307L);

        FileUtils.deleteQuietly(new File(hdfsPath));
        Assert.assertTrue(new File(mainDir, String.format("job_tmp/%s", zipFile)).exists());
    }

    @Test
    public void testExtractLogFromWorkingDir() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String hdfsPath = getTestConfig().getHdfsWorkingDirectory();
        String normPath = hdfsPath.startsWith("file://") ? hdfsPath.substring(7) : hdfsPath;

        File remoteFile = new File(normPath, "_logs/yl-common-001");
        File commonJStack = new File(remoteFile, "jstack.1666");
        File commonGC = new File(remoteFile, "kylin.gc.pid" + ToolUtil.getKylinPid() + ".0");
        FileUtils.forceMkdir(remoteFile);
        writeToFile(commonJStack);
        writeToFile(commonGC);

        File remoteFile2 = new File(normPath, "_logs/yl-query-001");
        File queryJStack = new File(remoteFile2, "jstack.1667");
        File queryGC = new File(remoteFile2, "kylin.gc.pid" + ToolUtil.getKylinPid() + ".3");
        FileUtils.forceMkdir(remoteFile2);
        writeToFile(queryJStack);
        writeToFile(queryGC);

        KylinLogTool.extractLogFromWorkingDir(mainDir, Arrays.asList("yl-common-001"));
        Assert.assertTrue(commonJStack.exists() && commonGC.exists());

        FileUtils.cleanDirectory(mainDir);
        KylinLogTool.extractLogFromWorkingDir(mainDir, new ArrayList<>());
        Assert.assertTrue(commonJStack.exists() && commonGC.exists() && queryGC.exists() && queryJStack.exists());
    }

    @Test
    public void testExtractLogFromWorkingDirWithMultipleInstance() throws IOException, InterruptedException {
        KylinConfig config = getTestConfig();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path remoteDir = new Path(config.getHdfsWorkingDirectory(), "_logs");
        fs.mkdirs(remoteDir);

        config.setProperty("kylin.log.gc-and-jstack.retention-time", "3s");
        Path toDeletePath1 = new Path(remoteDir, "instance_1");
        Path toDeletePath2 = new Path(remoteDir, "instance_2");
        fs.mkdirs(toDeletePath1);
        fs.mkdirs(toDeletePath2);
        HadoopUtil.writeStringToHdfs("test", new Path(toDeletePath2, "gc.log"));
        HadoopUtil.writeStringToHdfs("test", new Path(toDeletePath2, "jstack.log"));

        TimeUnit.SECONDS.sleep(3);

        Path retentionPath1 = new Path(remoteDir, "instance_3");
        Path retentionPath2 = new Path(remoteDir, "instance_4");
        fs.mkdirs(retentionPath1);
        fs.mkdirs(retentionPath2);
        HadoopUtil.writeStringToHdfs("test", new Path(retentionPath2, "gc.log"));
        HadoopUtil.writeStringToHdfs("test", new Path(retentionPath2, "jstack.log"));

        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        File logDir = new File(mainDir, "logs");
        KylinLogTool.extractLogFromWorkingDir(mainDir, new ArrayList<>());
        Assert.assertFalse(new File(logDir, "instance_1").exists());
        Assert.assertFalse(new File(logDir, "instance_2").exists());
        Assert.assertTrue(new File(logDir, "instance_3").exists());
        Assert.assertTrue(new File(logDir, "instance_4").exists());
        Assert.assertEquals(2, new File(logDir, "instance_4").listFiles().length);
    }
    
    @Test
    public void testGetFirstLogTime() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String log1 = "traceId: eba55ae6-0936-9d42-25d2-adf00a55f06d 2023-10-25T18:16:44,803 INFO  [expansion_rate] "
                + "[Transaction-Thread-467] handler.AbstractJobHandler : Job JobParam";
        String log2 = "2023-10-25T18:17:35,792 INFO  [JobWorker(project:expansion_rate,jobid:d9694dc4)] "
                + "execution.AbstractExecutable : Execute in CHAIN mode.";
        File logFile1 = new File(mainDir, "log1.log");
        File logFile2 = new File(mainDir, "log2.log");
        FileUtils.writeStringToFile(logFile1, log1);
        FileUtils.writeStringToFile(logFile2, log2);

        KylinLogTool.ExtractLogByRangeTool tool = (KylinLogTool.ExtractLogByRangeTool) ReflectionTestUtils
                .getField(KylinLogTool.class, "DEFAULT_EXTRACT_LOG_BY_RANGE");
        Assert.assertEquals("2023-10-25T18:16:44", tool.getFirstTimeByLogFile(logFile1));
        Assert.assertEquals("2023-10-25T18:17:35", tool.getFirstTimeByLogFile(logFile2));
    }
}
