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

package org.apache.kylin.rest.config.initialize;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_NOT_EXIST;
import static org.apache.kylin.rest.constant.SnapshotStatus.OFFLINE;
import static org.apache.kylin.rest.constant.SnapshotStatus.ONLINE;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.metrics.prometheus.PrometheusMetrics;
import org.apache.kylin.common.scheduler.JobFinishedNotifier;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.var;

public class JobSchedulerListenerTest extends NLocalFileMetadataTestCase {
    static CountDownLatch latch;
    static JobSyncListener.JobInfo modelInfo = new JobSyncListener.JobInfo("f26641d7-2094-473b-972a-4e1cebe55091",
            "test_project", "9f85e8a0-3971-4012-b0e7-70763c471a01",
            Sets.newHashSet("061e2862-7a41-4516-977b-28045fcc57fe"), Sets.newHashSet(1L), 1000L, "SUCCEED",
            "INDEX_BUILD", new ArrayList<>(), new ArrayList<>(), null, 1626135824000L, 1626144908000L, null, null, null,
            null, null, null);

    static JobSyncListener.JobInfo tableInfo = new JobSyncListener.JobInfo("f26641d7-2094-473b-972a-4e1cebe55092",
            "test_project", null, null, null, 1000L, "READY", "SNAPSHOT_BUILD", new ArrayList<>(), new ArrayList<>(),
            JobSyncListener.SnapshotJobInfo.builder().build(), 1626135824000L, 1626144908000L, null, null, null, null,
            null, null);

    static boolean assertMeet = false;

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testPostJobInfoSucceed() {
        List<Integer> ports = Lists.newArrayList(10000, 20000, 30000);

        for (int port : ports) {
            try {
                latch = new CountDownLatch(1);
                KylinConfig config = Mockito.mock(KylinConfig.class);
                KylinConfig.setKylinConfigThreadLocal(config);
                Mockito.when(config.getJobFinishedNotifierUsername()).thenReturn("ADMIN");
                Mockito.when(config.getJobFinishedNotifierPassword()).thenReturn("KYLIN");
                Mockito.when(config.getJobFinishedNotifierUrl()).thenReturn("http://localhost:" + port + "/test");

                HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                server.createContext("/test", new ModelHandler());
                server.start();

                JobSyncListener.postJobInfo(modelInfo);

                latch.await(10, TimeUnit.SECONDS);
                server.stop(0);
            } catch (InterruptedException e) {
                Assert.fail();
            } catch (IOException e) {
                continue;
            }
            if (assertMeet) {
                break;
            }
        }
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testNotPostJobInfoBecauseOfNonUrl() {
        List<Integer> ports = Lists.newArrayList(10000, 20000, 30000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    KylinConfig config = Mockito.mock(KylinConfig.class);
                    KylinConfig.setKylinConfigThreadLocal(config);

                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    server.createContext("/test", new TimeoutHandler());
                    server.start();

                    JobSyncListener.postJobInfo(modelInfo);

                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testNotPostJobInfoBecauseOfReadyState() {
        List<Integer> ports = Lists.newArrayList(10000, 20000, 30000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    KylinConfig config = Mockito.mock(KylinConfig.class);
                    KylinConfig.setKylinConfigThreadLocal(config);
                    Mockito.when(config.getJobFinishedNotifierUrl()).thenReturn("http://localhost:" + port + "/test");

                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    server.createContext("/test", new TimeoutHandler());
                    server.start();
                    JobSyncListener.postJobInfo(tableInfo);

                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testNotPostJobInfoBecauseOfNonUrlAndReadyState() {
        List<Integer> ports = Lists.newArrayList(10000, 20000, 30000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    KylinConfig config = Mockito.mock(KylinConfig.class);
                    KylinConfig.setKylinConfigThreadLocal(config);

                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    server.createContext("/test", new TimeoutHandler());
                    server.start();

                    JobSyncListener.postJobInfo(tableInfo);

                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testPostJobInfoTimeout() {
        List<Integer> ports = Lists.newArrayList(10000, 20000, 30000);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
            for (int port : ports) {
                try {
                    latch = new CountDownLatch(1);
                    KylinConfig config = Mockito.mock(KylinConfig.class);
                    KylinConfig.setKylinConfigThreadLocal(config);
                    Mockito.when(config.getJobFinishedNotifierUrl()).thenReturn("http://localhost:" + port + "/test");

                    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                    server.createContext("/test", new TimeoutHandler());
                    server.start();

                    JobSyncListener.postJobInfo(modelInfo);

                    latch.await(10, TimeUnit.SECONDS);
                    server.stop(0);
                    break;
                } catch (InterruptedException e) {
                    Assert.fail();
                } catch (IOException e) {
                    continue;
                }
            }
            assertMeet = true;
            return true;
        });
        if (!assertMeet) {
            Assert.fail();
        }
    }

    @Test
    public void testExtractInfo() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        long duration = 1000L;
        long waitTime = 0L;
        String jobState = "SUCCEED";
        String jobType = "INDEX_BUILD";
        Set<String> segIds = new HashSet<>();
        segIds.add("11124840-b3e3-43db-bcab-2b78da666d00");
        // test do not exist segment
        segIds.add("11124840-b3e3-43db-bcab-2b78da666d11");
        Set<Long> layoutIds = new HashSet<>();
        layoutIds.add(1L);
        Set<Long> partitionIds = null;
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, Collections.emptySet(), waitTime, "", "", true, startTime, endTime, null, null);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertEquals(jobId, jobInfo.getJobId());
        Assert.assertEquals(project, jobInfo.getProject());
        Assert.assertEquals(subject, jobInfo.getModelId());
        Assert.assertEquals(duration, jobInfo.getDuration());
        Assert.assertEquals(jobState, jobInfo.getState());
        Assert.assertEquals(jobType, jobInfo.getJobType());
        Assert.assertTrue(jobInfo.getSegmentIds().containsAll(segIds));
        Assert.assertEquals(segIds.size(), jobInfo.getSegmentIds().size());
        JobSyncListener.SegRange segRange = jobInfo.getSegRanges().get(0);
        Assert.assertEquals("11124840-b3e3-43db-bcab-2b78da666d00", segRange.getSegmentId());
        Assert.assertEquals(1309891513770L, segRange.getStart());
        Assert.assertEquals(1509891513770L, segRange.getEnd());
        Assert.assertTrue(jobInfo.getIndexIds().containsAll(layoutIds));
        Assert.assertEquals(layoutIds.size(), jobInfo.getIndexIds().size());
    }

    @Test
    public void testExtractInfoNonDataflowOrSegmentId() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        long duration = 1000L;
        long waitTime = 0L;
        String jobState = "SUCCEED";
        String jobType = "INDEX_BUILD";

        // test segment ids is empty
        Set<String> segIds = new HashSet<>();
        Set<Long> layoutIds = new HashSet<>();
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;

        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, Collections.emptySet(), waitTime, "", "", true, startTime, endTime, null, null);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertTrue(jobInfo.getSegmentPartitionInfoList().isEmpty());

        // test dataflow is null
        subject = null;
        segIds.add("11124840-b3e3-43db-bcab-2b78da666d00");
        layoutIds.add(1L);
        notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType, segIds, layoutIds,
                Collections.emptySet(), waitTime, "", "", true, startTime, endTime, null, null);
        jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertTrue(jobInfo.getSegmentPartitionInfoList().isEmpty());
    }

    @Test
    public void testExtractTableJobInfo() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "DEFAULT.TEST_SNAPSHOT_TABLE";
        long duration = 1000L;
        long waitTime = 0L;
        String jobState = "SUCCEED";
        String jobType = "SNAPSHOT_BUILD";
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                null, null, Collections.emptySet(), waitTime, "", "", true, startTime, endTime, null, null);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);

        Assert.assertEquals(jobId, jobInfo.getJobId());
        Assert.assertEquals(project, jobInfo.getProject());
        Assert.assertNull(jobInfo.getModelId());
        Assert.assertEquals(duration, jobInfo.getDuration());
        Assert.assertEquals(jobState, jobInfo.getState());
        Assert.assertEquals(jobType, jobInfo.getJobType());
        Assert.assertNull(jobInfo.getSegmentIds());
        Assert.assertNull(jobInfo.getIndexIds());
        Assert.assertNull(jobInfo.getIndexIds());
        Assert.assertEquals(0, jobInfo.getSegRanges().size());

        JobSyncListener.SnapshotJobInfo snapshotJobInfo = jobInfo.getSnapshotJobInfo();
        Assert.assertEquals("TEST_SNAPSHOT_TABLE", snapshotJobInfo.getTable());
        Assert.assertEquals("DEFAULT", snapshotJobInfo.getDatabase());
        Assert.assertEquals(100, snapshotJobInfo.getTotalRows());
        Assert.assertEquals(1000, snapshotJobInfo.getStorage());
        Assert.assertEquals(ONLINE, snapshotJobInfo.getStatus());
        Assert.assertEquals("PROVINCE", snapshotJobInfo.getSelectPartitionCol());

        jobType = "SNAPSHOT_REFRESH";
        notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType, null, null,
                Collections.emptySet(), waitTime, "", "", true, startTime, endTime, null, null);
        jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertNotNull(jobInfo.getSnapshotJobInfo());
    }

    @Test
    public void testExtractTableJobInfoNonSnapshot() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "DEFAULT.TEST_COUNTRY";
        long duration = 1000L;
        long waitTime = 0L;
        String jobState = "SUCCEED";
        String jobType = "SNAPSHOT_BUILD";
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                null, null, Collections.emptySet(), waitTime, "", "", true, startTime, endTime, null, null);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);

        Assert.assertEquals(jobId, jobInfo.getJobId());
        Assert.assertEquals(project, jobInfo.getProject());
        Assert.assertNull(jobInfo.getModelId());
        Assert.assertEquals(duration, jobInfo.getDuration());
        Assert.assertEquals(jobState, jobInfo.getState());
        Assert.assertEquals(jobType, jobInfo.getJobType());
        Assert.assertNull(jobInfo.getSegmentIds());
        Assert.assertNull(jobInfo.getIndexIds());
        Assert.assertNull(jobInfo.getIndexIds());
        Assert.assertEquals(0, jobInfo.getSegRanges().size());

        JobSyncListener.SnapshotJobInfo snapshotJobInfo = jobInfo.getSnapshotJobInfo();
        Assert.assertEquals("TEST_COUNTRY", snapshotJobInfo.getTable());
        Assert.assertEquals("DEFAULT", snapshotJobInfo.getDatabase());
        Assert.assertEquals(0, snapshotJobInfo.getTotalRows());
        Assert.assertEquals(0, snapshotJobInfo.getStorage());
        Assert.assertEquals(OFFLINE, snapshotJobInfo.getStatus());
        Assert.assertNull(snapshotJobInfo.getSelectPartitionCol());
    }

    @Test
    public void testExtractThrowableWithNonKylinException() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        long duration = 1000L;
        long waitTime = 100L;
        String jobState = "SUICIDAL";
        String jobType = "INDEX_BUILD";
        Set<String> segIds = new HashSet<>();
        segIds.add("11124840-b3e3-43db-bcab-2b78da666d00");
        Set<Long> layoutIds = new HashSet<>();
        layoutIds.add(1L);
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;
        Throwable throwable = new RuntimeException();
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, null, waitTime, "", "", true, startTime, endTime, null, throwable);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertEquals("DISCARDED", jobInfo.getState());
        Assert.assertEquals("999", jobInfo.getCode());
        Assert.assertEquals("KE-060100201", jobInfo.getErrorCode());
        Assert.assertEquals("Please check whether the external environment(other systems, components, etc.) is normal.",
                jobInfo.getSuggestion());
        Assert.assertEquals("KE-060100201: An Exception occurred outside Kyligence Enterprise.", jobInfo.getMsg());
        Assert.assertTrue(jobInfo.getStacktrace().startsWith("java.lang.RuntimeException"));
    }

    @Test
    public void testExtractThrowableWithKylinException() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96";
        long duration = 1000L;
        long waitTime = 100L;
        String jobState = "ERROR";
        String jobType = "INDEX_BUILD";
        Set<String> segIds = new HashSet<>();
        segIds.add("11124840-b3e3-43db-bcab-2b78da666d00");
        Set<Long> layoutIds = new HashSet<>();
        layoutIds.add(1L);
        Set<Long> partitionIds = null;
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;
        Throwable throwable = new KylinException(JOB_NOT_EXIST, jobId);
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, partitionIds, waitTime, "", "", true, startTime, endTime, null, throwable);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertEquals("999", jobInfo.getCode());
        Assert.assertEquals("ERROR", jobInfo.getState());
        Assert.assertEquals("KE-010032219", jobInfo.getErrorCode());
        Assert.assertEquals("", jobInfo.getSuggestion());
        Assert.assertEquals("KE-010032219: Can't find job \"test_job_id\". Please check and try again.",
                jobInfo.getMsg());
        Assert.assertTrue(jobInfo.getStacktrace().startsWith("KE-010032219: Can't find job \"test_job_id\"."));
    }

    @Test
    public void testExtractInfoMultiPartition() {
        String jobId = "test_job_id";
        String project = "default";
        String subject = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        long duration = 1000L;
        String jobState = "SUCCEED";
        String jobType = "INDEX_BUILD";
        Set<String> segIds = new HashSet<>();
        segIds.add("0db919f3-1359-496c-aab5-b6f3951adc0e");
        segIds.add("ff839b0b-2c23-4420-b332-0df70e36c343");
        Set<Long> layoutIds = new HashSet<>();
        layoutIds.add(1L);
        Set<Long> partitionIds = new HashSet<>();
        partitionIds.add(7L);
        partitionIds.add(8L);
        long startTime = 1626135824000L;
        long endTime = 1626144908000L;
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, partitionIds, 0L, null, "", true, startTime, endTime, null, null);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertTrue(jobInfo.getSegmentIds().containsAll(segIds));
        Assert.assertEquals(segIds.size(), jobInfo.getSegmentIds().size());
        Assert.assertEquals(2, jobInfo.getSegmentPartitionInfoList().size());
        Assert.assertEquals("0db919f3-1359-496c-aab5-b6f3951adc0e",
                jobInfo.getSegmentPartitionInfoList().get(0).getSegmentId());
        var partitionInfos = jobInfo.getSegmentPartitionInfoList().get(0).getPartitionInfo();
        Assert.assertEquals(7, partitionInfos.get(0).getPartitionId());
        Assert.assertEquals(8, partitionInfos.get(1).getPartitionId());

        Assert.assertEquals("ff839b0b-2c23-4420-b332-0df70e36c343",
                jobInfo.getSegmentPartitionInfoList().get(1).getSegmentId());

        // test layout is null
        JobFinishedNotifier notifier2 = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, partitionIds, 0L, null, "", true, startTime, endTime, null, null);
        notifier2.getSegmentPartitionsMap().put("0db919f3-1359-496c-aab5-b6f3951adc0e", null);
        jobInfo = JobSyncListener.extractJobInfo(notifier2);
        Assert.assertEquals(1, jobInfo.getSegmentPartitionInfoList().size());
        Assert.assertEquals("ff839b0b-2c23-4420-b332-0df70e36c343",
                jobInfo.getSegmentPartitionInfoList().get(0).getSegmentId());
        partitionInfos = jobInfo.getSegmentPartitionInfoList().get(0).getPartitionInfo();
        Assert.assertEquals(7, partitionInfos.get(0).getPartitionId());
        Assert.assertEquals(8, partitionInfos.get(1).getPartitionId());

        // test layout is empty
        JobFinishedNotifier notifier3 = new JobFinishedNotifier(jobId, project, subject, duration, jobState, jobType,
                segIds, layoutIds, partitionIds, 0L, null, "", true, startTime, endTime, null, null);
        notifier3.getSegmentPartitionsMap().put("ff839b0b-2c23-4420-b332-0df70e36c343", new HashSet<>());
        jobInfo = JobSyncListener.extractJobInfo(notifier3);
        Assert.assertEquals(1, jobInfo.getSegmentPartitionInfoList().size());
        Assert.assertEquals("0db919f3-1359-496c-aab5-b6f3951adc0e",
                jobInfo.getSegmentPartitionInfoList().get(0).getSegmentId());
        partitionInfos = jobInfo.getSegmentPartitionInfoList().get(0).getPartitionInfo();
        Assert.assertEquals(7, partitionInfos.get(0).getPartitionId());
        Assert.assertEquals(8, partitionInfos.get(1).getPartitionId());
    }

    @Test
    public void testRecordPrometheusMetric() {
        JobSyncListener jobSyncListener = Mockito.spy(JobSyncListener.class);
        JobFinishedNotifier notifier = Mockito.mock(JobFinishedNotifier.class);
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        Mockito.when(notifier.getJobType()).thenReturn(JobTypeEnum.STREAMING_BUILD.toString());
        Mockito.when(notifier.getProject()).thenReturn("project");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metrics.prometheus-enabled", "false");
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "model", ExecutableState.ERROR);
        Collection<Meter> meters1 = meterRegistry.getMeters();
        Assert.assertEquals(0, meters1.size());

        KylinConfig.getInstanceFromEnv().setProperty("kylin.metrics.prometheus-enabled", "true");
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "", ExecutableState.ERROR);
        Collection<Meter> meters2 = meterRegistry.getMeters();
        Assert.assertEquals(1, meters2.size());

        Mockito.when(notifier.getJobType()).thenReturn(JobTypeEnum.INDEX_BUILD.toString());
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "model", ExecutableState.ERROR);
        Collection<Meter> meters3 = meterRegistry.find(PrometheusMetrics.MODEL_BUILD_DURATION.getValue()).meters();
        Collection<Meter> meters4 = meterRegistry.find(PrometheusMetrics.JOB_MINUTES.getValue()).meters();
        Assert.assertEquals(1, meters3.size());
        Assert.assertEquals(2, meters4.size());

        Mockito.when(notifier.getJobType()).thenReturn("TEST");
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "model", ExecutableState.SUCCEED);

        Mockito.when(notifier.getJobType()).thenReturn("TEST2");
        jobSyncListener.recordPrometheusMetric(notifier, meterRegistry, "model", ExecutableState.RUNNING);
        Assert.assertEquals(7, meterRegistry.getMeters().size());
    }

    @Test
    public void testSwitchChinese() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.job.callback-language", "cn");
        String jobId = "test_job_id";
        String project = "default";
        Throwable throwable = new KylinException(JOB_NOT_EXIST, jobId);
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, null, 0, null, null, null, null, null,
                0L, null, "", true, 0, 10, null, throwable);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertTrue(containChinese(jobInfo.getMsg()));
    }

    @Test
    public void testDefaultLanguage() {
        String jobId = "test_job_id";
        String project = "default";
        Throwable throwable = new KylinException(JOB_NOT_EXIST, jobId);
        JobFinishedNotifier notifier = new JobFinishedNotifier(jobId, project, null, 0L, null, null, null, null, null,
                0L, null, "", true, 0L, 0L, null, throwable);
        JobSyncListener.JobInfo jobInfo = JobSyncListener.extractJobInfo(notifier);
        Assert.assertFalse(containChinese(jobInfo.getMsg()));
    }

    private static boolean containChinese(String str) {
        Pattern p = Pattern.compile("[\u4E00-\u9FA5]");
        Matcher m = p.matcher(str);
        return m.find();
    }

    static class ModelHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            try {
                assert "Basic QURNSU46S1lMSU4="
                        .equals(httpExchange.getRequestHeaders().get(HttpHeaders.AUTHORIZATION).get(0));
                InputStream in = httpExchange.getRequestBody();
                String s = IOUtils.toString(in);
                if (s.equals(JsonUtil.writeValueAsString(modelInfo))) {
                    assertMeet = true;
                }
            } finally {
                httpExchange.sendResponseHeaders(HttpStatus.SC_OK, 0L);
                httpExchange.close();
                latch.countDown();
            }
        }
    }

    static class TimeoutHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange httpExchange) {
            latch.countDown();
        }
    }
}
