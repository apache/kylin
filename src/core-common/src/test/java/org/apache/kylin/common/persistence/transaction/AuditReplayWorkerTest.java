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

package org.apache.kylin.common.persistence.transaction;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.InvalidTimeoutException;
import org.springframework.transaction.TransactionUsageException;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
@SuppressWarnings("unchecked")
public class AuditReplayWorkerTest {

    private static final String LOCAL_INSTANCE = "127.0.0.1";
    private final Charset charset = Charset.defaultCharset();

    @Test
    void testStartSchedule() throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);

        val auditLogStore = workerStore.getAuditLogStore();
        val replayWorker = (AuditLogReplayWorker) ReflectionTestUtils.getField(auditLogStore, "replayWorker");
        Assertions.assertNotNull(replayWorker);
        replayWorker.startSchedule(2, true);
        Assertions.assertEquals(2, auditLogStore.getLogOffset());

        replayWorker.startSchedule(3, false);
        Assertions.assertEquals(3, auditLogStore.getLogOffset());
        auditLogStore.close();
    }

    @Test
    void testRestoreSmallId(JdbcInfo info) throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);

        val auditLogStore = workerStore.getAuditLogStore();
        val jdbcTemplate = info.getJdbcTemplate();

        val url = getTestConfig().getMetadataUrl();
        String unitId = RandomUtil.randomUUIDStr();
        val auditLogTableName = url.getIdentifier() + "_audit_log";
        String sql = "insert into %s (id, meta_key,meta_content,meta_ts,meta_mvcc,unit_id,operator,instance) values (?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, auditLogTableName),
                Arrays.asList(
                        new Object[] { 900, "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 4, "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));

        auditLogStore.restore(3);
        Assertions.assertEquals(900, auditLogStore.getLogOffset());
        auditLogStore.close();
    }

    @Test
    void testRestoreUpdateOffset() throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        val auditLogStore = workerStore.getAuditLogStore();
        val replayWorker = (AuditLogReplayWorker) ReflectionTestUtils.getField(auditLogStore, "replayWorker");
        Assertions.assertNotNull(replayWorker);
        replayWorker.updateOffset(101);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());

        replayWorker.updateOffset(99);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());
        auditLogStore.close();
    }

    @Test
    void testRestoreHasCatchUp() throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        val auditLogStore = workerStore.getAuditLogStore();
        val replayWorker = (AuditLogReplayWorker) ReflectionTestUtils.getField(auditLogStore, "replayWorker");
        Assertions.assertNotNull(replayWorker);
        replayWorker.updateOffset(101);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());

        val hasCatch = (Boolean) ReflectionTestUtils.invokeMethod(replayWorker, "hasCatch", 100L);
        Assertions.assertNotNull(hasCatch);
        Assertions.assertTrue(hasCatch);

        val hasCatchFalse = (Boolean) ReflectionTestUtils.invokeMethod(replayWorker, "hasCatch", 102L);
        Assertions.assertNotNull(hasCatchFalse);
        Assertions.assertFalse(hasCatchFalse);
        auditLogStore.close();
    }

    @Test
    void testCatchupInternal_Stopped() throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        val auditLogStore = workerStore.getAuditLogStore();
        val replayWorker = (AuditLogReplayWorker) ReflectionTestUtils.getField(auditLogStore, "replayWorker");
        Assertions.assertNotNull(replayWorker);
        replayWorker.updateOffset(101);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());
        val isStopped = (AtomicBoolean) ReflectionTestUtils.getField(replayWorker, "isStopped");
        Assertions.assertNotNull(isStopped);
        isStopped.set(true);
        ReflectionTestUtils.invokeMethod(replayWorker, "catchupInternal", 1);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());
        auditLogStore.close();
    }

    @Test
    void testCatchupInternal_TransactionUsageException() throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        val auditLogStore = workerStore.getAuditLogStore();
        val replayWorker = (AuditLogReplayWorker) ReflectionTestUtils.getField(auditLogStore, "replayWorker");
        Assertions.assertNotNull(replayWorker);
        replayWorker.updateOffset(101);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());
        val isStopped = (AtomicBoolean) ReflectionTestUtils.getField(replayWorker, "isStopped");
        Assertions.assertNotNull(isStopped);
        isStopped.set(false);
        val replayWorkerSpy = Mockito.spy(replayWorker);
        Mockito.doThrow(new InvalidTimeoutException("xxxx", 123)).when(replayWorkerSpy).catchupToMaxId(1L);
        try {
            ReflectionTestUtils.invokeMethod(replayWorkerSpy, "catchupToMaxId", 1L);
            Assertions.fail();
        } catch (TransactionUsageException e) {
            Assertions.assertEquals("xxxx", e.getMessage());
        }
        auditLogStore.close();
    }

    @Test
    void testCatchupInternal_OtherException() {
        val replayWorker = getAuditLogReplayWorker();
        val replayWorkerSpy = Mockito.spy(replayWorker);

        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorkerSpy, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(1, 123));
        Assertions.assertFalse(delayIdQueue.isEmpty());

        val exception = new IllegalArgumentException("xxxx");
        Mockito.doThrow(exception).when(replayWorkerSpy).catchupToMaxId(0L);
        Mockito.doNothing().when(replayWorkerSpy).handleReloadAll(exception);
        replayWorkerSpy.catchupInternal(0);

        Assertions.assertTrue(delayIdQueue.isEmpty());
        replayWorker.close(true);
    }

    @Test
    void testCollectReplayDelayedId_EmptyQueue() {
        val replayWorker = getAuditLogReplayWorker();
        Assertions.assertNotNull(replayWorker);
        ReflectionTestUtils.invokeMethod(replayWorker, "collectReplayDelayedId", 1);
        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorker, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);

        Assertions.assertTrue(CollectionUtils.isEmpty(delayIdQueue));
        replayWorker.close(true);
    }

    @Test
    void testCollectReplayDelayedId_NotEmptyQueue() {
        val replayWorker = getAuditLogReplayWorker();
        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorker, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(1L, System.currentTimeMillis()));
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(2L, System.currentTimeMillis()));

        val delayedId = (List<Long>) ReflectionTestUtils.invokeMethod(replayWorker, "collectReplayDelayedId", 10);

        Assertions.assertEquals(Arrays.asList(1L, 2L), delayedId);
        replayWorker.close(true);
    }

    @Test
    void testCollectReplayDelayedId_MaxCount() {
        val replayWorker = getAuditLogReplayWorker();
        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorker, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(1L, System.currentTimeMillis()));
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(2L, System.currentTimeMillis()));
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(3L, System.currentTimeMillis()));

        val delayedId = (List<Long>) ReflectionTestUtils.invokeMethod(replayWorker, "collectReplayDelayedId", 2);

        Assertions.assertEquals(Arrays.asList(1L, 2L), delayedId);
        Assertions.assertEquals(3, delayIdQueue.size());
        replayWorker.close(true);
    }

    @Test
    void testCollectReplayDelayedId_Timeout() {
        val timeout = getTestConfig().getEventualReplayDelayItemTimeout();
        val replayWorker = getAuditLogReplayWorker();
        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorker, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(1L, System.currentTimeMillis() - timeout * 2));
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(2L, System.currentTimeMillis()));
        delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(3L, System.currentTimeMillis()));

        val delayedId = (List<Long>) ReflectionTestUtils.invokeMethod(replayWorker, "collectReplayDelayedId", 2);

        Assertions.assertEquals(Arrays.asList(1L, 2L), delayedId);
        Assertions.assertEquals(2, delayIdQueue.size());
        replayWorker.close(true);
    }

    @Test
    void testWaitMaxIdOk() {
        val replayWorker = getAuditLogReplayWorker();
        {
            val maxIdOk = (Boolean) ReflectionTestUtils.invokeMethod(replayWorker, "waitMaxIdOk", 100L, 100L);
            Assertions.assertNotNull(maxIdOk);
            Assertions.assertTrue(maxIdOk);
        }

        {
            val maxIdOk = (Boolean) ReflectionTestUtils.invokeMethod(replayWorker, "waitMaxIdOk", 101L, 100L);
            Assertions.assertNotNull(maxIdOk);
            Assertions.assertFalse(maxIdOk);
        }

        {
            getTestConfig().setProperty("kylin.auditlog.replay-need-consecutive-log", KylinConfigBase.FALSE);
            val maxIdOk = (Boolean) ReflectionTestUtils.invokeMethod(replayWorker, "waitMaxIdOk", 101L, 100L);
            Assertions.assertNotNull(maxIdOk);
            Assertions.assertTrue(maxIdOk);
        }
        replayWorker.close(true);
    }

    @Test
    void testRecordStepAbsentIdList_EmptyList() {
        val replayWorker = getAuditLogReplayWorker();
        val stepWin = new AbstractAuditLogReplayWorker.FixedWindow(100, 150);
        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorker, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);

        ReflectionTestUtils.invokeMethod(replayWorker, "recordStepAbsentIdList", stepWin, Collections.EMPTY_LIST);
        Assertions.assertTrue(delayIdQueue.isEmpty());
        replayWorker.close(true);
    }

    @Test
    void testRecordStepAbsentIdList_SameLength() {
        val replayWorker = getAuditLogReplayWorker();
        val stepWin = new AbstractAuditLogReplayWorker.FixedWindow(100, 101);
        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorker, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);

        ReflectionTestUtils.invokeMethod(replayWorker, "recordStepAbsentIdList", stepWin,
                Collections.singletonList(new AuditLog(101L, "adaasd",
                        ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)), 1L, 1L, null, null, null)));
        Assertions.assertTrue(delayIdQueue.isEmpty());
        replayWorker.close(true);
    }

    @Test
    void testRecordStepAbsentIdList_SkipTooOldAudit() {
        val replayWorker = getAuditLogReplayWorker();
        val stepWin = new AbstractAuditLogReplayWorker.FixedWindow(100, 104);
        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorker, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);
        val timeout = (Long) ReflectionTestUtils.getField(replayWorker, "idEarliestTimeoutMills");
        Assertions.assertNotNull(timeout);

        val auditLogs = Arrays.asList(
                new AuditLog(101L, "adaasd", ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)),
                        System.currentTimeMillis() - timeout * 2, 1L, null, null, null),
                new AuditLog(102L, "adaasd", ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)),
                        System.currentTimeMillis() - timeout * 2, 1L, null, null, null),
                new AuditLog(103L, "adaasd", ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)),
                        System.currentTimeMillis() - timeout * 2, 1L, null, null, null));

        ReflectionTestUtils.invokeMethod(replayWorker, "recordStepAbsentIdList", stepWin, auditLogs);

        Assertions.assertTrue(delayIdQueue.isEmpty());
        replayWorker.close(true);
    }

    @Test
    void testRecordStepAbsentIdList_CollectAbsentId() {
        val replayWorker = getAuditLogReplayWorker();
        val stepWin = new AbstractAuditLogReplayWorker.FixedWindow(99, 104);
        val delayIdQueue = (ConcurrentLinkedQueue<AuditLogReplayWorker.AuditIdTimeItem>) ReflectionTestUtils
                .getField(replayWorker, "delayIdQueue");
        Assertions.assertNotNull(delayIdQueue);
        val timeout = (Long) ReflectionTestUtils.getField(replayWorker, "idEarliestTimeoutMills");
        Assertions.assertNotNull(timeout);

        val auditLogs = Arrays.asList(
                new AuditLog(101L, "adaasd", ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)),
                        System.currentTimeMillis(), 1L, null, null, null),
                new AuditLog(102L, "adaasd", ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)),
                        System.currentTimeMillis(), 1L, null, null, null),
                new AuditLog(103L, "adaasd", ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)),
                        System.currentTimeMillis(), 1L, null, null, null));

        ReflectionTestUtils.invokeMethod(replayWorker, "recordStepAbsentIdList", stepWin, auditLogs);

        Assertions.assertEquals(Arrays.asList(100L, 104L), delayIdQueue.stream()
                .map(AuditLogReplayWorker.AuditIdTimeItem::getAuditLogId).collect(Collectors.toList()));
        replayWorker.close(true);
    }

    @Test
    void testFindAbsentId_EmptyList() {
        val replayWorker = getAuditLogReplayWorker();
        val stepWin = new AbstractAuditLogReplayWorker.FixedWindow(99, 104);
        val result = (List<Long>) ReflectionTestUtils.invokeMethod(replayWorker, "findAbsentId",
                Collections.emptyList(), stepWin);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
        replayWorker.close(true);
    }

    @Test
    void testFindAbsentId_CollectId() {
        val replayWorker = getAuditLogReplayWorker();
        val stepWin = new AbstractAuditLogReplayWorker.FixedWindow(99, 104);
        val result = (List<Long>) ReflectionTestUtils.invokeMethod(replayWorker, "findAbsentId",
                Arrays.asList(101L, 104L), stepWin);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(Arrays.asList(100L, 102L, 103L), result);
        replayWorker.close(true);
    }

    private AuditLogReplayWorker getAuditLogReplayWorker() {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        val auditLogStore = workerStore.getAuditLogStore();
        val replayWorker = (AuditLogReplayWorker) ReflectionTestUtils.getField(auditLogStore, "replayWorker");
        Assertions.assertNotNull(replayWorker);
        return replayWorker;
    }

}
