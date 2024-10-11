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
package org.apache.kylin.common.persistence.metadata;

import static org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore.SELECT_LIST_TERM;
import static org.apache.kylin.common.persistence.metadata.JdbcAuditLogStoreTool.mockAuditLogForProjectEntry;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.getJdbcTemplate;
import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceTool;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.lock.MemoryLockUtils;
import org.apache.kylin.common.persistence.metadata.jdbc.AuditLogRowMapper;
import org.apache.kylin.common.persistence.transaction.AuditLogReplayWorker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
class JdbcAuditLogStoreTest {

    private static final String LOCAL_INSTANCE = "127.0.0.1";
    private final Charset charset = Charset.defaultCharset();

    @AfterEach
    public void destroy() throws Exception {
        val jdbcTemplate = getJdbcTemplate(KylinConfig.getInstanceFromEnv());
        jdbcTemplate.batchUpdate("SHUTDOWN;");
    }

    private void prepare2Resource() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            MemoryLockUtils.lockAndRecord("PROJECT/abc");
            MemoryLockUtils.lockAndRecord("PROJECT/abc2");
            MemoryLockUtils.lockAndRecord("PROJECT/abc3");
            store.checkAndPutResource("PROJECT/abc", ByteSource
                    .wrap(("{\"name\" : \"abc\",\"uuid\" : \"" + UUID.randomUUID() + "\"}").getBytes(charset)), -1);
            store.checkAndPutResource("PROJECT/abc2", ByteSource
                    .wrap(("{\"name\" : \"abc2\",\"uuid\" : \"" + UUID.randomUUID() + "\"}").getBytes(charset)), -1);
            ByteSource wrap2 = ByteSource
                    .wrap(("{\"name\" : \"abc3\",\"uuid\" : \"" + UUID.randomUUID() + "\"}").getBytes(charset));
            store.checkAndPutResource("PROJECT/abc3", wrap2, -1);
            store.checkAndPutResource("PROJECT/abc3", wrap2, 0);
            store.deleteResource("PROJECT/abc");
            return 0;
        }, "_global");
    }

    @Test
    void testUpdateResourceWithLog(JdbcInfo info) throws Exception {
        prepare2Resource();
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        val all = jdbcTemplate.query(SELECT_LIST_TERM + " from " + url.getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX,
                new AuditLogRowMapper());

        Assert.assertEquals(5, all.size());
        Assert.assertEquals("PROJECT/abc", all.get(0).getResPath());
        Assert.assertEquals("PROJECT/abc2", all.get(1).getResPath());
        Assert.assertEquals("PROJECT/abc3", all.get(2).getResPath());
        Assert.assertEquals("PROJECT/abc3", all.get(3).getResPath());
        Assert.assertEquals("PROJECT/abc", all.get(4).getResPath());

        Assert.assertEquals(Long.valueOf(0), all.get(0).getMvcc());
        Assert.assertEquals(Long.valueOf(0), all.get(1).getMvcc());
        Assert.assertEquals(Long.valueOf(0), all.get(2).getMvcc());
        Assert.assertEquals(Long.valueOf(1), all.get(3).getMvcc());
        Assert.assertEquals(false, all.get(0).isDiffFlag());
        Assert.assertEquals(false, all.get(1).isDiffFlag());
        Assert.assertEquals(false, all.get(2).isDiffFlag());
        Assert.assertEquals(true, all.get(3).isDiffFlag());
        Assert.assertNull(all.get(4).getMvcc());

        Assert.assertEquals(1, all.stream().map(AuditLog::getUnitId).distinct().count());

        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken("USER1", "ADMIN"));
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.deleteResource("PROJECT/abc2");
            store.deleteResource("PROJECT/abc3");
            return 0;
        }, "_global");

        val allStep2 = jdbcTemplate.query(SELECT_LIST_TERM + " from " + url.getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX,
                new AuditLogRowMapper());

        Assert.assertEquals(7, allStep2.size());
        Assert.assertNull(allStep2.get(5).getMvcc());
        Assert.assertNull(allStep2.get(6).getMvcc());
        Assert.assertEquals("USER1", allStep2.get(5).getOperator());
        Assert.assertEquals("USER1", allStep2.get(6).getOperator());
        Assert.assertEquals(2, allStep2.stream().map(AuditLog::getUnitId).distinct().count());
        AuditLogStore auditLogStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getAuditLogStore();
        auditLogStore.close();
    }

    @Test
    void testRestore(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        mockAuditLogForProjectEntry("abc", info, false);
        mockAuditLogForProjectEntry("abc2", info, false);
        String abc3Uuid = RandomUtil.randomUUIDStr();
        mockAuditLogForProjectEntry(abc3Uuid, "abc3", info, false, 0, "unitId");
        mockAuditLogForProjectEntry(abc3Uuid, "abc3", info, false, 1, "unitId");
        mockAuditLogForProjectEntry("abc", info, true);
        workerStore.catchup();
        NavigableSet<String> strings = workerStore.listResourcesRecursively(MetadataType.ALL.name());
        Assertions.assertEquals(3, strings.size());
        for (int i = 0; i < 1000; i++) {
            val projectName = "p" + (i + 1000);
            String unitId = RandomUtil.randomUUIDStr();
            mockAuditLogForProjectEntry("a_" + projectName, info, false);
            mockAuditLogForProjectEntry("b_" + projectName, info, false);
            mockAuditLogForProjectEntry("c_" + projectName, info, false);
            mockAuditLogForProjectEntry(RandomUtil.randomUUIDStr(), "c_" + projectName, info, false,
                    1, unitId);
            mockAuditLogForProjectEntry("a_" + projectName, info, true);
        }
        workerStore.getAuditLogStore().catchupWithTimeout();
        Awaitility.await().atMost(8, TimeUnit.SECONDS)
                .until(() -> 2003 == workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        workerStore.getAuditLogStore().catchupWithTimeout();
        Assertions.assertEquals(2003, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
        workerStore.getAuditLogStore().close();
    }

    @Test
    void testRestoreWithDev(JdbcInfo info) throws Exception {
        KylinConfig testConfig = getTestConfig();
        val workerStore = ResourceStore.getKylinMetaStore(testConfig);
        workerStore.createMetaStoreUuidIfNotExist();
        // Make sure the env does not affect the creation of UUID
        testConfig.setProperty("kylin.env", "DEV");
        Assert.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        prepare2Resource();
        workerStore.catchup();
        NavigableSet<String> strings = workerStore.listResourcesRecursively(MetadataType.ALL.name());
        Assert.assertEquals(3, strings.size());
        for (int i = 0; i < 1000; i++) {
            val projectName = "p" + (i + 1000);
            String unitId = RandomUtil.randomUUIDStr();
            mockAuditLogForProjectEntry("a_" + projectName, info, false);
            mockAuditLogForProjectEntry("b_" + projectName, info, false);
            mockAuditLogForProjectEntry("c_" + projectName, info, false);
            mockAuditLogForProjectEntry(RandomUtil.randomUUIDStr(), "c_" + projectName, info, false,
                    1, unitId);
            mockAuditLogForProjectEntry("a_" + projectName, info, true);
        }
        workerStore.getAuditLogStore().catchupWithTimeout();
        Awaitility.await().atMost(8, TimeUnit.SECONDS)
                .until(() -> 2003 == workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        workerStore.getAuditLogStore().catchupWithTimeout();
        Assert.assertEquals(2003, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
        testConfig.setProperty("kylin.env", "UT");
        workerStore.getAuditLogStore().close();
    }

    @Test
    @OverwriteProp.OverwriteProps({ //
            // To skip check the mvcc when save json content diff
            @OverwriteProp(key = "kylin.metadata.audit-log-json-patch-enabled", value = "false"), //
    })
    void testHandleVersionConflict(JdbcInfo info) throws Exception {
        getTestConfig().setProperty("kylin.auditlog.replay-groupby-project-reload-enable", "false");
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        RawResource rawResource = RawResourceTool.createProjectRawResource("abc", 2);
        workerStore.getMetadataStore().save(rawResource.getMetaType(), rawResource);

        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());

        mockAuditLogForProjectEntry("abc", info, false);
        mockAuditLogForProjectEntry(RandomUtil.randomUUIDStr(), "abc", info, false, 2,
                RandomUtil.randomUUIDStr());
        workerStore.catchup();
        // catch up exception, load PROJECT/abc from metadata
        Assertions.assertEquals(2, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
        // mvcc will be fixed to 1
        Assertions.assertEquals(1, workerStore.getResource("PROJECT/abc").getMvcc());
    }

    @Test
    public void testWaitLogAllCommit(JdbcInfo info) throws Exception {
        getTestConfig().setProperty("kylin.auditlog.replay-groupby-project-reload-enable", "false");
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());

        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        String sql = "insert into %s (id, meta_key,meta_content,meta_ts,meta_mvcc,unit_id,operator,instance,project,"
                + "diff_flag) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX),
                Arrays.asList(
                        new Object[] { 22, "PROJECT/abc",
                                ("{ \"uuid\" : \"" + UUID.randomUUID()
                                        + "\",\"meta_key\" : \"abc\",\"name\" : \"abc\"}").getBytes(charset),
                                System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE, null, false },
                        new Object[] { 4, "PROJECT/abc2",
                                ("{ \"uuid\" : \"" + UUID.randomUUID()
                                        + "\",\"meta_key\" : \"abc2\",\"name\" : \"abc2\"}").getBytes(charset),
                                System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE, null, false }));
        workerStore.getAuditLogStore().restore(3);
        Assert.assertEquals(3, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    void testWaitLogAllCommit_DelayQueue(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        String sql = "insert into %s (id, meta_key,meta_content,meta_ts,meta_mvcc,unit_id,operator,instance,project,"
                + "diff_flag) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX),
                Arrays.asList(
                        new Object[] { 900, "PROJECT/abc",
                                ("{ \"uuid\" : \"" + UUID.randomUUID()
                                        + "\",\"meta_key\" : \"abc\",\"name\" : \"abc\"}").getBytes(charset),
                                System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE, null, false },
                        new Object[] { 4, "PROJECT/abc2",
                                ("{ \"uuid\" : \"" + UUID.randomUUID()
                                        + "\",\"meta_key\" : \"abc2\",\"name\" : \"abc2\"}").getBytes(charset),
                                System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE, null, false }));

        // It will execute a 5s once scheduled thread
        workerStore.getAuditLogStore().restore(3);
        Assertions.assertEquals(3, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX),
                Arrays.asList(
                        new Object[] { 800, "PROJECT/abc3",
                                ("{ \"uuid\" : \"" + UUID.randomUUID()
                                        + "\",\"meta_key\" : \"abc\",\"name\" : \"abc\"}").getBytes(charset),
                                System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE, null, false },
                        new Object[] { 801, "PROJECT/abc4",
                                ("{ \"uuid\" : \"" + UUID.randomUUID()
                                        + "\",\"meta_key\" : \"abc2\",\"name\" : \"abc2\"}").getBytes(charset),
                                System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE, null, false }));

        workerStore.getAuditLogStore().catchup();
        // Wait for delay queue to execute
        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> 5 == workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        workerStore.getAuditLogStore().close();
    }

    @Test
    void testFetchById(JdbcInfo info) throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        mockAuditLogForProjectEntry("abc", info, false);
        mockAuditLogForProjectEntry("abc2", info, false);
        mockAuditLogForProjectEntry("abc3", info, false);
        mockAuditLogForProjectEntry("abc4", info, false);
        val idList = Arrays.asList(2L, 3L);
        val fetchResult = ((JdbcAuditLogStore) workerStore.getAuditLogStore()).fetch(idList);

        Assertions.assertEquals(2, fetchResult.size());
        Assertions.assertEquals(idList, fetchResult.stream().map(AuditLog::getId).collect(Collectors.toList()));
        workerStore.getAuditLogStore().close();
    }

    @Test
    void testGetMinMaxId(JdbcInfo info) throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        UnitOfWork.doInTransactionWithRetry(() -> {
            MemoryLockUtils.lockAndRecord(ResourceStore.METASTORE_UUID_TAG);
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
            return null;
        }, "p1");

        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        String sql = "insert into %s (id, meta_key,meta_content,meta_ts,meta_mvcc,unit_id,operator,instance,project,"
                + "diff_flag) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX),
                Arrays.asList(
                        new Object[] { 900, "PROJECT/abc",
                                ("{ \"uuid\" : \"" + UUID.randomUUID()
                                        + "\",\"meta_key\" : \"abc\",\"name\" : \"abc\"}").getBytes(charset),
                                System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE, null, false },
                        new Object[] { 4, "PROJECT/abc2",
                                ("{ \"uuid\" : \"" + UUID.randomUUID()
                                        + "\",\"meta_key\" : \"abc2\",\"name\" : \"abc2\"}").getBytes(charset),
                                System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE, null, false }));
        val auditLogStore = workerStore.getAuditLogStore();
        Assertions.assertEquals(900, auditLogStore.getMaxId());
        Assertions.assertEquals(1, auditLogStore.getMinId());
        auditLogStore.close();
    }

    @Test
    void testSaveWait_WithoutSleep(JdbcInfo info) throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent("PROJECT/test",
                new RawResource("test", RawResourceTool.createByteSource("test"), System.currentTimeMillis(), 0)));
        val unitMessages = new UnitMessages(events);
        val auditLogStore = workerStore.getAuditLogStore();
        auditLogStore.save(unitMessages);

        val result = auditLogStore.fetch(0, 2);
        Assertions.assertEquals(1, result.size());
        auditLogStore.close();
    }

    @Test
    void testSaveWait_WithSleep(JdbcInfo info) throws IOException {
        getTestConfig().setProperty("kylin.env.unitofwork-simulation-enabled", KylinConfigBase.TRUE);
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent("PROJECT/test",
                new RawResource("test", RawResourceTool.createByteSource("test"), System.currentTimeMillis(), 0)));
        val unitMessages = new UnitMessages(events);
        val auditLogStore = workerStore.getAuditLogStore();

        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT).sleepMills(100)
                .maxRetry(1).processor(() -> {
                    auditLogStore.save(unitMessages);
                    return 0;
                }).build());

        val result = auditLogStore.fetch(0, 2);
        Assertions.assertEquals(1, result.size());
        auditLogStore.close();
    }

    @Test
    void testSaveWait_WithSleepNotInTrans(JdbcInfo info) throws IOException {
        getTestConfig().setProperty("kylin.env.unitofwork-simulation-enabled", KylinConfigBase.TRUE);
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent("PROJECT/test",
                new RawResource("test", RawResourceTool.createByteSource("test"), System.currentTimeMillis(), 0)));
        val unitMessages = new UnitMessages(events);
        val auditLogStore = workerStore.getAuditLogStore();

        auditLogStore.save(unitMessages);

        val result = auditLogStore.fetch(0, 2);
        Assertions.assertEquals(1, result.size());
        auditLogStore.close();
    }

    @Test
    void testRestoreWithoutOrder(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        String unitId1 = RandomUtil.randomUUIDStr();
        String unitId2 = RandomUtil.randomUUIDStr();
        String uuid = RandomUtil.randomUUIDStr();
        String uuid3 = RandomUtil.randomUUIDStr();
        mockAuditLogForProjectEntry(uuid, "abc", info, false, 0, unitId1);
        mockAuditLogForProjectEntry(RandomUtil.randomUUIDStr(), "abc2", info, false, 0, unitId2);
        mockAuditLogForProjectEntry(uuid3, "abc3", info, false, 0, unitId1);
        mockAuditLogForProjectEntry(uuid3, "abc3", info, false, 1, unitId2);
        mockAuditLogForProjectEntry(uuid, "abc", info, true, 1, unitId1);
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());

        String uuid4 = RandomUtil.randomUUIDStr();
        // Use the json patch content
        AuditLog abc4 = JdbcAuditLogStoreTool.createProjectAuditLog("abc4", 0, uuid4, unitId1, false);
        List<Event> events = Arrays.asList(Event.fromLog(abc4),
                Event.fromLog(JdbcAuditLogStoreTool.createProjectAuditLog("abc4", 1, uuid4, unitId2, abc4)),
                Event.fromLog(JdbcAuditLogStoreTool.createProjectAuditLog("abc5", 0, RandomUtil.randomUUIDStr(),
                        unitId1, false)));
        val unitMessages = new UnitMessages(events);
        val auditLogStore = workerStore.getAuditLogStore();
        auditLogStore.save(unitMessages);
        workerStore.catchup();
        Assert.assertEquals(4, workerStore.listResourcesRecursively(MetadataType.PROJECT.name()).size());

        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    void testRestore_WhenOtherAppend(JdbcInfo info) throws Exception {
        KylinConfig testConfig = getTestConfig();
        val workerStore = ResourceStore.getKylinMetaStore(testConfig);

        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        val jdbcTemplate = info.getJdbcTemplate();
        val auditLogTableName = testConfig.getMetadataUrl().getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX;

        val stopped = new AtomicBoolean(false);
        new Thread(() -> {
            int i = 0;
            String uuid = RandomUtil.randomUUIDStr();
            String uuid2 = RandomUtil.randomUUIDStr();
            String uuid3 = RandomUtil.randomUUIDStr();
            val unitId = RandomUtil.randomUUIDStr();
            AuditLog abc = JdbcAuditLogStoreTool.createProjectAuditLog("abc", i, uuid, unitId, false);
            AuditLog abc2 = JdbcAuditLogStoreTool.createProjectAuditLog("abc2", i, uuid, unitId, false);
            AuditLog abc3 = JdbcAuditLogStoreTool.createProjectAuditLog("abc3", i, uuid, unitId, false);
            while (!stopped.get()) {
                // Use the json patch content
                List<Event> events = i == 0
                        ? Arrays.asList(Event.fromLog(abc), Event.fromLog(abc2), Event.fromLog(abc3))
                        : Arrays.asList(
                                Event.fromLog(
                                        abc = JdbcAuditLogStoreTool.createProjectAuditLog("abc", i, uuid, unitId, abc)),
                                Event.fromLog(abc2 = JdbcAuditLogStoreTool.createProjectAuditLog("abc2", i, uuid2,
                                        unitId, abc2)),
                                Event.fromLog(abc3 = JdbcAuditLogStoreTool.createProjectAuditLog("abc3", i, uuid3,
                                        unitId, abc3)));
                val unitMessages = new UnitMessages(events);
                val auditLogStore = workerStore.getAuditLogStore();
                auditLogStore.save(unitMessages);
                i++;
            }
        }).start();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(
                () -> jdbcTemplate.queryForObject("select count(1) from " + auditLogTableName, Long.class) > 1000);
        workerStore.catchup();

        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(
                () -> jdbcTemplate.queryForObject("select count(1) from " + auditLogTableName, Long.class) > 2000);

        Assert.assertEquals(4, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        stopped.compareAndSet(false, true);
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @OverwriteProp(key = "kylin.metadata.audit-log.max-size", value = "1000")
    @Test
    void testRotate(JdbcInfo info) throws Exception {
        val config = getTestConfig();
        val jdbcTemplate = info.getJdbcTemplate();
        val url = config.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        val transactionManager = new DataSourceTransactionManager(dataSource);
        val auditLogStore = new JdbcAuditLogStore(config, jdbcTemplate, transactionManager,
                config.getMetadataUrl().getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX);
        auditLogStore.createIfNotExist();

        val auditLogTableName = config.getMetadataUrl().getIdentifier() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX;
        for (int i = 0; i < 1000; i++) {
            val projectName = "p" + (i + 1000);
            String unitId = RandomUtil.randomUUIDStr();
            mockAuditLogForProjectEntry("a_" + projectName, info, false);
            mockAuditLogForProjectEntry("b_" + projectName, info, false);
            mockAuditLogForProjectEntry("c_" + projectName, info, false);
            mockAuditLogForProjectEntry(RandomUtil.randomUUIDStr(), "c_" + projectName, info, false,
                    1, unitId);
            mockAuditLogForProjectEntry("a_" + projectName, info, true);
        }
        auditLogStore.rotate();
        long count = jdbcTemplate.queryForObject("select count(1) from " + auditLogTableName, Long.class);
        Assert.assertEquals(1000, count);

        getTestConfig().setProperty("kylin.metadata.audit-log.max-size", "1500");
        auditLogStore.rotate();
        count = jdbcTemplate.queryForObject("select count(1) from " + auditLogTableName, Long.class);
        Assert.assertEquals(1000, count);

        auditLogStore.close();
    }

    @OverwriteProp(key = "kylin.metadata.audit-log.max-size", value = "2000")
    @OverwriteProp(key = "kylin.metadata.audit-log.delete-batch-size", value = "100")
    @Test
    void testRotateDelete(JdbcInfo info) throws Exception {
        val config = getTestConfig();
        val jdbcTemplate = info.getJdbcTemplate();
        val url = config.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        val transactionManager = new DataSourceTransactionManager(dataSource);
        val auditLogStore = new JdbcAuditLogStore(config, jdbcTemplate, transactionManager,
                info.getTableName() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX);
        auditLogStore.createIfNotExist();

        val auditLogTableName = info.getTableName() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX;
        for (int i = 0; i < 1000; i++) {
            val projectNamePrefix = "p" + (i + 1000);
            String unitId = RandomUtil.randomUUIDStr();
            mockAuditLogForProjectEntry(projectNamePrefix + "abc", info, false);
            mockAuditLogForProjectEntry(projectNamePrefix + "abc2", info, false);
            mockAuditLogForProjectEntry(projectNamePrefix + "abc3", info, false);
            mockAuditLogForProjectEntry(RandomUtil.randomUUIDStr(), projectNamePrefix + "abc3", info, false, 1, unitId);
            mockAuditLogForProjectEntry(projectNamePrefix + "abc", info, true);
        }
        auditLogStore.rotate();
        long count = jdbcTemplate.queryForObject("select count(1) from " + auditLogTableName, Long.class);
        Assert.assertEquals(2000, count);

        auditLogStore.rotate();
        count = jdbcTemplate.queryForObject("select count(1) from " + auditLogTableName, Long.class);
        Assert.assertEquals(2000, count);

        auditLogStore.close();
    }

    @Test
    public void testGet() throws IOException {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            MemoryLockUtils.lockAndRecord("PROJECT/123");
            store.checkAndPutResource("PROJECT/123",
                    ByteSource.wrap(
                            ("{ \"uuid\" : \"" + UUID.randomUUID() + "\",\"meta_key\" : \"123\",\"name\" : \"123\"}")
                                    .getBytes(charset)),
                    -1);
            return 0;
        }, "p1");

        AuditLogStore auditLogStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getAuditLogStore();

        AuditLog auditLog = auditLogStore.get("PROJECT/123", 0);
        Assert.assertNotNull(auditLog);

        auditLog = auditLogStore.get("PROJECT/126", 0);
        Assert.assertNull(auditLog);

        auditLog = auditLogStore.get("PROJECT/abc", 1);
        Assert.assertNull(auditLog);
        auditLogStore.close();
    }

    @Test
    void testMannualHandleReplay(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        mockAuditLogForProjectEntry("abc", info, false);
        AuditLogStore auditLogStore = new JdbcAuditLogStore(getTestConfig());
        auditLogStore.catchupWithTimeout();
        Assert.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    void testStopReplay() throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        workerStore.getAuditLogStore().close();
        Assertions.assertThrows(RejectedExecutionException.class, () -> {
            workerStore.catchup();
            ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
        });
        workerStore.getAuditLogStore().close();
    }

    @Test
    void testRestartReplay(JdbcInfo jdbcInfo) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);

        Assert.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        String unitId = RandomUtil.randomUUIDStr();
        mockAuditLogForProjectEntry("a", jdbcInfo, false);
        mockAuditLogForProjectEntry("b", jdbcInfo, false);
        mockAuditLogForProjectEntry("c", jdbcInfo, false);
        mockAuditLogForProjectEntry(RandomUtil.randomUUIDStr(), "c", jdbcInfo, false, 1, unitId);
        mockAuditLogForProjectEntry("a", jdbcInfo, true);
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());

        workerStore.getAuditLogStore().pause();
        mockAuditLogForProjectEntry("d", jdbcInfo, false);
        mockAuditLogForProjectEntry("e", jdbcInfo, false);

        workerStore.getAuditLogStore().reInit();
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 5 == workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        workerStore.getAuditLogStore().close();
    }

    @Test
    void testQueryNodeAuditLogCatchup(JdbcInfo jdbcInfo) {
        KylinConfig kylinConfig = getTestConfig();
        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        resourceStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG,
                new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assertions.assertEquals(1, resourceStore.listResourcesRecursively(MetadataType.ALL.name()).size());

        // write 3 msg to audit log
        mockAuditLogForProjectEntry("abc", jdbcInfo, false);
        mockAuditLogForProjectEntry("abc2", jdbcInfo, false);
        mockAuditLogForProjectEntry("abc3", jdbcInfo, false);

        // no catchup, so resourceStore.offset = 0
        Assertions.assertEquals(0, resourceStore.getOffset());
        ((JdbcAuditLogStore) resourceStore.getAuditLogStore()).forceClose();

        // mock another ke query node start and catchup audit log
        KylinConfig queryConfig = KylinConfig.createKylinConfig(kylinConfig);
        queryConfig.setProperty("kylin.server.mode", "query");
        queryConfig.setProperty("kylin.server.store-type", "jdbc");
        val queryResourceStore = ResourceStore.getKylinMetaStore(queryConfig);
        // queryResourceStore.offset must be 3
        Assertions.assertEquals(3, queryResourceStore.getOffset());
        val auditLogStore = queryResourceStore.getAuditLogStore();
        val replayWorker = (AuditLogReplayWorker) ReflectionTestUtils.getField(auditLogStore, "replayWorker");
        Assertions.assertNotNull(replayWorker);
        Assertions.assertEquals(0, replayWorker.getLogOffset());
        val mockWorker = Mockito.spy(replayWorker);
        Mockito.doNothing().when(mockWorker).catchupToMaxId(Mockito.anyLong());
        ReflectionTestUtils.setField(auditLogStore, "replayWorker", mockWorker);
        queryResourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
        // mock catchup, Make sure replay Auditlog is not executed
        queryResourceStore.catchup();
        Assertions.assertEquals(3,
                ((JdbcAuditLogStore) queryResourceStore.getMetadataStore().getAuditLogStore()).replayWorker
                        .getLogOffset());

        mockAuditLogForProjectEntry("abc4", jdbcInfo, false);
        mockAuditLogForProjectEntry("abc5", jdbcInfo, false);
        // Make sure replay auditlog is executed
        ReflectionTestUtils.setField(auditLogStore, "replayWorker", replayWorker);
        queryResourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
        // catchup 'abc4' metadata
        queryResourceStore.catchup();
        // catchup offset from 'abc4'
        queryResourceStore.catchup();
        Assertions.assertEquals(5,
                ((JdbcAuditLogStore) queryResourceStore.getMetadataStore().getAuditLogStore()).replayWorker
                        .getLogOffset());
        ((JdbcAuditLogStore) queryResourceStore.getAuditLogStore()).forceClose();
    }
}
