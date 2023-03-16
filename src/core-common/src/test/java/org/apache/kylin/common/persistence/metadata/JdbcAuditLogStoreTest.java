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

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.metadata.jdbc.AuditLogRowMapper;
import org.apache.kylin.common.persistence.transaction.AuditLogReplayWorker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
class JdbcAuditLogStoreTest {

    private static final String LOCAL_INSTANCE = "127.0.0.1";
    private final Charset charset = Charset.defaultCharset();

    @Test
    void testUpdateResourceWithLog(JdbcInfo info) throws Exception {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/abc", ByteSource.wrap("abc".getBytes(charset)), -1);
            store.checkAndPutResource("/p1/abc2", ByteSource.wrap("abc".getBytes(charset)), -1);
            store.checkAndPutResource("/p1/abc3", ByteSource.wrap("abc".getBytes(charset)), -1);
            store.checkAndPutResource("/p1/abc3", ByteSource.wrap("abc2".getBytes(charset)), 0);
            store.deleteResource("/p1/abc");
            return 0;
        }, "p1");
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        val all = jdbcTemplate.query("select * from " + url.getIdentifier() + "_audit_log", new AuditLogRowMapper());

        Assert.assertEquals(5, all.size());
        Assert.assertEquals("/p1/abc", all.get(0).getResPath());
        Assert.assertEquals("/p1/abc2", all.get(1).getResPath());
        Assert.assertEquals("/p1/abc3", all.get(2).getResPath());
        Assert.assertEquals("/p1/abc3", all.get(3).getResPath());
        Assert.assertEquals("/p1/abc", all.get(4).getResPath());

        Assert.assertEquals(Long.valueOf(0), all.get(0).getMvcc());
        Assert.assertEquals(Long.valueOf(0), all.get(1).getMvcc());
        Assert.assertEquals(Long.valueOf(0), all.get(2).getMvcc());
        Assert.assertEquals(Long.valueOf(1), all.get(3).getMvcc());
        Assert.assertNull(all.get(4).getMvcc());

        Assert.assertEquals(1, all.stream().map(AuditLog::getUnitId).distinct().count());

        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken("USER1", "ADMIN"));
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.deleteResource("/p1/abc2");
            store.deleteResource("/p1/abc3");
            return 0;
        }, "p1");

        val allStep2 = jdbcTemplate.query("select * from " + url.getIdentifier() + "_audit_log",
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
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 1,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc", null, null, null, unitId, null, LOCAL_INSTANCE }));
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively("/").size());

        for (int i = 0; i < 1000; i++) {
            val projectName = "p" + (i + 1000);
            jdbcTemplate.batchUpdate(
                    String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"),
                    Arrays.asList(
                            new Object[] { "/_global/" + projectName + "/abc", "abc".getBytes(charset),
                                    System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE },
                            new Object[] { "/_global/" + projectName + "/abc2", "abc".getBytes(charset),
                                    System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE },
                            new Object[] { "/_global/" + projectName + "/abc3", "abc".getBytes(charset),
                                    System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE },
                            new Object[] { "/_global/" + projectName + "/abc3", "abc".getBytes(charset),
                                    System.currentTimeMillis(), 1, unitId, null, LOCAL_INSTANCE },
                            new Object[] { "/_global/" + projectName + "/abc", null, null, null, unitId, null }));
        }
        workerStore.getAuditLogStore().catchupWithTimeout();
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 2003 == workerStore.listResourcesRecursively("/").size());
        workerStore.getAuditLogStore().catchupWithTimeout();
        Assert.assertEquals(2003, workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
        workerStore.getAuditLogStore().close();
    }

    @Test
    void testHandleVersionConflict(JdbcInfo info) throws Exception {
        getTestConfig().setProperty("kylin.auditlog.replay-groupby-project-reload-enable", "false");
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        RawResource rawResource = new RawResource("/_global/p1/abc3", ByteSource.wrap("abc".getBytes(charset)),
                System.currentTimeMillis(), 2);
        workerStore.getMetadataStore().putResource(rawResource, "sdfasf", -1L);

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());

        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();

        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 2,
                                unitId, null, LOCAL_INSTANCE }));
        workerStore.catchup();
        // catch up exception, load /_global/p1/abc3 from metadata
        Assert.assertEquals(2, workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    public void testWaitLogAllCommit(JdbcInfo info) throws Exception {
        getTestConfig().setProperty("kylin.auditlog.replay-groupby-project-reload-enable", "false");
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        String sql = "insert into %s (id, meta_key,meta_content,meta_ts,meta_mvcc,unit_id,operator,instance) values (?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { 22, "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 4, "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));
        workerStore.getAuditLogStore().restore(3);
        Assert.assertEquals(3, workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    void testWaitLogAllCommit_DelayQueue(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        String sql = "insert into %s (id, meta_key,meta_content,meta_ts,meta_mvcc,unit_id,operator,instance) values (?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { 900, "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 4, "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));
        workerStore.getAuditLogStore().restore(3);
        Assertions.assertEquals(3, workerStore.listResourcesRecursively("/").size());

        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { 800, "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 801, "/_global/p1/abc4", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));
        workerStore.getAuditLogStore().catchup();

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> 5 == workerStore.listResourcesRecursively("/").size());

        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    void testFetchById(JdbcInfo info) throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        String sql = "insert into %s (id, meta_key,meta_content,meta_ts,meta_mvcc,unit_id,operator,instance) values (?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { 1, "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 2, "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 3, "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 4, "/_global/p1/abc4", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));
        val idList = Arrays.asList(2L, 3L);
        val fetchResult = ((JdbcAuditLogStore) workerStore.getAuditLogStore()).fetch(idList);

        Assertions.assertEquals(2, fetchResult.size());
        Assertions.assertEquals(idList, fetchResult.stream().map(AuditLog::getId).collect(Collectors.toList()));
        workerStore.getAuditLogStore().close();
    }

    @Test
    void testGetMinMaxId(JdbcInfo info) throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        String sql = "insert into %s (id, meta_key,meta_content,meta_ts,meta_mvcc,unit_id,operator,instance) values (?, ?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, sql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { 11, "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 22, "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 33, "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { 101, "/_global/p1/abc4", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));
        val auditLogStore = workerStore.getAuditLogStore();
        Assertions.assertEquals(101, auditLogStore.getMaxId());
        Assertions.assertEquals(11, auditLogStore.getMinId());
        auditLogStore.close();
    }

    @Test
    void testSaveWait_WithoutSleep(JdbcInfo info) throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent(new RawResource("/p1/test",
                ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)), System.currentTimeMillis(), 0)));
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
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent(new RawResource("/p1/test",
                ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)), System.currentTimeMillis(), 0)));
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
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        List<Event> events = Collections.singletonList(new ResourceCreateOrUpdateEvent(new RawResource("/p1/test",
                ByteSource.wrap("test content".getBytes(StandardCharsets.UTF_8)), System.currentTimeMillis(), 0)));
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
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId1 = RandomUtil.randomUUIDStr();
        String unitId2 = RandomUtil.randomUUIDStr();
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId1, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId2, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId1, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 1,
                                unitId2, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc", null, null, null, unitId1, null, LOCAL_INSTANCE }));
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    void testRestore_WhenOtherAppend(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        val auditLogTableName = info.getTableName() + "_audit_log";

        val stopped = new AtomicBoolean(false);
        new Thread(() -> {
            int i = 0;
            while (!stopped.get()) {
                val projectName = "p0";
                val unitId = RandomUtil.randomUUIDStr();
                jdbcTemplate.batchUpdate(String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, auditLogTableName),
                        Arrays.asList(
                                new Object[] { "/_global/" + projectName + "/abc", "abc".getBytes(charset),
                                        System.currentTimeMillis(), i, unitId, null, LOCAL_INSTANCE },
                                new Object[] { "/_global/" + projectName + "/abc2", "abc".getBytes(charset),
                                        System.currentTimeMillis(), i, unitId, null, LOCAL_INSTANCE },
                                new Object[] { "/_global/" + projectName + "/abc3", "abc".getBytes(charset),
                                        System.currentTimeMillis(), i, unitId, null, LOCAL_INSTANCE }));
                i++;
            }
        }).start();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(
                () -> jdbcTemplate.queryForObject("select count(1) from " + auditLogTableName, Long.class) > 1000);
        workerStore.catchup();

        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(
                () -> jdbcTemplate.queryForObject("select count(1) from " + auditLogTableName, Long.class) > 2000);

        Assert.assertEquals(4, workerStore.listResourcesRecursively("/").size());
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
                info.getTableName() + "_audit_log");
        auditLogStore.createIfNotExist();

        val auditLogTableName = info.getTableName() + "_audit_log";
        for (int i = 0; i < 1000; i++) {
            val projectName = "p" + (i + 1000);
            String unitId = RandomUtil.randomUUIDStr();
            jdbcTemplate.batchUpdate(String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, auditLogTableName),
                    Arrays.asList(
                            new Object[] { "/" + projectName + "/abc", "abc".getBytes(charset),
                                    System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE },
                            new Object[] { "/" + projectName + "/abc2", "abc".getBytes(charset),
                                    System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE },
                            new Object[] { "/" + projectName + "/abc3", "abc".getBytes(charset),
                                    System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE },
                            new Object[] { "/" + projectName + "/abc3", "abc".getBytes(charset),
                                    System.currentTimeMillis(), 1, unitId, null, LOCAL_INSTANCE },
                            new Object[] { "/" + projectName + "/abc", null, null, null, unitId, null,
                                    LOCAL_INSTANCE }));
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

    @Test
    public void testGet() throws IOException {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/p1/123", ByteSource.wrap("123".getBytes(charset)), -1);
            return 0;
        }, "p1");

        AuditLogStore auditLogStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getAuditLogStore();

        AuditLog auditLog = auditLogStore.get("/p1/123", 0);
        Assert.assertNotNull(auditLog);

        auditLog = auditLogStore.get("/p1/126", 0);
        Assert.assertNull(auditLog);

        auditLog = auditLogStore.get("/p1/abc", 1);
        Assert.assertNull(auditLog);
        auditLogStore.close();
    }

    @Test
    void testMannualHandleReplay(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        changeProject("abc", false, info);
        AuditLogStore auditLogStore = new JdbcAuditLogStore(getTestConfig());
        auditLogStore.catchupWithTimeout();
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    void testStopReplay() throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
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
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);

        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = jdbcInfo.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 1,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc", null, null, null, unitId, null, LOCAL_INSTANCE }));
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively("/").size());

        workerStore.getAuditLogStore().pause();
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abcd", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abce", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));

        workerStore.getAuditLogStore().reInit();
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 5 == workerStore.listResourcesRecursively("/").size());
        workerStore.getAuditLogStore().close();
    }

    @Test
    void testQueryNodeAuditLogCatchup(JdbcInfo jdbcInfo) {
        KylinConfig kylinConfig = getTestConfig();
        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        resourceStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, resourceStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = jdbcInfo.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        // write 3 msg to audit log
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/project/abc1", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/project/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/project/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));
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

        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_image", "{\"offset\":5}".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/project/abc4", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE }));
        // Make sure replay auditlog is executed
        ReflectionTestUtils.setField(auditLogStore, "replayWorker", replayWorker);
        queryResourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
        // catchup '/_image' metadata
        queryResourceStore.catchup();
        // catchup offset from '/_image'
        queryResourceStore.catchup();
        Assertions.assertEquals(5,
                ((JdbcAuditLogStore) queryResourceStore.getMetadataStore().getAuditLogStore()).replayWorker
                        .getLogOffset());
        ((JdbcAuditLogStore) queryResourceStore.getAuditLogStore()).forceClose();
    }

    void changeProject(String project, boolean isDel, JdbcInfo info) throws Exception {
        val jdbcTemplate = info.getJdbcTemplate();

        val url = getTestConfig().getMetadataUrl();
        String unitId = RandomUtil.randomUUIDStr();

        Object[] log = isDel
                ? new Object[] { "/_global/project/" + project + ".json", null, System.currentTimeMillis(), 0, unitId,
                        null, LOCAL_INSTANCE }
                : new Object[] { "/_global/project/" + project + ".json", "abc".getBytes(charset),
                        System.currentTimeMillis(), 0, unitId, null, LOCAL_INSTANCE };
        List<Object[]> logs = new ArrayList<>();
        logs.add(log);
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"), logs);
    }

}
