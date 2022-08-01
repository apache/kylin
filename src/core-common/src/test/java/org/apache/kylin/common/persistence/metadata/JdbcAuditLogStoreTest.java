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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.metadata.jdbc.AuditLogRowMapper;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
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
    }

    @Test
    public void testHandleVersionConflict(JdbcInfo info) throws Exception {
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

    @RetryingTest(3)
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
        workerStore.catchup();
        // catch up exception, load /_global/p1/abc3 from metadata
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    public void testRestoreWithoutOrder(JdbcInfo info) throws Exception {
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
    public void testRestore_WhenOtherAppend(JdbcInfo info) throws Exception {
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
    public void testRotate(JdbcInfo info) throws Exception {
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
    public void testGet() {
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
    }

    @Test
    public void testMannualHandleReplay(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        changeProject("abc", false, info);
        AuditLogStore auditLogStore = new JdbcAuditLogStore(getTestConfig());
        auditLogStore.forceCatchup();
        auditLogStore.catchupWithTimeout();
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    @Test
    public void testStopReplay() throws IOException {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        workerStore.getAuditLogStore().close();
        Assertions.assertThrows(RejectedExecutionException.class, () -> {
            workerStore.catchup();
            ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
        });
    }

    @Test
    public void testRestartReplay(JdbcInfo jdbcInfo) throws Exception {
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
