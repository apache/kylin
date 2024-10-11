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
package org.apache.kylin.common.persistence;

import static org.apache.kylin.common.persistence.MetadataType.ALL;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.lock.MemoryLockUtils;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.transaction.AuditLogReplayWorker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
public class JdbcAuditLogRecoveryTest {

    static final String META_TABLE_KEY = "META_TABLE_KEY";
    static final String META_TABLE_CONTENT = "META_TABLE_CONTENT";
    static final String META_TABLE_TS = "META_TABLE_TS";
    static final String META_TABLE_MVCC = "META_TABLE_MVCC";
    static final String INSERT_SQL = "insert into %s ("
            + Joiner.on(",").join(META_TABLE_KEY, META_TABLE_CONTENT, META_TABLE_TS, META_TABLE_MVCC)
            + ") values (?, ?, ?, ?)";
    static final String UPDATE_SQL = "update %s set " + META_TABLE_CONTENT + "=?, " + META_TABLE_MVCC + "=?, "
            + META_TABLE_TS + "=? where " + META_TABLE_KEY + "=?";

    static final String AUDIT_LOG_TABLE_KEY = "meta_key";
    static final String AUDIT_LOG_TABLE_CONTENT = "meta_content";
    static final String AUDIT_LOG_TABLE_TS = "meta_ts";
    static final String AUDIT_LOG_TABLE_MVCC = "meta_mvcc";
    static final String AUDIT_LOG_TABLE_UNIT = "unit_id";
    static final String AUDIT_LOG_TABLE_OPERATOR = "operator";
    static final String AUDIT_LOG_TABLE_INSTANCE = "instance";
    static final String INSERT_AUDIT_LOG_SQL = "insert into %s ("
            + Joiner.on(",").join(AUDIT_LOG_TABLE_KEY, AUDIT_LOG_TABLE_CONTENT, AUDIT_LOG_TABLE_TS,
                    AUDIT_LOG_TABLE_MVCC, AUDIT_LOG_TABLE_UNIT, AUDIT_LOG_TABLE_OPERATOR, AUDIT_LOG_TABLE_INSTANCE)
            + ") values (?, ?, ?, ?, ?, ?, ?)";
    private final Charset charset = Charset.defaultCharset();

    @AfterEach
    public void destroy() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("SHUTDOWN;");
    }

    @Test
    @OverwriteProp.OverwriteProps({ //
            @OverwriteProp(key = "kylin.metadata.url", value = "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=") //
    })
    public void testAuditLogOutOfOrder() throws Exception {
        val listener = new StatusListener();
        EventBusFactory.getInstance().register(listener, true);

        val systemStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val auditLogStore = (JdbcAuditLogStore) systemStore.getAuditLogStore();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            MemoryLockUtils.lockAndRecord("PROJECT/a");
            store.checkAndPutResource("PROJECT/a", new StringEntity("a", RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
            return null;
        }, "a");
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            MemoryLockUtils.lockAndRecord("PROJECT/b");
            store.checkAndPutResource("PROJECT/b", new StringEntity("b", RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
            return null;
        }, "b");
        Assert.assertEquals(2, systemStore.listResourcesRecursively(ALL.name()).size());

        Thread t = new Thread(() -> {
            val t1 = new Thread(() -> {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    Thread.sleep(500);
                    val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    val path = "PROJECT/a-" + System.currentTimeMillis();
                    MemoryLockUtils.lockAndRecord(path);
                    val originAbc = store.getResource(path, true);
                    store.checkAndPutResource(path, RawResourceTool.createByteSourceByPath(path),
                            System.currentTimeMillis(), originAbc == null ? -1 : originAbc.getMvcc());
                    return 0;
                }, "a");
                try {
                    auditLogStore.catchupWithTimeout();
                } catch (Exception e) {
                    log.error("catchup 1st phase failed", e);
                }
            });
            t1.start();

            Map<String, Long> versions = Maps.newHashMap();
            int size = 200;
            MetadataStore metadataStore = systemStore.getMetadataStore();
            IntStream.range(1000, 1000 + size).forEach(id -> {
                String metaKey = "b-" + id;
                String path = "PROJECT/b-" + id;
                RawResource result = systemStore.getResource(path, true);
                val newMvcc = result == null ? 0 : result.getMvcc() + 1;
                // When newMvcc is 0, do not record auditLog, so that the subsequent catchupWithTimeout error
                if (newMvcc == 0) {
                    metadataStore.save(MetadataType.PROJECT,
                            RawResourceTool.createProjectRawResource(metaKey, newMvcc));
                } else {
                    metadataStore.save(MetadataType.PROJECT,
                            RawResourceTool.createProjectRawResource(metaKey, newMvcc));
                    List<Event> events = Collections
                            .singletonList(new ResourceCreateOrUpdateEvent(path, new RawResource(metaKey,
                                    RawResourceTool.createByteSource(metaKey), System.currentTimeMillis(), newMvcc)));
                    val unitMessages = new UnitMessages(events);
                    metadataStore.getAuditLogStore().save(unitMessages);
                    versions.put(path, newMvcc);
                }
            });

            try {
                t1.join();
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.debug("wait for thread join failed", e);
            }
            try {
                auditLogStore.catchupWithTimeout();
            } catch (Exception e) {
                log.error("catchup 2nd phase failed", e);
            }

            try {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    IntStream.range(1000, 1000 + size).forEach(id -> {
                        String path = "PROJECT/b-" + id;
                        MemoryLockUtils.lockAndRecord(path);
                        val originAbc = store.getResource(path, true);
                        store.checkAndPutResource(path, RawResourceTool.createByteSourceByPath(path + "-version2"),
                                System.currentTimeMillis(), originAbc == null ? -1 : originAbc.getMvcc());
                    });
                    return 0;
                }, "b");
                auditLogStore.catchupWithTimeout();
            } catch (Exception e) {
                log.error("catchup 3rd phase failed", e);
            }
        });
        t.start();
        // reload is triggered, onEnd is called, and the status becomes -1
        Awaitility.await().atMost(200, TimeUnit.SECONDS).until(() -> listener.status == -1);

        // AuditLog store is skipped, the jsonPath error is reported, and the save fails, so there are only 2
        Assert.assertEquals(203, systemStore.listResourcesRecursively(ALL.name()).size());
        t.join();
    }

    static class StatusListener {
        int status = 0;

        @Subscribe
        public void onStart(AuditLogReplayWorker.StartReloadEvent start) {
            status = 1;
        }

        @Subscribe
        public void onEnd(AuditLogReplayWorker.EndReloadEvent end) {
            status = -1;
        }
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }
}
