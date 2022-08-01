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

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.transaction.AuditLogReplayWorker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.google.common.base.Joiner;

import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
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
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
    }

    @OverwriteProp(key = "kylin.metadata.url", value = "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=")
    @Test
    public void testAuditLogOutOfOrder() throws Exception {
        val listener = new StatusListener();
        val url = getTestConfig().getMetadataUrl();
        val table = url.getIdentifier();
        EventBusFactory.getInstance().register(listener, true);

        val systemStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val jdbcTemplate = getJdbcTemplate();
        val auditLogStore = (JdbcAuditLogStore) systemStore.getAuditLogStore();
        val txManager = auditLogStore.getTransactionManager();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/project/p1.json", new StringEntity(RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
            return null;
        }, "p1");
        UnitOfWork.doInTransactionWithRetry(() -> {
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.checkAndPutResource("/_global/project/p2.json", new StringEntity(RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
            return null;
        }, "p2");
        Assert.assertEquals(2, systemStore.listResourcesRecursively("/").size());

        new Thread(() -> {
            val definition = new DefaultTransactionDefinition();
            definition.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
            val status = txManager.getTransaction(definition);

            val t1 = new Thread(() -> {
                UnitOfWork.doInTransactionWithRetry(() -> {
                    Thread.sleep(500);
                    val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    val path = "/p1/abc-" + System.currentTimeMillis();
                    val originAbc = store.getResource(path);
                    store.checkAndPutResource(path, ByteSource.wrap("abc".getBytes(charset)),
                            System.currentTimeMillis(), originAbc == null ? -1 : originAbc.getMvcc());
                    return 0;
                }, "p1");
                try {
                    auditLogStore.catchupWithTimeout();
                } catch (Exception e) {
                    log.debug("catchup 1st phase failed", e);
                }
            });
            t1.start();

            val unitId = RandomUtil.randomUUIDStr();
            Map<String, Long> versions = Maps.newHashMap();
            int size = 200;
            IntStream.range(1000, 1000 + size).forEach(id -> {
                String path = "/p2/abc" + id;
                long ts = System.currentTimeMillis();
                RawResource result = systemStore.getResource(path);
                val newMvcc = result == null ? 0 : result.getMvcc() + 1;
                if (newMvcc == 0) {
                    jdbcTemplate.update(String.format(Locale.ROOT, INSERT_SQL, table), ps -> {
                        ps.setString(1, path);
                        ps.setBytes(2, path.getBytes(charset));
                        ps.setLong(3, ts);
                        ps.setLong(4, newMvcc);
                    });
                } else {
                    jdbcTemplate.update(String.format(Locale.ROOT, UPDATE_SQL, table), ps -> {
                        ps.setBytes(1, path.getBytes(charset));
                        ps.setLong(2, newMvcc);
                        ps.setLong(3, ts);
                        ps.setString(4, path);
                    });
                }
                jdbcTemplate.update(String.format(Locale.ROOT, INSERT_AUDIT_LOG_SQL, table + "_audit_log"), ps -> {
                    ps.setString(1, path);
                    ps.setBytes(2, path.getBytes(charset));
                    ps.setLong(3, ts);
                    ps.setLong(4, newMvcc);
                    ps.setString(5, unitId);
                    ps.setString(6, null);
                    ps.setString(7, "127.0.0.1:7072");
                });
                versions.put(path, newMvcc);

            });

            try {
                t1.join();
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.debug("wait for thread join failed", e);
            }
            txManager.commit(status);
            try {
                auditLogStore.catchupWithTimeout();
            } catch (Exception e) {
                log.debug("catchup 2nd phase failed", e);
            }

            UnitOfWork.doInTransactionWithRetry(() -> {
                val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                IntStream.range(1000, 1000 + size).forEach(id -> {
                    String path = "/p2/abc" + id;
                    val originAbc = store.getResource(path);
                    store.checkAndPutResource(path, ByteSource.wrap((path + "-version2").getBytes(charset)),
                            System.currentTimeMillis(), originAbc == null ? -1 : originAbc.getMvcc());
                });
                return 0;
            }, "p2");
            try {
                auditLogStore.catchupWithTimeout();
            } catch (Exception e) {
                log.debug("catchup 3rd phase failed", e);
            }
        }).start();
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> listener.status == -1);

        Assert.assertEquals(203, systemStore.listResourcesRecursively("/").size());
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
