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
package org.apache.kylin.common.persistence.metadata.jdbc;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Locale;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.metadata.JdbcPartialAuditLogStore;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
class JdbcPartialAuditLogStoreTest {
    private static final String LOCAL_INSTANCE = "127.0.0.1";
    private final Charset charset = Charset.defaultCharset();

    @Test
    void testPartialAuditLogRestore() throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(),
                resPath -> resPath.startsWith("/_global/p2/"));
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);
        auditLogStore.restore(101);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());
        auditLogStore.close();
    }

    @Test
    void testPartialFetchAuditLog(JdbcInfo jdbcInfo) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(),
                resPath -> resPath.startsWith("/_global/p2/"));
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        val insertSql = (String) ReflectionTestUtils.getField(JdbcAuditLogStore.class, "INSERT_SQL");

        Assertions.assertNotNull(insertSql, "cannot get insert sql fromm JdbcAuditLogStore");

        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = jdbcInfo.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, insertSql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p2/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p2/abc4", "abc".getBytes(charset), System.currentTimeMillis(), 1,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc/t1", null, null, null, unitId, null, LOCAL_INSTANCE }));

        auditLogStore.catchupWithMaxTimeout();
        val totalR = workerStore.listResourcesRecursively("/_global");
        Assertions.assertEquals(2, totalR.size());
        auditLogStore.close();
    }

    @Test
    void testPartialFetchAuditLogEmptyFilter(JdbcInfo jdbcInfo) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(), null);
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);

        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        val insertSql = (String) ReflectionTestUtils.getField(JdbcAuditLogStore.class, "INSERT_SQL");

        Assertions.assertNotNull(insertSql, "cannot get insert sql fromm JdbcAuditLogStore");

        Assertions.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = jdbcInfo.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        jdbcTemplate.batchUpdate(String.format(Locale.ROOT, insertSql, url.getIdentifier() + "_audit_log"),
                Arrays.asList(
                        new Object[] { "/_global/p1/abc", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p1/abc2", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p2/abc3", "abc".getBytes(charset), System.currentTimeMillis(), 0,
                                unitId, null, LOCAL_INSTANCE },
                        new Object[] { "/_global/p2/abc4", "abc".getBytes(charset), System.currentTimeMillis(), 1,
                                unitId, null, LOCAL_INSTANCE }));

        auditLogStore.catchupWithMaxTimeout();
        val totalR = workerStore.listResourcesRecursively("/_global");
        Assertions.assertEquals(4, totalR.size());
        auditLogStore.close();
    }
}
