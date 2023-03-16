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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;
import org.junitpioneer.jupiter.RetryingTest;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
public class JdbcAuditLogGroupbyReplayerTest {

    private static final String LOCAL_INSTANCE = "127.0.0.1";
    private final Charset charset = Charset.defaultCharset();
    public ExpectedException thrown = ExpectedException.none();

    @RetryingTest(3)
    @Disabled
    public void testReplayGroupbyProject(JdbcInfo info) throws Exception {
        val workerStore = initResourceStore();
        String project1 = "abc1";
        String project2 = "abc2";
        changeProject(project1, info, false);
        changeProject(project2, info, false);
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively("/").size());

        addProjectLog(project2, info, 6000);
        addProjectLog(project1, info, 6000);
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 12003 == workerStore.listResourcesRecursively("/").size());
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 12002 == workerStore.getAuditLogStore().getLogOffset());
        workerStore.getAuditLogStore().catchupWithTimeout();
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    private ResourceStore initResourceStore() {
        getTestConfig().setProperty("kylin.auditlog.replay-groupby-project-reload-enable", "true");
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively("/").size());
        return workerStore;
    }

    @Test
    public void testHandleProjectChange(JdbcInfo info) throws Exception {
        val workerStore = initResourceStore();
        String project = "abc1";
        changeProject(project, info, false);
        workerStore.catchup();
        Assert.assertEquals(2, workerStore.listResourcesRecursively("/").size());
        changeProject(project, info, true);
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 1 == workerStore.listResourcesRecursively("/").size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    private void addProjectLog(String project, JdbcInfo info, int logNum) throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val jdbcTemplate = info.getJdbcTemplate();
        String unitId = RandomUtil.randomUUIDStr();
        List<Object[]> logs = Lists.newArrayList();
        for (int i = 0; i < logNum; i++) {
            logs.add(new Object[] { "/" + project + "/abc/b" + i, "abc".getBytes(charset), System.currentTimeMillis(),
                    0, unitId, null, LOCAL_INSTANCE });
        }
        jdbcTemplate.batchUpdate(
                String.format(Locale.ROOT, JdbcAuditLogStore.INSERT_SQL, url.getIdentifier() + "_audit_log"), logs);
    }

    void changeProject(String project, JdbcInfo info, boolean isDel) throws Exception {
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
