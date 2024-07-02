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

import static org.apache.kylin.common.persistence.metadata.JdbcAuditLogStoreTool.prepareJdbcAuditLogStore;
import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;
import org.junitpioneer.jupiter.RetryingTest;

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
        JdbcAuditLogStoreTool.mockAuditLogForProjectEntry(project1, info, false);
        JdbcAuditLogStoreTool.mockAuditLogForProjectEntry(project2, info, false);
        workerStore.catchup();
        Assert.assertEquals(3, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());

        prepareJdbcAuditLogStore(project1, info.getJdbcTemplate(), 6000);
        prepareJdbcAuditLogStore(project2, info.getJdbcTemplate(), 6000);
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 12003 == workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 12002 == workerStore.getAuditLogStore().getLogOffset());
        workerStore.getAuditLogStore().catchupWithTimeout();
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }

    private ResourceStore initResourceStore() {
        getTestConfig().setProperty("kylin.auditlog.replay-groupby-project-reload-enable", "true");
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assert.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        return workerStore;
    }

    @Test
    public void testHandleProjectChange(JdbcInfo info) throws Exception {
        val workerStore = initResourceStore();
        String project = "abc1";
        JdbcAuditLogStoreTool.mockAuditLogForProjectEntry(project, info, false);
        workerStore.catchup();
        Assert.assertEquals(2, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        JdbcAuditLogStoreTool.mockAuditLogForProjectEntry(project, info, true);
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 1 == workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        ((JdbcAuditLogStore) workerStore.getAuditLogStore()).forceClose();
    }
}
