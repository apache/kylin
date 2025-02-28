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

import static org.apache.kylin.common.persistence.ResourceStore.METASTORE_UUID_TAG;
import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.BadSqlGrammarException;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
public class JdbcAuditLogReplayerTest {
    @Test
    public void testDatabaseNotAvailable(JdbcInfo info) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        workerStore.checkAndPutResource(METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);

        val auditLogStore = workerStore.getAuditLogStore();
        val jdbcTemplate = info.getJdbcTemplate();
        JdbcAuditLogStoreTool.mockAuditLogForProjectEntry("abc", info, false);
        auditLogStore.restore(0);
        Assert.assertEquals(2, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());

        val auditLogTableName = info.getTableName() + JdbcAuditLogStore.AUDIT_LOG_SUFFIX;

        jdbcTemplate.batchUpdate("ALTER TABLE " + auditLogTableName + " RENAME TO TEST_AUDIT_LOG_TEST",
                "ALTER TABLE " + info.getTableName() + "_project RENAME TO TEST_TEST");

        //replay fail
        try {
            auditLogStore.catchupWithTimeout();
        } catch (BadSqlGrammarException e) {
            log.info("expected exception {}", e.getMessage());
        }

        //restore audit log
        jdbcTemplate.update("ALTER TABLE TEST_AUDIT_LOG_TEST RENAME TO " + auditLogTableName);
        JdbcAuditLogStoreTool.mockAuditLogForProjectEntry("abcd", info, false);

        //replay to maxOffset
        Awaitility.await().atMost(6, TimeUnit.SECONDS)
                .until(() -> 3 == workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        //important for release a replay thread
        auditLogStore.close();
    }
}
