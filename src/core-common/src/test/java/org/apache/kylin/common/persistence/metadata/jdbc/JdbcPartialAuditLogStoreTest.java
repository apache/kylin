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

import java.util.Arrays;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStoreTool;
import org.apache.kylin.common.persistence.metadata.JdbcPartialAuditLogStore;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.JdbcInfo;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
@OverwriteProp(key = "kylin.metadata.url", value = "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=")
class JdbcPartialAuditLogStoreTest {

    @Test
    void testPartialAuditLogRestore() throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(), null);
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);
        auditLogStore.restore(101);
        Assertions.assertEquals(101, auditLogStore.getLogOffset());
        auditLogStore.close();
    }

    @Test
    void testPartialFetchAuditLog() throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        var auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(), "uuid1");
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);

        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        auditLogStore.batchInsert(Arrays.asList(
                JdbcAuditLogStoreTool.createProjectAuditLog("abc", "uuid1", 0),
                JdbcAuditLogStoreTool.createProjectAuditLog("abc", null, 0),
                JdbcAuditLogStoreTool.createProjectAuditLog("abc2", "uuid1", 0),
                JdbcAuditLogStoreTool.createProjectAuditLog("abc2", "uuid2", 0)
                )
        );
        auditLogStore.catchupWithMaxTimeout();
        var totalR = workerStore.listResourcesRecursively("PROJECT");
        Assertions.assertEquals(2, totalR.size());
        auditLogStore.close();
    }

    @Test
    void testPartialFetchAuditLogEmptyFilter(JdbcInfo jdbcInfo) throws Exception {
        val workerStore = ResourceStore.getKylinMetaStore(getTestConfig());
        var auditLogStore = new JdbcPartialAuditLogStore(getTestConfig(), null);
        workerStore.getMetadataStore().setAuditLogStore(auditLogStore);

        workerStore.checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(RandomUtil.randomUUIDStr()),
                StringEntity.serializer);
        Assertions.assertEquals(1, workerStore.listResourcesRecursively(MetadataType.ALL.name()).size());
        auditLogStore.batchInsert(Arrays.asList(
                JdbcAuditLogStoreTool.createProjectAuditLog("abc", 0),
                JdbcAuditLogStoreTool.createProjectAuditLog("abc2", 0),
                JdbcAuditLogStoreTool.createProjectAuditLog("abc3", 0),
                JdbcAuditLogStoreTool.createProjectAuditLog("abc4", 0),
                JdbcAuditLogStoreTool.createProjectAuditLog("t1", 0)
                )
        );
        auditLogStore.catchupWithMaxTimeout();
        var totalR = workerStore.listResourcesRecursively("PROJECT");
        Assertions.assertEquals(5, totalR.size());
        auditLogStore.close();
    }
}
