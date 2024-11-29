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
package org.apache.kylin.tool.upgrade;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.resources.SystemRawResource;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MetadataInfo
@JdbcMetadataInfo
class MigrateKEMetadataToolTest {

    @Test
    void testUpgradeMetadata2System() throws IOException {
        final Charset ENCODING = StandardCharsets.UTF_8;

        // UPGRADE Metadata which refer to GlobalAclVersion
        Map<String, String> originMap = new HashMap<>();
        originMap.put("uuid", "e6fca42f-032e-7b75-5f66-21971a842cda");
        originMap.put("last_modified", "0");
        originMap.put("create_time", "1732269833897");
        originMap.put("version", "4.0.0.0");
        originMap.put("acl_version", "data-permission-separate");
        String upgradeMetadataString = JsonUtil.writeValueAsString(originMap);
        DataInputStream dataInputStream = new DataInputStream(
                new ByteArrayInputStream(upgradeMetadataString.getBytes(ENCODING)));
        MigrateKEMetadataTool migrateKEMetadataTool = new MigrateKEMetadataTool();
        MetadataStore.MemoryMetaData data = Mockito.mock(MetadataStore.MemoryMetaData.class);
        // test loadByStream to convert UPGRADE metadata to SYSTEM metadata
        RawResource rawResource = migrateKEMetadataTool.loadByStream("/_global/upgrade/acl_version.json", 0, data,
                dataInputStream);
        Assertions.assertInstanceOf(SystemRawResource.class, rawResource);
        Map<String, String> aclVersionMap = JsonUtil
                .readValueAsMap(new String(rawResource.getContent(), ENCODING).trim());
        // check the name to fit SYSTEM metadata
        Assertions.assertEquals(ResourceStore.UPGRADE_META_KEY_TAG, aclVersionMap.get("name"));
        for (String key : originMap.keySet()) {
            Assertions.assertEquals(originMap.get(key), aclVersionMap.get(key));
        }
        // check metadata could be put into store to fit db schema
        UnitOfWork.doInTransactionWithRetry(() -> {
            ResourceStore resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            resourceStore.checkAndPutResource("SYSTEM/acl_version", rawResource.getByteSource(), -1);
            return null;
        }, "restore");
    }
}
