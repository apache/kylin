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

import java.util.Collections;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.resources.ProjectRawResource;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class MemoryAuditLogStoreTest {

    @Test
    void testGet() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        MemoryAuditLogStore logStore = new MemoryAuditLogStore(config);

        ProjectRawResource prj = new ProjectRawResource();
        prj.setMvcc(0);
        prj.setName("p1");
        prj.setTs(System.currentTimeMillis());
        prj.setContent("abc".getBytes());
        prj.setProject("_global");
        ResourceCreateOrUpdateEvent event = new ResourceCreateOrUpdateEvent("PROJECT/p1", prj);
        UnitMessages unitMessages = new UnitMessages(Collections.singletonList(event));
        logStore.save(unitMessages);

        Assertions.assertNull(logStore.get("PROJECT/p2", 0));
        Assertions.assertNull(logStore.get("PROJECT/p1", 1));
        Assertions.assertNotNull(logStore.get("PROJECT/p1", 0));
    }
}
