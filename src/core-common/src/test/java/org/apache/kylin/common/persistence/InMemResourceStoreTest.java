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

import java.io.IOException;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStoreTestBase.TestEntity;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import lombok.val;

@MetadataInfo(onlyProps = true)
@OverwriteProp(key = "kylin.env", value = "UT")
class InMemResourceStoreTest {

    @Test
    public void testFileStore() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStoreTestBase.wrapInNewUrl(config.getMetadataUrl().toString(), config,
                ResourceStoreTestBase::testAStore);
    }

    @Test
    public void testMemoryLeak() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStoreTestBase.testPotentialMemoryLeak(config.getMetadataUrl().toString(), config);
    }

    @Test
    public void testUUID() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStoreTestBase.wrapInNewUrl(config.getMetadataUrl().toString(), config,
                ResourceStoreTestBase::testGetUUID);
    }

    @Disabled("wait for metaStore to be refactored.")
    @Test
    void testReload() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore systemResourceStore = ResourceStore.getKylinMetaStore(config);

        final JsonSerializer<TestEntity> serializer = new JsonSerializer<>(TestEntity.class);
        systemResourceStore.checkAndPutResource(MetadataType.mergeKeyWithType("test", MetadataType.SYSTEM),
                new TestEntity("data2"), serializer);

        KylinConfig newConfig = KylinConfig.createKylinConfig(config);
        ResourceStore copyResourceStore = ResourceStore.getKylinMetaStore(newConfig);
        Assert.assertFalse(isSameResourceStore(systemResourceStore, copyResourceStore));

        systemResourceStore.reload();
        Assert.assertTrue(isSameResourceStore(systemResourceStore, copyResourceStore));
    }

    private boolean isSameResourceStore(ResourceStore resourceStore1, ResourceStore resourceStore2) {
        val paths1 = resourceStore1.listResourcesRecursively(MetadataType.ALL.name());
        val paths2 = resourceStore2.listResourcesRecursively(MetadataType.ALL.name());
        return Objects.equals(paths1, paths2);
    }

}
