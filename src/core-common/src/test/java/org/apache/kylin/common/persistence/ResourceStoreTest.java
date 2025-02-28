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

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

@MetadataInfo
public class ResourceStoreTest {

    @Test
    public void testCreateMetaStoreUuidIfNotExist() {
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).createMetaStoreUuidIfNotExist();
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .deleteResource(ResourceStore.METASTORE_UUID_TAG);
        Set<String> res = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .listResources(MetadataType.SYSTEM.name());
        Assert.assertFalse("failed", res.contains(ResourceStore.METASTORE_UUID_TAG));
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).createMetaStoreUuidIfNotExist();
        res = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .listResources(MetadataType.SYSTEM.name());
        Assert.assertTrue("failed", res.contains(ResourceStore.METASTORE_UUID_TAG));
    }

    @Test
    public void testCreateMetaStoreUuidUsingTwoKylinConfig() {
        KylinConfig A = KylinConfig.getInstanceFromEnv();
        ResourceStore AStore = ResourceStore.getKylinMetaStore(A);//system store
        KylinConfig B = KylinConfig.createKylinConfig(A);
        ResourceStore BStore = ResourceStore.getKylinMetaStore(B);//new store
        //delete AStore,BStore uuid
        ResourceStore.getKylinMetaStore(A).createMetaStoreUuidIfNotExist();
        ResourceStore.getKylinMetaStore(A).deleteResource(ResourceStore.METASTORE_UUID_TAG);
        ResourceStore.getKylinMetaStore(B).createMetaStoreUuidIfNotExist();
        ResourceStore.getKylinMetaStore(B).deleteResource(ResourceStore.METASTORE_UUID_TAG);
        Set<String> res = AStore.listResources(MetadataType.SYSTEM.name());
        Assert.assertFalse(res.contains(ResourceStore.METASTORE_UUID_TAG));
        res = BStore.listResources(MetadataType.SYSTEM.name());
        Assert.assertFalse(res.contains(ResourceStore.METASTORE_UUID_TAG));

        //create BStore uuid
        BStore.createMetaStoreUuidIfNotExist();

        res = BStore.listResources(MetadataType.SYSTEM.name());
        Assert.assertTrue(res.contains(ResourceStore.METASTORE_UUID_TAG));//B have uuid
        res = AStore.listResources(MetadataType.SYSTEM.name());
        Assert.assertFalse(res.contains(ResourceStore.METASTORE_UUID_TAG));//A did not

        //try create again
        AStore.createMetaStoreUuidIfNotExist();
        BStore.createMetaStoreUuidIfNotExist();

    }
}
