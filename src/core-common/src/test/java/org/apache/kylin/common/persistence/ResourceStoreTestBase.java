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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.NavigableSet;
import java.util.function.Consumer;

import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * Be called by LocalFileResourceStoreTest and ITHDFSResourceStoreTest.
 */
public class ResourceStoreTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ResourceStoreTestBase.class);

    private static final String PERFORMANCE_TEST_ROOT_PATH = "/performance";

    private static final int TEST_RESOURCE_COUNT = 100;

    public static void wrapInNewUrl(String url, KylinConfig kylinConfig, Consumer<ResourceStore> f) {
        String oldUrl = replaceMetadataUrl(kylinConfig, url);
        f.accept(ResourceStore.getKylinMetaStore(kylinConfig));
        replaceMetadataUrl(kylinConfig, oldUrl);
    }

    public static String mockUrl(String tag, KylinConfig kylinConfig) {
        String str = kylinConfig.getMetadataUrlPrefix() + "@" + tag;
        return str;
    }

    public static void testAStore(ResourceStore store) {
        testBasics(store);
        testGetAllResources(store);
    }

    public static void testPerformance(ResourceStore store) {
        logger.info("Test basic functions");
        testAStore(store);
        logger.info("Basic function ok. Start to test performance for class : {}", store.getClass());
        logger.info("Write metadata time : {}", testWritePerformance(store));
        logger.info("Read metadata time {}", testReadPerformance(store));
        logger.info("Performance test end. Class : {}", store.getClass());
    }

    private static void testGetAllResources(ResourceStore store) {
        final String folder = "/testFolder";
        List<TestEntity> result;

        // reset any leftover garbage
        ResourceTool.resetR(store, folder);

        final JsonSerializer<TestEntity> serializer = new JsonSerializer<>(TestEntity.class);

        store.checkAndPutResource(folder + "/res1", new TestEntity("data1"), serializer);

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        long startTime = System.currentTimeMillis();

        store.checkAndPutResource(folder + "/res2", new TestEntity("data2"), serializer);
        store.checkAndPutResource(folder + "/sub/res3", new TestEntity("data3"), serializer);
        store.checkAndPutResource(folder + "/res4", new TestEntity("data4"), serializer);

        result = store.getAllResources(folder, serializer);
        assertEntity(result.get(0), "data1", 0);
        assertEntity(result.get(1), "data2", 0);
        assertEntity(result.get(2), "data4", 0);
        Assert.assertEquals(3, result.size());

        result.get(1).setVersion("new_data2");
        store.checkAndPutResource(folder + "/res2", result.get(1), serializer);

        result = store.getAllResources(folder, startTime, Long.MAX_VALUE, serializer);

        assertEntity(result.get(0), "new_data2", 1);
        Assert.assertEquals(2, result.size());

        //clean
        ResourceTool.resetR(store, folder);
    }

    private static void assertEntity(TestEntity entity, String data, long mvcc) {
        assertEquals(data, entity.getVersion());
        assertTrue((System.currentTimeMillis() - entity.getLastModified()) < 10000);//should very recent
        assertEquals(mvcc, entity.getMvcc());
    }

    private static void testBasics(ResourceStore store) {
        String dir1 = "/cube";
        String path1 = "/cube/_test.json";
        StringEntity content1 = new StringEntity("anything");
        String dir2 = "/table";
        String path2 = "/table/_test.json";
        StringEntity content2 = new StringEntity("something");

        // cleanup legacy if any
        store.deleteResource(path1);
        store.deleteResource(path2);

        StringEntity t;

        // put/get
        store.checkAndPutResource(path1, content1, StringEntity.serializer);
        Assert.assertTrue(store.exists(path1));
        t = store.getResource(path1, StringEntity.serializer);
        assertEquals(content1, t);

        store.checkAndPutResource(path2, content2, StringEntity.serializer);
        Assert.assertTrue(store.exists(path2));
        t = store.getResource(path2, StringEntity.serializer);
        assertEquals(content2, t);

        // overwrite
        t.setStr("new string");
        store.checkAndPutResource(path2, t, StringEntity.serializer);

        // in contrast to 3.x, ts will no longer cause write conflict
        t = store.getResource(path2, StringEntity.serializer);
        t.setLastModified(t.getLastModified() - 1);
        store.checkAndPutResource(path2, t, StringEntity.serializer);

        // write conflict
        boolean versionConflictExceptionSeen = false;
        try {
            t = store.getResource(path2, StringEntity.serializer);
            t.setMvcc(t.getMvcc() - 1);
            store.checkAndPutResource(path2, t, StringEntity.serializer);
            Assert.fail("write conflict should trigger IllegalStateException");
        } catch (VersionConflictException e) {
            // expected
            versionConflictExceptionSeen = true;
        }

        Assert.assertEquals(true, versionConflictExceptionSeen);

        // list
        NavigableSet<String> list = null;

        list = store.listResources(dir1);
        System.out.println(list);
        Assert.assertTrue(list.contains(path1));
        Assert.assertFalse(list.contains(path2));

        list = store.listResources(dir2);
        Assert.assertTrue(list.contains(path2));
        Assert.assertFalse(list.contains(path1));

        list = store.listResources("/");

        Assert.assertTrue(list.contains(dir1));
        Assert.assertTrue(list.contains(dir2));

        Assert.assertFalse(list.contains(path1));
        Assert.assertFalse(list.contains(path2));

        list = store.listResources(path1);
        Assert.assertNull(list);
        list = store.listResources(path2);
        Assert.assertNull(list);

        // delete/exist
        store.deleteResource(path1);
        Assert.assertFalse(store.exists(path1));
        list = store.listResources(dir1);
        Assert.assertTrue(list == null || !list.contains(path1));

        store.deleteResource(path2);
        Assert.assertFalse(store.exists(path2));
        list = store.listResources(dir2);
        Assert.assertTrue(list == null || !list.contains(path2));
    }

    private static long testWritePerformance(ResourceStore store) {
        store.deleteResource(PERFORMANCE_TEST_ROOT_PATH);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < TEST_RESOURCE_COUNT; i++) {
            String resourcePath = PERFORMANCE_TEST_ROOT_PATH + "/res_" + i;
            StringEntity content = new StringEntity("something_" + i);
            store.checkAndPutResource(resourcePath, content, StringEntity.serializer);
        }
        return System.currentTimeMillis() - startTime;
    }

    private static long testReadPerformance(ResourceStore store) {
        long startTime = System.currentTimeMillis();
        int step = 0; //avoid compiler optimization
        for (int i = 0; i < TEST_RESOURCE_COUNT; i++) {
            String resourcePath = PERFORMANCE_TEST_ROOT_PATH + "/res_" + i;
            StringEntity t = store.getResource(resourcePath, StringEntity.serializer);
            step |= t.toString().length();
        }
        logger.info("step : {}", step);
        return System.currentTimeMillis() - startTime;
    }

    private static String replaceMetadataUrl(KylinConfig kylinConfig, String newUrl) {
        String oldUrl = kylinConfig.getMetadataUrl().toString();
        kylinConfig.setProperty("kylin.metadata.url", newUrl);
        return oldUrl;
    }

    public static void testPotentialMemoryLeak(String url, KylinConfig kylinConfig) {
        String oldUrl = replaceMetadataUrl(kylinConfig, url);

        ResourceStore.clearCache();
        Assert.assertFalse(ResourceStore.isPotentialMemoryLeak());
        ResourceStore.getKylinMetaStore(kylinConfig);
        // Put 100 caches into resource store
        for (int i = 0; i < TEST_RESOURCE_COUNT; ++i) {
            // Make a deep copy of kylinConfig is important
            ResourceStore.getKylinMetaStore(KylinConfig.createKylinConfig(kylinConfig));
        }
        Assert.assertTrue(ResourceStore.isPotentialMemoryLeak());
        // Clear one cache
        ResourceStore.clearCache(kylinConfig);
        Assert.assertFalse(ResourceStore.isPotentialMemoryLeak());
        // Clear all cache
        ResourceStore.clearCache();
        Assert.assertFalse(ResourceStore.isPotentialMemoryLeak());

        replaceMetadataUrl(kylinConfig, oldUrl);

    }

    public static void testGetUUID(ResourceStore store) {
        // Generate new UUID
        String uuid1 = store.getMetaStoreUUID();
        Assert.assertNotNull(uuid1);
    }

    public static class TestEntity extends RootPersistentEntity {
        public TestEntity(String version) {
            this.version = version;
        }

        public TestEntity() {
        }
    }
}
