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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Be called by LocalFileResourceStoreTest, ITHBaseResourceStoreTest and ITHDFSResourceStoreTest.
 */
public class ResourceStoreTest {

    private static final Logger logger = LoggerFactory.getLogger(ResourceStoreTest.class);

    private static final String PERFORMANCE_TEST_ROOT_PATH = "/performance";

    private static final int TEST_RESOURCE_COUNT = 100;

    public static void testAStore(String url, KylinConfig kylinConfig) throws Exception {
        String oldUrl = replaceMetadataUrl(kylinConfig, url);
        testAStore(ResourceStore.getStore(kylinConfig));
        replaceMetadataUrl(kylinConfig, oldUrl);
    }

    public static void testPerformance(String url, KylinConfig kylinConfig) throws Exception {
        String oldUrl = replaceMetadataUrl(kylinConfig, url);
        testPerformance(ResourceStore.getStore(kylinConfig));
        replaceMetadataUrl(kylinConfig, oldUrl);
    }

    public static String mockUrl(String tag, KylinConfig kylinConfig) {
        String str = kylinConfig.getMetadataUrlPrefix() + "@" + tag;
        return str;
    }

    private static void testAStore(ResourceStore store) throws IOException {
        testBasics(store);
        testGetAllResources(store);
    }

    private static void testPerformance(ResourceStore store) throws IOException {
        logger.info("Test basic functions");
        testAStore(store);
        logger.info("Basic function ok. Start to test performance for class : " + store.getClass());
        logger.info("Write metadata time : " + testWritePerformance(store));
        logger.info("Read metadata time  " + testReadPerformance(store));
        logger.info("Performance test end. Class : " + store.getClass());
    }

    private static void testGetAllResources(ResourceStore store) throws IOException {
        final String folder = "/testFolder";
        List<StringEntity> result;

        // reset any leftover garbage
        ResourceTool.resetR(store, folder);

        store.putResource(folder + "/res1", new StringEntity("data1"), 1000, StringEntity.serializer);
        store.putResource(folder + "/res2", new StringEntity("data2"), 2000, StringEntity.serializer);
        store.putResource(folder + "/sub/res3", new StringEntity("data3"), 3000, StringEntity.serializer);
        store.putResource(folder + "/res4", new StringEntity("data4"), 4000, StringEntity.serializer);

        result = store.getAllResources(folder, StringEntity.class, StringEntity.serializer);
        assertEntity(result.get(0), "data1", 1000);
        assertEntity(result.get(1), "data2", 2000);
        assertEntity(result.get(2), "data4", 4000);
        assertEquals(3, result.size());

        result = store.getAllResources(folder, 2000, 4000, StringEntity.class, StringEntity.serializer);
        assertEntity(result.get(0), "data2", 2000);
        assertEquals(1, result.size());

        ResourceTool.resetR(store, folder);
    }

    private static void assertEntity(StringEntity entity, String data, int ts) {
        assertEquals(data, entity.str);
        assertEquals(ts, entity.lastModified);
    }

    private static void testBasics(ResourceStore store) throws IOException {
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
        store.putResource(path1, content1, StringEntity.serializer);
        assertTrue(store.exists(path1));
        t = store.getResource(path1, StringEntity.class, StringEntity.serializer);
        assertEquals(content1, t);

        store.putResource(path2, content2, StringEntity.serializer);
        assertTrue(store.exists(path2));
        t = store.getResource(path2, StringEntity.class, StringEntity.serializer);
        assertEquals(content2, t);

        // overwrite
        t.str = "new string";
        store.putResource(path2, t, StringEntity.serializer);

        // write conflict
        try {
            t.setLastModified(t.getLastModified() - 1);
            store.putResource(path2, t, StringEntity.serializer);
            fail("write conflict should trigger IllegalStateException");
        } catch (WriteConflictException e) {
            // expected
        }

        // list
        NavigableSet<String> list = null;

        list = store.listResources(dir1);
        System.out.println(list);
        assertTrue(list.contains(path1));
        assertTrue(list.contains(path2) == false);

        list = store.listResources(dir2);
        assertTrue(list.contains(path2));
        assertTrue(list.contains(path1) == false);

        list = store.listResources("/");
        assertTrue(list.contains(dir1));
        assertTrue(list.contains(dir2));
        assertTrue(list.contains(path1) == false);
        assertTrue(list.contains(path2) == false);

        list = store.listResources(path1);
        assertNull(list);
        list = store.listResources(path2);
        assertNull(list);

        // delete/exist
        store.deleteResource(path1);
        assertTrue(store.exists(path1) == false);
        list = store.listResources(dir1);
        assertTrue(list == null || list.contains(path1) == false);

        store.deleteResource(path2);
        assertTrue(store.exists(path2) == false);
        list = store.listResources(dir2);
        assertTrue(list == null || list.contains(path2) == false);
    }

    private static long testWritePerformance(ResourceStore store) throws IOException {
        store.deleteResource(PERFORMANCE_TEST_ROOT_PATH);
        StringEntity content = new StringEntity("something");
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < TEST_RESOURCE_COUNT; i++) {
            String resourcePath = PERFORMANCE_TEST_ROOT_PATH + "/res_" + i;
            store.putResource(resourcePath, content, 0, StringEntity.serializer);
        }
        return System.currentTimeMillis() - startTime;
    }

    private static long testReadPerformance(ResourceStore store) throws IOException {
        long startTime = System.currentTimeMillis();
        int step = 0; //avoid compiler optimization
        for (int i = 0; i < TEST_RESOURCE_COUNT; i++) {
            String resourcePath = PERFORMANCE_TEST_ROOT_PATH + "/res_" + i;
            StringEntity t = store.getResource(resourcePath, StringEntity.class, StringEntity.serializer);
            step |= t.toString().length();
        }
        logger.info("step : " + step);
        return System.currentTimeMillis() - startTime;
    }

    public static String replaceMetadataUrl(KylinConfig kylinConfig, String newUrl) {
        String oldUrl = kylinConfig.getMetadataUrl().toString();
        kylinConfig.setProperty("kylin.metadata.url", newUrl);
        return oldUrl;
    }

}
