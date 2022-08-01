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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import lombok.val;

@MetadataInfo(onlyProps = true)
public class ThreadViewResourceStoreTest {

    private ResourceStore getStore() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        return new ThreadViewResourceStore((InMemResourceStore) underlying, config);
    }

    @Test
    public void testRecreateMetadataInOneTransaction() {
        String dir = "/default/table_desc/TEST_KYLIN_FACT.json";
        StringEntity table = new StringEntity("TEST_KYLIN_FACT");

        Assertions.assertThrows(TransactionException.class, () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                config.setProperty("kylin.env", "DEV");
                ResourceStore underlying = ResourceStore.getKylinMetaStore(config);

                // first delete resource
                underlying.deleteResource(dir);

                // then create a new one with the same res path
                underlying.checkAndPutResource(dir, table, StringEntity.serializer);

                return 0;
            }, "default");
        });
    }

    @Test
    @SuppressWarnings("MethodLength")
    public void testOverlay() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        underlying.checkAndPutResource("/UUID", new StringEntity(RandomUtil.randomUUIDStr()), StringEntity.serializer);

        String dir1 = "/cube";
        String dir2 = "/table";

        String path1 = "/cube/_test.json";
        String path2 = "/table/_test.json";
        String path3 = "/table/_test2.json";
        String pathXyz = "/xyz";
        String pathCubeX = "/cubex";

        StringEntity content1 = new StringEntity("1");
        StringEntity content2 = new StringEntity("2");
        StringEntity content3 = new StringEntity("3");
        StringEntity contentxyz = new StringEntity("xyz");
        StringEntity contentcubex = new StringEntity("cubex");

        //reinit
        underlying.deleteResource(path1);
        underlying.deleteResource(path2);
        underlying.deleteResource(path3);
        underlying.deleteResource(pathXyz);
        underlying.deleteResource(pathCubeX);
        underlying.checkAndPutResource(path1, content1, StringEntity.serializer);
        underlying.checkAndPutResource(path2, content2, StringEntity.serializer);
        underlying.checkAndPutResource(path3, content3, StringEntity.serializer);
        underlying.checkAndPutResource(pathXyz, contentxyz, StringEntity.serializer);
        underlying.checkAndPutResource(pathCubeX, contentcubex, StringEntity.serializer);

        val rs = new ThreadViewResourceStore((InMemResourceStore) underlying, config);

        //test init
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(6, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, path2, path3, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(5, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertTrue(rs.exists(path2));
            assertTrue(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        //test delete
        val old_se_2 = rs.getResource(path2, StringEntity.serializer);

        rs.deleteResource(path2);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(5, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, path3, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(5, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertTrue(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        //test double delete
        rs.deleteResource(path2);
        assertFalse(rs.exists(path2));

        try {
            rs.checkAndPutResource(path2, old_se_2, StringEntity.serializer);
            fail();
        } catch (IllegalStateException e) {
            //expected
        }

        StringEntity new_se_2 = new StringEntity("new_2");
        rs.checkAndPutResource(path2, new_se_2, StringEntity.serializer);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(6, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, path2, path3, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(5, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertTrue(rs.exists(path2));
            assertTrue(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        //test delete, this time we're deleting from overlay
        rs.deleteResource(path2);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(5, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, path3, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(5, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertTrue(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        // now play with path3

        rs.deleteResource(path3);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(4, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet(path1, "/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(4, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet(dir1, "/UUID", pathXyz, pathCubeX)));
            assertTrue(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertFalse(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        // now play with path1
        rs.deleteResource(path1);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(3, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet("/UUID", pathXyz, pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(3, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet("/UUID", pathXyz, pathCubeX)));
            assertFalse(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertFalse(rs.exists(path3));
            assertTrue(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        // now play with pathXyz
        rs.deleteResource(pathXyz);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(2, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet("/UUID", pathCubeX)));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(2, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet("/UUID", pathCubeX)));
            assertFalse(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertFalse(rs.exists(path3));
            assertFalse(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
        }

        // add some new
        StringEntity se1 = new StringEntity("se1");
        StringEntity se2 = new StringEntity("se2");
        rs.checkAndPutResource("/a/b", se1, StringEntity.serializer);
        rs.checkAndPutResource("/z", se2, StringEntity.serializer);
        {
            NavigableSet<String> list1 = rs.listResourcesRecursively("/");
            assertEquals(4, list1.size());
            assertTrue(list1.containsAll(Sets.newHashSet("/UUID", pathCubeX, "/a/b", "/z")));
            NavigableSet<String> list2 = rs.listResources("/");
            assertEquals(4, list2.size());
            assertTrue(list2.containsAll(Sets.newHashSet("/UUID", pathCubeX, "/a", "/z")));
            assertFalse(rs.exists(path1));
            assertFalse(rs.exists(path2));
            assertFalse(rs.exists(path3));
            assertFalse(rs.exists(pathXyz));
            assertTrue(rs.exists(pathCubeX));
            assertTrue(rs.exists("/a/b"));
            assertTrue(rs.exists("/z"));
        }

        // normal update
        StringEntity cubex = rs.getResource(pathCubeX, StringEntity.serializer);
        cubex.setStr("cubex2");
        rs.checkAndPutResource(pathCubeX, cubex, StringEntity.serializer);

        StringEntity se2_old = rs.getResource("/z", StringEntity.serializer);
        assertEquals(0, se2_old.getMvcc());
        se2_old.setStr("abccc");
        rs.checkAndPutResource("/z", se2_old, StringEntity.serializer);
        StringEntity se2_new = rs.getResource("/z", StringEntity.serializer);
        assertEquals(1, se2_old.getMvcc());
        assertEquals(1, se2_new.getMvcc());
        assertEquals("abccc", se2_new.getStr());

        se2_new.setStr("abccc2");
        se2_new.setMvcc(0);
        try {
            rs.checkAndPutResource("/z", se2_new, StringEntity.serializer);
            fail();
        } catch (VersionConflictException e) {
            //expected
        }

        // check mvcc
        assertEquals(6, underlying.listResourcesRecursively("/").size());
        assertEquals(0, underlying.getResource(path1, StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource(path2, StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource(path3, StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource("/UUID", StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource(pathCubeX, StringEntity.serializer).getMvcc());
        assertEquals(0, underlying.getResource(pathXyz, StringEntity.serializer).getMvcc());

        assertEquals(4, rs.listResourcesRecursively("/").size());
        assertEquals(1, rs.getResource(pathCubeX, StringEntity.serializer).getMvcc());
        assertEquals(0, rs.getResource("/a/b", StringEntity.serializer).getMvcc());
        assertEquals(1, rs.getResource("/z", StringEntity.serializer).getMvcc());
        assertEquals(0, rs.getResource("/UUID", StringEntity.serializer).getMvcc());

        try {
            Field overlay1 = ThreadViewResourceStore.class.getDeclaredField("overlay");
            Unsafe.changeAccessibleObject(overlay1, true);
            InMemResourceStore overlay = (InMemResourceStore) overlay1.get(rs);
            assertEquals(1, overlay.getResource(pathCubeX, StringEntity.serializer).getMvcc());
            assertEquals(0, overlay.getResource("/a/b", StringEntity.serializer).getMvcc());
            assertEquals(1, overlay.getResource("/z", StringEntity.serializer).getMvcc());
            assertFalse(overlay.exists("/UUID"));

            Field data1 = InMemResourceStore.class.getDeclaredField("data");
            Unsafe.changeAccessibleObject(data1, true);
            ConcurrentSkipListMap<String, VersionedRawResource> data = (ConcurrentSkipListMap<String, VersionedRawResource>) data1
                    .get(overlay);
            long count = data.values().stream().filter(x -> x == TombVersionedRawResource.getINSTANCE()).count();
            assertEquals(4, count);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAStore() {
        ResourceStoreTestBase.testAStore(getStore());
    }

    @Test
    public void testUUID() {
        ResourceStoreTestBase.testGetUUID(getStore());
    }

}
