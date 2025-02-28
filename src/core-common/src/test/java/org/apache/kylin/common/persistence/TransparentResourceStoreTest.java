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

import static org.apache.kylin.common.persistence.MetadataType.ALL;
import static org.apache.kylin.common.persistence.MetadataType.SYSTEM;
import static org.apache.kylin.common.persistence.ResourceStore.METASTORE_UUID_TAG;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.val;

@MetadataInfo(onlyProps = true)
class TransparentResourceStoreTest {

    String dir1 = "INDEX_PLAN";
    String dir2 = "TABLE_INFO";
    String dirXyz = "PROJECT";

    String path1 = "INDEX_PLAN/_test.json";
    String path2 = "TABLE_INFO/_test.json";
    String path3 = "TABLE_INFO/_test2.json";
    String pathXyz = dirXyz + "/xyz";
    String pathCubeX = "INDEX_PLAN/cubex";

    private ResourceStore getStore() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        return new TransparentResourceStore((InMemResourceStore) underlying, config);
    }

    @Test
    void testRecreateMetadataInOneTransaction() {
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
                config.setProperty("kylin.env", "UT");
                return 0;
            }, "default");
        });
    }

    @Test
    @SuppressWarnings("MethodLength")
    void testOverlay() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        underlying.checkAndPutResource(METASTORE_UUID_TAG, new StringEntity("UUID", RandomUtil.randomUUIDStr()),
                StringEntity.serializer);

        //reinit
        doInit(underlying);

        val rs = underlying;

        //test init
        checkInitResult(rs);

        //test delete
        val old_se_2 = rs.getResource(path2, StringEntity.serializer);

        rs.deleteResource(path2);
        checkDeleteResult(rs);

        //test double delete
        rs.deleteResource(path2);
        assertFalse(rs.exists(path2));

        try {
            rs.checkAndPutResource(path2, old_se_2, StringEntity.serializer);
            Assertions.fail();
        } catch (IllegalStateException e) {
            //expected
        }

        StringEntity new_se_2 = new StringEntity("new_2");
        rs.checkAndPutResource(path2, new_se_2, StringEntity.serializer);
        checkCreateResult(rs);

        //test delete, this time we're deleting from underlying
        rs.deleteResource(path2);
        checkDeleteResult(rs);

        // now play with path3

        rs.deleteResource(path3);
        checkDeletePath3Result(rs);

        // now play with path1
        rs.deleteResource(path1);
        checkDeletePath1Result(rs);

        // now play with pathXyz
        rs.deleteResource(pathXyz);
        checkDeletePathXyzResult(rs);

        // add some new
        StringEntity se1 = new StringEntity("se1");
        StringEntity se2 = new StringEntity("se2");
        rs.checkAndPutResource("MODEL/b", se1, StringEntity.serializer);
        rs.checkAndPutResource("MODEL/z", se2, StringEntity.serializer);
        checkAddResult(rs);

        // normal update
        StringEntity cubex = rs.getResource(pathCubeX, StringEntity.serializer);
        cubex.setStr("cubex2");
        rs.checkAndPutResource(pathCubeX, cubex, StringEntity.serializer);

        StringEntity se2_old = rs.getResource("MODEL/z", StringEntity.serializer);
        assertEquals(0, se2_old.getMvcc());
        se2_old.setStr("abccc");
        rs.checkAndPutResource("MODEL/z", se2_old, StringEntity.serializer);
        StringEntity se2_new = rs.getResource("MODEL/z", StringEntity.serializer);
        assertEquals(1, se2_old.getMvcc());
        assertEquals(1, se2_new.getMvcc());
        assertEquals("abccc", se2_new.getStr());

        se2_new.setStr("abccc2");
        se2_new.setMvcc(0);
        try {
            rs.checkAndPutResource("MODEL/z", se2_new, StringEntity.serializer);
            fail();
        } catch (VersionConflictException e) {
            //expected
        }

        // check mvcc

        checkMvcc(rs);
    }

    void doInit(ResourceStore underlying) {
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
    }

    void checkInitResult(ResourceStore rs) {
        NavigableSet<String> list1 = rs.listResourcesRecursively(ALL.name());
        assertEquals(6, list1.size());
        assertTrue(list1.containsAll(
                Sets.newHashSet(path1, path2, path3, ResourceStore.METASTORE_UUID_TAG, pathXyz, pathCubeX)));
        NavigableSet<String> list2 = rs.listResources(MetadataType.ALL.name());
        assertEquals(31, list2.size());
        assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, SYSTEM.name(), dirXyz)));
        assertTrue(rs.exists(path1));
        assertTrue(rs.exists(path2));
        assertTrue(rs.exists(path3));
        assertTrue(rs.exists(pathXyz));
        assertTrue(rs.exists(pathCubeX));
    }

    void checkDeleteResult(ResourceStore rs) {
        NavigableSet<String> list1 = rs.listResourcesRecursively(ALL.name());
        assertEquals(5, list1.size());
        assertTrue(
                list1.containsAll(Sets.newHashSet(path1, path3, ResourceStore.METASTORE_UUID_TAG, pathXyz, pathCubeX)));
        NavigableSet<String> list2 = rs.listResources(ALL.name());
        assertEquals(31, list2.size());
        assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, SYSTEM.name(), dirXyz)));
        assertTrue(rs.exists(path1));
        assertFalse(rs.exists(path2));
        assertTrue(rs.exists(path3));
        assertTrue(rs.exists(pathXyz));
        assertTrue(rs.exists(pathCubeX));
    }

    void checkCreateResult(ResourceStore rs) {
        NavigableSet<String> list1 = rs.listResourcesRecursively(ALL.name());
        assertEquals(6, list1.size());
        assertTrue(list1.containsAll(
                Sets.newHashSet(path1, path2, path3, ResourceStore.METASTORE_UUID_TAG, pathXyz, pathCubeX)));
        NavigableSet<String> list2 = rs.listResources(ALL.name());
        assertEquals(31, list2.size());
        assertTrue(list2.containsAll(Sets.newHashSet(dir1, dir2, SYSTEM.name(), dirXyz)));
        assertTrue(rs.exists(path1));
        assertTrue(rs.exists(path2));
        assertTrue(rs.exists(path3));
        assertTrue(rs.exists(pathXyz));
        assertTrue(rs.exists(pathCubeX));
    }

    void checkDeletePath3Result(ResourceStore rs) {
        NavigableSet<String> list1 = rs.listResourcesRecursively(ALL.name());
        assertEquals(4, list1.size());
        assertTrue(list1.containsAll(Sets.newHashSet(path1, ResourceStore.METASTORE_UUID_TAG, pathXyz, pathCubeX)));
        NavigableSet<String> list2 = rs.listResources(MetadataType.ALL.name());
        assertEquals(31, list2.size());
        assertTrue(list2.containsAll(Sets.newHashSet(dir1, SYSTEM.name(), dirXyz)));
        assertTrue(rs.exists(path1));
        assertFalse(rs.exists(path2));
        assertFalse(rs.exists(path3));
        assertTrue(rs.exists(pathXyz));
        assertTrue(rs.exists(pathCubeX));
    }

    void checkDeletePath1Result(ResourceStore rs) {
        NavigableSet<String> list1 = rs.listResourcesRecursively(MetadataType.ALL.name());
        assertEquals(3, list1.size());
        assertTrue(list1.containsAll(Sets.newHashSet(ResourceStore.METASTORE_UUID_TAG, pathXyz, pathCubeX)));
        NavigableSet<String> list2 = rs.listResources(MetadataType.ALL.name());
        assertEquals(31, list2.size());
        assertTrue(list2.containsAll(Sets.newHashSet(SYSTEM.name(), dirXyz)));
        assertFalse(rs.exists(path1));
        assertFalse(rs.exists(path2));
        assertFalse(rs.exists(path3));
        assertTrue(rs.exists(pathXyz));
        assertTrue(rs.exists(pathCubeX));
    }

    void checkDeletePathXyzResult(ResourceStore rs) {
        NavigableSet<String> list1 = rs.listResourcesRecursively(MetadataType.ALL.name());
        assertEquals(2, list1.size());
        assertTrue(list1.containsAll(Sets.newHashSet(ResourceStore.METASTORE_UUID_TAG, pathCubeX)));
        NavigableSet<String> list2 = rs.listResources(MetadataType.ALL.name());
        assertEquals(31, list2.size());
        assertTrue(list2.containsAll(Sets.newHashSet(SYSTEM.name(), dirXyz)));
        assertFalse(rs.exists(path1));
        assertFalse(rs.exists(path2));
        assertFalse(rs.exists(path3));
        assertFalse(rs.exists(pathXyz));
        assertTrue(rs.exists(pathCubeX));
    }

    void checkAddResult(ResourceStore rs) {
        NavigableSet<String> list1 = rs.listResourcesRecursively(MetadataType.ALL.name());
        assertEquals(4, list1.size());
        assertTrue(
                list1.containsAll(Sets.newHashSet(ResourceStore.METASTORE_UUID_TAG, pathCubeX, "MODEL/b", "MODEL/z")));
        NavigableSet<String> list2 = rs.listResources(MetadataType.ALL.name());
        assertEquals(31, list2.size());
        assertTrue(list2.containsAll(Sets.newHashSet(dir1, SYSTEM.name(), "MODEL")));
        assertFalse(rs.exists(path1));
        assertFalse(rs.exists(path2));
        assertFalse(rs.exists(path3));
        assertFalse(rs.exists(pathXyz));
        assertTrue(rs.exists(pathCubeX));
        assertTrue(rs.exists("MODEL/b"));
        assertTrue(rs.exists("MODEL/z"));
    }

    void checkMvcc(ResourceStore rs) {
        assertEquals(4, rs.listResourcesRecursively(MetadataType.ALL.name()).size());
        assertEquals(1, rs.getResource(pathCubeX, StringEntity.serializer).getMvcc());
        assertEquals(0, rs.getResource("MODEL/b", StringEntity.serializer).getMvcc());
        assertEquals(1, rs.getResource("MODEL/z", StringEntity.serializer).getMvcc());
        assertEquals(0, rs.getResource(ResourceStore.METASTORE_UUID_TAG, StringEntity.serializer).getMvcc());
    }

    @Test
    void testAStore() {
        ResourceStoreTestBase.testAStore(getStore());
    }

    @Test
    void testUUID() {
        ResourceStoreTestBase.testGetUUID(getStore());
    }

}
