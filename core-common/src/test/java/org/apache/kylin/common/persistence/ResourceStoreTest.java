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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.lang.StringUtils;

/**
 * Be called by LocalFileResourceStoreTest and ITHBaseResourceStoreTest.
 */
public class ResourceStoreTest {

    public static void testAStore(ResourceStore store) throws IOException {
        testBasics(store);
        testGetAllResources(store);
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
        } catch (IllegalStateException e) {
            // expected
        }

        // list
        NavigableSet<String> list;

        list = store.listResources(dir1);
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

    @SuppressWarnings("serial")
    public static class StringEntity extends RootPersistentEntity {

        public static final Serializer<StringEntity> serializer = new Serializer<StringEntity>() {
            @Override
            public void serialize(StringEntity obj, DataOutputStream out) throws IOException {
                out.writeUTF(obj.str);
            }

            @Override
            public StringEntity deserialize(DataInputStream in) throws IOException {
                String str = in.readUTF();
                return new StringEntity(str);
            }
        };

        String str;

        public StringEntity(String str) {
            this.str = str;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + ((str == null) ? 0 : str.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!(obj instanceof StringEntity))
                return false;
            return StringUtils.equals(this.str, ((StringEntity) obj).str);
        }

        @Override
        public String toString() {
            return str;
        }
    }

}
