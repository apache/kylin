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

package org.apache.kylin.storage.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ITHBaseResourceStoreTest extends HBaseMetadataTestCase {

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testHBaseStore() throws Exception {
        testAStore(ResourceStore.getStore(KylinConfig.getInstanceFromEnv()));
    }

    @Test
    public void testHBaseStoreWithLargeCell() throws Exception {
        String path = "/cube/_test_large_cell.json";
        String largeContent = "THIS_IS_A_LARGE_CELL";
        StringEntity content = new StringEntity(largeContent);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int origSize = config.getHBaseKeyValueSize();
        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());

        try {
            config.setProperty("kylin.hbase.client.keyvalue.maxsize", String.valueOf(largeContent.length() - 1));

            store.deleteResource(path);

            store.putResource(path, content, StringEntity.serializer);
            assertTrue(store.exists(path));
            StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
            assertEquals(content, t);

            Path redirectPath = ((HBaseResourceStore) store).bigCellHDFSPath(path);
            Configuration hconf = HBaseConnection.getCurrentHBaseConfiguration();
            FileSystem fileSystem = FileSystem.get(hconf);
            assertTrue(fileSystem.exists(redirectPath));

            FSDataInputStream in = fileSystem.open(redirectPath);
            assertEquals(largeContent, in.readUTF());
            in.close();

            store.deleteResource(path);
        } finally {
            config.setProperty("kylin.hbase.client.keyvalue.maxsize", "" + origSize);
            store.deleteResource(path);
        }
    }

    void testAStore(ResourceStore store) throws IOException {
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
        ArrayList<String> list;

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

    public static class StringEntity extends RootPersistentEntity {

        static final Serializer<StringEntity> serializer = new Serializer<StringEntity>() {
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
