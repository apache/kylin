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
package org.apache.kylin.dict;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Created by sunyerui on 16/7/12.
 */
public class CachedTreeMapTest {

    public static class Key implements WritableComparable {
        int keyInt;

        public static Key of(int keyInt) {
            Key newKey = new Key();
            newKey.keyInt = keyInt;
            return newKey;
        }

        @Override
        public int compareTo(Object o) {
            return keyInt - ((Key)o).keyInt;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(keyInt);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            keyInt = in.readInt();
        }

        @Override
        public String toString() {
            return String.valueOf(keyInt);
        }
    }

    public static boolean VALUE_WRITE_ERROR_TOGGLE = false;
    public static class Value implements Writable {
        String valueStr;

        public static Value of(String valueStr) {
            Value newValue = new Value();
            newValue.valueStr = valueStr;
            return newValue;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            if (VALUE_WRITE_ERROR_TOGGLE) {
                out.write(new byte[0]);
                return;
            }
            out.writeUTF(valueStr);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            valueStr = in.readUTF();
        }
    }

    public static class CachedFileFilter implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            return pathname.getName().startsWith(CachedTreeMap.CACHED_PREFIX);
        }
    }

    public static class VersionFilter implements FileFilter {
        @Override
        public boolean accept(File pathname) {
            return pathname.getName().startsWith(CachedTreeMap.VERSION_PREFIX);
        }
    }


    static final UUID uuid = UUID.randomUUID();
    static final String baseDir = "/tmp/kylin_cachedtreemap_test/" + uuid;
    static final String workingDir = baseDir + "/working";

    private static void cleanup() {
        Path basePath = new Path(baseDir);
        try {
            HadoopUtil.getFileSystem(basePath).delete(basePath, true);
        } catch (IOException e) {}
        VALUE_WRITE_ERROR_TOGGLE = false;
    }

    @After
    public void afterTest() {
        cleanup();
    }

    @AfterClass
    public static void tearDown() {
        cleanup();
    }

    @Test
    public void testCachedTreeMap() throws IOException {
        CachedTreeMap map = createMutableMap();
        map.put(Key.of(1), Value.of("a"));
        map.put(Key.of(2), Value.of("b"));
        map.put(Key.of(3), Value.of("c"));
        map.put(Key.of(4), Value.of("d"));
        map.put(Key.of(5), Value.of("e"));

        File dir = new File(workingDir);
        assertEquals(3, dir.listFiles(new CachedFileFilter()).length);

        flushAndCommit(map, true, true, false);
        assertFalse(new File(workingDir).exists());

        dir = new File(map.getLatestVersion());
        assertEquals(5, dir.listFiles(new CachedFileFilter()).length);

        CachedTreeMap map2 = createImmutableMap();
        assertEquals(5, map2.size());
        assertEquals("b", ((Value)map2.get(Key.of(2))).valueStr);

        try {
            map2.put(Key.of(6), Value.of("f"));
            fail("Should be error when put value into immutable map");
        } catch (AssertionError error) {}
    }

    @Test
    public void testMultiVersions() throws IOException, InterruptedException {
        CachedTreeMap map = createMutableMap();
        Thread.sleep(3000);
        map.put(Key.of(1), Value.of("a"));
        map.put(Key.of(2), Value.of("b"));
        map.put(Key.of(3), Value.of("c"));
        flushAndCommit(map, true, true, false);

        CachedTreeMap map2 = createImmutableMap();
        assertEquals("b", ((Value)map2.get(Key.of(2))).valueStr);

        // re-open dict, append new data
        map = createMutableMap();
        map.put(Key.of(4), Value.of("d"));
        flushAndCommit(map, true, true, true);

        // new data is not visible for map2
        assertNull(map2.get(Key.of(4)));

        // append data, and be visible for new immutable map
        map.put(Key.of(5), Value.of("e"));
        flushAndCommit(map, true, true, true);

        CachedTreeMap map3 = createImmutableMap();
        assertEquals("d", ((Value)map3.get(Key.of(4))).valueStr);
        assertEquals("e", ((Value)map3.get(Key.of(5))).valueStr);

        // Check versions retention
        File dir = new File(baseDir);
        assertEquals(3, dir.listFiles(new VersionFilter()).length);
    }

    @Test
    public void testKeepAppend() throws IOException {
        CachedTreeMap map = createMutableMap();
        map.put(Key.of(1), Value.of("a"));
        map.put(Key.of(2), Value.of("b"));
        map.put(Key.of(3), Value.of("c"));
        map.put(Key.of(4), Value.of("d"));
        map.put(Key.of(5), Value.of("e"));

        // flush with keepAppend false, map can't be append
        flushAndCommit(map, true, true, false);
        // append into map has closed
        try {
            map.put(Key.of(6), Value.of("f"));
            fail();
        } catch (AssertionError e) {
            assertEquals("Only support put method with immutable false and keepAppend true", e.getMessage());
        }

        CachedTreeMap map2 = createImmutableMap();
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);
        assertEquals("d", ((Value)map2.get(Key.of(4))).valueStr);
        assertEquals("e", ((Value)map2.get(Key.of(5))).valueStr);

        map = createMutableMap();
        map.put(Key.of(6), Value.of("f"));
        map.put(Key.of(7), Value.of("g"));
        map.put(Key.of(8), Value.of("h"));
        // flush with keepAppend true
        flushAndCommit(map, true, true, true);
        map.put(Key.of(9), Value.of("i"));
        // can still append data
        flushAndCommit(map, true, true, false);

        map2 = createImmutableMap();
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);
        assertEquals("d", ((Value)map2.get(Key.of(4))).valueStr);
        assertEquals("f", ((Value)map2.get(Key.of(6))).valueStr);
        assertEquals("i", ((Value)map2.get(Key.of(9))).valueStr);
    }

    @Test
    public void testVersionRetention() throws IOException, InterruptedException {
        File dir = new File(baseDir);
        // TTL for 3s and keep 3 versions
        CachedTreeMap map = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
            .immutable(false).maxSize(2).keyClazz(Key.class).valueClazz(Value.class)
            .maxVersions(3).versionTTL(1000 * 3).build();
        map.put(Key.of(1), Value.of("a"));

        // has version 0 when create map
        assertEquals(1, dir.listFiles(new VersionFilter()).length);
        Thread.sleep(2500);

        // flush version 1
        flushAndCommit(map, true, true, true);
        assertEquals(2, dir.listFiles(new VersionFilter()).length);

        // flush version 2
        flushAndCommit(map, true, true, true);
        assertEquals(3, dir.listFiles(new VersionFilter()).length);

        // flush version 3
        flushAndCommit(map, true, true, true);
        // won't delete version since 3s TTL
        assertEquals(4, dir.listFiles(new VersionFilter()).length);

        // sleep to make version 0 expired
        Thread.sleep(500);
        // flush verion 4
        flushAndCommit(map, true, true, false);
        assertEquals(4, dir.listFiles(new VersionFilter()).length);

        // TTL for 100ms and keep 2 versions
        map = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
            .immutable(false).maxSize(2).keyClazz(Key.class).valueClazz(Value.class)
            .maxVersions(2).versionTTL(100).build();
        flushAndCommit(map, true, true, false);
        assertEquals(2, dir.listFiles(new VersionFilter()).length);
    }

    @Test
    public void testWithOldFormat() throws IOException {
        File dir = new File(baseDir);
        CachedTreeMap map = createMutableMap();
        map.put(Key.of(1), Value.of("a"));
        map.put(Key.of(2), Value.of("b"));
        map.put(Key.of(3), Value.of("c"));
        map.put(Key.of(4), Value.of("d"));
        map.put(Key.of(5), Value.of("e"));
        flushAndCommit(map, true, true, true);

        // move version dir to base dir, to simulate the older format
        Path versionPath = new Path(map.getLatestVersion());
        Path tmpVersionPath = new Path(versionPath.getParent().getParent(), versionPath.getName());
        FileSystem fs = HadoopUtil.getFileSystem(versionPath);
        fs.rename(versionPath, tmpVersionPath);
        fs.delete(new Path(baseDir), true);
        fs.rename(tmpVersionPath, new Path(baseDir));
        assertEquals(0, dir.listFiles(new VersionFilter()).length);
        assertEquals(5, dir.listFiles(new CachedFileFilter()).length);

        CachedTreeMap map2 = createImmutableMap();
        assertEquals(5, map2.size());
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);
        assertEquals("e", ((Value)map2.get(Key.of(5))).valueStr);

        assertEquals(1, dir.listFiles(new VersionFilter()).length);
        assertEquals(0, dir.listFiles(new CachedFileFilter()).length);
    }

    @Test
    public void testWriteFailed() throws IOException {
        // normal case
        CachedTreeMap map = createMutableMap();
        map.put(Key.of(1), Value.of("a"));
        map.put(Key.of(2), Value.of("b"));
        map.put(Key.of(3), Value.of("c"));
        map.remove(Key.of(3));
        map.put(Key.of(4), Value.of("d"));

        flushAndCommit(map, true, true, false);

        CachedTreeMap map2 = createImmutableMap();
        assertEquals(3, map2.size());
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);

        // suppose write value failed and didn't commit data
        map = createMutableMap();
        VALUE_WRITE_ERROR_TOGGLE = true;
        map.put(Key.of(1), Value.of("aa"));
        map.put(Key.of(2), Value.of("bb"));
        VALUE_WRITE_ERROR_TOGGLE = false;
        map.put(Key.of(3), Value.of("cc"));
        map.put(Key.of(4), Value.of("dd"));
        // suppose write value failed and didn't commit data
        flushAndCommit(map, true, false, false);

        // read map data should not be modified
        map2 = createImmutableMap();
        assertEquals(3, map2.size());
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);

        assertTrue(new File(workingDir).exists());
    }

    private CachedTreeMap createImmutableMap() throws IOException {
        CachedTreeMap map = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
            .immutable(true).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        try (DataInputStream in = map.openIndexInput()) {
            map.readFields(in);
        }
        return map;
    }

    private CachedTreeMap createMutableMap() throws IOException {
        CachedTreeMap map = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
            .immutable(false).maxSize(2).maxVersions(3).versionTTL(1000 * 3).keyClazz(Key.class).valueClazz(Value.class).build();
        try (DataInputStream in = map.openIndexInput()) {
            map.readFields(in);
        } catch (IOException e) {}
        return map;
    }

    private void flushAndCommit(CachedTreeMap map, boolean doFlush, boolean doCommit, boolean keepAppend) throws IOException {
        if (doFlush) {
            try (DataOutputStream out = map.openIndexOutput()) {
                map.write(out);
            }
        }

        if (doCommit) {
            map.commit(keepAppend);
        }
    }
}

