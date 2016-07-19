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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

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
            return pathname.getName().startsWith("cached_");
        }
    }

    public static final String baseDir = "/tmp/kylin_cachedtreemap_test/";
    public static final String backupDir = "/tmp/kylin_cachedtreemap_test.bak/";
    public static final String tmpDir = "/tmp/kylin_cachedtreemap_test.tmp/";

    private static void cleanup() {
        File dir = new File(baseDir);
        if (dir.exists()) {
            for (File f : dir.listFiles()) {
                f.delete();
            }
            dir.delete();
        }

        dir = new File(tmpDir);
        if (dir.exists()) {
            for (File f : dir.listFiles()) {
                f.delete();
            }
            dir.delete();
        }

        dir = new File(backupDir);
        if (dir.exists()) {
            for (File f : dir.listFiles()) {
                f.delete();
            }
            dir.delete();
        }

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
        CachedTreeMap map = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(false).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map.put(Key.of(1), Value.of("a"));
        map.put(Key.of(2), Value.of("b"));
        map.put(Key.of(3), Value.of("c"));
        map.put(Key.of(4), Value.of("d"));
        map.put(Key.of(5), Value.of("e"));

        File dir = new File(tmpDir);
        assertEquals(3, dir.listFiles(new CachedFileFilter()).length);

        DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpDir+"/.index"));
        map.write(out);
        out.flush();
        out.close();
        map.commit(false);

        dir = new File(baseDir);
        assertEquals(5, dir.listFiles(new CachedFileFilter()).length);

        DataInputStream in = new DataInputStream(new FileInputStream(baseDir+".index"));
        CachedTreeMap map2 = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(true).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map2.readFields(in);
        assertEquals(5, map2.size());
        assertEquals("b", ((Value)map2.get(Key.of(2))).valueStr);

        try {
            map2.put(Key.of(6), Value.of("f"));
            fail("Should be error when put value into immutable map");
        } catch (AssertionError error) {
        }

        assertFalse(new File(tmpDir).exists());
        assertFalse(new File(backupDir).exists());
    }

    @Test
    public void testWriteFailed() throws IOException {
        // normal case
        CachedTreeMap map = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(false).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map.put(Key.of(1), Value.of("a"));
        map.put(Key.of(2), Value.of("b"));
        map.put(Key.of(3), Value.of("c"));
        map.remove(Key.of(3));
        map.put(Key.of(4), Value.of("d"));

        DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpDir+".index"));
        map.write(out);
        out.flush();
        out.close();
        map.commit(false);

        DataInputStream in = new DataInputStream(new FileInputStream(baseDir+".index"));
        CachedTreeMap map2 = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(true).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map2.readFields(in);
        assertEquals(3, map2.size());
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);

        // suppose write value failed and didn't commit data
        map = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(false).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        VALUE_WRITE_ERROR_TOGGLE = true;
        map.put(Key.of(1), Value.of("aa"));
        map.put(Key.of(2), Value.of("bb"));
        VALUE_WRITE_ERROR_TOGGLE = false;
        map.put(Key.of(3), Value.of("cc"));
        map.put(Key.of(4), Value.of("dd"));
        out = new DataOutputStream(new FileOutputStream(tmpDir+".index"));
        map.write(out);
        out.flush();
        out.close();
        // suppose write value failed and didn't commit data
        //map.commit(false);

        // read map data should not be modified
        in = new DataInputStream(new FileInputStream(baseDir+".index"));
        map2 = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(true).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map2.readFields(in);
        assertEquals(3, map2.size());
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);

        assertTrue(new File(tmpDir).exists());
        assertFalse(new File(backupDir).exists());
    }

    @Test
    public void testCommit() throws IOException {
        CachedTreeMap map = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(false).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map.put(Key.of(1), Value.of("a"));
        map.put(Key.of(2), Value.of("b"));
        map.put(Key.of(3), Value.of("c"));
        map.put(Key.of(4), Value.of("d"));

        DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpDir+".index"));
        map.write(out);
        out.flush();
        out.close();
        map.commit(true);

        assertTrue(new File(tmpDir).exists());
        assertFalse(new File(backupDir).exists());

        DataInputStream in = new DataInputStream(new FileInputStream(baseDir+".index"));
        CachedTreeMap map2 = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(true).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map2.readFields(in);
        assertEquals(4, map2.size());
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);

        // continue modify map, but not commit
        map.put(Key.of(1), Value.of("aa"));
        map.put(Key.of(2), Value.of("bb"));
        map.put(Key.of(3), Value.of("cc"));
        map.put(Key.of(5), Value.of("e"));
        map.put(Key.of(6), Value.of("f"));
        out = new DataOutputStream(new FileOutputStream(tmpDir+".index"));
        map.write(out);
        out.flush();
        out.close();

        assertTrue(new File(tmpDir).exists());
        assertEquals(6, new File(tmpDir).listFiles(new CachedFileFilter()).length);
        assertEquals(4, new File(baseDir).listFiles(new CachedFileFilter()).length);

        in = new DataInputStream(new FileInputStream(baseDir+".index"));
        map2 = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(true).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map2.readFields(in);
        assertEquals(4, map2.size());
        assertEquals("a", ((Value)map2.get(Key.of(1))).valueStr);

        // commit data
        map.commit(false);
        assertFalse(new File(tmpDir).exists());
        assertEquals(6, new File(baseDir).listFiles(new CachedFileFilter()).length);

        in = new DataInputStream(new FileInputStream(baseDir+".index"));
        map2 = CachedTreeMap.CachedTreeMapBuilder.newBuilder().baseDir(baseDir)
                .persistent(true).immutable(true).maxSize(2).keyClazz(Key.class).valueClazz(Value.class).build();
        map2.readFields(in);
        assertEquals(6, map2.size());
        assertEquals("aa", ((Value)map2.get(Key.of(1))).valueStr);
        assertEquals("f", ((Value)map2.get(Key.of(6))).valueStr);
    }
}

