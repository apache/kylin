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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Created by sunyerui on 16/5/2.
 * TODO Depends on HDFS for now, ideally just depends on storage interface
 */
public class CachedTreeMap<K extends WritableComparable, V extends Writable> extends TreeMap<K, V> implements Writable {
    private static final Logger logger = LoggerFactory.getLogger(CachedTreeMap.class);

    private final Class<K> keyClazz;
    private final Class<V> valueClazz;
    transient volatile Collection<V> values;
    private final LoadingCache<K, V> valueCache;
    private final TreeSet<String> fileList;
    private final Configuration conf;
    private final String baseDir;
    private final String tmpDir;
    private final FileSystem fs;
    private final boolean persistent;
    private final boolean immutable;
    private long writeValueTime = 0;
    private long readValueTime = 0;

    private static final int BUFFER_SIZE = 8 * 1024 * 1024;

    public static class CachedTreeMapBuilder<K, V> {
        private Class<K> keyClazz;
        private Class<V> valueClazz;
        private int maxCount = 8;
        private String baseDir;
        private boolean persistent;
        private boolean immutable;

        public static CachedTreeMapBuilder newBuilder() {
            return new CachedTreeMapBuilder();
        }

        private CachedTreeMapBuilder() {
        }

        public CachedTreeMapBuilder keyClazz(Class<K> clazz) {
            this.keyClazz = clazz;
            return this;
        }

        public CachedTreeMapBuilder valueClazz(Class<V> clazz) {
            this.valueClazz = clazz;
            return this;
        }

        public CachedTreeMapBuilder<K, V> maxSize(int maxCount) {
            this.maxCount = maxCount;
            return this;
        }

        public CachedTreeMapBuilder<K, V> baseDir(String baseDir) {
            this.baseDir = baseDir;
            return this;
        }

        public CachedTreeMapBuilder<K, V> persistent(boolean persistent) {
            this.persistent = persistent;
            return this;
        }

        public CachedTreeMapBuilder<K, V> immutable(boolean immutable) {
            this.immutable = immutable;
            return this;
        }

        public CachedTreeMap build() throws IOException {
            if (baseDir == null) {
                throw new RuntimeException("CachedTreeMap need a baseDir to cache data");
            }
            if (keyClazz == null || valueClazz == null) {
                throw new RuntimeException("CachedTreeMap need key and value clazz to serialize data");
            }
            CachedTreeMap map = new CachedTreeMap(maxCount, keyClazz, valueClazz, baseDir, persistent, immutable);
            return map;
        }
    }

    private CachedTreeMap(int maxCount, Class<K> keyClazz, Class<V> valueClazz, String baseDir, boolean persistent, boolean immutable) throws IOException {
        super();
        this.keyClazz = keyClazz;
        this.valueClazz = valueClazz;
        this.fileList = new TreeSet<>();
        this.conf = new Configuration();
        if (baseDir.endsWith("/")) {
            this.baseDir = baseDir.substring(0, baseDir.length()-1);
        } else {
            this.baseDir = baseDir;
        }
        this.tmpDir = this.baseDir + ".tmp";
        this.fs = FileSystem.get(new Path(baseDir).toUri(), conf);
        this.persistent = persistent;
        this.immutable = immutable;
        CacheBuilder builder = CacheBuilder.newBuilder().removalListener(new RemovalListener<K, V>() {
            @Override
            public void onRemoval(RemovalNotification<K, V> notification) {
                logger.info(String.format("Evict cache key %s(%d) with value %s caused by %s, size %d/%d ", notification.getKey(), notification.getKey().hashCode(), notification.getValue(), notification.getCause(), size(), valueCache.size()));
                switch (notification.getCause()) {
                case SIZE:
                    writeValue(notification.getKey(), notification.getValue());
                    break;
                case EXPLICIT:
                    deleteValue(notification.getKey());
                    break;
                default:
                    throw new RuntimeException("unexpected evict reason " + notification.getCause());
                }
            }
        });
        // For immutable values, load all values as much as possible, and evict by soft reference to free memory when gc
        if (this.immutable) {
            builder.softValues();
        } else {
            builder.maximumSize(maxCount);
            // For mutable map, copy all data into tmp and modify on tmp data, avoiding suddenly server crash made data corrupt
            if (fs.exists(new Path(tmpDir))) {
                fs.delete(new Path(tmpDir), true);
            }
            if (fs.exists(new Path(this.baseDir))) {
                FileUtil.copy(fs, new Path(this.baseDir), fs, new Path(tmpDir), false, true, conf);
            } else {
                fs.mkdirs(new Path(this.baseDir));
            }
        }
        this.valueCache = builder.build(new CacheLoader<K, V>() {
            @Override
            public V load(K key) throws Exception {
                V value = readValue(key);
                logger.info(String.format("Load cache by key %s(%d) with value %s", key, key.hashCode(), value));
                return value;
            }
        });
    }

    private String generateFileName(K key) {
        String file = (immutable ? baseDir : tmpDir) + "/cached_" + key.toString();
        return file;
    }

    public String getCurrentDir() {
        return immutable ? baseDir : tmpDir;
    }

    public void commit(boolean stillMutable) throws IOException {
        assert !immutable : "Only support commit method with immutable false";

        Path basePath = new Path(baseDir);
        Path backupPath = new Path(baseDir+".bak");
        Path tmpPath = new Path(tmpDir);
        try {
            fs.rename(basePath, backupPath);
        } catch (IOException e) {
            logger.info("CachedTreeMap commit backup basedir failed, " + e, e);
            throw e;
        }

        try {
            if (stillMutable) {
                FileUtil.copy(fs, tmpPath, fs, basePath, false, true, conf);
            } else {
                fs.rename(tmpPath, basePath);
            }
            fs.delete(backupPath, true);
        } catch (IOException e) {
            fs.rename(backupPath, basePath);
            logger.info("CachedTreeMap commit move/copy tmpdir failed, " + e, e);
            throw e;
        }
    }

    public void loadEntry(CachedTreeMap other) {
        for (Object key : other.keySet()) {
            super.put((K)key, null);
        }
    }

    private void writeValue(K key, V value) {
        if (immutable) {
            return;
        }
        long t0 = System.currentTimeMillis();
        String fileName = generateFileName(key);
        Path filePath = new Path(fileName);
        try (FSDataOutputStream out = fs.create(filePath, true, BUFFER_SIZE, (short) 5, BUFFER_SIZE * 8)) {
            value.write(out);
            if (!persistent) {
                fs.deleteOnExit(filePath);
            }
        } catch (Exception e) {
            logger.error(String.format("write value into %s exception: %s", fileName, e), e);
            throw new RuntimeException(e.getCause());
        } finally {
            fileList.add(fileName);
            writeValueTime += System.currentTimeMillis() - t0;
        }
    }

    private V readValue(K key) throws Exception {
        long t0 = System.currentTimeMillis();
        String fileName = generateFileName(key);
        Path filePath = new Path(fileName);
        try (FSDataInputStream input = fs.open(filePath, BUFFER_SIZE)) {
            V value = valueClazz.newInstance();
            value.readFields(input);
            return value;
        } catch (Exception e) {
            logger.error(String.format("read value from %s exception: %s", fileName, e), e);
            return null;
        } finally {
            readValueTime += System.currentTimeMillis() - t0;
        }
    }

    private void deleteValue(K key) {
        if (persistent && immutable) {
            return;
        }
        String fileName = generateFileName(key);
        Path filePath = new Path(fileName);
        try {
            if (fs.exists(filePath)) {
                fs.delete(filePath, true);
            }
        } catch (Exception e) {
            logger.error(String.format("delete value file %s exception: %s", fileName, e), e);
        } finally {
            fileList.remove(fileName);
        }
    }

    @Override
    public V put(K key, V value) {
        assert !immutable : "Only support put method with immutable false";
        super.put(key, null);
        valueCache.put(key, value);
        return null;
    }

    @Override
    public V get(Object key) {
        if (super.containsKey(key)) {
            try {
                return valueCache.get((K) key);
            } catch (ExecutionException e) {
                logger.error(String.format("get value with key %s exception: ", key, e), e);
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public V remove(Object key) {
        assert !immutable : "Only support remove method with immutable false";
        super.remove(key);
        valueCache.invalidate(key);
        return null;
    }

    @Override
    public void clear() {
        super.clear();
        values = null;
        valueCache.invalidateAll();
    }

    public Collection<V> values() {
        Collection<V> vs = values;
        return (vs != null) ? vs : (values = new Values());
    }

    class Values extends AbstractCollection<V> {
        @Override
        public Iterator<V> iterator() {
            return new ValueIterator<>();
        }

        @Override
        public int size() {
            return CachedTreeMap.this.size();
        }
    }

    class ValueIterator<V> implements Iterator<V> {
        Iterator<K> keyIterator;
        K currentKey;

        public ValueIterator() {
            keyIterator = CachedTreeMap.this.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return keyIterator.hasNext();
        }

        @Override
        public V next() {
            currentKey = keyIterator.next();
            try {
                return (V) valueCache.get(currentKey);
            } catch (ExecutionException e) {
                logger.error(String.format("get value with key %s exception: ", currentKey, e), e);
                return null;
            }
        }

        @Override
        public void remove() {
            assert !immutable : "Only support remove method with immutable false";
            keyIterator.remove();
            valueCache.invalidate(currentKey);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        assert persistent : "Only support serialize with persistent true";
        out.writeInt(size());
        for (K key : keySet()) {
            key.write(out);
            V value = valueCache.getIfPresent(key);
            if (null != value) {
                writeValue(key, value);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        assert persistent : "Only support deserialize with persistent true";
        int size = in.readInt();
        try {
            for (int i = 0; i < size; i++) {
                K key = keyClazz.newInstance();
                key.readFields(in);
                super.put(key, null);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    // clean up all tmp files
    @Override
    public void finalize() throws Throwable {
        if (persistent) {
            return;
        }
        try {
            this.clear();
            for (String file : fileList) {
                try {
                    Path filePath = new Path(file);
                    fs.delete(filePath, true);
                } catch (Throwable t) {
                    //do nothing?
                }
            }
        } catch (Throwable t) {
            //do nothing
        } finally {
            super.finalize();
        }
    }
}
