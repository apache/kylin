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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.kylin.common.util.HadoopUtil;
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
    private final Configuration conf;
    private final Path baseDir;
    private final Path versionDir;
    private final Path workingDir;
    private final FileSystem fs;
    private final boolean immutable;
    private final int maxVersions;
    private final long versionTTL;
    private boolean keepAppend;

    public static final int BUFFER_SIZE = 8 * 1024 * 1024;

    public static final String CACHED_PREFIX = "cached_";
    public static final String VERSION_PREFIX = "version_";

    public static class CachedTreeMapBuilder<K, V> {
        private Class<K> keyClazz;
        private Class<V> valueClazz;
        private int maxCount = 8;
        private String baseDir;
        private boolean immutable;
        private int maxVersions;
        private long versionTTL;

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

        public CachedTreeMapBuilder<K, V> immutable(boolean immutable) {
            this.immutable = immutable;
            return this;
        }

        public CachedTreeMapBuilder<K, V> maxVersions(int maxVersions) {
            this.maxVersions = maxVersions;
            return this;
        }

        public CachedTreeMapBuilder<K, V> versionTTL(long versionTTL) {
            this.versionTTL = versionTTL;
            return this;
        }

        public CachedTreeMap build() throws IOException {
            if (baseDir == null) {
                throw new RuntimeException("CachedTreeMap need a baseDir to cache data");
            }
            if (keyClazz == null || valueClazz == null) {
                throw new RuntimeException("CachedTreeMap need key and value clazz to serialize data");
            }
            CachedTreeMap map = new CachedTreeMap(maxCount, keyClazz, valueClazz, baseDir, immutable, maxVersions, versionTTL);
            return map;
        }
    }

    private CachedTreeMap(int maxCount, Class<K> keyClazz, Class<V> valueClazz, String basePath,
                          boolean immutable, int maxVersions, long versionTTL) throws IOException {
        super();
        this.keyClazz = keyClazz;
        this.valueClazz = valueClazz;
        this.immutable = immutable;
        this.keepAppend = true;
        this.maxVersions = maxVersions;
        this.versionTTL = versionTTL;
        this.conf = HadoopUtil.getCurrentConfiguration();
        if (basePath.endsWith("/")) {
            basePath = basePath.substring(0, basePath.length()-1);
        }
        this.baseDir = new Path(basePath);
        this.fs = HadoopUtil.getFileSystem(baseDir, conf);
        if (!fs.exists(baseDir)) {
            fs.mkdirs(baseDir);
        }
        this.versionDir = getLatestVersion(conf, fs, baseDir);
        this.workingDir = new Path(baseDir, "working");
        if (!this.immutable) {
            // For mutable map, copy all data into working dir and work on it, avoiding suddenly server crash made data corrupt
            if (fs.exists(workingDir)) {
                fs.delete(workingDir, true);
            }
            FileUtil.copy(fs, versionDir, fs, workingDir, false, true, conf);
        }
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
                }
            }
        });
        if (this.immutable) {
            // For immutable values, load all values as much as possible, and evict by soft reference to free memory when gc
            builder.softValues();
        } else {
            builder.maximumSize(maxCount);
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
        String file = getCurrentDir() + "/" + CACHED_PREFIX + key.toString();
        return file;
    }

    private String getCurrentDir() {
        return immutable ? versionDir.toString() : workingDir.toString();
    }

    private static String[] listAllVersions(FileSystem fs, Path baseDir) throws IOException {
        FileStatus[] fileStatus = fs.listStatus(baseDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                if (path.getName().startsWith(VERSION_PREFIX)) {
                    return true;
                }
                return false;
            }
        });
        TreeSet<String> versions = new TreeSet<>();
        for (FileStatus status : fileStatus) {
            versions.add(status.getPath().toString());
        }
        return versions.toArray(new String[versions.size()]);
    }

    // only for test
    public String getLatestVersion() throws IOException {
        return getLatestVersion(conf, fs, baseDir).toUri().getPath();
    }

    public static Path getLatestVersion(Configuration conf, FileSystem fs, Path baseDir) throws IOException {
        String[] versions = listAllVersions(fs, baseDir);
        if (versions.length > 0) {
            return new Path(versions[versions.length - 1]);
        } else {
            // Old format, directly use base dir, convert to new format
            Path newVersionDir = new Path(baseDir, VERSION_PREFIX + System.currentTimeMillis());
            Path tmpNewVersionDir = new Path(baseDir, "tmp_" + VERSION_PREFIX + System.currentTimeMillis());
            Path indexFile = new Path(baseDir, ".index");
            FileStatus[] cachedFiles;
            try {
                cachedFiles = fs.listStatus(baseDir, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        if (path.getName().startsWith(CACHED_PREFIX)) {
                            return true;
                        }
                        return false;
                    }
                });
                fs.mkdirs(tmpNewVersionDir);
                if (fs.exists(indexFile) && cachedFiles.length > 0) {
                    FileUtil.copy(fs, indexFile, fs, tmpNewVersionDir, false, true, conf);
                    for (FileStatus file : cachedFiles) {
                        FileUtil.copy(fs, file.getPath(), fs, tmpNewVersionDir, false, true, conf);
                    }
                }
                fs.rename(tmpNewVersionDir, newVersionDir);
                if (fs.exists(indexFile) && cachedFiles.length > 0) {
                    fs.delete(indexFile, true);
                    for (FileStatus file : cachedFiles) {
                        fs.delete(file.getPath(), true);
                    }
                }
            } finally {
                if (fs.exists(tmpNewVersionDir)) {
                    fs.delete(tmpNewVersionDir, true);
                }
            }
            return newVersionDir;
        }
    }

    public void commit(boolean keepAppend) throws IOException {
        assert this.keepAppend && !immutable : "Only support commit method with immutable false and keepAppend true";

        Path newVersionDir = new Path(baseDir, VERSION_PREFIX + System.currentTimeMillis());
        if (keepAppend) {
            // Copy to tmp dir, and rename to new version, make sure it's complete when be visible
            Path tmpNewVersionDir = new Path(baseDir, "tmp_" + VERSION_PREFIX + System.currentTimeMillis());
            try {
                FileUtil.copy(fs, workingDir, fs, tmpNewVersionDir, false, true, conf);
                fs.rename(tmpNewVersionDir, newVersionDir);
            } finally {
                if (fs.exists(tmpNewVersionDir)) {
                    fs.delete(tmpNewVersionDir, true);
                }
            }
        } else {
            fs.rename(workingDir, newVersionDir);
        }
        this.keepAppend = keepAppend;

        // Check versions count, delete expired versions
        String[] versions = listAllVersions(fs, baseDir);
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < versions.length - maxVersions; i++) {
            String versionString = versions[i].substring(versions[i].lastIndexOf(VERSION_PREFIX) + VERSION_PREFIX.length());
            long version = Long.parseLong(versionString);
            if (version + versionTTL < timestamp) {
                fs.delete(new Path(versions[i]), true);
            }
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
        String fileName = generateFileName(key);
        Path filePath = new Path(fileName);
        try (FSDataOutputStream out = fs.create(filePath, true, BUFFER_SIZE, (short) 5, BUFFER_SIZE * 8L)) {
            value.write(out);
        } catch (Exception e) {
            logger.error(String.format("write value into %s exception: %s", fileName, e), e);
            throw new RuntimeException(e.getCause());
        }
    }

    private V readValue(K key) throws Exception {
        String fileName = generateFileName(key);
        Path filePath = new Path(fileName);
        try (FSDataInputStream input = fs.open(filePath, BUFFER_SIZE)) {
            V value = valueClazz.newInstance();
            value.readFields(input);
            return value;
        } catch (Exception e) {
            logger.error(String.format("read value from %s exception: %s", fileName, e), e);
            return null;
        }
    }

    private void deleteValue(K key) {
        if (immutable) {
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
        }
    }

    @Override
    public V put(K key, V value) {
        assert keepAppend && !immutable : "Only support put method with immutable false and keepAppend true";
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
                logger.error(String.format("get value with key %s exception: %s", key, e), e);
                return null;
            }
        } else {
            return null;
        }
    }

    @Override
    public V remove(Object key) {
        assert keepAppend && !immutable : "Only support remove method with immutable false keepAppend true";
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
                logger.error(String.format("get value with key %s exception: %s", currentKey, e), e);
                return null;
            }
        }

        @Override
        public void remove() {
            assert keepAppend && !immutable : "Only support remove method with immutable false and keepAppend true";
            keyIterator.remove();
            valueCache.invalidate(currentKey);
        }
    }

    public FSDataOutputStream openIndexOutput() throws IOException {
        assert keepAppend && !immutable : "Only support write method with immutable false and keepAppend true";
        Path indexPath = new Path(getCurrentDir(), ".index");
        return fs.create(indexPath, true, 8 * 1024 * 1024, (short) 5, 8 * 1024 * 1024 * 8);
    }

    public FSDataInputStream openIndexInput() throws IOException {
        Path indexPath = new Path(getCurrentDir(), ".index");
        return fs.open(indexPath, 8 * 1024 * 1024);
    }

    public static FSDataInputStream openLatestIndexInput(Configuration conf, String baseDir) throws IOException {
        Path basePath = new Path(baseDir);
        FileSystem fs = HadoopUtil.getFileSystem(basePath, conf);
        Path indexPath = new Path(getLatestVersion(conf, fs, basePath), ".index");
        return fs.open(indexPath, 8 * 1024 * 1024);
    }

    @Override
    public void write(DataOutput out) throws IOException {
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
}
