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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.global.AppendDictSlice;
import org.apache.kylin.dict.global.AppendDictSliceKey;
import org.apache.kylin.dict.global.GlobalDictHDFSStore;
import org.apache.kylin.dict.global.GlobalDictMetadata;
import org.apache.kylin.dict.global.GlobalDictStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * A dictionary based on Trie data structure that maps enumerations of byte[] to
 * int IDs, used for global dictionary.
 * <p>
 * Trie data is split into sub trees, called {@link AppendDictSlice}.
 * <p>
 * With Trie the memory footprint of the mapping is kinda minimized at the cost
 * CPU, if compared to HashMap of ID Arrays. Performance test shows Trie is
 * roughly 10 times slower, so there's a cache layer overlays on top of Trie and
 * gracefully fall back to Trie using a weak reference.
 * <p>
 * The implementation is NOT thread-safe for now.
 * <p>
 * TODO making it thread-safe
 *
 * @author sunyerui
 */
@SuppressWarnings({ "rawtypes", "unchecked", "serial" })
public class AppendTrieDictionary<T> extends CacheDictionary<T> {
    public static final byte[] HEAD_MAGIC = new byte[] { 0x41, 0x70, 0x70, 0x65, 0x63, 0x64, 0x54, 0x72, 0x69, 0x65,
            0x44, 0x69, 0x63, 0x74 }; // "AppendTrieDict"
    public static final int HEAD_SIZE_I = HEAD_MAGIC.length;
    private static final Logger logger = LoggerFactory.getLogger(AppendTrieDictionary.class);

    transient private Boolean isSaveAbsolutePath = false;
    transient private String baseDir;
    transient private GlobalDictMetadata metadata;
    transient private LoadingCache<AppendDictSliceKey, AppendDictSlice> dictCache;

    private int evictionThreshold = 0;

    public void init(String baseDir) throws IOException {
        this.baseDir = convertToAbsolutePath(baseDir);
        final GlobalDictStore globalDictStore = new GlobalDictHDFSStore(this.baseDir);
        Long[] versions = globalDictStore.listAllVersions();

        if (versions.length == 0) {
            this.metadata = new GlobalDictMetadata(0, 0, 0, 0, null, new TreeMap<AppendDictSliceKey, String>());
            return; // for the removed SegmentAppendTrieDictBuilder
        }

        final long latestVersion = versions[versions.length - 1];
        final Path latestVersionPath = globalDictStore.getVersionDir(latestVersion);
        this.metadata = globalDictStore.getMetadata(latestVersion);
        this.bytesConvert = metadata.bytesConverter;

        // see: https://github.com/google/guava/wiki/CachesExplained
        this.evictionThreshold = KylinConfig.getInstanceFromEnv().getDictionarySliceEvicationThreshold();
        int cacheMaximumSize = KylinConfig.getInstanceFromEnv().getCachedDictMaxSize();
        CacheBuilder cacheBuilder = CacheBuilder.newBuilder().softValues();

        // To be compatible with Guava 11
        boolean methodExists = methodExistsInClass(CacheBuilder.class, "recordStats");
        if (methodExists) {
            cacheBuilder = cacheBuilder.recordStats();
        }
        if (cacheMaximumSize > 0) {
            cacheBuilder = cacheBuilder.maximumSize(cacheMaximumSize);
            logger.info("Set dict cache maximum size to " + cacheMaximumSize);
        }
        this.dictCache = cacheBuilder
                .removalListener(new RemovalListener<AppendDictSliceKey, AppendDictSlice>() {
                    @Override
                    public void onRemoval(RemovalNotification<AppendDictSliceKey, AppendDictSlice> notification) {
                        logger.info("Evict slice with key {} and value {} caused by {}, size {}/{}",
                                notification.getKey(), notification.getValue(), notification.getCause(),
                                dictCache.size(), metadata.sliceFileMap.size());
                    }
                }).build(new CacheLoader<AppendDictSliceKey, AppendDictSlice>() {
                    @Override
                    public AppendDictSlice load(AppendDictSliceKey key) throws Exception {
                        AppendDictSlice slice = globalDictStore.readSlice(latestVersionPath.toString(),
                                metadata.sliceFileMap.get(key));
                        logger.trace("Load slice with key {} and value {}", key, slice);
                        return slice;
                    }
                });
    }

    @Override
    public int getIdFromValueBytesWithoutCache(byte[] value, int offset, int len, int roundingFlag) {
        byte[] val = Arrays.copyOfRange(value, offset, offset + len);
        AppendDictSliceKey sliceKey = metadata.sliceFileMap.floorKey(AppendDictSliceKey.wrap(val));
        if (sliceKey == null) {
            sliceKey = metadata.sliceFileMap.firstKey();
        }
        AppendDictSlice slice;
        try {
            slice = dictCache.get(sliceKey);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to load slice with key " + sliceKey, e.getCause());
        }
        CacheStats stats = dictCache.stats();
        if (evictionThreshold > 0 && stats.evictionCount() > evictionThreshold * metadata.sliceFileMap.size()
                && stats.loadCount() > (evictionThreshold + 1) * metadata.sliceFileMap.size()) {
            logger.warn(
                    "Too many dict slice evictions and reloads, maybe the memory is not enough to hold all the dictionary");
            throw new RuntimeException("Too many dict slice evictions: " + stats + " for "
                    + metadata.sliceFileMap.size() + " dict slices. "
                    + "Maybe the memory is not enough to hold all the dictionary, try to enlarge the mapreduce/spark executor memory.");
        }
        return slice.getIdFromValueBytesImpl(value, offset, len, roundingFlag);
    }

    public CacheStats getCacheStats() {
        return dictCache.stats();
    }

    public GlobalDictMetadata getDictMetadata() {
        return metadata;
    }

    @Override
    public int getMinId() {
        return metadata.baseId;
    }

    @Override
    public int getMaxId() {
        return metadata.maxId;
    }

    @Override
    public int getSizeOfId() {
        return Integer.SIZE / Byte.SIZE;
    }

    @Override
    public int getSizeOfValue() {
        return metadata.maxValueLength;
    }

    @Override
    protected byte[] getValueBytesFromIdWithoutCache(int id) {
        throw new UnsupportedOperationException("AppendTrieDictionary can't retrieve value from id");
    }

    @Override
    public AppendTrieDictionary copyToAnotherMeta(KylinConfig srcConfig, KylinConfig dstConfig) throws IOException {
        GlobalDictStore store = new GlobalDictHDFSStore(baseDir);
        String dstBaseDir = store.copyToAnotherMeta(srcConfig, dstConfig);
        AppendTrieDictionary newDict = new AppendTrieDictionary();
        newDict.init(dstBaseDir);
        return newDict;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(convertToRelativePath(baseDir));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        init(in.readUTF());
    }

    @Override
    public void dump(PrintStream out) {
        out.println(String.format(Locale.ROOT, "Total %d values and %d slices", metadata.nValues,
                metadata.sliceFileMap.size()));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(baseDir);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof AppendTrieDictionary) {
            AppendTrieDictionary that = (AppendTrieDictionary) o;
            return Objects.equals(this.baseDir, that.baseDir);
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "AppendTrieDictionary(%s)", baseDir);
    }

    @Override
    public boolean contains(Dictionary other) {
        return false;
    }

    /**
     * JIRA: https://issues.apache.org/jira/browse/KYLIN-2945
     * if pass a absolute path, it may produce some problems like cannot find global dict after migration.
     * so convert to relative path can avoid it and be better to maintain flexibility.
     *
     */
    private String convertToRelativePath(String path) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String hdfsWorkingDir = kylinConfig.getHdfsWorkingDirectory();
        if (!isSaveAbsolutePath && path.startsWith(hdfsWorkingDir)) {
            return path.substring(hdfsWorkingDir.length());
        }
        return path;
    }

    private String convertToAbsolutePath(String path) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Path basicPath = new Path(path);
        if (basicPath.toUri().getScheme() == null)
            return kylinConfig.getHdfsWorkingDirectory() + path;

        String[] paths = path.split("/resources/GlobalDict/");
        if (paths.length == 2)
            return kylinConfig.getHdfsWorkingDirectory() + "/resources/GlobalDict/" + paths[1];

        paths = path.split("/resources/SegmentDict/");
        if (paths.length == 2) {
            return kylinConfig.getHdfsWorkingDirectory() + "/resources/SegmentDict/" + paths[1];
        } else {
            throw new RuntimeException(
                    "the basic directory of global dictionary only support the format which contains '/resources/GlobalDict/' or '/resources/SegmentDict/'");
        }
    }

    private boolean methodExistsInClass(Class clazz, String method) {
        boolean existence = false;
        try {
            clazz.getMethod(method);
            existence = true;
        } catch (NoSuchMethodException e) {
            logger.info("Class " + clazz.getName() + " doesn't have method " + method);
        }
        return existence;
    }

    /**
     * only for test
     *
     * @param flag
     */
    void setSaveAbsolutePath(Boolean flag) {
        this.isSaveAbsolutePath = flag;
    }
}