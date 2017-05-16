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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

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
    public static final byte[] HEAD_MAGIC = new byte[] { 0x41, 0x70, 0x70, 0x65, 0x63, 0x64, 0x54, 0x72, 0x69, 0x65, 0x44, 0x69, 0x63, 0x74 }; // "AppendTrieDict"
    public static final int HEAD_SIZE_I = HEAD_MAGIC.length;
    private static final Logger logger = LoggerFactory.getLogger(AppendTrieDictionary.class);

    transient private String baseDir;
    transient private GlobalDictMetadata metadata;
    transient private LoadingCache<AppendDictSliceKey, AppendDictSlice> dictCache;

    public void init(String baseDir) throws IOException {
        this.baseDir = baseDir;
        final GlobalDictStore globalDictStore = new GlobalDictHDFSStore(baseDir);
        Long[] versions = globalDictStore.listAllVersions();

        if (versions.length == 0) {
            this.metadata = new GlobalDictMetadata(0, 0, 0, 0, null, new TreeMap<AppendDictSliceKey, String>());
            return; // for the removed SegmentAppendTrieDictBuilder
        }

        final long latestVersion = versions[versions.length - 1];
        final Path latestVersionPath = globalDictStore.getVersionDir(latestVersion);
        this.metadata = globalDictStore.getMetadata(latestVersion);
        this.bytesConvert = metadata.bytesConverter;
        this.dictCache = CacheBuilder.newBuilder().softValues().removalListener(new RemovalListener<AppendDictSliceKey, AppendDictSlice>() {
            @Override
            public void onRemoval(RemovalNotification<AppendDictSliceKey, AppendDictSlice> notification) {
                logger.info("Evict slice with key {} and value {} caused by {}, size {}/{}", notification.getKey(), notification.getValue(), notification.getCause(), dictCache.size(), metadata.sliceFileMap.size());
            }
        }).build(new CacheLoader<AppendDictSliceKey, AppendDictSlice>() {
            @Override
            public AppendDictSlice load(AppendDictSliceKey key) throws Exception {
                AppendDictSlice slice = globalDictStore.readSlice(latestVersionPath.toString(), metadata.sliceFileMap.get(key));
                logger.info("Load slice with key {} and value {}", key, slice);
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
            throw new RuntimeException("Failed to load slice with key " + sliceKey, e.getCause());
        }
        return slice.getIdFromValueBytesImpl(value, offset, len, roundingFlag);
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
        out.writeUTF(baseDir);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        init(in.readUTF());
    }

    @Override
    public void dump(PrintStream out) {
        out.println(String.format("Total %d values and %d slices", metadata.nValues, metadata.sliceFileMap.size()));
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
        return String.format("AppendTrieDictionary(%s)", baseDir);
    }

    @Override
    public boolean contains(Dictionary other) {
        return false;
    }
}