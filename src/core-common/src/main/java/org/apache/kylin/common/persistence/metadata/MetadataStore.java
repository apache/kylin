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
package org.apache.kylin.common.persistence.metadata;

import static org.apache.kylin.common.persistence.MetadataType.CASE_INSENSITIVE_METADATA;
import static org.apache.kylin.common.persistence.MetadataType.NEED_CACHED_METADATA;
import static org.apache.kylin.common.persistence.MetadataType.splitKeyWithType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.VersionedRawResource;
import org.apache.kylin.common.persistence.transaction.ITransactionManager;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MetadataStore implements ITransactionManager {

    static final Set<String> IMMUTABLE_PREFIX = Sets.newHashSet("SYSTEM/UUID");
    static final String PATH_DELIMITER = "/";

    public static MetadataStore createMetadataStore(KylinConfig config) {
        StorageURL url = config.getMetadataUrl();
        log.info("Creating metadata store by KylinConfig {} from {}", config, url.toString());
        String clsName = config.getMetadataStoreImpls().get(url.getScheme());
        try {
            Class<? extends MetadataStore> cls = ClassUtil.forName(clsName, MetadataStore.class);
            return cls.getConstructor(KylinConfig.class).newInstance(config);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create metadata store", e);
        }
    }

    @Getter
    @Setter
    AuditLogStore auditLogStore;

    public MetadataStore(KylinConfig kylinConfig) {
        // for reflection
    }

    /**
     * @return the resources under the rootPath
     */
    public abstract NavigableSet<String> listAll();

    public abstract void dump(ResourceStore store) throws IOException, InterruptedException, ExecutionException;

    public void dump(ResourceStore store, Collection<String> resources) throws Exception {
        for (String resPath : resources) {
            val raw = store.getResource(resPath);
            save(raw.getMetaType(), raw);
        }
    }

    /**
     * upload local files to snapshot, will not change resource store synchronized, perhaps should call ResourceStore.clearCache() manually
     * @param folder local directory contains snapshot, ${folder}/events contains event store, ${folder}/kylin.properties etc.
     */
    public void uploadFromFile(File folder) {
        foreachFile(folder, res -> {
            try {
                if (IMMUTABLE_PREFIX.contains(res.getMetaKey())) {
                    return;
                }
                save(res.getMetaType(), res);
            } catch (Exception e) {
                throw new IllegalArgumentException("put resource " + res.getMetaKey() + " failed", e);
            }
        });
    }

    static void foreachFile(File root, Consumer<RawResource> resourceConsumer) {
        if (!root.exists()) {
            return;
        }
        val files = FileUtils.listFiles(root, null, true);
        files.forEach(f -> {
            try (val fis = new FileInputStream(f)) {
                val resPath = f.getPath().replace(root.getPath() + PATH_DELIMITER, "");
                val bs = ByteSource.wrap(IOUtils.toByteArray(fis));
                RawResource raw;
                if (resPath.contains("/")) {
                    Pair<MetadataType, String> meteKeyAndType = splitKeyWithType(resPath);

                    raw = RawResource.constructResource(meteKeyAndType.getFirst(), bs);
                    raw.setMetaKey(meteKeyAndType.getSecond());
                    raw.setTs(f.lastModified());
                } else {
                    //only used when the file is kylin.properties
                    raw = new RawResource(resPath, bs, f.lastModified(), 0);
                }
                resourceConsumer.accept(raw);
            } catch (IOException e) {
                throw new IllegalArgumentException("cannot not read file " + f, e);
            }
        });
    }

    public abstract MemoryMetaData reloadAll() throws IOException;

    public abstract <T extends RawResource> List<T> get(MetadataType type, RawResourceFilter filter, boolean needLock,
                                           boolean needContent);

    public abstract int save(MetadataType type, final RawResource raw);

    public static class MemoryMetaData {
        @Getter
        private final Map<MetadataType, Map<String, VersionedRawResource>> data;

        @Getter
        @Setter
        private Long offset;

        private MemoryMetaData(Map<MetadataType, Map<String, VersionedRawResource>> data) {
            this.data = data;
        }

        public static MemoryMetaData createEmpty() {
            Map<MetadataType, Map<String, VersionedRawResource>> data = new ConcurrentHashMap<>();
            NEED_CACHED_METADATA.forEach(type -> data.put(type,
                    CASE_INSENSITIVE_METADATA.contains(type)
                            ? new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER)
                            : new ConcurrentHashMap<>()));
            return new MemoryMetaData(data);
        }

        public boolean containOffset() {
            return offset != null;
        }

        public void put(MetadataType type, VersionedRawResource versionedRawResource) {
            if (NEED_CACHED_METADATA.contains(type)) {
                data.get(type).put(versionedRawResource.getRawResource().getMetaKey(), versionedRawResource);
            }
        }
    }
}
