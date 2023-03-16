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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.VersionedRawResource;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MetadataStore {

    static final Set<String> IMMUTABLE_PREFIX = Sets.newHashSet("/UUID");

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
        auditLogStore = new NoopAuditLogStore();
    }

    @Getter
    @Setter
    EpochStore epochStore;

    protected abstract void save(String path, ByteSource bs, long ts, long mvcc, String unitPath, long epochId)
            throws Exception;

    public abstract void move(String srcPath, String destPath) throws Exception;

    public abstract NavigableSet<String> list(String rootPath);

    public abstract RawResource load(String path) throws IOException;

    public void batchUpdate(UnitMessages unitMessages, boolean skipAuditLog, String unitPath, long epochId)
            throws Exception {
        for (Event event : unitMessages.getMessages()) {
            if (event instanceof ResourceCreateOrUpdateEvent) {
                val rawResource = ((ResourceCreateOrUpdateEvent) event).getCreatedOrUpdated();
                putResource(rawResource, unitPath, epochId);
            } else if (event instanceof ResourceDeleteEvent) {
                deleteResource(((ResourceDeleteEvent) event).getResPath(), unitPath, epochId);
            }
        }
        if (!skipAuditLog) {
            auditLogStore.save(unitMessages);
        }
        UnitOfWork.get().onUnitUpdated();
    }

    public void putResource(RawResource res, String unitPath, long epochId) throws Exception {
        save(res.getResPath(), res.getByteSource(), res.getTimestamp(), res.getMvcc(), unitPath, epochId);
    }

    public void deleteResource(String resPath, String unitName, long epochId) throws Exception {
        save(resPath, null, 0, 0, unitName, epochId);
    }

    public void dump(ResourceStore store) throws Exception {
        dump(store, "/");
    }

    public void dump(ResourceStore store, String rootPath) throws Exception {
        val resources = store.listResourcesRecursively(rootPath);
        if (resources == null || resources.isEmpty()) {
            log.info("there is no resources in rootPath ({}),please check the rootPath.", rootPath);
            return;
        }
        for (String resPath : resources) {
            val raw = store.getResource(resPath);
            putResource(raw, null, UnitOfWork.DEFAULT_EPOCH_ID);
        }
    }

    public void dump(ResourceStore store, Collection<String> resources) throws Exception {
        for (String resPath : resources) {
            val raw = store.getResource(resPath);
            putResource(raw, null, UnitOfWork.DEFAULT_EPOCH_ID);
        }
    }

    /**
     * upload local files to snapshot, will not change resource store synchronized, perhaps should call ResourceStore.clearCache() manually
     * @param folder local directory contains snapshot, ${folder}/metadata contains resource store, ${folder}/events contains event store, ${folder}/kylin.properties etc.
     */
    public void uploadFromFile(File folder) {
        foreachFile(folder, res -> {
            try {
                if (IMMUTABLE_PREFIX.contains(res.getResPath())) {
                    return;
                }
                save(res.getResPath(), res.getByteSource(), res.getTimestamp(), res.getMvcc(), null,
                        UnitOfWork.DEFAULT_EPOCH_ID);
            } catch (Exception e) {
                throw new IllegalArgumentException("put resource " + res.getResPath() + " failed", e);
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
                val resPath = f.getPath().replace(root.getPath(), "");
                val bs = ByteSource.wrap(IOUtils.toByteArray(fis));
                val raw = new RawResource(resPath, bs, f.lastModified(), 0);
                resourceConsumer.accept(raw);
            } catch (IOException e) {
                throw new IllegalArgumentException("cannot not read file " + f, e);
            }
        });
    }

    public MemoryMetaData reloadAll() throws IOException {
        MemoryMetaData data = MemoryMetaData.createEmpty();
        val all = list("/");
        for (String resPath : all) {
            val raw = load(resPath);
            data.put(resPath, new VersionedRawResource(raw));
        }
        return data;
    }

    public static class MemoryMetaData {
        @Getter
        private ConcurrentSkipListMap<String, VersionedRawResource> data;

        @Getter
        @Setter
        private Long offset;

        private MemoryMetaData(ConcurrentSkipListMap<String, VersionedRawResource> data) {
            this.data = data;
        }

        public static MemoryMetaData createEmpty() {
            ConcurrentSkipListMap<String, VersionedRawResource> data = KylinConfig.getInstanceFromEnv()
                    .isMetadataKeyCaseInSensitiveEnabled() ? new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER)
                            : new ConcurrentSkipListMap<>();
            return new MemoryMetaData(data);
        }

        public boolean containOffset() {
            return offset != null;
        }

        public void put(String resPath, VersionedRawResource versionedRawResource) {
            data.put(resPath, versionedRawResource);
        }
    }
}
