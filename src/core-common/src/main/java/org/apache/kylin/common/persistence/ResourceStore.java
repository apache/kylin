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

import static org.apache.kylin.common.persistence.MetadataType.ALL;
import static org.apache.kylin.common.persistence.MetadataType.ALL_TYPE_STR;
import static org.apache.kylin.common.persistence.MetadataType.NON_GLOBAL_METADATA_TYPE;
import static org.apache.kylin.common.persistence.MetadataType.mergeKeyWithType;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.AuditLogStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.cache.CacheLoader;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

/**
 * A general purpose resource store to persist small metadata, like JSON files.
 *
 * In additional to raw bytes save and load, the store takes special care for concurrent modifications
 * by using a timestamp based test-and-set mechanism to detect (and refuse) dirty writes.
 */
public abstract class ResourceStore implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ResourceStore.class);
    public static final String GLOBAL_PROJECT = "_global";
    public static final String TABLE_EXD_RESOURCE_ROOT = "/table_exd";
    public static final String METASTORE_IMAGE_META_KEY_TAG = "_image";
    public static final String METASTORE_IMAGE = mergeKeyWithType(METASTORE_IMAGE_META_KEY_TAG, MetadataType.SYSTEM);
    public static final String METASTORE_UUID_META_KEY_TAG = "UUID";
    // if KE ever upgraded from 4.1,
    // there is a record with data-permission-separate in backup core-meta/_global/upgrade dir.
    public static final String UPGRADE_META_KEY_PREFIX = "/_global/upgrade/";
    // for data permission separate
    public static final String UPGRADE_META_KEY_TAG = "acl_version";
    public static final String METASTORE_UUID_TAG = mergeKeyWithType(METASTORE_UUID_META_KEY_TAG, MetadataType.SYSTEM);
    public static final String METASTORE_TRASH_RECORD_KEY = "trash_record";
    public static final String METASTORE_TRASH_RECORD = mergeKeyWithType(METASTORE_TRASH_RECORD_KEY,
            MetadataType.SYSTEM);
    public static final String REC_FILE = "rec";
    public static final String COMPRESSED_FILE = "metadata.zip";

    public static final String VERSION_FILE_META_KEY_TAG = "VERSION";
    public static final String VERSION_FILE = mergeKeyWithType(VERSION_FILE_META_KEY_TAG, MetadataType.SYSTEM);

    private static final String KYLIN_PROPS = "kylin.properties";

    private static final Cache<KylinConfig, ResourceStore> META_CACHE = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getMetadataCacheMaxNum())
            .expireAfterAccess(KylinConfig.getInstanceFromEnv().getMetadataCacheMaxDuration(), TimeUnit.MINUTES)
            .build(new CacheLoader<KylinConfig, ResourceStore>() {
                @Override
                public ResourceStore load(KylinConfig config) {
                    return createResourceStore(config);
                }
            });

    @Getter
    protected MetadataStore metadataStore;
    @Setter
    @Getter
    long offset;
    @Setter
    @Getter
    Callback<Boolean> checker;

    /**
     * Get a resource store for Kylin's metadata.
     */
    public static ResourceStore getKylinMetaStore(KylinConfig config) {
        try {
            return META_CACHE.get(config, () -> createResourceStore(config));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isPotentialMemoryLeak() {
        return META_CACHE.size() > 100;
    }

    public static void clearCache() {
        META_CACHE.invalidateAll();
    }

    public static void clearCache(KylinConfig config) {
        META_CACHE.invalidate(config);
    }

    public static void setRS(KylinConfig config, ResourceStore rs) {
        META_CACHE.put(config, rs);
    }

    /**
     * Create a resource store for general purpose, according specified by given StorageURL.
     */
    private static ResourceStore createResourceStore(KylinConfig config) {
        try (val resourceStore = new InMemResourceStore(config)) {
            val snapshotStore = MetadataStore.createMetadataStore(config);
            resourceStore.init(snapshotStore);
            return resourceStore;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create metadata store", e);
        }
    }

    // ============================================================================

    protected final KylinConfig kylinConfig;

    protected ResourceStore(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public final KylinConfig getConfig() {
        return kylinConfig;
    }

    /**
     * List resources and sub-folders under a given folder.
     * Path must be one of MetadataType, return null if it is not.
     */
    public final NavigableSet<String> listResources(String path) {
        return listResources(path, new RawResourceFilter());
    }

    public final NavigableSet<String> listResources(String path, RawResourceFilter filter) {
        return listResourcesImpl(path, filter, false);
    }

    public final NavigableSet<String> listResourcesRecursively(String type) {
        return listResourcesRecursively(type, new RawResourceFilter());
    }

    public final NavigableSet<String> listResourcesRecursivelyByProject(String project) {
        NavigableSet<String> resources = new TreeSet<>();
        RawResourceFilter filter = RawResourceFilter.equalFilter("project", project);
        NON_GLOBAL_METADATA_TYPE.forEach(type -> resources.addAll(listResourcesRecursively(type.name(), filter)));
        resources.addAll(listResourcesRecursively(MetadataType.PROJECT.name(),
                RawResourceFilter.equalFilter("metaKey", project)));
        return resources;
    }

    public final NavigableSet<String> listResourcesRecursively(String folderPath, RawResourceFilter filter) {
        return listResourcesImpl(folderPath, filter, true);
    }

    protected abstract NavigableSet<String> listResourcesImpl(String folderPath, RawResourceFilter filter,
            boolean recursive);

    protected void init(MetadataStore metadataStore) throws Exception {
        this.metadataStore = metadataStore;
        reload();
    }

    public String getMetaStoreUUID() {
        StringEntity entity = getResource(ResourceStore.METASTORE_UUID_TAG, StringEntity.serializer);
        return String.valueOf(entity);
    }

    /**
     * Return true if a resource exists, return false in case of folder or non-exist
     */
    public final boolean exists(String resPath) {
        return existsImpl(resPath);
    }

    protected abstract boolean existsImpl(String resPath);

    public abstract int batchLock(MetadataType type, RawResourceFilter filter);

    /**
     * Read a resource, return null in case of not found or is a folder.
     */
    public final <T extends RootPersistentEntity> T getResource(String resPath, Serializer<T> serializer) {
        return getResource(resPath, serializer, false);
    }

    public final <T extends RootPersistentEntity> T getResource(String resPath, Serializer<T> serializer,
            boolean needLock) {
        RawResource res = getResourceImpl(resPath, needLock);
        if (res == null)
            return null;

        return getResourceFromRawResource(res, serializer);
    }

    private <T extends RootPersistentEntity> T getResourceFromRawResource(RawResource res, Serializer<T> serializer) {
        try (InputStream is = ByteSource.wrap(res.getContent()).openStream();
                DataInputStream din = new DataInputStream(is)) {
            T r = serializer.deserialize(din);
            r.setLastModified(res.getTs());
            r.setMvcc(res.getMvcc());
            r.setResourceName(res.getMetaKey());
            return r;
        } catch (IOException e) {
            logger.warn("error when deserializing resource: " + res.getMetaKey(), e);
            return null;
        }
    }

    public final RawResource getResource(String resPath) {
        return getResource(resPath, false);
    }

    public final RawResource getResource(String resPath, boolean needLock) {
        return getResourceImpl(resPath, needLock);
    }

    /**
     * Read all resources under a folder. Return empty list if folder not exist.
     */
    public final <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, Serializer<T> serializer) {
        return getAllResources(folderPath, Long.MIN_VALUE, Long.MAX_VALUE, serializer);
    }

    /**
     * Read all resources under a folder having create time between given range. Return empty list if folder not exist.
     */
    public final <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, long timeStart,
            long timeEndExclusive, Serializer<T> serializer) {
        final List<RawResource> allResources = getMatchedResourcesWithoutContent(folderPath, new RawResourceFilter());
        if (allResources == null || allResources.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> result = new ArrayList<>();

        for (RawResource rawResource : allResources) {
            T element = getResourceFromRawResource(rawResource, serializer);
            if (null != element && timeStart <= element.getCreateTime() && element.getCreateTime() < timeEndExclusive) {
                result.add(element);
            }
        }
        return result;
    }

    /**
     * return empty list if given path is not a folder or not exists
     */
    @VisibleForTesting
    public List<RawResource> getMatchedResourcesWithoutContent(String folderPath, RawResourceFilter filter) {
        NavigableSet<String> resources = listResources(folderPath, filter);
        if (resources == null)
            return Collections.emptyList();

        List<RawResource> result = Lists.newArrayListWithCapacity(resources.size());

        for (String res : resources) {
            RawResource nullAbleResource = getResourceImpl(res, false);
            if (nullAbleResource != null) {
                result.add(nullAbleResource);
            }
        }
        return result;
    }

    /**
     * returns null if not exists
     */
    protected abstract RawResource getResourceImpl(String resPath, boolean needLock);

    /**
     * check & set, overwrite a resource
     */
    public final <T extends RootPersistentEntity> void checkAndPutResource(String resPath, T obj,
            Serializer<T> serializer) {

        long oldMvcc = obj.getMvcc();
        obj.setMvcc(oldMvcc + 1);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        try {
            serializer.serialize(obj, dout);
            dout.close();
            buf.close();
        } catch (IOException e) {
            Throwables.propagate(e);
        }

        ByteSource byteSource = ByteSource.wrap(buf.toByteArray());

        val x = checkAndPutResource(resPath, byteSource, oldMvcc);
        obj.setLastModified(x.getTs());
    }

    /**
     * checks old timestamp when overwriting existing
     */
    public abstract RawResource checkAndPutResource(String resPath, ByteSource byteSource, long oldMvcc);

    public abstract RawResource checkAndPutResource(String resPath, ByteSource byteSource, long timeStamp,
            long oldMvcc);

    /**
     * delete a resource, does nothing on a folder
     */
    public final void deleteResource(String resPath) {
        logger.trace("Deleting resource {}", resPath);
        deleteResourceImpl(resPath);
    }

    protected abstract void deleteResourceImpl(String resPath);

    /**
     * get a readable string of a resource path
     */
    public final String getReadableResourcePath(String resPath) {
        return getReadableResourcePathImpl(resPath);
    }

    protected abstract String getReadableResourcePathImpl(String resPath);

    public void putResourceWithoutCheck(String resPath, ByteSource bs, long timeStamp, long newMvcc) {
        putResourceWithoutCheck(resPath, bs, timeStamp, newMvcc, false);
    }

    public void putResourceWithoutCheck(String resPath, ByteSource bs, long timeStamp, long newMvcc, boolean force) {
        throw new NotImplementedException("Only implemented in InMemoryResourceStore");
    }

    public void catchup() {
        val auditLogStore = getAuditLogStore();
        val raw = getResource(METASTORE_IMAGE);
        try {
            long restoreOffset = this.offset;
            if (raw != null) {
                val imageDesc = JsonUtil.readValue(raw.getByteSource().read(), ImageDesc.class);
                restoreOffset = imageDesc.getOffset();
            }
            auditLogStore.restore(restoreOffset);
        } catch (IOException ignore) {
        }
    }

    public abstract void reload() throws IOException;

    public List<String> collectResourceRecursively(MetadataType type, RawResourceFilter filter) {
        if (filter.getConditions().isEmpty()) {
            return new ArrayList<>(listResourcesRecursively(type.name()));
        }
        List<RawResource> matchedResources = getMatchedResourcesWithoutContent(type.name(), filter);
        return matchedResources.stream().map(RawResource::generateKeyWithType).collect(Collectors.toList());
    }

    public void close() {
        clearCache(this.getConfig());
    }

    public static void dumpResourceMaps(File metaDir, Map<String, RawResource> dumpMap) {
        long startTime = System.currentTimeMillis();
        if (!metaDir.exists()) {
            metaDir.mkdirs();
        }
        for (Map.Entry<String, RawResource> entry : dumpMap.entrySet()) {
            RawResource res = entry.getValue();
            if (res == null) {
                throw new IllegalStateException("No resource found at -- " + entry.getKey());
            }
            try {
                File f = Paths.get(metaDir.getAbsolutePath(), res.generateKeyWithType()).toFile();
                f.getParentFile().mkdirs();
                try (FileOutputStream out = new FileOutputStream(f);
                        InputStream input = res.getByteSource().openStream()) {
                    IOUtils.copy(input, out);
                    if (!f.setLastModified(res.getTs())) {
                        logger.info("{} modified time change failed", f);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("dump " + res.generateKeyWithType() + " failed", e);
            }
        }

        logger.debug("Dump resources to {} took {} ms", metaDir, System.currentTimeMillis() - startTime);
    }

    public static void dumpResources(KylinConfig kylinConfig, File metaDir, Set<String> dumpList,
            Properties properties) {
        long startTime = System.currentTimeMillis();

        metaDir.mkdirs();
        ResourceStore from = ResourceStore.getKylinMetaStore(kylinConfig);

        if (dumpList == null) {
            dumpList = from.listResourcesRecursively(ALL.name());
        }

        for (String path : dumpList) {
            RawResource res = from.getResource(path);
            if (res == null)
                throw new IllegalStateException("No resource found at -- " + path);
            try {
                File f = Paths.get(metaDir.getAbsolutePath(), res.generateFilePath()).toFile();
                f.getParentFile().mkdirs();
                try (FileOutputStream out = new FileOutputStream(f);
                        InputStream in = res.getByteSource().openStream()) {
                    IOUtils.copy(in, out);
                    if (!f.setLastModified(res.getTs())) {
                        logger.info("{} modified time change failed", f);
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException("dump " + res.getMetaKey() + " failed", e);
            }
        }

        if (properties != null) {
            File kylinPropsFile = new File(metaDir, KYLIN_PROPS);
            try (FileOutputStream os = new FileOutputStream(kylinPropsFile)) {
                properties.store(os, kylinPropsFile.getAbsolutePath());
            } catch (Exception e) {
                throw new IllegalStateException("save kylin.properties failed", e);
            }

        }

        logger.debug("Dump resources to {} took {} ms", metaDir, System.currentTimeMillis() - startTime);
    }

    public static void dumpResources(KylinConfig kylinConfig, File metaDir, Set<String> dumpList) {
        dumpResources(kylinConfig, metaDir, dumpList, null);
    }

    public static void dumpResources(KylinConfig kylinConfig, String dumpDir) {
        dumpResources(kylinConfig, new File(dumpDir), null, null);
    }

    public static void dumpKylinProps(File metaDir, Properties props) {
        if (Objects.isNull(metaDir)) {
            return;
        }
        if (!metaDir.exists()) {
            metaDir.mkdirs();
        }
        if (Objects.isNull(props)) {
            return;
        }
        File propsFile = new File(metaDir, KYLIN_PROPS);
        try (FileOutputStream os = new FileOutputStream(propsFile)) {
            props.store(os, propsFile.getAbsolutePath());
        } catch (Exception e) {
            throw new IllegalStateException("dump kylin props failed", e);
        }
    }

    public void copy(String resPath, ResourceStore destRS) {
        val resource = getResource(resPath);
        if (resource != null) {
            //res is a file
            destRS.putResourceWithoutCheck(resPath, resource.getByteSource(), resource.getTs(), resource.getMvcc());
        } else {
            if (!ALL_TYPE_STR.contains(resPath)) {
                return;
            }
            NavigableSet<String> resources = listResourcesRecursively(resPath);
            if (resources == null || resources.isEmpty()) {
                return;
            }
            for (val res : resources) {
                val rawResource = getResource(res);
                if (rawResource == null) {
                    logger.warn("The resource {} doesn't exists,there may be transaction problems here", res);
                    continue;
                }
                destRS.putResourceWithoutCheck(res, rawResource.getByteSource(), rawResource.getTs(),
                        rawResource.getMvcc());
            }
        }
    }

    public AuditLogStore getAuditLogStore() {
        return getMetadataStore().getAuditLogStore();
    }

    public void createMetaStoreUuidIfNotExist() {
        if (!exists(METASTORE_UUID_TAG)) {
            checkAndPutResource(METASTORE_UUID_TAG, new StringEntity("UUID", RandomUtil.randomUUIDStr()),
                    StringEntity.serializer);
        }
    }

    public interface Callback<T> {
        /**
         * check metadata
         */
        T check(UnitMessages event);
    }
}
