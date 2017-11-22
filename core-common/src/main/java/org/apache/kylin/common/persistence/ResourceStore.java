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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * A general purpose resource store to persist small metadata, like JSON files.
 * 
 * In additional to raw bytes save and load, the store takes special care for concurrent modifications
 * by using a timestamp based test-and-set mechanism to detect (and refuse) dirty writes.
 */
abstract public class ResourceStore {

    private static final Logger logger = LoggerFactory.getLogger(ResourceStore.class);

    public static final String CUBE_RESOURCE_ROOT = "/cube";
    public static final String CUBE_DESC_RESOURCE_ROOT = "/cube_desc";
    public static final String DATA_MODEL_DESC_RESOURCE_ROOT = "/model_desc";
    public static final String DICT_RESOURCE_ROOT = "/dict";
    public static final String PROJECT_RESOURCE_ROOT = "/project";
    public static final String SNAPSHOT_RESOURCE_ROOT = "/table_snapshot";
    public static final String TABLE_EXD_RESOURCE_ROOT = "/table_exd";
    public static final String TABLE_RESOURCE_ROOT = "/table";
    public static final String EXTERNAL_FILTER_RESOURCE_ROOT = "/ext_filter";
    public static final String HYBRID_RESOURCE_ROOT = "/hybrid";
    public static final String EXECUTE_RESOURCE_ROOT = "/execute";
    public static final String EXECUTE_OUTPUT_RESOURCE_ROOT = "/execute_output";
    public static final String STREAMING_RESOURCE_ROOT = "/streaming";
    public static final String KAFKA_RESOURCE_ROOT = "/kafka";
    public static final String STREAMING_OUTPUT_RESOURCE_ROOT = "/streaming_output";
    public static final String CUBE_STATISTICS_ROOT = "/cube_statistics";
    public static final String BAD_QUERY_RESOURCE_ROOT = "/bad_query";
    public static final String DRAFT_RESOURCE_ROOT = "/draft";

    public static final String METASTORE_UUID_TAG = "/UUID";

    private static final ConcurrentMap<KylinConfig, ResourceStore> CACHE = new ConcurrentHashMap<>();

    private static ResourceStore createResourceStore(KylinConfig kylinConfig) {
        StorageURL metadataUrl = kylinConfig.getMetadataUrl();
        logger.info("Using metadata url " + metadataUrl + " for resource store");
        String clsName = kylinConfig.getResourceStoreImpls().get(metadataUrl.getScheme());
        try {
            Class<? extends ResourceStore> cls = ClassUtil.forName(clsName, ResourceStore.class);
            ResourceStore store = cls.getConstructor(KylinConfig.class).newInstance(kylinConfig);
            if (!store.exists(METASTORE_UUID_TAG)) {
                store.putResource(METASTORE_UUID_TAG, new StringEntity(store.createMetaStoreUUID()), 0, StringEntity.serializer);
            }
            return store;
        } catch (Throwable e) {
            throw new IllegalArgumentException("Failed to find metadata store by url: " + metadataUrl, e);
        }
    }

    public static ResourceStore getStore(KylinConfig kylinConfig) {
        if (CACHE.containsKey(kylinConfig)) {
            return CACHE.get(kylinConfig);
        }
        synchronized (ResourceStore.class) {
            if (CACHE.containsKey(kylinConfig)) {
                return CACHE.get(kylinConfig);
            } else {
                CACHE.putIfAbsent(kylinConfig, createResourceStore(kylinConfig));
            }
        }
        return CACHE.get(kylinConfig);
    }

    // ============================================================================

    final protected KylinConfig kylinConfig;

    protected ResourceStore(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    /**
     * List resources and sub-folders under a given folder, return null if given path is not a folder
     */
    final public NavigableSet<String> listResources(String folderPath) throws IOException {
        String path = norm(folderPath);
        return listResourcesImpl(path);
    }

    /**
     * return null if given path is not a folder or not exists
     */
    abstract protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException;

    protected String createMetaStoreUUID() throws IOException {
        return UUID.randomUUID().toString();
    }

    public String getMetaStoreUUID() throws IOException {
        if (!exists(ResourceStore.METASTORE_UUID_TAG)) {
            putResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(createMetaStoreUUID()), 0, StringEntity.serializer);
        }
        StringEntity entity = getResource(ResourceStore.METASTORE_UUID_TAG, StringEntity.class, StringEntity.serializer);
        return entity.toString();
    }

    /**
     * Return true if a resource exists, return false in case of folder or non-exist
     */
    final public boolean exists(String resPath) throws IOException {
        return existsImpl(norm(resPath));
    }

    abstract protected boolean existsImpl(String resPath) throws IOException;

    /**
     * Read a resource, return null in case of not found or is a folder.
     */
    final public <T extends RootPersistentEntity> T getResource(String resPath, Class<T> clz, Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        RawResource res = getResourceImpl(resPath);
        if (res == null)
            return null;

        DataInputStream din = new DataInputStream(res.inputStream);
        try {
            T r = serializer.deserialize(din);
            r.setLastModified(res.timestamp);
            return r;
        } finally {
            IOUtils.closeQuietly(din);
            IOUtils.closeQuietly(res.inputStream);
        }
    }

    final public RawResource getResource(String resPath) throws IOException {
        return getResourceImpl(norm(resPath));
    }

    final public long getResourceTimestamp(String resPath) throws IOException {
        return getResourceTimestampImpl(norm(resPath));
    }

    /**
     * Read all resources under a folder. Return empty list if folder not exist.
     */
    final public <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, Class<T> clazz, Serializer<T> serializer) throws IOException {
        return getAllResources(folderPath, Long.MIN_VALUE, Long.MAX_VALUE, clazz, serializer);
    }

    /**
     * Read all resources under a folder having last modified time between given range. Return empty list if folder not exist.
     */
    final public <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, long timeStart, long timeEndExclusive, Class<T> clazz, Serializer<T> serializer) throws IOException {
        final List<RawResource> allResources = getAllResourcesImpl(folderPath, timeStart, timeEndExclusive);
        if (allResources == null || allResources.isEmpty()) {
            return Collections.emptyList();
        }
        List<T> result = Lists.newArrayListWithCapacity(allResources.size());
        try {
            for (RawResource rawResource : allResources) {
                final T element = serializer.deserialize(new DataInputStream(rawResource.inputStream));
                element.setLastModified(rawResource.timestamp);
                result.add(element);
            }
            return result;
        } finally {
            for (RawResource rawResource : allResources) {
                if (rawResource != null)
                    IOUtils.closeQuietly(rawResource.inputStream);
            }
        }
    }

    /**
     * return empty list if given path is not a folder or not exists
     */
    abstract protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException;

    /**
     * returns null if not exists
     */
    abstract protected RawResource getResourceImpl(String resPath) throws IOException;

    /**
     * returns 0 if not exists
     */
    abstract protected long getResourceTimestampImpl(String resPath) throws IOException;

    /**
     * overwrite a resource without write conflict check
     */
    final public <T extends RootPersistentEntity> void putResourceWithoutCheck(String resPath, T obj, long ts,
            Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        logger.trace("Directly saving resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        serializer.serialize(obj, dout);
        dout.close();
        buf.close();
        ByteArrayInputStream is = new ByteArrayInputStream(buf.toByteArray());
        putResourceCheckpoint(resPath, is, ts);
        is.close();
    }

    /**
     * overwrite a resource without write conflict check
     */
    final public void putResource(String resPath, InputStream content, long ts) throws IOException {
        resPath = norm(resPath);
        logger.trace("Directly saving resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");
        putResourceCheckpoint(resPath, content, ts);
    }

    private void putResourceCheckpoint(String resPath, InputStream content, long ts) throws IOException {
        beforeChange(resPath);
        putResourceImpl(resPath, content, ts);
    }

    abstract protected void putResourceImpl(String resPath, InputStream content, long ts) throws IOException;

    /**
     * check & set, overwrite a resource
     */
    final public <T extends RootPersistentEntity> long putResource(String resPath, T obj, Serializer<T> serializer) throws IOException {
        return putResource(resPath, obj, System.currentTimeMillis(), serializer);
    }

    /**
     * check & set, overwrite a resource
     */
    final public <T extends RootPersistentEntity> long putResource(String resPath, T obj, long newTS, Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        //logger.debug("Saving resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");

        long oldTS = obj.getLastModified();
        obj.setLastModified(newTS);

        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(buf);
            serializer.serialize(obj, dout);
            dout.close();
            buf.close();

            newTS = checkAndPutResourceCheckpoint(resPath, buf.toByteArray(), oldTS, newTS);
            obj.setLastModified(newTS); // update again the confirmed TS
            return newTS;
        } catch (IOException e) {
            obj.setLastModified(oldTS); // roll back TS when write fail
            throw e;
        } catch (RuntimeException e) {
            obj.setLastModified(oldTS); // roll back TS when write fail
            throw e;
        }
    }

    private long checkAndPutResourceCheckpoint(String resPath, byte[] content, long oldTS, long newTS) throws IOException {
        beforeChange(resPath);
        return checkAndPutResourceImpl(resPath, content, oldTS, newTS);
    }

    /**
     * checks old timestamp when overwriting existing
     */
    abstract protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException;

    /**
     * delete a resource, does nothing on a folder
     */
    final public void deleteResource(String resPath) throws IOException {
        logger.trace("Deleting resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");
        deleteResourceCheckpoint(norm(resPath));
    }

    private void deleteResourceCheckpoint(String resPath) throws IOException {
        beforeChange(resPath);
        deleteResourceImpl(resPath);
    }

    abstract protected void deleteResourceImpl(String resPath) throws IOException;

    /**
     * get a readable string of a resource path
     */
    final public String getReadableResourcePath(String resPath) {
        return getReadableResourcePathImpl(norm(resPath));
    }

    abstract protected String getReadableResourcePathImpl(String resPath);

    private String norm(String resPath) {
        resPath = resPath.trim();
        while (resPath.startsWith("//"))
            resPath = resPath.substring(1);
        while (resPath.endsWith("/"))
            resPath = resPath.substring(0, resPath.length() - 1);
        if (resPath.startsWith("/") == false)
            resPath = "/" + resPath;
        return resPath;
    }

    // ============================================================================

    ThreadLocal<Checkpoint> checkpointing = new ThreadLocal<>();

    public Checkpoint checkpoint() {
        Checkpoint cp = checkpointing.get();
        if (cp != null)
            throw new IllegalStateException("A checkpoint has been open for this thread: " + cp);

        cp = new Checkpoint();
        checkpointing.set(cp);
        return cp;
    }

    private void beforeChange(String resPath) throws IOException {
        Checkpoint cp = checkpointing.get();
        if (cp != null)
            cp.beforeChange(resPath);
    }

    public class Checkpoint implements Closeable {

        LinkedHashMap<String, byte[]> origResData = new LinkedHashMap<>();
        LinkedHashMap<String, Long> origResTimestamp = new LinkedHashMap<>();

        private void beforeChange(String resPath) throws IOException {
            if (origResData.containsKey(resPath))
                return;

            RawResource raw = getResourceImpl(resPath);
            if (raw == null) {
                origResData.put(resPath, null);
                origResTimestamp.put(resPath, null);
            } else {
                origResData.put(resPath, readAll(raw.inputStream));
                origResTimestamp.put(resPath, raw.timestamp);
            }
        }

        private byte[] readAll(InputStream inputStream) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, out);
            inputStream.close();
            out.close();
            return out.toByteArray();
        }

        public void rollback() {
            checkThread();

            for (String resPath : origResData.keySet()) {
                logger.debug("Rollbacking " + resPath);
                try {
                    byte[] data = origResData.get(resPath);
                    Long ts = origResTimestamp.get(resPath);
                    if (data == null || ts == null)
                        deleteResourceImpl(resPath);
                    else
                        putResourceImpl(resPath, new ByteArrayInputStream(data), ts);
                } catch (IOException ex) {
                    logger.error("Failed to rollback " + resPath, ex);
                }
            }
        }

        @Override
        public void close() throws IOException {
            checkThread();

            origResData = null;
            origResTimestamp = null;
            checkpointing.set(null);
        }

        private void checkThread() {
            Checkpoint cp = checkpointing.get();
            if (this != cp)
                throw new IllegalStateException();
        }
    }

    // ============================================================================

    public static interface Visitor {
        void visit(String path) throws IOException;
    }

    public void scanRecursively(String path, Visitor visitor) throws IOException {
        NavigableSet<String> children = listResources(path);
        if (children != null) {
            for (String child : children)
                scanRecursively(child, visitor);
            return;
        }

        if (exists(path))
            visitor.visit(path);
    }

    public List<String> collectResourceRecursively(String root, final String suffix) throws IOException {
        final ArrayList<String> collector = Lists.newArrayList();
        scanRecursively(root, new Visitor() {
            @Override
            public void visit(String path) {
                if (path.endsWith(suffix))
                    collector.add(path);
            }
        });
        return collector;
    }

    public static String dumpResources(KylinConfig kylinConfig, Collection<String> dumpList) throws IOException {
        File tmp = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmp); // we need a directory, so delete the file first

        File metaDir = new File(tmp, "meta");
        metaDir.mkdirs();

        // write kylin.properties
        File kylinPropsFile = new File(metaDir, "kylin.properties");
        kylinConfig.exportToFile(kylinPropsFile);

        ResourceStore from = ResourceStore.getStore(kylinConfig);
        KylinConfig localConfig = KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath());
        ResourceStore to = ResourceStore.getStore(localConfig);
        for (String path : dumpList) {
            RawResource res = from.getResource(path);
            if (res == null)
                throw new IllegalStateException("No resource found at -- " + path);
            to.putResource(path, res.inputStream, res.timestamp);
            res.inputStream.close();
        }

        String metaDirURI = OptionsHelper.convertToFileURL(metaDir.getAbsolutePath());
        if (metaDirURI.startsWith("/")) // note Path on windows is like "d:/../..."
            metaDirURI = "file://" + metaDirURI;
        else
            metaDirURI = "file:///" + metaDirURI;
        logger.info("meta dir is: " + metaDirURI);

        return metaDirURI;
    }
}
