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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
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

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

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
    public static final String TEMP_STATMENT_RESOURCE_ROOT = "/temp_statement";
    public static final String TABLE_RESOURCE_ROOT = "/table";
    public static final String EXTERNAL_FILTER_RESOURCE_ROOT = "/ext_filter";
    public static final String HYBRID_RESOURCE_ROOT = "/hybrid";
    public static final String EXECUTE_RESOURCE_ROOT = "/execute";
    public static final String EXECUTE_OUTPUT_RESOURCE_ROOT = "/execute_output";
    public static final String STREAMING_RESOURCE_ROOT = "/streaming";
    public static final String STREAMING_V2_RESOURCE_ROOT = "/streaming_v2";
    public static final String KAFKA_RESOURCE_ROOT = "/kafka";
    public static final String STREAMING_OUTPUT_RESOURCE_ROOT = "/streaming_output";
    public static final String CUBE_STATISTICS_ROOT = "/cube_statistics";
    public static final String BAD_QUERY_RESOURCE_ROOT = "/bad_query";
    public static final String DRAFT_RESOURCE_ROOT = "/draft";
    public static final String USER_ROOT = "/user";
    public static final String EXT_SNAPSHOT_RESOURCE_ROOT = "/ext_table_snapshot";

    public static final String METASTORE_UUID_TAG = "/UUID";

    private static final ConcurrentMap<KylinConfig, ResourceStore> CACHE = new ConcurrentHashMap<>();

    private static ResourceStore createResourceStore(KylinConfig kylinConfig) {
        StorageURL metadataUrl = kylinConfig.getMetadataUrl();
        logger.info("Using metadata url {} for resource store", metadataUrl);
        String clsName = kylinConfig.getResourceStoreImpls().get(metadataUrl.getScheme());
        try {
            Class<? extends ResourceStore> cls = ClassUtil.forName(clsName, ResourceStore.class);
            ResourceStore store = cls.getConstructor(KylinConfig.class).newInstance(kylinConfig);
            if (!store.exists(METASTORE_UUID_TAG)) {
                store.checkAndPutResource(METASTORE_UUID_TAG, new StringEntity(store.createMetaStoreUUID()), 0,
                        StringEntity.serializer);
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

    final public KylinConfig getConfig() {
        return kylinConfig;
    }

    protected String createMetaStoreUUID() throws IOException {
        return UUID.randomUUID().toString();
    }

    public String getMetaStoreUUID() throws IOException {
        if (!exists(ResourceStore.METASTORE_UUID_TAG)) {
            checkAndPutResource(ResourceStore.METASTORE_UUID_TAG, new StringEntity(createMetaStoreUUID()), 0,
                    StringEntity.serializer);
        }
        StringEntity entity = getResource(ResourceStore.METASTORE_UUID_TAG, StringEntity.serializer);
        return entity == null ? "" : entity.toString();
    }

    /**
     * Collect resources recursively under a folder, return empty list if folder does not exist
     */
    final public List<String> collectResourceRecursively(final String folderPath, final String suffix)
            throws IOException {
        return new ExponentialBackoffRetry(this).doWithRetry(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                final ArrayList<String> collector = Lists.newArrayList();
                visitFolder(folderPath, true, new Visitor() {
                    @Override
                    public void visit(RawResource resource) {
                        String path = resource.path();
                        if (suffix == null || path.endsWith(suffix))
                            collector.add(path);
                    }
                });
                return collector;
            }
        });
    }

    /**
     * List resources and sub-folders under a given folder, return null if folder does not exist or is empty
     */
    final public NavigableSet<String> listResources(String folderPath) throws IOException {
        return listResourcesImpl(norm(folderPath));
    }

    // sub-class may choose to override for better performance
    protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException {
        List<String> list = collectResourceRecursively(folderPath, null);
        if (list.isEmpty())
            return null;

        TreeSet<String> result = new TreeSet();
        String root = norm(folderPath);

        for (String p : list) {
            int cut = p.indexOf('/', root.length() + 1);
            result.add(cut < 0 ? p : p.substring(0, cut));
        }
        return result;
    }

    /**
     * List resources recursively under a folder, return null if folder does not exist or is empty
     */
    final public NavigableSet<String> listResourcesRecursively(String folderPath) throws IOException {
        return listResourcesRecursivelyImpl(norm(folderPath));
    }

    // sub-class may choose to override
    protected NavigableSet<String> listResourcesRecursivelyImpl(String folderPath) throws IOException {
        List<String> list = collectResourceRecursively(folderPath, null);
        if (list.isEmpty())
            return null;
        else
            return new TreeSet<String>(list);
    }

    /**
     * Read all resources under a folder. Return empty list if folder not exist.
     *
     * NOTE: Exceptions thrown by ContentReader are swallowed in order to load every resource at best effort.
     */
    final public <T extends RootPersistentEntity> List<T> getAllResources(String folderPath, Serializer<T> serializer)
            throws IOException {
        return getAllResources(folderPath, false, null, new ContentReader(serializer));
    }

    /**
     * Read all resources under a folder having last modified time between given range. Return empty list if folder not exist.
     *
     * NOTE: Exceptions thrown by ContentReader are swallowed in order to load every resource at best effort.
     */
    final public <T extends RootPersistentEntity> List<T> getAllResources(final String folderPath,
            final boolean recursive, final VisitFilter filter, final ContentReader<T> reader) throws IOException {

        return new ExponentialBackoffRetry(this).doWithRetry(new Callable<List<T>>() {
            @Override
            public List<T> call() throws Exception {
                final ArrayList<T> collector = Lists.newArrayList();
                visitFolderAndContent(folderPath, recursive, filter, new Visitor() {
                    @Override
                    public void visit(RawResource resource) throws IOException {
                        try {
                            T entity = reader.readContent(resource);
                            if (entity != null) {
                                collector.add(entity);
                            }
                        } catch (Exception ex) {
                            logger.error("Error reading resource " + resource.path(), ex);
                        }
                    }
                });
                return collector;
            }
        });
    }

    /**
     * Read all resources under a folder having last modified time between given range. Return empty map if folder not exist.
     *
     * NOTE: Different from the getAllResources, this return value will contain the resource path.
     */
    final public <T extends RootPersistentEntity> Map<String, T> getAllResourcesMap(final String folderPath,
                                                                                    final boolean recursive, final VisitFilter filter, final ContentReader<T> reader) throws IOException {

        return new ExponentialBackoffRetry(this).doWithRetry(() -> {
            final LinkedHashMap<String, T> collector = new LinkedHashMap<>();
            visitFolderAndContent(folderPath, recursive, filter, new Visitor() {
                @Override
                public void visit(RawResource resource) throws IOException {
                    try {
                        T entity = reader.readContent(resource);
                        if (entity != null) {
                            collector.put(resource.path(), entity);
                        }
                    } catch (Exception ex) {
                        logger.error("Error reading resource " + resource.path(), ex);
                    }
                }
            });
            return collector;
        });
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
    final public <T extends RootPersistentEntity> T getResource(String resPath, Serializer<T> serializer)
            throws IOException {
        return getResource(resPath, new ContentReader<T>(serializer));
    }

    /**
     * Read a resource, return null in case of not found or is a folder.
     */
    final public <T extends RootPersistentEntity> T getResource(String resPath, ContentReader<T> reader)
            throws IOException {
        resPath = norm(resPath);
        RawResource res = getResourceWithRetry(resPath);
        if (res == null)
            return null;

        return reader.readContent(res);
    }

    /**
     * Caution: Caller must close the returned RawResource.
     */
    final public RawResource getResource(String resPath) throws IOException {
        return getResourceWithRetry(norm(resPath));
    }

    /**
     * Returns null if not exists.
     *
     * NOTE: Broken content exception should be wrapped by RawResource, and return to caller to decide how to handle.
     */
    abstract protected RawResource getResourceImpl(String resPath) throws IOException;

    private RawResource getResourceWithRetry(final String resPath) throws IOException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(this);
        return retry.doWithRetry(() -> getResourceImpl(resPath));
    }

    final public long getResourceTimestamp(String resPath) throws IOException {
        return getResourceTimestampWithRetry(norm(resPath));
    }

    /**
     * returns 0 if not exists
     */
    abstract protected long getResourceTimestampImpl(String resPath) throws IOException;

    final public long getResourceTimestampWithRetry(String resPath) throws IOException {
        final String path = norm(resPath);

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(this);
        return retry.doWithRetry(() -> getResourceTimestampImpl(path));
    }

    /**
     * Overwrite a resource without write conflict check
     * @return bytes written
     */
    final public <T extends RootPersistentEntity> long putResource(String resPath, T obj, long ts,
            Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        obj.setLastModified(ts);
        ContentWriter writer = ContentWriter.create(obj, serializer);
        putResourceCheckpoint(resPath, writer, ts);
        return writer.bytesWritten();
    }

    /**
     * Overwrite a resource without write conflict check
     * @return bytes written
     */
    final public <T extends RootPersistentEntity> long putBigResource(String resPath, T obj, long ts,
            Serializer<T> serializer) throws IOException {
        resPath = norm(resPath);
        obj.setLastModified(ts);
        ContentWriter writer = ContentWriter.create(obj, serializer);
        writer.markBigContent();
        putResourceCheckpoint(resPath, writer, ts);
        return writer.bytesWritten();
    }

    final public void putBigResource(String resPath, InputStream content, long ts) throws IOException {
        resPath = norm(resPath);
        ContentWriter writer = ContentWriter.create(content);
        writer.markBigContent();
        putResourceCheckpoint(resPath, writer, ts);
    }

    /**
     * Overwrite a resource without write conflict check
     * @return bytes written
     */
    final public long putResource(String resPath, InputStream content, long ts) throws IOException {
        resPath = norm(resPath);
        ContentWriter writer = ContentWriter.create(content);
        putResourceCheckpoint(resPath, writer, ts);
        return writer.bytesWritten();
    }

    private void putResourceCheckpoint(String resPath, ContentWriter content, long ts) throws IOException {
        logger.trace("Directly saving resource {} (Store {})", resPath, kylinConfig.getMetadataUrl());
        beforeChange(resPath);
        putResourceWithRetry(resPath, content, ts);
    }

    abstract protected void putResourceImpl(String resPath, ContentWriter content, long ts) throws IOException;

    protected void putResourceWithRetry(final String resPath, final ContentWriter content, final long ts)
            throws IOException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(this);
        retry.doWithRetry(() -> {
            putResourceImpl(resPath, content, ts);
            return null;
        });
    }

    /**
     * check & set, overwrite a resource
     */
    public final <T extends RootPersistentEntity> void checkAndPutResource(String resPath, T obj,
            Serializer<T> serializer) throws IOException, WriteConflictException {
        checkAndPutResource(resPath, obj, System.currentTimeMillis(), serializer);
    }

    /**
     * check & set, overwrite a resource
     */
    public final <T extends RootPersistentEntity> void checkAndPutResource(String resPath, T obj, long newTS,
            Serializer<T> serializer) throws IOException, WriteConflictException {
        resPath = norm(resPath);

        long oldTS = obj.getLastModified();
        obj.setLastModified(newTS);

        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(buf);
            serializer.serialize(obj, dout);
            dout.close();
            buf.close();

            long confirmedTS = checkAndPutResource(resPath, buf.toByteArray(), oldTS, newTS);
            obj.setLastModified(confirmedTS); // update again the confirmed TS
            //return confirmedTS;

        } catch (IOException | RuntimeException e) {
            obj.setLastModified(oldTS); // roll back TS when write fail
            throw e;
        }
    }

    /**
     * Check & set, overwrite a resource.
     *
     * @return a confirmed TS, as some store may lose timestamp precision.
     */
    public final long checkAndPutResource(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, WriteConflictException {
        return checkAndPutResourceCheckpoint(norm(resPath), content, oldTS, newTS);
    }

    private long checkAndPutResourceCheckpoint(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, WriteConflictException {
        beforeChange(resPath);
        return checkAndPutResourceWithRetry(resPath, content, oldTS, newTS);
    }

    /**
     * checks old timestamp when overwriting existing
     */
    protected abstract long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, WriteConflictException;

    private long checkAndPutResourceWithRetry(final String resPath, final byte[] content, final long oldTS,
                                              final long newTS) throws IOException, WriteConflictException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(this);
        return retry.doWithRetry(() -> checkAndPutResourceImpl(resPath, content, oldTS, newTS));
    }

    /**
     * update resource timestamp to timestamp
     */
    public final void updateTimestamp(String resPath, long timestamp) throws IOException {
        logger.trace("Updating resource: {} with timestamp {} (Store {})", resPath, timestamp,
                kylinConfig.getMetadataUrl());
        updateTimestampCheckPoint(norm(resPath), timestamp);
    }

    private void updateTimestampCheckPoint(String resPath, long timestamp) throws IOException {
        beforeChange(resPath);
        updateTimestampWithRetry(resPath, timestamp);
    }

    private void updateTimestampWithRetry(final String resPath, final long timestamp) throws IOException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(this);
        retry.doWithRetry(() -> {
            updateTimestampImpl(resPath, timestamp);
            return null;
        });
    }

    protected abstract void updateTimestampImpl(String resPath, long timestamp) throws IOException;

    /**
     * delete a resource, does nothing on a folder
     */
    public final void deleteResource(String resPath) throws IOException {
        logger.trace("Deleting resource {} (Store {})", resPath, kylinConfig.getMetadataUrl());
        deleteResourceCheckpoint(norm(resPath));
    }

    /**
     * Delete a resource with comparing its timestamp
     * Success to delete if resource lastModified < timestamp + 1000 (considering timestamp precision loose)
     * throw an IOException when the resource lastModified >= timestamp + 1000
     * See  https://issues.apache.org/jira/browse/KYLIN-4030
     */
    public final void deleteResource(String resPath, long timestamp) throws IOException {
        logger.trace("Deleting resource {} within timestamp {} (Store {})", resPath, timestamp,
                kylinConfig.getMetadataUrl());
        deleteResourceCheckpoint(norm(resPath), timestamp);
    }

    private void deleteResourceCheckpoint(String resPath) throws IOException {
        beforeChange(resPath);
        deleteResourceWithRetry(resPath);
    }

    private void deleteResourceCheckpoint(String resPath, long timestamp) throws IOException {
        beforeChange(resPath);
        deleteResourceWithRetry(resPath, timestamp);
    }

    protected abstract void deleteResourceImpl(String resPath) throws IOException;

    protected abstract void deleteResourceImpl(String resPath, long timestamp) throws IOException;

    private void deleteResourceWithRetry(final String resPath) throws IOException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(this);
        retry.doWithRetry(() -> {
            deleteResourceImpl(resPath);
            return null;
        });
    }

    private void deleteResourceWithRetry(final String resPath, final long timestamp) throws IOException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(this);
        retry.doWithRetry(() -> {
            deleteResourceImpl(resPath, timestamp);
            return null;
        });
    }

    protected boolean checkTimeStampBeforeDelete(long originLastModified, long timestamp) {
        // note here is originLastModified may be 0
        // 0 means resource doesn't exists in general, it's safe to pass the check
        boolean passCheck = false;
        if (originLastModified > timestamp) {
            // file system may loose time precision with milliseconds
            // because of the new born resource time, so here if time diff less than 1000 ms, we will treat it the same
            long timeDiff = originLastModified - timestamp;
            passCheck = timeDiff < 1000;
        } else {
            // if timestamp >= originLastModified, it's safe to delete
            passCheck = true;
        }
        logger.trace("check timestamp before delete: {}, [originLastModified: {}, timestamp: {}]", passCheck,
                originLastModified, timestamp);
        return passCheck;
    }

    /**
     * called by ExponentialBackoffRetry, to check if an exception is due to unreachable server and worth retry
     */
    protected boolean isUnreachableException(Throwable ex) {
        List<String> connectionExceptions = Lists
                .newArrayList(kylinConfig.getResourceStoreConnectionExceptions().split(","));
        boolean hasException = false;
        for (String exception : connectionExceptions) {
            hasException = containsException(ex, exception);

            if (hasException)
                break;
        }
        return hasException;
    }

    private boolean containsException(Throwable ex, String targetException) {
        Throwable t = ex;
        int depth = 0;
        while (t != null && depth < 5) {
            depth++;
            if (t.getClass().getName().equals(targetException)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    /**
     * get a readable string of a resource path
     */
    public final String getReadableResourcePath(String resPath) {
        return getReadableResourcePathImpl(norm(resPath));
    }

    protected abstract String getReadableResourcePathImpl(String resPath);

    private String norm(String resPath) {
        resPath = resPath.trim();
        while (resPath.startsWith("//"))
            resPath = resPath.substring(1);
        while (resPath.endsWith("/"))
            resPath = resPath.substring(0, resPath.length() - 1);
        if (!resPath.startsWith("/"))
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

            RawResource raw = getResourceWithRetry(resPath);
            if (raw == null) {
                origResData.put(resPath, null);
                origResTimestamp.put(resPath, null);
            } else {
                origResData.put(resPath, readAll(raw.content()));
                origResTimestamp.put(resPath, raw.lastModified());
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
                logger.debug("Rollbacking {}", resPath);
                try {
                    byte[] data = origResData.get(resPath);
                    Long ts = origResTimestamp.get(resPath);
                    if (data == null || ts == null)
                        deleteResourceWithRetry(resPath);
                    else
                        putResourceWithRetry(resPath, ContentWriter.create(data), ts);
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
        void visit(RawResource resource) throws IOException;
    }

    public static class VisitFilter {
        public String pathPrefix = null;
        public long lastModStart = Long.MIN_VALUE;
        public long lastModEndExclusive = Long.MAX_VALUE;

        public VisitFilter() {
        }

        public VisitFilter(String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        public VisitFilter(long lastModStart, long lastModEndExclusive) {
            this(null, lastModStart, lastModEndExclusive);
        }

        public VisitFilter(String pathPrefix, long lastModStart, long lastModEndExclusive) {
            this.pathPrefix = pathPrefix;
            this.lastModStart = lastModStart;
            this.lastModEndExclusive = lastModEndExclusive;
        }

        public boolean hasPathPrefixFilter() {
            return pathPrefix != null;
        }

        public boolean hasTimeFilter() {
            return lastModStart != Long.MIN_VALUE || lastModEndExclusive != Long.MAX_VALUE;
        }

        public boolean matches(String resPath, long lastModified) {
            if (hasPathPrefixFilter()) {
                if (!resPath.startsWith(pathPrefix))
                    return false;
            }

            if (hasTimeFilter()) {
                if (!(lastModStart <= lastModified && lastModified < lastModEndExclusive))
                    return false;
            }

            return true;
        }
    }

    /**
     * Visit all resource under a folder (optionally recursively), without loading the content of resource.
     * Low level API, DON'T support ExponentialBackoffRetry, caller should do necessary retry
     */
    public final void visitFolder(String folderPath, boolean recursive, Visitor visitor) throws IOException {
        visitFolderInner(folderPath, recursive, null, false, visitor);
    }

    /**
     * Visit all resource under a folder (optionally recursively), without loading the content of resource.
     * Low level API, DON'T support ExponentialBackoffRetry, caller should do necessary retry
     */
    public final void visitFolder(String folderPath, boolean recursive, VisitFilter filter, Visitor visitor)
            throws IOException {
        visitFolderInner(folderPath, recursive, filter, false, visitor);
    }

    /**
     * Visit all resource and their content under a folder (optionally recursively).
     * Low level API, DON'T support ExponentialBackoffRetry, caller should do necessary retry
     */
    public final void visitFolderAndContent(String folderPath, boolean recursive, VisitFilter filter, Visitor visitor)
            throws IOException {
        visitFolderInner(folderPath, recursive, filter, true, visitor);
    }

    // Low level API, DON'T support ExponentialBackoffRetry, caller should do necessary retry
    private void visitFolderInner(String folderPath, boolean recursive, VisitFilter filter, boolean loadContent,
            Visitor visitor) throws IOException {
        if (filter == null)
            filter = new VisitFilter();

        folderPath = norm(folderPath);
        if (filter.hasPathPrefixFilter()) {
            String folderPrefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
            Preconditions.checkArgument(filter.pathPrefix.startsWith(folderPrefix));
        }

        visitFolderImpl(folderPath, recursive, filter, loadContent, visitor);
    }

    /**
     * Visit all resource under a folder.
     *
     * - optionally, include sub-folders recursively
     * - optionally, filter by prefix of resource path
     * - optionally, filter by last modified time
     * - optionally, visit content as well
     *
     * NOTE: Broken content exception should be wrapped by RawResource, and return to caller to decide how to handle.
     */
    protected abstract void visitFolderImpl(String folderPath, boolean recursive, VisitFilter filter,
            boolean loadContent, Visitor visitor) throws IOException;

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
            to.putResource(path, res.content(), res.lastModified());
            res.close();
        }

        String metaDirURI = OptionsHelper.convertToFileURL(metaDir.getAbsolutePath());
        if (metaDirURI.startsWith("/")) // note Path on windows is like "d:/../..."
            metaDirURI = "file://" + metaDirURI;
        else
            metaDirURI = "file:///" + metaDirURI;
        logger.info("meta dir is: {}", metaDirURI);

        return metaDirURI;
    }

    public static boolean isPotentialMemoryLeak() {
        return CACHE.size() > 100;
    }
}
