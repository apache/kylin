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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

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

    private static final ConcurrentHashMap<KylinConfig, ResourceStore> CACHE = new ConcurrentHashMap<KylinConfig, ResourceStore>();

    private static final ArrayList<Class<? extends ResourceStore>> knownImpl = new ArrayList<Class<? extends ResourceStore>>();

    private static ArrayList<Class<? extends ResourceStore>> getKnownImpl() {
        if (knownImpl.isEmpty()) {
            knownImpl.add(FileResourceStore.class);
            try {
                knownImpl.add(ClassUtil.forName("org.apache.kylin.storage.hbase.HBaseResourceStore", ResourceStore.class));
            } catch (Throwable e) {
                logger.warn("Failed to load HBaseResourceStore impl class: " + e.toString());
            }
        }
        return knownImpl;
    }

    private static ResourceStore createResourceStore(KylinConfig kylinConfig) {
        List<Throwable> es = new ArrayList<Throwable>();
        logger.info("Using metadata url " + kylinConfig.getMetadataUrl() + " for resource store");
        for (Class<? extends ResourceStore> cls : getKnownImpl()) {
            try {
                return cls.getConstructor(KylinConfig.class).newInstance(kylinConfig);
            } catch (Throwable e) {
                es.add(e);
            }
        }
        for (Throwable exceptionOrError : es) {
            logger.error("Create new store instance failed ", exceptionOrError);
        }
        throw new IllegalArgumentException("Failed to find metadata store by url: " + kylinConfig.getMetadataUrl());
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

    abstract protected NavigableSet<String> listResourcesImpl(String folderPath) throws IOException;

    /**
     * Return true if a resource exists, return false in case of folder or non-exist
     */
    final public boolean exists(String resPath) throws IOException {
        return existsImpl(norm(resPath));
    }

    abstract protected boolean existsImpl(String resPath) throws IOException;

    /**
     * Read a resource, return null in case of not found or is a folder
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

    abstract protected List<RawResource> getAllResourcesImpl(String folderPath, long timeStart, long timeEndExclusive) throws IOException;

    /** returns null if not exists */
    abstract protected RawResource getResourceImpl(String resPath) throws IOException;

    /** returns 0 if not exists */
    abstract protected long getResourceTimestampImpl(String resPath) throws IOException;

    /**
     * overwrite a resource without write conflict check
     */
    final public void putResource(String resPath, InputStream content, long ts) throws IOException {
        resPath = norm(resPath);
        logger.trace("Directly saving resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");
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

            newTS = checkAndPutResourceImpl(resPath, buf.toByteArray(), oldTS, newTS);
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

    /**
     * checks old timestamp when overwriting existing
     */
    abstract protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS) throws IOException, IllegalStateException;

    /**
     * delete a resource, does nothing on a folder
     */
    final public void deleteResource(String resPath) throws IOException {
        logger.trace("Deleting resource " + resPath + " (Store " + kylinConfig.getMetadataUrl() + ")");
        deleteResourceImpl(norm(resPath));
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
        kylinConfig.writeProperties(kylinPropsFile);

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
