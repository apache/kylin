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

package org.apache.kylin.metadata.cachesync;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

abstract public class CachedCrudAssist<T extends RootPersistentEntity> {

    private static final Logger logger = LoggerFactory.getLogger(CachedCrudAssist.class);

    final private ResourceStore store;
    final private Class<T> entityType;
    final private String resRootPath;
    final private String resPathSuffix;
    final private Serializer<T> serializer;
    final private SingleValueCache<String, T> cache;

    private boolean checkCopyOnWrite;

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, Class<T> entityType,
            SingleValueCache<String, T> cache) {
        this(store, resourceRootPath, MetadataConstants.FILE_SURFIX, entityType, cache);
    }

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, String resourcePathSuffix,
            Class<T> entityType, SingleValueCache<String, T> cache) {
        this.store = store;
        this.entityType = entityType;
        this.resRootPath = resourceRootPath;
        this.resPathSuffix = resourcePathSuffix;
        this.serializer = new JsonSerializer<T>(entityType);
        this.cache = cache;

        this.checkCopyOnWrite = store.getConfig().isCheckCopyOnWrite();

        Preconditions.checkArgument(resRootPath.startsWith("/"));
        Preconditions.checkArgument(resRootPath.endsWith("/") == false);
    }

    public Serializer<DataModelDesc> getSerializer() {
        return (Serializer<DataModelDesc>) serializer;
    }

    public void setCheckCopyOnWrite(boolean check) {
        this.checkCopyOnWrite = check;
    }

    // Make copy of an entity such that update can apply on the copy.
    // Note cached and shared object MUST NOT be updated directly.
    public T copyForWrite(T entity) {
        if (entity.isCachedAndShared() == false)
            return entity;

        T copy;
        try {
            byte[] bytes;
            try (ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    DataOutputStream dout = new DataOutputStream(buf)) {
                serializer.serialize(entity, dout);
                bytes = buf.toByteArray();
            }

            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                copy = serializer.deserialize(in);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        copy.setCachedAndShared(false);
        initEntityAfterReload(copy, entity.resourceName());
        return copy;
    }

    private String resourcePath(String resourceName) {
        return resRootPath + "/" + resourceName + resPathSuffix;
    }

    private String resourceName(String resourcePath) {
        Preconditions.checkArgument(resourcePath.startsWith(resRootPath));
        Preconditions.checkArgument(resourcePath.endsWith(resPathSuffix));
        return resourcePath.substring(resRootPath.length() + 1, resourcePath.length() - resPathSuffix.length());
    }

    public void reloadAll() throws IOException {
        logger.debug("Reloading " + entityType.getSimpleName() + " from " + store.getReadableResourcePath(resRootPath));

        cache.clear();

        List<String> paths = store.collectResourceRecursively(resRootPath, resPathSuffix);
        for (String path : paths) {
            reloadQuietlyAt(path);
        }

        logger.debug("Loaded " + cache.size() + " " + entityType.getSimpleName() + "(s) out of " + paths.size()
                + " resource");
    }

    public T reload(String resourceName) {
        return reloadAt(resourcePath(resourceName));
    }

    public T reloadQuietly(String resourceName) {
        return reloadQuietlyAt(resourcePath(resourceName));
    }

    private T reloadQuietlyAt(String path) {
        try {
            return reloadAt(path);
        } catch (Exception ex) {
            logger.error("Error loading " + entityType.getSimpleName() + " at " + path, ex);
            return null;
        }
    }

    public T reloadAt(String path) {
        try {
            T entity = store.getResource(path, entityType, serializer);
            if (entity == null) {
                logger.warn("No " + entityType.getSimpleName() + " found at " + path + ", returning null");
                cache.removeLocal(resourceName(path));
                return null;
            }

            // mark cached object
            entity.setCachedAndShared(true);
            entity = initEntityAfterReload(entity, resourceName(path));

            if (path.equals(resourcePath(entity.resourceName())) == false)
                throw new IllegalStateException("The entity " + entity + " read from " + path
                        + " will save to a different path " + resourcePath(entity.resourceName()));
            
            cache.putLocal(entity.resourceName(), entity);
            return entity;
        } catch (Exception e) {
            throw new IllegalStateException("Error loading " + entityType.getSimpleName() + " at " + path, e);
        }
    }

    abstract protected T initEntityAfterReload(T entity, String resourceName);

    public T save(T entity) throws IOException {
        Preconditions.checkArgument(entity != null);
        completeUuidIfNeeded(entity);
        Preconditions.checkArgument(entityType.isInstance(entity));

        String resName = entity.resourceName();
        Preconditions.checkArgument(resName != null && resName.length() > 0);

        if (checkCopyOnWrite) {
            if (entity.isCachedAndShared() || cache.get(resName) == entity) {
                throw new IllegalStateException("Copy-on-write violation! The updating entity " + entity
                        + " is a shared object in " + entityType.getSimpleName() + " cache, which should not be.");
            }
        }

        String path = resourcePath(resName);
        logger.debug("Saving {} at {}", entityType.getSimpleName(), path);

        store.putResource(path, entity, serializer);
        
        // just to trigger the event broadcast, the entity won't stay in cache
        cache.put(resName, entity);

        // keep the pass-in entity out of cache, the caller may use it for further update
        // return a reloaded new object
        return reload(resName);
    }

    private void completeUuidIfNeeded(T entity) {
        if (entity.getUuid() == null) {
            entity.updateRandomUuid();
        }
    }

    public void delete(T entity) throws IOException {
        delete(entity.resourceName());
    }

    public void delete(String resName) throws IOException {
        Preconditions.checkArgument(resName != null);

        String path = resourcePath(resName);
        logger.debug("Deleting {} at {}", entityType.getSimpleName(), path);

        store.deleteResource(path);
        cache.remove(resName);
    }

}
