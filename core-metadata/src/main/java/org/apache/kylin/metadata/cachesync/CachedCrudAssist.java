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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

abstract public class CachedCrudAssist<T extends RootPersistentEntity> {

    private static final Logger logger = LoggerFactory.getLogger(CachedCrudAssist.class);

    final private ResourceStore store;
    final private Class<T> entityType;
    final private String resRootPath;
    final private Serializer<T> serializer;
    final private SingleValueCache<String, T> cache;

    private boolean checkCopyOnWrite;

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, Class<T> entityType,
            SingleValueCache<String, T> cache) {
        this.store = store;
        this.entityType = entityType;
        this.resRootPath = resourceRootPath;
        this.serializer = new JsonSerializer<T>(entityType);
        this.cache = cache;

        this.checkCopyOnWrite = store.getConfig().isCheckCopyOnWrite();

        Preconditions.checkArgument(resRootPath.startsWith("/"));
        Preconditions.checkArgument(resRootPath.endsWith("/") == false);
    }

    public void setCheckOnWrite(boolean check) {
        this.checkCopyOnWrite = check;
    }

    private String resourcePath(String resourceName) {
        return resRootPath + "/" + resourceName + MetadataConstants.FILE_SURFIX;
    }

    public void reloadAll() throws IOException {
        logger.debug("Reloading " + entityType.getName() + " from " + store.getReadableResourcePath(resRootPath));

        cache.clear();

        List<String> paths = store.collectResourceRecursively(resRootPath, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadQuietlyAt(path);
        }

        logger.debug("Loaded " + cache.size() + " " + entityType.getName() + "(s)");
    }

    public T reload(String resourceName) throws IOException {
        return reloadAt(resourcePath(resourceName));
    }

    public T reloadQuietly(String resourceName) {
        return reloadQuietlyAt(resourcePath(resourceName));
    }

    private T reloadQuietlyAt(String path) {
        try {
            return reloadAt(path);
        } catch (Exception ex) {
            logger.error("Error loading " + entityType.getName() + " at " + path, ex);
            return null;
        }
    }

    public T reloadAt(String path) throws IOException {
        T entity = store.getResource(path, entityType, serializer);
        if (entity == null) {
            logger.warn("No " + entityType.getName() + " found at " + path + ", returning null");
            return null;
        }
        
        initEntityAfterReload(entity);
        cache.putLocal(entity.resourceName(), entity);
        return entity;
    }

    abstract protected void initEntityAfterReload(T entity);

    public T save(T entity) throws IOException {
        Preconditions.checkArgument(entity != null);
        Preconditions.checkArgument(entityType.isInstance(entity));

        String resName = entity.resourceName();
        if (checkCopyOnWrite) {
            if (cache.get(resName) == entity) {
                throw new IllegalStateException("Copy-on-write violation! The updating entity " + entity
                        + " is a shared object in " + entityType.getName() + " cache, which should not be.");
            }
        }

        store.putResource(resourcePath(resName), entity, serializer);
        cache.put(resName, entity);
        return entity;
    }

    public void delete(T entity) throws IOException {
        delete(entity.resourceName());
    }

    public void delete(String resName) throws IOException {
        Preconditions.checkArgument(resName != null);
        store.deleteResource(resourcePath(resName));
        cache.remove(resName);
    }

}
