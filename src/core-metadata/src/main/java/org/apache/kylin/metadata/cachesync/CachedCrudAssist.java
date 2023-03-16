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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.ThreadUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.util.BrokenEntityProxy;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CustomKeyEquivalenceCacheBuilder;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class CachedCrudAssist<T extends RootPersistentEntity> {

    private final ResourceStore store;
    private final Class<T> entityType;
    private final String resRootPath;
    private final String resPathSuffix;
    private final Serializer<T> serializer;
    @Getter(AccessLevel.PROTECTED)
    private final Cache<String, T> cache;

    private final CacheReloadChecker<T> checker;

    private boolean checkCopyOnWrite;

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, Class<T> entityType) {
        this(store, resourceRootPath, MetadataConstants.FILE_SURFIX, entityType);
    }

    public CachedCrudAssist(ResourceStore store, String resourceRootPath, String resourcePathSuffix,
            Class<T> entityType) {
        this.store = store;
        this.entityType = entityType;
        this.resRootPath = resourceRootPath;
        this.resPathSuffix = resourcePathSuffix;
        this.serializer = new JsonSerializer<>(entityType);
        this.cache = CustomKeyEquivalenceCacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
        this.checker = new CacheReloadChecker<>(store, this);

        this.checkCopyOnWrite = store.getConfig().isCheckCopyOnWrite();

        Preconditions.checkArgument(resourceRootPath.equals("") || resRootPath.startsWith("/"));
        Preconditions.checkArgument(!resRootPath.endsWith("/"));
    }

    public Serializer<T> getSerializer() {
        return serializer;
    }

    public void setCheckCopyOnWrite(boolean check) {
        this.checkCopyOnWrite = check;
    }

    // Make copy of an entity such that update can apply on the copy.
    // Note cached and shared object MUST NOT be updated directly.
    public T copyForWrite(T entity) {
        return JsonUtil.copyForWrite(entity, serializer, this::initEntityAfterReload);
    }

    public T copyBySerialization(T entity) {
        return JsonUtil.copyBySerialization(entity, serializer, this::initEntityAfterReload);
    }

    String resourcePath(String resourceName) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(resourceName),
                "The resource name \"{}\" cannot contain white character", resourceName);
        return resRootPath + "/" + resourceName + resPathSuffix;
    }

    private String resourceName(String resourcePath) {
        Preconditions.checkArgument(resourcePath.startsWith(resRootPath));
        Preconditions.checkArgument(resourcePath.endsWith(resPathSuffix));
        return resourcePath.substring(resRootPath.length() + 1, resourcePath.length() - resPathSuffix.length());
    }

    public void reloadAll() {
        log.trace("Reloading {} from {}", entityType.getSimpleName(), store.getReadableResourcePath(resRootPath));

        cache.invalidateAll();

        List<String> paths = store.collectResourceRecursively(resRootPath, resPathSuffix);
        for (String path : paths) {
            reloadQuietlyAt(path);
        }

        log.trace("Loaded {} {}(s) out of {} resource from {}", cache.size(), entityType.getSimpleName(), paths.size(),
                store.getReadableResourcePath(resRootPath));
    }

    private T reload(String resourceName) {
        return reloadAt(resourcePath(resourceName));
    }

    private T reloadQuietlyAt(String path) {
        try {
            return reloadAt(path);
        } catch (Exception ex) {
            log.error("Error loading {} at {}", entityType.getSimpleName(), path, ex);
            return null;
        }
    }

    public T reloadAt(String path) {
        T entity = null;
        try {

            entity = store.getResource(path, serializer);
            if (entity == null) {
                throw new IllegalStateException(
                        "No " + entityType.getSimpleName() + " found at " + path + ", returning null");
            }

            // mark cached object
            entity.setCachedAndShared(true);
            entity = initEntityAfterReload(entity, resourceName(path));

            if (!path.equalsIgnoreCase(resourcePath(entity.resourceName())))
                throw new IllegalStateException("The entity " + entity + " read from " + path
                        + " will save to a different path " + resourcePath(entity.resourceName()));
        } catch (Exception e) {
            log.warn("Error loading {} at {} entity, return a BrokenEntity", entityType.getSimpleName(), path, e);
            entity = initBrokenEntity(entity, resourceName(path));
        }

        cache.put(entity.resourceName(), entity);
        return entity;
    }

    public boolean exists(String resourceName) {
        return store.getResource(resourcePath(resourceName)) != null;
    }

    public T get(String resourceName) {
        val raw = store.getResource(resourcePath(resourceName));
        if (raw == null) {
            cache.invalidate(resourceName);
            return null;
        }
        if (checker.needReload(resourceName)) {
            reloadAt(resourcePath(resourceName));
        }
        return cache.getIfPresent(resourceName);
    }

    public void invalidateCache(String resourceName) {
        this.cache.invalidate(resourceName);
    }

    abstract protected T initEntityAfterReload(T entity, String resourceName);

    protected T initBrokenEntity(T entity, String resourceName) {
        String resourcePath = resourcePath(resourceName);

        val brokenEntity = BrokenEntityProxy.getProxy(entityType, resourcePath);
        brokenEntity.setUuid(resourceName);

        if (entity != null)
            brokenEntity.setMvcc(entity.getMvcc());

        return brokenEntity;
    }

    public T save(T entity) {
        Preconditions.checkArgument(entity != null);
        Preconditions.checkArgument(entity.getUuid() != null);
        Preconditions.checkArgument(entityType.isInstance(entity));

        String resName = entity.resourceName();
        Preconditions.checkArgument(resName != null && resName.length() > 0);

        if (checkCopyOnWrite) {
            if (entity.isCachedAndShared() || cache.getIfPresent(resName) == entity) {
                throw new IllegalStateException("Copy-on-write violation! The updating entity " + entity
                        + " is a shared object in " + entityType.getSimpleName() + " cache, which should not be.");
            }
        }

        String path = resourcePath(resName);
        log.trace("Saving {} at {}", entityType.getSimpleName(), path);

        store.checkAndPutResource(path, entity, serializer);

        // keep the pass-in entity out of cache, the caller may use it for further update
        // return a reloaded new object
        return reload(resName);
    }

    public void delete(T entity) {
        delete(entity.resourceName());
    }

    public void delete(String resName) {
        Preconditions.checkArgument(resName != null);

        String path = resourcePath(resName);
        log.debug("Deleting {} at {}", entityType.getSimpleName(), path);

        store.deleteResource(path);
        cache.invalidate(resName);
    }

    public List<T> listAll() {
        List<T> all = store.collectResourceRecursively(resRootPath, resPathSuffix).stream()
                .map(path -> get(resourceName(path))).filter(Objects::nonNull)
                .collect(Collectors.toCollection(Lists::<T> newArrayList));
        if (UnitOfWork.isAlreadyInTransaction() && log.isTraceEnabled()) {
            log.trace("list all,\n{}", ThreadUtil.getKylinStackTrace());
        }
        return all;
    }

    public List<T> listPartial(Predicate<String> predicate) {
        return store.collectResourceRecursively(resRootPath, resPathSuffix).stream() //
                .filter(path -> predicate.test(path) || checkPathSuffix(path)) //
                .map(path -> get(resourceName(path))) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toList());
    }

    private boolean checkPathSuffix(String path) {
        val pathPair = StringUtils.split(path, "/");
        return pathPair.length > 0 && pathPair[pathPair.length - 1].length() == 36;
    }

    protected List<T> listAllValidCache() {
        val all = Lists.<T> newArrayList();
        for (val e : cache.asMap().entrySet()) {
            if (exists(e.getKey()))
                all.add(e.getValue());
        }
        return all;
    }

    public boolean contains(String name) {
        return store.getResource(resourcePath(name)) != null;
    }
}
