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
package io.kyligence.kap.secondstorage.metadata;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import com.google.common.collect.Lists;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;

@NotThreadSafe
public abstract class Manager<T extends RootPersistentEntity> implements IManager<T> {

    protected final KylinConfig config;
    protected final String project;
    protected final CachedCrudAssist<T> crud;

    protected ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    protected Manager(KylinConfig cfg, final String project) {

        if (logger().isInfoEnabled() && !UnitOfWork.isAlreadyInTransaction()) {
            logger().info("Initializing {} with KylinConfig Id: {} for project {}", name(),
                    System.identityHashCode(cfg), project);
        }

        this.config = cfg;
        this.project = project;
        this.crud = new CachedCrudAssist<T>(getStore(), rootPath(), entityType()) {
            @SuppressWarnings("unchecked")
            @Override
            protected T initEntityAfterReload(T entity, String resourceName) {
                if (entity instanceof IManagerAware) {
                    IManagerAware<T> managerAware = (IManagerAware<T>) entity;
                    managerAware.setManager(Manager.this);
                }
                return entity;
            }
        };
        this.crud.setCheckCopyOnWrite(true);
    }

    @SuppressWarnings("unchecked")
    protected T save(T entity) {
        if (entity instanceof IManagerAware) {
            IManagerAware<T> managerAware = (IManagerAware<T>) entity;
            managerAware.verify();
        }
        return crud.save(entity);
    }

    protected T copy(T entity) {
        return crud.copyBySerialization(entity);
    }

    // Create
    protected abstract T newRootEntity(String cubeName);

    public T makeSureRootEntity(String cubeName) {
        return get(cubeName).orElseGet(() -> createAS(newRootEntity(cubeName)));
    }

    public T createAS(T entity) {
        if (entity.getUuid() == null)
            throw new IllegalArgumentException();
        if (crud.contains(entity.getUuid()))
            throw new IllegalArgumentException("Entity '" + entity.getUuid() + "' already exists");
        // overwrite point
        return save(entity);
    }

    // Read
    public List<T> listAll() {
        return Lists.newArrayList(crud.listAll());
    }

    public Optional<T> get(String uuid) {
        if (StringUtils.isEmpty(uuid)) {
            return Optional.empty();
        }
        return Optional.ofNullable(crud.get(uuid));
    }

    //Update
    protected T internalUpdate(T entity) {
        if (entity.isCachedAndShared())
            throw new IllegalStateException();

        if (entity.getUuid() == null)
            throw new IllegalArgumentException();

        String name = entity.getUuid();
        if (!crud.contains(name))
            throw new IllegalArgumentException("Entity '" + name + "' does not exist.");

        return save(entity);
    }

    public T update(String uuid, Consumer<T> updater) {
        return get(uuid).map(this::copy).map(copied -> {
            updater.accept(copied);
            return internalUpdate(copied);
        }).orElse(null);
    }

    protected T upsert(String uuid, Consumer<T> updater, Supplier<T> creator) {
        return get(uuid).map(this::copy).map(copied -> {
            updater.accept(copied);
            return internalUpdate(copied);
        }).orElseGet(() -> {
            T newEntity = creator.get();
            updater.accept(newEntity);
            return createAS(newEntity);
        });
    }

    //Delete
    public void delete(T entity) {
        crud.delete(entity);
    }
}
