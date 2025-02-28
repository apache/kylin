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
package org.apache.kylin.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.CcModelRelationDesc;
import org.apache.kylin.metadata.model.TableModelRelationDesc;
import org.apache.kylin.metadata.query.QueryRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public abstract class Manager<T extends RootPersistentEntity> implements IManager<T> {

    private static final Logger logger = LoggerFactory.getLogger(Manager.class);
    protected static final HashMap<Class<?>, MetadataType> entityClassTypeMap = new HashMap<>();
    static {
        // used to create anonymous manager
        entityClassTypeMap.put(CcModelRelationDesc.class, MetadataType.CC_MODEL_RELATION);
        entityClassTypeMap.put(TableModelRelationDesc.class, MetadataType.TABLE_MODEL_RELATION);
        entityClassTypeMap.put(QueryRecord.class, MetadataType.QUERY_RECORD);
    }
    protected final KylinConfig config;
    protected final String project;
    protected CachedCrudAssist<T> crud;

    @SuppressWarnings("unchecked")
    public static <T extends RootPersistentEntity> Manager<T> getInstance(KylinConfig config, String project,
            Class<T> cls) {
       if (RootPersistentEntity.class.isAssignableFrom(cls) && entityClassTypeMap.containsKey(cls)) {
            // returns an anonymous manager
            return config.getManager(project, Manager.class, cls);
        } else {
            throw new IllegalArgumentException("Cannot create manager for class: " + cls);
        }
    }

    // called by reflection
    static <T extends RootPersistentEntity> Manager<T> newInstance(Class<T> entityClass, KylinConfig cfg,
            final String project) {
        return new Manager<T>(cfg, project, (MetadataType) entityClassTypeMap.get(entityClass)) {
            @Override
            public Logger logger() {
                return logger;
            }

            @Override
            public String name() {
                return entityClass.getName().replace(".class", "Manager");
            }

            @Override
            public Class<T> entityType() {
                return entityClass;
            }
        };
    }

    protected ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    protected Manager(KylinConfig cfg, final String project, MetadataType type) {

        if (logger().isInfoEnabled() && !UnitOfWork.isAlreadyInTransaction()) {
            logger().info("Initializing {} with KylinConfig Id: {} for project {}", name(),
                    System.identityHashCode(cfg), project);
        }

        this.config = cfg;
        this.project = project;
        initCrud(type, project);
    }

    protected void initCrud(MetadataType type, String project) {
        this.crud = new CachedCrudAssist<T>(getStore(), type, project, entityType()) {
            @SuppressWarnings("unchecked")
            @Override
            protected T initEntityAfterReload(T entity, String resourceName) {
                return entity;
            }
        };
        this.crud.setCheckCopyOnWrite(true);
    }

    public String getResourcePath(String resourceName) {
        return crud.resourcePath(resourceName);
    }

    @SuppressWarnings("unchecked")
    protected T save(T entity) {
        return crud.save(entity);
    }

    public T copy(T entity) {
        return crud.copyBySerialization(entity);
    }
    
    protected T copyForWrite(T entity) {
        return crud.copyForWrite(entity);
    }

    public T createAS(T entity) {
        if (entity.getUuid() == null)
            throw new IllegalArgumentException();
        T copy = copyForWrite(entity);
        if (crud.contains(copy.resourceName()))
            throw new IllegalArgumentException("Entity '" + entity.getUuid() + "' already exists");
        // overwrite point
        return save(copy);
    }

    // Read
    public List<T> listAll() {
        return Lists.newArrayList(crud.listAll());
    }

    public List<T> listByFilter(RawResourceFilter filter) {
        return Lists.newArrayList(crud.listByFilter(filter));
    }

    public Optional<T> get(String resourceName) {
        if (StringUtils.isEmpty(resourceName)) {
            return Optional.empty();
        }
        return Optional.ofNullable(crud.get(resourceName));
    }

    //Update
    protected T internalUpdate(T entity) {
        if (entity.isCachedAndShared())
            throw new IllegalStateException();

        if (entity.getUuid() == null)
            throw new IllegalArgumentException();

        String name = entity.resourceName();
        if (!crud.contains(name))
            throw new IllegalArgumentException("Entity '" + name + "' does not exist.");

        return save(entity);
    }

    public T update(String resourceName, Consumer<T> updater) {
        return get(resourceName).map(this::copyForWrite).map(copied -> {
            updater.accept(copied);
            return internalUpdate(copied);
        }).orElse(null);
    }

    public T upsert(String resourceName, Consumer<T> updater, Supplier<T> creator) {
        return get(resourceName).map(this::copyForWrite).map(copied -> {
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
