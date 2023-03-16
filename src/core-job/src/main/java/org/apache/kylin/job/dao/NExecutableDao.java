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

package org.apache.kylin.job.dao;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import lombok.val;

/**
 */
public class NExecutableDao {

    private static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<>(ExecutablePO.class);
    private static final Logger logger = LoggerFactory.getLogger(NExecutableDao.class);

    public static NExecutableDao getInstance(KylinConfig config, String project) {
        return config.getManager(project, NExecutableDao.class);
    }

    // called by reflection
    static NExecutableDao newInstance(KylinConfig config, String project) {
        return new NExecutableDao(config, project);
    }
    // ============================================================================

    private String project;

    private KylinConfig config;

    private CachedCrudAssist<ExecutablePO> crud;

    private Map<String, ExecutablePO> updating = new HashMap<>();

    private NExecutableDao(KylinConfig config, String project) {
        logger.trace("Using metadata url: {}", config);
        this.project = project;
        this.config = config;
        String resourceRootPath = "/" + project + ResourceStore.EXECUTE_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<ExecutablePO>(getStore(), resourceRootPath, "", ExecutablePO.class) {
            @Override
            protected ExecutablePO initEntityAfterReload(ExecutablePO entity, String resourceName) {
                entity.setProject(project);
                return entity;
            }
        };
    }

    public List<ExecutablePO> getJobs() {
        return crud.listAll();
    }

    public List<ExecutablePO> getPartialJobs(Predicate<String> predicate) {
        return crud.listPartial(predicate);
    }

    public List<ExecutablePO> getJobs(long timeStart, long timeEndExclusive) {
        return crud.listAll().stream()
                .filter(x -> x.getLastModified() >= timeStart && x.getLastModified() < timeEndExclusive)
                .collect(Collectors.toList());
    }

    public List<String> getJobIds() {
        return crud.listAll().stream().sorted(Comparator.comparing(ExecutablePO::getCreateTime))
                .sorted(Comparator.comparing(ExecutablePO::getPriority)).map(RootPersistentEntity::resourceName)
                .collect(Collectors.toList());
    }

    public ExecutablePO getJobByUuid(String uuid) {
        return crud.get(uuid);
    }

    public ExecutablePO addJob(ExecutablePO job) {
        if (getJobByUuid(job.getUuid()) != null) {
            throw new IllegalArgumentException("job id:" + job.getUuid() + " already exists");
        }
        crud.save(job);
        return job;
    }

    // for ut
    @VisibleForTesting
    public void deleteAllJob() {
        getJobs().forEach(job -> deleteJob(job.getId()));
    }

    public void deleteJob(String uuid) {
        crud.delete(uuid);
    }

    public void updateJob(String uuid, Predicate<ExecutablePO> updater) {
        val job = getJobByUuid(uuid);
        Preconditions.checkNotNull(job);
        val copyForWrite = JsonUtil.copyBySerialization(job, JOB_SERIALIZER, null);
        if (updater.test(copyForWrite)) {
            crud.save(copyForWrite);
        }
    }

    public void updateJobWithoutSave(String uuid, Predicate<ExecutablePO> updater) {
        ExecutablePO executablePO;
        if (updating.containsKey(uuid)) {
            executablePO = updating.get(uuid);
        } else {
            ExecutablePO executablePOFromCache = getJobByUuid(uuid);
            Preconditions.checkNotNull(executablePOFromCache);
            val copyForWrite = JsonUtil.copyBySerialization(executablePOFromCache, JOB_SERIALIZER, null);
            updating.put(uuid, copyForWrite);
            executablePO = copyForWrite;
        }
        updater.test(executablePO);
    }

    public void saveUpdatedJob() {
        for (ExecutablePO executablePO : updating.values()) {
            crud.save(executablePO);
        }
        updating = new HashMap<>();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(config);
    }

}
