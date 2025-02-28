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

package org.apache.kylin.metadata.project;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hystrix.NCircuitBreaker;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class NProjectManager {
    private static final Logger logger = LoggerFactory.getLogger(NProjectManager.class);

    public static NProjectManager getInstance(KylinConfig config) {
        return config.getManager(NProjectManager.class);
    }

    // called by reflection
    static NProjectManager newInstance(KylinConfig config) {
        return new NProjectManager(config);
    }

    // ============================================================================

    private final KylinConfig config;
    private final NProjectLoader projectLoader;

    private final CachedCrudAssist<ProjectInstance> crud;

    public NProjectManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NProjectManager with KylinConfig Id: {}", System.identityHashCode(config));
        this.config = config;
        this.projectLoader = new NProjectLoader(config);
        crud = new CachedCrudAssist<ProjectInstance>(getStore(), MetadataType.PROJECT, null, ProjectInstance.class) {
            @Override
            protected ProjectInstance initEntityAfterReload(ProjectInstance entity, String projectName) {
                entity.init(config);
                return entity;
            }
        };
    }

    public List<ProjectInstance> listAllProjects() {
        return crud.listAll();
    }

    public void reloadAll() {
        crud.reloadAll();
    }

    public void invalidCache(String resourceName) {
        crud.invalidateCache(resourceName);
    }

    public ProjectInstance getProject(String projectName) {
        return crud.get(projectName);
    }

    public ProjectInstance getProjectById(String projectId) {
        return crud.listByFilter(RawResourceFilter.equalFilter("uuid", projectId)).stream().findAny().orElse(null);
    }

    public ProjectInstance createProject(String projectName, String owner, String description,
            LinkedHashMap<String, String> overrideProps) {
        logger.info("Creating project " + projectName);

        ProjectInstance currentProject = getProject(projectName);
        if (currentProject == null) {
            //circuit breaker
            NCircuitBreaker.verifyProjectCreation(listAllProjects().size());

            currentProject = copyForWrite(ProjectInstance.create(projectName, owner, description, overrideProps));
            currentProject.initConfig(config);
        } else {
            throw new IllegalStateException("The project named " + projectName + "already exists");
        }
        checkOverrideProps(currentProject);
        return save(currentProject);
    }

    private void checkOverrideProps(ProjectInstance prj) {
        LinkedHashMap<String, String> overrideProps = prj.getOverrideKylinProps();

        if (overrideProps == null) {
            return;
        }
        for (Map.Entry<String, String> entry : overrideProps.entrySet()) {
            if (StringUtils.isAnyBlank(entry.getKey(), entry.getValue())) {
                throw new IllegalStateException("Property key/value must not be blank");
            }
        }
    }

    public ProjectInstance dropProject(String projectName) {
        if (StringUtils.isEmpty(projectName))
            throw new IllegalArgumentException("Project name not given");

        ProjectInstance projectInstance = getProject(projectName);

        if (projectInstance == null) {
            throw new IllegalStateException("The project named " + projectName + " does not exist");
        }

        if (projectInstance.getRealizationCount(null) != 0) {
            throw new IllegalStateException("The project named " + projectName
                    + " can not be deleted because there's still realizations in it. Delete them first.");
        }

        logger.info("Dropping project '" + projectInstance.getName() + "'");
        crud.delete(projectName);
        return projectInstance;
    }

    // update project itself
    public ProjectInstance updateProject(ProjectInstance project, String newName, String newDesc,
            LinkedHashMap<String, String> overrideProps) {
        Preconditions.checkArgument(project.getName().equals(newName));
        return updateProject(newName, copyForWrite -> {
            copyForWrite.setName(newName);
            copyForWrite.setDescription(newDesc);
            copyForWrite.setOverrideKylinProps(overrideProps);
            if (copyForWrite.getUuid() == null)
                copyForWrite.updateRandomUuid();
        });
    }

    private ProjectInstance updateProject(ProjectInstance project) {
        if (getProject(project.getName()) == null) {
            throw new IllegalArgumentException("Project '" + project.getName() + "' does not exist!");
        }
        return save(project);
    }

    public ProjectInstance copyForWrite(ProjectInstance projectInstance) {
        Preconditions.checkNotNull(projectInstance);
        return crud.copyForWrite(projectInstance);
    }

    private ProjectInstance save(ProjectInstance prj) {
        Preconditions.checkArgument(prj != null);
        if (getStore().getConfig().isCheckCopyOnWrite()) {
            if (prj.isCachedAndShared()) {
                throw new IllegalStateException(
                        "Copy-on-write violation! The updating entity " + prj + " is a shared object in "
                                + ProjectInstance.class.getSimpleName() + " cache, which should not be.");
            }
        }
        crud.save(prj);
        return prj;
    }

    public String getDefaultDatabase(String project) {
        return getProject(project).getDefaultDatabase();
    }

    public Set<IRealization> listAllRealizations(String project) {
        return projectLoader.listAllRealizations(project);
    }

    public List<NDataModel> listHealthyModels(String project) {
        return listAllRealizations(project).stream().map(IRealization::getModel).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String factTable) {
        return projectLoader.listEffectiveRewriteMeasures(project, factTable);
    }

    KylinConfig getConfig() {
        return config;
    }

    ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public void forceDropProject(String project) {
        if (StringUtils.isEmpty(project))
            throw new IllegalArgumentException("Project name not given");

        ProjectInstance projectInstance = getProject(project);

        if (projectInstance == null) {
            throw new IllegalStateException("The project named " + project + " does not exist");
        }
        RawResourceFilter filter = RawResourceFilter.simpleFilter(RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE,
                "project", project);
        MetadataType.NON_GLOBAL_METADATA_TYPE.forEach(type -> {
            val paths = Optional.ofNullable(getStore().listResourcesRecursively(type.name(), filter))
                    .orElse(Sets.newTreeSet());
            for (val path : paths) {
                getStore().deleteResource(path);
            }
        });
        crud.delete(project);
    }

    public interface NProjectUpdater {
        void modify(ProjectInstance copyForWrite);
    }

    public ProjectInstance updateProject(String project, NProjectUpdater updater) {
        val cached = getProject(project);
        val copy = copyForWrite(cached);
        updater.modify(copy);
        return updateProject(copy);
    }

    public static KylinConfig getProjectConfig(String project) {
        KylinConfig envConfig = KylinConfig.getInstanceFromEnv();
        if (project == null) {
            logger.warn("The project name is null, return KylinCong.getInstanceFromEnv() instead.");
            return envConfig;
        }
        ProjectInstance projectInstance = NProjectManager.getInstance(envConfig).getProject(project);
        return projectInstance == null ? envConfig : projectInstance.getConfig();
    }

    public static Set<IRealization> getRealizations(KylinConfig config, String project, String tableName) {
        return getInstance(config).projectLoader.getRealizationsByTable(project, tableName.toUpperCase(Locale.ROOT));
    }
}
