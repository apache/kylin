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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class ProjectManager {
    private static final Logger logger = LoggerFactory.getLogger(ProjectManager.class);

    public static ProjectManager getInstance(KylinConfig config) {
        return config.getManager(ProjectManager.class);
    }

    // called by reflection
    static ProjectManager newInstance(KylinConfig config) throws IOException {
        return new ProjectManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private ProjectL2Cache l2Cache;

    // project name => ProjectInstance
    private CaseInsensitiveStringCache<ProjectInstance> projectMap;
    private CachedCrudAssist<ProjectInstance> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    private AutoReadWriteLock prjMapLock = new AutoReadWriteLock();

    private ProjectManager(KylinConfig config) throws IOException {
        logger.info("Initializing ProjectManager with metadata url " + config);
        this.config = config;
        this.projectMap = new CaseInsensitiveStringCache<ProjectInstance>(config, "project");
        this.l2Cache = new ProjectL2Cache(this);
        this.crud = new CachedCrudAssist<ProjectInstance>(getStore(), ResourceStore.PROJECT_RESOURCE_ROOT,
                ProjectInstance.class, projectMap) {
            @Override
            protected ProjectInstance initEntityAfterReload(ProjectInstance prj, String resourceName) {
                prj.init();
                return prj;
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new ProjectSyncListener(), "project");
    }

    private class ProjectSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String project = cacheKey;

            if (event == Event.DROP) {
                removeProjectLocal(project);
                return;
            }

            reloadProjectQuietly(project);
            broadcaster.notifyProjectSchemaUpdate(project);
            broadcaster.notifyProjectDataUpdate(project);
        }
    }

    public void clearL2Cache(String projectname) {
        l2Cache.clear(projectname);
    }

    public void reloadProjectL2Cache(String project) {
        l2Cache.reloadCacheByProject(project);
    }

    public ProjectInstance reloadProjectQuietly(String project) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            ProjectInstance prj = crud.reloadQuietly(project);
            clearL2Cache(project);
            return prj;
        }
    }

    public void reloadAll() throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            crud.reloadAll();
            clearL2Cache(null);
        }
    }

    public List<ProjectInstance> listAllProjects() {
        try (AutoLock lock = prjMapLock.lockForRead()) {
            return new ArrayList<ProjectInstance>(projectMap.values());
        }
    }

    public ProjectInstance getProject(String projectName) {
        // Null check is needed for ConcurrentMap does not supporting .get(null)
        if (projectName == null)
            return null;

        try (AutoLock lock = prjMapLock.lockForRead()) {
            return projectMap.get(projectName);
        }
    }

    public ProjectInstance getPrjByUuid(String uuid) {
        try (AutoLock lock = prjMapLock.lockForRead()) {
            for (ProjectInstance prj : projectMap.values()) {
                if (uuid.equals(prj.getUuid()))
                    return prj;
            }
            return null;
        }
    }

    public ProjectInstance createProject(String projectName, String owner, String description,
            LinkedHashMap<String, String> overrideProps) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            logger.info("Creating project " + projectName);

            ProjectInstance currentProject = getProject(projectName);
            if (currentProject == null) {
                currentProject = ProjectInstance.create(projectName, owner, description, overrideProps, null, null);
            } else {
                throw new IllegalStateException("The project named " + projectName + "already exists");
            }
            checkOverrideProps(currentProject);

            return save(currentProject);
        }
    }

    private void checkOverrideProps(ProjectInstance prj) throws IOException {
        LinkedHashMap<String, String> overrideProps = prj.getOverrideKylinProps();

        if (overrideProps != null) {
            Iterator<Map.Entry<String, String>> iterator = overrideProps.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();

                if (StringUtils.isAnyBlank(entry.getKey(), entry.getValue())) {
                    throw new IllegalStateException("Property key/value must not be blank");
                }
            }
        }
    }

    public ProjectInstance dropProject(String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            if (projectName == null)
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

            crud.delete(projectInstance);
            BadQueryHistoryManager.getInstance(config).removeBadQueryHistory(projectName);

            clearL2Cache(projectName);
            return projectInstance;
        }
    }

    // update project itself
    public ProjectInstance updateProject(ProjectInstance project, String newName, String newDesc,
            LinkedHashMap<String, String> overrideProps) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            Preconditions.checkArgument(project.getName().equals(newName));
            project.setName(newName);
            project.setDescription(newDesc);
            project.setOverrideKylinProps(overrideProps);

            if (project.getUuid() == null)
                project.updateRandomUuid();

            return save(project);
        }
    }

    // update project itself
    public ProjectInstance updateProjectOwner(ProjectInstance project, String newOwner) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            project.setOwner(newOwner);

            if (project.getUuid() == null)
                project.updateRandomUuid();

            return save(project);
        }
    }

    public void removeProjectLocal(String proj) {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            projectMap.removeLocal(proj);
            clearL2Cache(proj);
        }
    }

    public ProjectInstance addModelToProject(String modelName, String newProjectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            removeModelFromProjects(modelName);

            ProjectInstance prj = getProject(newProjectName);
            if (prj == null) {
                throw new IllegalArgumentException("Project " + newProjectName + " does not exist.");
            }
            prj.addModel(modelName);

            return save(prj);
        }
    }

    public void removeModelFromProjects(String modelName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            for (ProjectInstance projectInstance : findProjectsByModel(modelName)) {
                projectInstance.removeModel(modelName);
                save(projectInstance);
            }
        }
    }

    public ProjectInstance moveRealizationToProject(RealizationType type, String realizationName, String newProjectName,
            String owner) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            removeRealizationsFromProjects(type, realizationName);
            return addRealizationToProject(type, realizationName, newProjectName, owner);
        }
    }

    private ProjectInstance addRealizationToProject(RealizationType type, String realizationName, String project,
            String user) throws IOException {
        if (StringUtils.isEmpty(project)) {
            throw new IllegalArgumentException("Project name should not be empty.");
        }
        ProjectInstance newProject = getProject(project);
        if (newProject == null) {
            newProject = this.createProject(project, user,
                    "This is a project automatically added when adding realization " + realizationName + "(" + type
                            + ")",
                    null);
        }
        newProject.addRealizationEntry(type, realizationName);
        save(newProject);

        return newProject;
    }

    public void removeRealizationsFromProjects(RealizationType type, String realizationName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            for (ProjectInstance projectInstance : findProjects(type, realizationName)) {
                projectInstance.removeRealization(type, realizationName);
                save(projectInstance);
            }
        }
    }

    public ProjectInstance addTableDescToProject(String[] tableIdentities, String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            TableMetadataManager metaMgr = getTableManager();
            ProjectInstance projectInstance = getProject(projectName);
            for (String tableId : tableIdentities) {
                TableDesc table = metaMgr.getTableDesc(tableId, projectName);
                if (table == null) {
                    throw new IllegalStateException("Cannot find table '" + tableId + "' in metadata manager");
                }
                projectInstance.addTable(table.getIdentity());
            }

            return save(projectInstance);
        }
    }

    public void removeTableDescFromProject(String tableId, String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            TableMetadataManager metaMgr = getTableManager();
            ProjectInstance projectInstance = getProject(projectName);
            TableDesc table = metaMgr.getTableDesc(tableId, projectName);
            if (table == null) {
                throw new IllegalStateException("Cannot find table '" + tableId + "' in metadata manager");
            }

            projectInstance.removeTable(table.getIdentity());
            save(projectInstance);
        }
    }

    public ProjectInstance addExtFilterToProject(String[] filters, String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            TableMetadataManager metaMgr = getTableManager();
            ProjectInstance projectInstance = getProject(projectName);
            for (String filterName : filters) {
                ExternalFilterDesc extFilter = metaMgr.getExtFilterDesc(filterName);
                if (extFilter == null) {
                    throw new IllegalStateException(
                            "Cannot find external filter '" + filterName + "' in metadata manager");
                }
                projectInstance.addExtFilter(filterName);
            }

            return save(projectInstance);
        }
    }

    public void removeExtFilterFromProject(String filterName, String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            TableMetadataManager metaMgr = getTableManager();
            ProjectInstance projectInstance = getProject(projectName);
            ExternalFilterDesc filter = metaMgr.getExtFilterDesc(filterName);
            if (filter == null) {
                throw new IllegalStateException("Cannot find external filter '" + filterName + "' in metadata manager");
            }

            projectInstance.removeExtFilter(filterName);
            save(projectInstance);
        }
    }

    /**
     * change the last project modify time
     * @param projectName
     * @throws IOException
     */
    public void touchProject(String projectName) throws IOException {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            ProjectInstance projectInstance = getProject(projectName);
            save(projectInstance);
        }
    }

    private ProjectInstance save(ProjectInstance prj) throws IOException {
        crud.save(prj);
        clearL2Cache(prj.getName());
        return prj;
    }

    public ProjectInstance getProjectOfModel(String model) {
        try (AutoLock lock = prjMapLock.lockForRead()) {
            for (ProjectInstance prj : projectMap.values()) {
                if (prj.getModels().contains(model))
                    return prj;
            }
            throw new IllegalStateException("No project found for model " + model);
        }
    }

    public List<ProjectInstance> findProjects(RealizationType type, String realizationName) {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            List<ProjectInstance> result = Lists.newArrayList();
            for (ProjectInstance prj : projectMap.values()) {
                for (RealizationEntry entry : prj.getRealizationEntries()) {
                    if (entry.getType().equals(type) && entry.getRealization().equals(realizationName)) {
                        result.add(prj);
                        break;
                    }
                }
            }
            return result;
        }
    }

    public List<ProjectInstance> findProjectsByModel(String modelName) {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            List<ProjectInstance> projects = new ArrayList<ProjectInstance>();
            for (ProjectInstance projectInstance : projectMap.values()) {
                if (projectInstance.containsModel(modelName)) {
                    projects.add(projectInstance);
                }
            }
            return projects;
        }
    }

    public List<ProjectInstance> findProjectsByTable(String tableIdentity) {
        try (AutoLock lock = prjMapLock.lockForWrite()) {
            List<ProjectInstance> projects = new ArrayList<ProjectInstance>();
            for (ProjectInstance projectInstance : projectMap.values()) {
                if (projectInstance.containsTable(tableIdentity)) {
                    projects.add(projectInstance);
                }
            }
            return projects;
        }
    }

    public Map<String, ExternalFilterDesc> listExternalFilterDescs(String project) {
        return l2Cache.listExternalFilterDesc(project);
    }

    public List<TableDesc> listDefinedTables(String project) {
        return l2Cache.listDefinedTables(project);
    }

    private Collection<TableDesc> listExposedTablesByRealizations(String project) {
        return l2Cache.listExposedTables(project);
    }

    public Collection<TableDesc> listExposedTables(String project, boolean exposeMore) {
        if (exposeMore) {
            return listDefinedTables(project);
        } else {
            return listExposedTablesByRealizations(project);
        }
    }

    public List<ColumnDesc> listExposedColumns(String project, TableDesc tableDesc, boolean exposeMore) {
        Set<ColumnDesc> exposedColumns = l2Cache.listExposedColumns(project, tableDesc.getIdentity());

        if (exposeMore) {
            Set<ColumnDesc> dedup = Sets.newHashSet(tableDesc.getColumns());
            dedup.addAll(exposedColumns);
            return Lists.newArrayList(dedup);
        } else {
            return Lists.newArrayList(exposedColumns);
        }
    }

    public Set<IRealization> listAllRealizations(String project) {
        return l2Cache.listAllRealizations(project);
    }

    public Set<IRealization> getRealizationsByTable(String project, String tableName) {
        return l2Cache.getRealizationsByTable(project, tableName.toUpperCase(Locale.ROOT));
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String factTable) {
        return l2Cache.listEffectiveRewriteMeasures(project, factTable.toUpperCase(Locale.ROOT), true);
    }

    public List<MeasureDesc> listEffectiveMeasures(String project, String factTable) {
        return l2Cache.listEffectiveRewriteMeasures(project, factTable.toUpperCase(Locale.ROOT), false);
    }

    KylinConfig getConfig() {
        return config;
    }

    ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    TableMetadataManager getTableManager() {
        return TableMetadataManager.getInstance(config);
    }

}
