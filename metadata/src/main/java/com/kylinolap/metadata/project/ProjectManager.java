/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.metadata.project;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.kylinolap.metadata.model.*;
import com.kylinolap.metadata.realization.IRealization;
import com.kylinolap.metadata.realization.RealizationRegistry;
import com.kylinolap.metadata.realization.RealizationType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.common.restclient.SingleValueCache;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author xduo
 */
public class ProjectManager {
    private static final Logger logger = LoggerFactory.getLogger(ProjectManager.class);
    private static final ConcurrentHashMap<KylinConfig, ProjectManager> CACHE = new ConcurrentHashMap<KylinConfig, ProjectManager>();
    private static final Serializer<ProjectInstance> PROJECT_SERIALIZER = new JsonSerializer<ProjectInstance>(ProjectInstance.class);

    private KylinConfig config;
    // project name => ProjrectDesc
    private SingleValueCache<String, ProjectInstance> projectMap = new SingleValueCache<String, ProjectInstance>(Broadcaster.TYPE.PROJECT);
    // project name => tables
    private Multimap<String, ProjectTable> projectTables = Multimaps.synchronizedMultimap(HashMultimap.<String, ProjectTable> create());

    public static ProjectManager getInstance(KylinConfig config) {
        ProjectManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (ProjectManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new ProjectManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init ProjectManager from " + config, e);
            }
        }
    }

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    private ProjectManager(KylinConfig config) throws IOException {
        logger.info("Initializing ProjectManager with metadata url " + config);
        this.config = config;

        loadAllProjects();
    }

    public List<ProjectInstance> listAllProjects() {
        return new ArrayList<ProjectInstance>(projectMap.values());
    }

    public List<ProjectInstance> getProjects(RealizationType type, String realizationName) {
        return this.findProjects(type, realizationName);
    }

    public ProjectInstance dropProject(String projectName) throws IOException {
        if (projectName == null)
            throw new IllegalArgumentException("Project name not given");

        ProjectInstance projectInstance = getProject(projectName);

        if (projectInstance == null) {
            throw new IllegalStateException("The project named " + projectName + " does not exist");
        }

        if (projectInstance.getRealizationCount(null) != 0) {
            throw new IllegalStateException("The project named " + projectName + " can not be deleted because there's still realizations in it. Delete them first.");
        }

        logger.info("Dropping project '" + projectInstance.getName() + "'");
        deleteResource(projectInstance);

        return projectInstance;
    }

    public ProjectInstance getProject(String projectName) {
        if (projectName == null)
            return null;
        projectName = ProjectInstance.getNormalizedProjectName(projectName);
        return projectMap.get(projectName);
    }

    public ProjectInstance createProject(String projectName, String owner, String description) throws IOException {

        logger.info("Creating project '" + projectName);

        ProjectInstance currentProject = getProject(projectName);
        if (currentProject == null) {
            currentProject = ProjectInstance.create(projectName, owner, description, null);
        } else {
            throw new IllegalStateException("The project named " + projectName + "already exists");
        }

        saveResource(currentProject);

        return currentProject;
    }

    public ProjectInstance updateProject(ProjectInstance project, String newName, String newDesc) throws IOException {
        if (!project.getName().equals(newName)) {
            ProjectInstance newProject = this.createProject(newName, project.getOwner(), newDesc);
            newProject.setCreateTime(project.getCreateTime());
            newProject.recordUpdateTime(System.currentTimeMillis());
            newProject.setRealizationEntries(project.getRealizationEntries());

            deleteResource(project);
            saveResource(newProject);

            return newProject;
        } else {
            project.setName(newName);
            project.setDescription(newDesc);

            if (project.getUuid() == null)
                project.updateRandomUuid();

            saveResource(project);

            return project;
        }
    }

    public ProjectInstance updateRealizationToProject(RealizationType type, String realizationName, String newProjectName, String owner) throws IOException {
        removeRealizationsFromProjects(type, realizationName);
        return addRealizationToProject(type, realizationName, newProjectName, owner);
    }

    private ProjectInstance addRealizationToProject(RealizationType type, String realizationName, String project, String user) throws IOException {
        String newProjectName = ProjectInstance.getNormalizedProjectName(project);
        ProjectInstance newProject = getProject(newProjectName);
        if (newProject == null) {
            newProject = this.createProject(newProjectName, user, "This is a project automatically added when adding realization " + realizationName + "(" + type + ")");
        }
        newProject.addRealizationEntry(type, realizationName);
        saveResource(newProject);

        return newProject;
    }

    ////////////////////////////////////////////////////////
    // project table related

    public ProjectInstance addTableDescToProject(String tables, String projectName) throws IOException {
        ProjectInstance projectInstance = getProject(projectName);
        String[] tokens = StringUtils.split(tables, ",");
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i].trim();
            if (StringUtils.isNotEmpty(token)) {
                projectInstance.addTable(token);
            }
        }

        List<TableDesc> exposedTables = listExposedTables(projectName);
        for (TableDesc table : exposedTables) {
            projectInstance.addTable(table.getName());
        }

        saveResource(projectInstance);
        return projectInstance;
    }

    public List<TableDesc> listDefinedTablesInProject(String project) throws IOException {
        if (null == project) {
            return Collections.emptyList();
        }
        project = ProjectInstance.getNormalizedProjectName(project);
        ProjectInstance projectInstance = getProject(project);
        int originTableCount = projectInstance.getTablesCount();
        //sync exposed table to project when list
        List<TableDesc> exposedTables = listExposedTables(project);
        for (TableDesc table : exposedTables) {
            projectInstance.addTable(table.getName());
        }
        //only save project json if new tables are sync in
        if (originTableCount < projectInstance.getTablesCount()) {
            saveResource(projectInstance);
        }

        List<TableDesc> tables = Lists.newArrayList();
        for (String table : projectInstance.getTables()) {
            TableDesc tableDesc = getMetadataManager().getTableDesc(table);
            if (tableDesc != null) {
                tables.add(tableDesc);
            }
        }

        return tables;
    }

    //
    ////////////////////////////////////////////////////////

    public void removeRealizationsFromProjects(RealizationType type, String realizationName) throws IOException {
        for (ProjectInstance projectInstance : findProjects(type, realizationName)) {
            projectInstance.removeRealization(type, realizationName);
            saveResource(projectInstance);
        }
    }

    public List<TableDesc> listExposedTables(String project) {
        project = ProjectInstance.getNormalizedProjectName(project);
        List<TableDesc> tables = Lists.newArrayList();

        for (ProjectTable table : projectTables.get(project)) {
            TableDesc tableDesc = getMetadataManager().getTableDesc(table.getName());
            if (tableDesc != null) {
                tables.add(tableDesc);
            }
        }

        return tables;
    }

    public List<ColumnDesc> listExposedColumns(String project, String table) {
        project = ProjectInstance.getNormalizedProjectName(project);

        MetadataManager metaMgr = getMetadataManager();
        TableDesc tableDesc = metaMgr.getTableDesc(table);
        List<ColumnDesc> columns = Lists.newArrayList();

        for (String column : this.getProjectTable(project, table).getColumns()) {
            columns.add(tableDesc.findColumnByName(column));
        }

        return columns;
    }

    public boolean isExposedTable(String project, String table) {
        project = ProjectInstance.getNormalizedProjectName(project);

        return projectTables.containsEntry(project, new ProjectTable(table));
    }

    public boolean isExposedColumn(String project, String table, String col) {
        project = ProjectInstance.getNormalizedProjectName(project);

        return getProjectTable(project, table).getColumns().contains(col);
    }

    public List<IRealization> listAllRealizations(String project, RealizationType type) {
        project = ProjectInstance.getNormalizedProjectName(project);

        HashSet<IRealization> ret = new HashSet<IRealization>();

        ProjectInstance projectInstance = getProject(project);
        if (projectInstance != null) {
            for (RealizationEntry dm : projectInstance.getRealizationEntries()) {
                if (dm.getType() == type || type == null) {//type == null means any type
                    RealizationRegistry registry = RealizationRegistry.getInstance(config);
                    registry.loadRealizations();
                    IRealization realization = registry.getRealization(dm.getType(), dm.getRealization());
                    ret.add(realization);
                }
            }
        }

        return Lists.newArrayList(ret);
    }

    public List<IRealization> listAllRealizations(String project) {
        return listAllRealizations(project, null);
    }

    public List<IRealization> getRealizationsByTable(String project, String tableName) {
        project = ProjectInstance.getNormalizedProjectName(project);
        tableName = tableName.toUpperCase();
        List<IRealization> realizations = new ArrayList<IRealization>();

        ProjectTable projectTable = getProjectTable(project, tableName);
        realizations.addAll(projectTable.getRealizations());

        return realizations;
    }

    public List<IRealization> getOnlineRealizationByFactTable(String project, String factTableName) {
        project = ProjectInstance.getNormalizedProjectName(project);
        factTableName = factTableName.toUpperCase();
        List<IRealization> realizations = new ArrayList<IRealization>();
        ProjectTable projectTable = this.getProjectTable(project, factTableName);
        for (IRealization realization : projectTable.getRealizations()) {
            if (realization.getFactTable().equalsIgnoreCase(factTableName) && realization.isReady()) {
                realizations.add(realization);
            }
        }

        return realizations;
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String factTable) {
        factTable = factTable.toUpperCase();
        List<MeasureDesc> result = Lists.newArrayList();

        for (IRealization realization : getProjectTable(project, factTable).getRealizations()) {
            if (realization.isReady() == false)
                continue;
            if (!realization.getFactTable().equalsIgnoreCase(factTable))
                continue;

            for (MeasureDesc m : realization.getMeasures()) {
                FunctionDesc func = m.getFunction();
                if (func.needRewrite())
                    result.add(m);
            }
        }

        return result;
    }

    public void loadProjectCache(ProjectInstance project, boolean triggerUpdate) throws IOException {

        //TODO project instance inited twice here
        loadProject(project.getResourcePath(), triggerUpdate);
        loadTables(project.getResourcePath());
    }

    public void removeProjectCache(ProjectInstance project) {
        String projectName = ProjectInstance.getNormalizedProjectName(project.getName());
        if (projectMap.containsKey(projectName)) {
            projectMap.remove(projectName);
            projectTables.removeAll(projectName);
        }
    }

    private void mapTableToRealization(ProjectInstance projectInstance, IRealization realization) {

        List<TblColRef> allColumns = realization.getAllColumns();
        if (allColumns == null || allColumns.size() == 0) {
            logger.warn("No columns found from realization '" + realization.getName());
            return;
        }

        for (TblColRef tblColRef : allColumns) {
            String project = ProjectInstance.getNormalizedProjectName(projectInstance.getName());

            if (this.getMetadataManager().getTableDesc(tblColRef.getTable()) == null) {
                throw new RuntimeException("Table Desc for " + tblColRef.getTable() + "does not exist");
            }

            ProjectTable factProjTable = this.getProjectTable(project, tblColRef.getTable(), true);
            if (!factProjTable.getRealizations().contains(realization)) {
                factProjTable.getRealizations().add(realization);
            }
        }
    }

    private List<ProjectInstance> findProjects(RealizationType type, String realizationName) {
        List<ProjectInstance> projects = Lists.newArrayList();
        for (ProjectInstance projectInstance : projectMap.values()) {
            if (projectInstance.containsRealization(type, realizationName)) {
                projects.add(projectInstance);
            }
        }

        return projects;
    }

    public synchronized ProjectInstance loadProject(String path, boolean triggerUpdate) throws IOException {
        ResourceStore store = getStore();

        ProjectInstance projectInstance = store.getResource(path, ProjectInstance.class, PROJECT_SERIALIZER);
        projectInstance.init();

        if (StringUtils.isBlank(projectInstance.getName()))
            throw new IllegalStateException("Project name must not be blank");

        if (triggerUpdate) {
            projectMap.put(projectInstance.getName().toUpperCase(), projectInstance);
        } else {
            projectMap.putLocal(projectInstance.getName().toUpperCase(), projectInstance);
        }

        return projectInstance;
    }

    private synchronized void loadTables(String path) throws IOException {
        ResourceStore store = getStore();

        ProjectInstance projectInstance = store.getResource(path, ProjectInstance.class, PROJECT_SERIALIZER);
        projectInstance.init();

        String project = ProjectInstance.getNormalizedProjectName(projectInstance.getName());
        projectTables.removeAll(project);

        for (IRealization realization : this.listAllRealizations(projectInstance.getName())) {
            markExposedTablesAndColumns(projectInstance.getName(), realization);
            mapTableToRealization(projectInstance, realization);
        }
    }

    private void loadAllProjects() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.PROJECT_RESOURCE_ROOT, ".json");

        logger.debug("Loading Project from folder " + store.getReadableResourcePath(ResourceStore.PROJECT_RESOURCE_ROOT));

        for (String path : paths) {
            loadProject(path, false);
            loadTables(path);
        }

        logger.debug("Loaded " + paths.size() + " Project(s)");
    }

    private void saveResource(ProjectInstance proj) throws IOException {
        ResourceStore store = getStore();
        store.putResource(proj.getResourcePath(), proj, PROJECT_SERIALIZER);
        afterProjectUpdated(proj);
    }

    private void deleteResource(ProjectInstance proj) throws IOException {
        ResourceStore store = getStore();
        store.deleteResource(proj.getResourcePath());
        this.afterProjectDropped(proj);
    }

    private void afterProjectUpdated(ProjectInstance updatedProject) {
        try {
            this.loadProjectCache(updatedProject, true);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
    }

    private void afterProjectDropped(ProjectInstance droppedProject) {
        this.removeProjectCache(droppedProject);
    }

    // sync on update
    private void markExposedTablesAndColumns(String projectName, IRealization realization) {
        if (!realization.isReady())
            return;

        //TODO removed redunctant code here, check correctness

        for (TblColRef col : realization.getAllColumns()) {
            markExposedTableAndColumn(projectName, col.getTable(), col.getName());
        }
    }

    private void markExposedTableAndColumn(String project, String table, String column) {
        project = ProjectInstance.getNormalizedProjectName(project);
        TableDesc t = this.getMetadataManager().getTableDesc(table);
        if (t == null)
            throw new IllegalStateException("No SourceTable found by name '" + table);

        ProjectTable projTable = getProjectTable(project, table, true);

        ColumnDesc srcCol = t.findColumnByName(column);
        if (srcCol == null)
            throw new IllegalStateException("No SourceColumn found by name '" + table + "/" + column);

        if (!projTable.getColumns().contains(srcCol.getName())) {
            projTable.getColumns().add(srcCol.getName());
        }
    }

    private ProjectTable getProjectTable(String project, final String table) {
        return getProjectTable(project, table, false);
    }

    private ProjectTable getProjectTable(String project, final String table, boolean autoCreate) {
        String tableIdentity = TableDesc.getTableIdentity(table);

        ProjectTable projectTable = null;
        project = ProjectInstance.getNormalizedProjectName(project);

        if (this.projectTables.containsEntry(project, new ProjectTable(tableIdentity))) {
            Iterator<ProjectTable> projsIter = this.projectTables.get(project).iterator();
            while (projsIter.hasNext()) {
                ProjectTable oneTable = projsIter.next();
                if (oneTable.getName().equalsIgnoreCase(tableIdentity)) {
                    projectTable = oneTable;
                    break;
                }
            }
        } else {
            projectTable = new ProjectTable(tableIdentity);

            if (autoCreate) {
                this.projectTables.put(project, projectTable);
            }
        }

        return projectTable;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }
}
