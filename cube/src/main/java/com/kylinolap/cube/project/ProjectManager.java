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

package com.kylinolap.cube.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.DimensionDesc;
import com.kylinolap.metadata.model.cube.FunctionDesc;
import com.kylinolap.metadata.model.cube.JoinDesc;
import com.kylinolap.metadata.model.cube.MeasureDesc;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;

/**
 * @author xduo
 * 
 */
public class ProjectManager {
    private static final Logger logger = LoggerFactory.getLogger(ProjectManager.class);

    // static cached instances
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
                throw new IllegalStateException("Failed to init CubeManager from " + config, e);
            }
        }
    }

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    private ProjectManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeManager with metadata url " + config);
        this.config = config;

        loadAllProjects();
    }

    public static String getDefaultProjectName() {
        return ProjectInstance.DEFAULT_PROJECT_NAME;
    }

    public List<ProjectInstance> listAllProjects() {
        return new ArrayList<ProjectInstance>(projectMap.values());
    }

    public List<ProjectInstance> getProjects(String cubeName) {
        return this.findProjects(cubeName);
    }

    public ProjectInstance dropProject(String projectName) throws IOException {
        if (projectName == null)
            throw new IllegalArgumentException("Project name not given");

        ProjectInstance projectInstance = getProject(projectName);

        if (projectInstance == null) {
            throw new IllegalStateException("The project named " + projectName + " does not exist");
        }

        if (projectInstance.getCubes().size() != 0) {
            throw new IllegalStateException("The project named " + projectName + " can not be deleted because there's still cubes in it. Delete all the cubes first.");
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
            newProject.setCubes(project.getCubes());

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

    public boolean isCubeInProject(String projectName, CubeInstance cube) {
        return this.listAllCubes(projectName).contains(cube);
    }

    public ProjectInstance updateCubeToProject(String cubeName, String newProjectName, String owner) throws IOException {
        removeCubeFromProjects(cubeName);

        return addCubeToProject(cubeName, newProjectName, owner);
    }
    
    public ProjectInstance updateTableToProject(String tables,String projectName) throws IOException {
        ProjectInstance projectInstance = getProject(projectName);
        String[] tokens = StringUtils.split(tables, ",");
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i].trim();
            if (StringUtils.isNotEmpty(token)) {
                projectInstance.addTable(token);
            }
        }
        
        List<TableDesc> exposedTables = listExposedTables(projectName);
        for(TableDesc table : exposedTables){
            projectInstance.addTable(table.getName());
        }
        
        saveResource(projectInstance);
        return projectInstance;
    }
   
    
    public void removeCubeFromProjects(String cubeName) throws IOException {
        for (ProjectInstance projectInstance : findProjects(cubeName)) {
            projectInstance.removeCube(cubeName);

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

    
    
    public List<TableDesc> listDefinedTablesInProject(String project) throws IOException {
        project = ProjectInstance.getNormalizedProjectName(project);
        ProjectInstance projectInstance = getProject(project);
        
        //sync exposed table to project when list
        List<TableDesc> exposedTables = listExposedTables(project);
        for(TableDesc table : exposedTables){
            projectInstance.addTable(table.getName());
        }
        saveResource(projectInstance);
        
        List<TableDesc> tables = Lists.newArrayList();
        for (String table : projectInstance.getTables()) {
            TableDesc tableDesc = getMetadataManager().getTableDesc(table);
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

    public List<CubeInstance> listAllCubes(String project) {
        project = ProjectInstance.getNormalizedProjectName(project);

        HashSet<CubeInstance> ret = new HashSet<CubeInstance>();

        ProjectInstance projectInstance = getProject(project);
        if (projectInstance != null) {
            for (String cubeName : projectInstance.getCubes()) {
                CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
                if (null != cube) {
                    ret.add(cube);
                } else {
                    logger.error("Failed to load cube " + cubeName);
                }
            }
        }

        return new ArrayList<CubeInstance>(ret);
    }
    

    public List<CubeInstance> getCubesByTable(String project, String tableName) {
        project = ProjectInstance.getNormalizedProjectName(project);
        tableName = tableName.toUpperCase();
        List<CubeInstance> cubes = new ArrayList<CubeInstance>();

        ProjectTable projectTable = getProjectTable(project, tableName);
        cubes.addAll(projectTable.getCubes());

        return cubes;
    }

    public List<CubeInstance> getOnlineCubesByFactTable(String project, String factTableName) {
        project = ProjectInstance.getNormalizedProjectName(project);
        factTableName = factTableName.toUpperCase();
        List<CubeInstance> cubes = new ArrayList<CubeInstance>();
        ProjectTable projectTable = this.getProjectTable(project, factTableName);
        for (CubeInstance cube : projectTable.getCubes()) {
            if (cube.getDescriptor().isFactTable(factTableName) && cube.isReady()) {
                cubes.add(cube);
            }
        }

        return cubes;
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String factTable) {
        factTable = factTable.toUpperCase();

        HashSet<CubeDesc> relatedDesc = new HashSet<CubeDesc>();
        for (CubeInstance cube : getProjectTable(project, factTable).getCubes()) {
            if (cube.isReady() == false)
                continue;
            if (cube.getDescriptor().isFactTable(factTable) == false)
                continue;

            relatedDesc.add(cube.getDescriptor());
        }

        List<MeasureDesc> result = Lists.newArrayList();
        for (CubeDesc desc : relatedDesc) {
            for (MeasureDesc m : desc.getMeasures()) {
                FunctionDesc func = m.getFunction();
                if (func.needRewrite())
                    result.add(m);
            }
        }

        return result;
    }

    public void loadProjectCache(ProjectInstance project, boolean triggerUpdate) throws IOException {
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

    private void mapTableToCube(ProjectInstance projectInstance, CubeInstance cubeInstance) {
        // schema sanity check
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        if (cubeDesc == null) {
            logger.warn("No CubeDesc found by name '" + cubeInstance.getDescName() + "'");
            return;
        }

        // table ==> cube mapping
        String factTable = cubeDesc.getFactTable();
        logger.debug("Fact Table: " + factTable + " -- Cube: " + cubeInstance.getName());
        assert this.getMetadataManager().getTableDesc(factTable) != null;

        String project = ProjectInstance.getNormalizedProjectName(projectInstance.getName());
        ProjectTable factProjTable = this.getProjectTable(project, factTable, true);
        if (!factProjTable.getCubes().contains(cubeInstance)) {
            factProjTable.getCubes().add(cubeInstance);
        }

        for (DimensionDesc d : cubeDesc.getDimensions()) {
            String lookupTable = d.getTable();
            logger.debug("Lookup Table: " + lookupTable + " -- Cube: " + cubeInstance.getName());
            assert this.getMetadataManager().getTableDesc(lookupTable) != null;

            ProjectTable dimensionProjTable = this.getProjectTable(project, lookupTable);
            if (!dimensionProjTable.getCubes().contains(cubeInstance)) {
                dimensionProjTable.getCubes().add(cubeInstance);
            }
        }
    }

    private List<ProjectInstance> findProjects(String cubeName) {
        List<ProjectInstance> projects = new ArrayList<ProjectInstance>();
        for (ProjectInstance projectInstance : projectMap.values()) {
            if (projectInstance.containsCube(cubeName)) {
                projects.add(projectInstance);
            }
        }

        return projects;
    }

    private synchronized ProjectInstance loadProject(String path, boolean triggerUpdate) throws IOException {
        ResourceStore store = getStore();
        logger.debug("Loading CubeInstance " + store.getReadableResourcePath(path));

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
        logger.debug("Loading CubeInstance " + store.getReadableResourcePath(path));

        ProjectInstance projectInstance = store.getResource(path, ProjectInstance.class, PROJECT_SERIALIZER);
        projectInstance.init();

        String project = ProjectInstance.getNormalizedProjectName(projectInstance.getName());
        projectTables.removeAll(project);

        for (CubeInstance cubeInstance : this.listAllCubes(projectInstance.getName())) {
            markExposedTablesAndColumns(projectInstance.getName(), cubeInstance);
            mapTableToCube(projectInstance, cubeInstance);
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

    private ProjectInstance addCubeToProject(String cubeName, String project, String user) throws IOException {
        String newProjectName = ProjectInstance.getNormalizedProjectName(project);
        ProjectInstance newProject = getProject(newProjectName);
        if (newProject == null) {
            newProject = this.createProject(newProjectName, user, "This is a project automatically added when adding cube " + cubeName);
        }
        newProject.addCube(cubeName);
        saveResource(newProject);

        return newProject;
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
    private void markExposedTablesAndColumns(String projectName, CubeInstance cubeInstance) {
        if (!cubeInstance.isReady())
            return;

        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        String factTable = cubeDesc.getFactTable();
        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            String lookupTable = dim.getTable();
            JoinDesc join = dim.getJoin();
            if (join == null)
                continue; // for dimensions on fact table, there's no join

            if (join.getForeignKeyColumns() == null) {
                throw new IllegalStateException("Null FK for " + join);
            }
            for (TblColRef fkCol : join.getForeignKeyColumns()) {
                markExposedTableAndColumn(projectName, factTable, fkCol.getName(), dim);
            }
            if (join.getPrimaryKeyColumns() == null) {
                throw new IllegalStateException("Null PK for " + join);
            }
            for (TblColRef pkCol : join.getPrimaryKeyColumns()) {
                markExposedTableAndColumn(projectName, lookupTable, pkCol.getName(), dim);
            }
        }
        for (TblColRef col : cubeDesc.listAllColumns()) {
            markExposedTableAndColumn(projectName, col.getTable(), col.getName(), col);
        }
    }

    private void markExposedTableAndColumn(String project, String table, String column, Object refObj) {
        project = ProjectInstance.getNormalizedProjectName(project);
        TableDesc t = this.getMetadataManager().getTableDesc(table);
        if (t == null)
            throw new IllegalStateException("No SourceTable found by name '" + table + "', ref by " + refObj);
        table = t.getName(); // ensures upper case

        ProjectTable projTable = getProjectTable(project, table, true);

        ColumnDesc srcCol = t.findColumnByName(column);
        if (srcCol == null)
            throw new IllegalStateException("No SourceColumn found by name '" + table + "/" + column + "', ref by " + refObj);

        if (!projTable.getColumns().contains(srcCol.getName())) {
            projTable.getColumns().add(srcCol.getName());
        }
    }

    private ProjectTable getProjectTable(String project, final String table) {
        return getProjectTable(project, table, false);
    }

    private ProjectTable getProjectTable(String project, final String table, boolean autoCreate) {
        ProjectTable projectTable = null;
        project = ProjectInstance.getNormalizedProjectName(project);

        if (this.projectTables.containsEntry(project, new ProjectTable(table))) {
            Iterator<ProjectTable> projsIter = this.projectTables.get(project).iterator();
            while (projsIter.hasNext()) {
                ProjectTable oneTable = projsIter.next();
                if (oneTable.getName().equalsIgnoreCase(table)) {
                    projectTable = oneTable;
                    break;
                }
            }
        } else {
            projectTable = new ProjectTable(table);

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
