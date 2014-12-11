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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.kylinolap.metadata.project.ProjectInstance;
import com.kylinolap.metadata.project.ProjectManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.cube.model.MeasureDesc;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.TblColRef;

/**
 * @author xduo
 */
public class CubeRealizationManager {
    private static final Logger logger = LoggerFactory.getLogger(CubeRealizationManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, CubeRealizationManager> CACHE = new ConcurrentHashMap<KylinConfig, CubeRealizationManager>();

    private KylinConfig config;
    // project name => tables
    private Multimap<String, ProjectTable> projectTables = Multimaps.synchronizedMultimap(HashMultimap.<String, ProjectTable>create());

    private CubeRealizationManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeManager with metadata url " + config);
        this.config = config;

        loadAllProjects();
    }

    public static CubeRealizationManager getInstance(KylinConfig config) {
        CubeRealizationManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (CubeRealizationManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new CubeRealizationManager(config);
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

    public boolean isCubeInProject(String projectName, CubeInstance cube) {
        return this.listAllCubes(projectName).contains(cube);
    }

    public ProjectInstance addTablesToProject(String tables, String projectName) throws IOException {
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
            projectInstance.addTable(table.getIdentity());
        }

        saveResource(projectInstance);
        return projectInstance;
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
        if(null == project){
            return Collections.emptyList();
        }
        project = ProjectInstance.getNormalizedProjectName(project);
        ProjectInstance projectInstance = getProject(project);
        int originTableCount = projectInstance.getTablesCount();
        //sync exposed table to project when list
        List<TableDesc> exposedTables = listExposedTables(project);
        for (TableDesc table : exposedTables) {
            projectInstance.addTable(TableDesc.getTableIdentity(table));
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

    public List<CubeInstance> listAllCubes(String projectName) {
        return listAllCubes(ProjectManager.getInstance(config).getProject(projectName));
    }

    public List<CubeInstance> listAllCubes(ProjectInstance projectInstance) {
        if (projectInstance == null) {
            return Collections.emptyList();
        }
        HashSet<CubeInstance> ret = new HashSet<CubeInstance>();
        for (String cubeName : projectInstance.getCubes()) {
            CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
            if (null != cube) {
                ret.add(cube);
            } else {
                logger.error("Failed to load cube " + cubeName);
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
            if (cube.getDescriptor().getModel().isFactTable(factTableName) && cube.isReady()) {
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
            if (cube.getDescriptor().getModel().isFactTable(factTable) == false)
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

    public void unloadProject(ProjectInstance project) {
        String projectName = ProjectInstance.getNormalizedProjectName(project.getName());
        if (projectTables.containsKey(projectName)) {
            projectTables.removeAll(projectName);
        }
    }

    public void loadProject(ProjectInstance project) throws IOException {
        loadTables(project);
    }

    public final void loadAllProjects() throws IOException {
        List<ProjectInstance> projectInstances = ProjectManager.getInstance(config).listAllProjects();
        for (ProjectInstance projectInstance : projectInstances) {
            loadTables(projectInstance);
        }

        logger.debug("Loaded " + projectInstances.size() + " Project(s)");
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

    private List<ProjectInstance> findProjectsByCubeName(String cubeName) {
        List<ProjectInstance> projects = new ArrayList<ProjectInstance>();
        for (ProjectInstance projectInstance : getProjectManager().listAllProjects()) {
            if (projectInstance.containsCube(cubeName)) {
                projects.add(projectInstance);
            }
        }

        return projects;
    }

    private synchronized void loadTables(ProjectInstance projectInstance) throws IOException {
        String project = ProjectInstance.getNormalizedProjectName(projectInstance.getName());
        projectTables.removeAll(project);

        for (CubeInstance cubeInstance : this.listAllCubes(projectInstance.getName())) {
            markExposedTablesAndColumns(projectInstance.getName(), cubeInstance);
            mapTableToCube(projectInstance, cubeInstance);
        }
    }

    private ProjectInstance getProject(String projectName) {
        return ProjectManager.getInstance(config).getProject(projectName);
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
        if (t == null) {
            throw new IllegalStateException("No SourceTable found by name '" + table + "', ref by " + refObj);
        }
        table = t.getIdentity(); // ensures upper case

        ProjectTable projTable = getProjectTable(project, table, true);

        ColumnDesc srcCol = t.findColumnByName(column);
        if (srcCol == null) {
            throw new IllegalStateException("No SourceColumn found by name '" + table + "/" + column + "', ref by " + refObj);
        }

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
            Collection<ProjectTable> projects = this.projectTables.get(project);
            for (ProjectTable pt : projects) {
                if (pt.getName().equalsIgnoreCase(table)) {
                    projectTable = pt;
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

    private void saveResource(ProjectInstance projectInstance) throws IOException {
        getProjectManager().updateProject(projectInstance);
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private ProjectManager getProjectManager() {
        return ProjectManager.getInstance(config);
    }

}
