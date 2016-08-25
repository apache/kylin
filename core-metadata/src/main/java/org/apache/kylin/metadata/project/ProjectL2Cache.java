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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This is a second level cache that is built on top of first level cached objects,
 * including Realization, TableDesc, ColumnDesc etc, to speed up query time metadata lookup.
 * <p/>
 * On any object update, the L2 cache simply gets wiped out because it's cheap to rebuild.
 */
class ProjectL2Cache {

    private static final Logger logger = LoggerFactory.getLogger(ProjectL2Cache.class);

    private ProjectManager mgr;
    private Map<String, ProjectCache> projectCaches = Maps.newConcurrentMap();

    ProjectL2Cache(ProjectManager mgr) {
        this.mgr = mgr;
    }

    public void clear() {
        projectCaches.clear();
    }

    public ExternalFilterDesc getExternalFilterDesc(String project, String extFilterName) {
        ProjectCache prjCache = getCache(project);
        return prjCache.extFilters.get(extFilterName);
    }

    public Map<String, ExternalFilterDesc> listExternalFilterDesc(String project) {
        ProjectCache prjCache = getCache(project);
        return Collections.unmodifiableMap(prjCache.extFilters);
    }

    public List<TableDesc> listDefinedTables(String project) {
        ProjectCache prjCache = getCache(project);
        List<TableDesc> result = Lists.newArrayListWithCapacity(prjCache.tables.size());
        for (TableCache tableCache : prjCache.tables.values()) {
            result.add(tableCache.tableDesc);
        }
        return result;
    }

    public Set<TableDesc> listExposedTables(String project) {
        ProjectCache prjCache = getCache(project);
        return Collections.unmodifiableSet(prjCache.exposedTables);
    }

    public Set<ColumnDesc> listExposedColumns(String project, String table) {
        TableCache tableCache = getCache(project).tables.get(table);
        if (tableCache == null)
            return Collections.emptySet();
        else
            return Collections.unmodifiableSet(tableCache.exposedColumns);
    }

    public boolean isExposedTable(String project, String table) {
        TableCache tableCache = getCache(project).tables.get(table);
        if (tableCache == null)
            return false;
        else
            return tableCache.exposed;
    }

    public boolean isExposedColumn(String project, String table, String col) {
        TableCache tableCache = getCache(project).tables.get(table);
        if (tableCache == null)
            return false;

        for (ColumnDesc colDesc : tableCache.exposedColumns) {
            if (colDesc.getName().equals(col))
                return true;
        }
        return false;
    }

    public Set<IRealization> listAllRealizations(String project) {
        ProjectCache prjCache = getCache(project);
        return Collections.unmodifiableSet(prjCache.realizations);
    }

    public Set<IRealization> getRealizationsByTable(String project, String table) {
        TableCache tableCache = getCache(project).tables.get(table);
        if (tableCache == null)
            return Collections.emptySet();
        else
            return Collections.unmodifiableSet(tableCache.realizations);
    }

    public List<IRealization> getOnlineRealizationByFactTable(String project, String factTable) {
        Set<IRealization> realizations = getRealizationsByTable(project, factTable);
        List<IRealization> result = Lists.newArrayListWithCapacity(realizations.size());
        for (IRealization r : realizations) {
            if (r.getFactTable().equalsIgnoreCase(factTable) && r.isReady()) {
                result.add(r);
            }
        }
        return result;
    }

    public List<MeasureDesc> listEffectiveRewriteMeasures(String project, String factTable, boolean onlyRewriteMeasure) {
        Set<IRealization> realizations = getRealizationsByTable(project, factTable);
        List<MeasureDesc> result = Lists.newArrayList();
        for (IRealization r : realizations) {
            if (r.getFactTable().equalsIgnoreCase(factTable) && r.isReady()) {
                for (MeasureDesc m : r.getMeasures()) {
                    FunctionDesc func = m.getFunction();
                    if (onlyRewriteMeasure) {
                        if (func.needRewrite())
                            result.add(m);
                    } else {
                        result.add(m);
                    }
                }
            }
        }
        return result;
    }

    // ============================================================================
    // build the cache
    // ----------------------------------------------------------------------------

    private ProjectCache getCache(String project) {
        project = ProjectInstance.getNormalizedProjectName(project);
        ProjectCache result = projectCaches.get(project);
        if (result == null) {
            result = loadCache(project);
            projectCaches.put(project, result);
        }
        return result;
    }

    private ProjectCache loadCache(String project) {
        logger.info("Loading L2 project cache for " + project);
        ProjectCache projectCache = new ProjectCache(project);

        ProjectInstance pi = mgr.getProject(project);

        if (pi == null)
            throw new IllegalArgumentException("Project '" + project + "' does not exist;");

        MetadataManager metaMgr = mgr.getMetadataManager();

        for (String tableName : pi.getTables()) {
            TableDesc tableDesc = metaMgr.getTableDesc(tableName);
            if (tableDesc != null) {
                projectCache.tables.put(tableDesc.getIdentity(), new TableCache(tableDesc));
            } else {
                logger.warn("Table '" + tableName + "' defined under project '" + project + "' is not found");
            }
        }

        for (String extFilterName : pi.getExtFilters()) {
            ExternalFilterDesc filterDesc = metaMgr.getExtFilterDesc(extFilterName);
            if (filterDesc != null) {
                projectCache.extFilters.put(extFilterName, filterDesc);
            } else {
                logger.warn("External Filter '" + extFilterName + "' defined under project '" + project + "' is not found");
            }
        }

        RealizationRegistry registry = RealizationRegistry.getInstance(mgr.getConfig());
        for (RealizationEntry entry : pi.getRealizationEntries()) {
            IRealization realization = registry.getRealization(entry.getType(), entry.getRealization());
            if (realization != null) {
                projectCache.realizations.add(realization);
            } else {
                logger.warn("Realization '" + entry + "' defined under project '" + project + "' is not found");
            }

            //check if there's raw table parasite
            //TODO: ugly impl here
            IRealization parasite = registry.getRealization(RealizationType.INVERTED_INDEX, entry.getRealization());
            if (parasite != null) {
                projectCache.realizations.add(parasite);
            }
        }

        for (IRealization realization : projectCache.realizations) {
            if (sanityCheck(projectCache, realization)) {
                mapTableToRealization(projectCache, realization);
                markExposedTablesAndColumns(projectCache, realization);
            }
        }

        return projectCache;
    }

    // check all columns reported by realization does exists
    private boolean sanityCheck(ProjectCache prjCache, IRealization realization) {
        if (realization == null)
            return false;

        MetadataManager metaMgr = mgr.getMetadataManager();

        List<TblColRef> allColumns = realization.getAllColumns();
        if (allColumns == null || allColumns.isEmpty()) {
            logger.error("Realization '" + realization.getCanonicalName() + "' does not report any columns");
            return false;
        }

        for (TblColRef col : allColumns) {
            TableDesc table = metaMgr.getTableDesc(col.getTable());
            if (table == null) {
                logger.error("Realization '" + realization.getCanonicalName() + "' reports column '" + col.getCanonicalName() + "', but its table is not found by MetadataManager");
                return false;
            }
            ColumnDesc foundCol = table.findColumnByName(col.getName());
            if (col.getColumnDesc().equals(foundCol) == false) {
                logger.error("Realization '" + realization.getCanonicalName() + "' reports column '" + col.getCanonicalName() + "', but it is not equal to '" + foundCol + "' according to MetadataManager");
                return false;
            }

            // auto-define table required by realization for some legacy test case
            if (prjCache.tables.get(table.getIdentity()) == null) {
                prjCache.tables.put(table.getIdentity(), new TableCache(table));
                logger.warn("Realization '" + realization.getCanonicalName() + "' reports column '" + col.getCanonicalName() + "' whose table is not defined in project '" + prjCache.project + "'");
            }
        }

        return true;
    }

    private void mapTableToRealization(ProjectCache prjCache, IRealization realization) {
        for (TblColRef col : realization.getAllColumns()) {
            TableCache tableCache = prjCache.tables.get(col.getTable());
            tableCache.realizations.add(realization);
        }
    }

    private void markExposedTablesAndColumns(ProjectCache prjCache, IRealization realization) {
        if (!realization.isReady()) {
            return;
        }

        for (TblColRef col : realization.getAllColumns()) {
            TableCache tableCache = prjCache.tables.get(col.getTable());
            prjCache.exposedTables.add(tableCache.tableDesc);
            tableCache.exposed = true;
            tableCache.exposedColumns.add(col.getColumnDesc());
        }
    }

    private static class ProjectCache {
        private String project;
        private Map<String, TableCache> tables = Maps.newHashMap();
        private Set<TableDesc> exposedTables = Sets.newHashSet();
        private Set<IRealization> realizations = Sets.newHashSet();
        private Map<String, ExternalFilterDesc> extFilters = Maps.newHashMap();

        ProjectCache(String project) {
            this.project = project;
        }
    }

    private static class TableCache {
        private boolean exposed = false;
        private TableDesc tableDesc;
        private Set<ColumnDesc> exposedColumns = Sets.newLinkedHashSet();
        private Set<IRealization> realizations = Sets.newLinkedHashSet();

        TableCache(TableDesc tableDesc) {
            this.tableDesc = tableDesc;
        }
    }

}
