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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableDesc.TableProject;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class TableMetadataManager {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TableMetadataManager.class);

    public static final Serializer<TableDesc> TABLE_SERIALIZER = new JsonSerializer<TableDesc>(TableDesc.class);
    
    private static final Serializer<TableExtDesc> TABLE_EXT_SERIALIZER = new JsonSerializer<TableExtDesc>(
            TableExtDesc.class);

    public static TableMetadataManager getInstance(KylinConfig config) {
        return config.getManager(TableMetadataManager.class);
    }

    // called by reflection
    static TableMetadataManager newInstance(KylinConfig config) throws IOException {
        return new TableMetadataManager(config);
    }

    // ============================================================================

    private KylinConfig config;

    // table name ==> SourceTable
    private CaseInsensitiveStringCache<TableDesc> srcTableMap;
    private CachedCrudAssist<TableDesc> srcTableCrud;
    private AutoReadWriteLock srcTableMapLock = new AutoReadWriteLock();

    // name => SourceTableExt
    private CaseInsensitiveStringCache<TableExtDesc> srcExtMap;
    private CachedCrudAssist<TableExtDesc> srcExtCrud;
    private AutoReadWriteLock srcExtMapLock = new AutoReadWriteLock();

    // name => ExternalFilterDesc
    private CaseInsensitiveStringCache<ExternalFilterDesc> extFilterMap;
    private CachedCrudAssist<ExternalFilterDesc> extFilterCrud;
    private AutoReadWriteLock extFilterMapLock = new AutoReadWriteLock();

    private TableMetadataManager(KylinConfig cfg) throws IOException {
        this.config = cfg;

        initSrcTable();
        initSrcExt();
        initExtFilter();
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    // ============================================================================
    // TableDesc methods
    // ============================================================================

    private void initSrcTable() throws IOException {
        this.srcTableMap = new CaseInsensitiveStringCache<>(config, "table");
        this.srcTableCrud = new CachedCrudAssist<TableDesc>(getStore(), ResourceStore.TABLE_RESOURCE_ROOT,
                TableDesc.class, srcTableMap) {
            @Override
            protected TableDesc initEntityAfterReload(TableDesc t, String resourceName) {
                String prj = TableDesc.parseResourcePath(resourceName).getProject();
                t.init(config, prj);
                return t;
            }
        };
        srcTableCrud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new SrcTableSyncListener(), "table");
    }

    private class SrcTableSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            try (AutoLock lock = srcTableMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    srcTableMap.removeLocal(cacheKey);
                else
                    srcTableCrud.reloadQuietly(cacheKey);
            }

            TableProject tableProject = TableDesc.parseResourcePath(cacheKey);
            String table = tableProject.getTable();
            String prj = tableProject.getProject();

            if (prj == null) {
                for (ProjectInstance p : ProjectManager.getInstance(config).findProjectsByTable(table)) {
                    broadcaster.notifyProjectSchemaUpdate(p.getName());
                }
            } else {
                broadcaster.notifyProjectSchemaUpdate(prj);
            }
        }
    }

    public void reloadSourceTableQuietly(String table, String project) {
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            srcTableCrud.reloadQuietly(TableDesc.makeResourceName(table, project));
        }
    }

    public List<TableDesc> listAllTables(String prj) {
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            return Lists.newArrayList(getAllTablesMap(prj).values());
        }
    }

    public Map<String, TableDesc> getAllTablesMap(String prj) {
        // avoid cyclic locks
        ProjectInstance project = (prj == null) ? null : ProjectManager.getInstance(config).getProject(prj);
        
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            //TODO prj == null case is now only used by test case and CubeMetaIngester
            //should refactor these test case and tool ASAP and stop supporting null case
            if (prj == null) {
                Map<String, TableDesc> globalTables = new LinkedHashMap<>();

                for (TableDesc t : srcTableMap.values()) {
                    globalTables.put(t.getIdentity(), t);
                }
                return globalTables;
            }

            Set<String> prjTableNames = project.getTables();

            Map<String, TableDesc> ret = new LinkedHashMap<>();
            for (String tableName : prjTableNames) {
                String tableIdentity = getTableIdentity(tableName);
                ret.put(tableIdentity, getProjectSpecificTableDesc(tableIdentity, prj));
            }
            return ret;
        }
    }

    /**
     * Get TableDesc by name
     */
    public TableDesc getTableDesc(String tableName, String prj) {
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            return getProjectSpecificTableDesc(getTableIdentity(tableName), prj);
        }
    }

    /**
     * Make sure the returned table desc is project-specific.
     * 
     * All locks on srcTableMapLock are WRITE LOCKS because of this method!!
     */
    private TableDesc getProjectSpecificTableDesc(String fullTableName, String prj) {
        String key = mapKey(fullTableName, prj);
        TableDesc result = srcTableMap.get(key);

        if (result == null) {
            try (AutoLock lock = srcTableMapLock.lockForWrite()) {
                result = srcTableMap.get(mapKey(fullTableName, null));
                if (result != null) {
                    result = new TableDesc(result);// deep copy of global tabledesc

                    result.setLastModified(0);
                    result.setProject(prj);
                    result.setBorrowedFromGlobal(true);

                    srcTableMap.putLocal(key, result);
                }
            }
        }
        return result;
    }

    /**
     * some legacy table name may not have DB prefix
     */
    private String getTableIdentity(String tableName) {
        if (!tableName.contains("."))
            return "DEFAULT." + tableName.toUpperCase(Locale.ROOT);
        else
            return tableName.toUpperCase(Locale.ROOT);
    }

    public void saveSourceTable(TableDesc srcTable, String prj) throws IOException {
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            srcTable.init(config, prj);
            srcTableCrud.save(srcTable);
        }
    }

    public void removeSourceTable(String tableIdentity, String prj) throws IOException {
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            TableDesc t = getTableDesc(tableIdentity, prj);
            if (t == null)
                return;

            srcTableCrud.delete(t);
        }
    }

    /**
     * the project-specific table desc will be expand by computed columns from the projects' models
     * when the projects' model list changed, project-specific table should be reset and get expanded
     * again
     */
    public void resetProjectSpecificTableDesc(String prj) throws IOException {
        // avoid cyclic locks
        ProjectInstance project = ProjectManager.getInstance(config).getProject(prj);
        
        try (AutoLock lock = srcTableMapLock.lockForWrite()) {
            for (String tableName : project.getTables()) {
                String tableIdentity = getTableIdentity(tableName);
                String key = mapKey(tableIdentity, prj);
                TableDesc originTableDesc = srcTableMap.get(key);
                if (originTableDesc == null) {
                    continue;
                }

                if (originTableDesc.isBorrowedFromGlobal()) {
                    srcTableMap.removeLocal(key);//delete it so that getProjectSpecificTableDesc will create again
                } else {
                    srcTableCrud.reload(key);
                }
            }
        }
    }

    private String mapKey(String identity, String prj) {
        return TableDesc.makeResourceName(identity, prj);
    }

    // ============================================================================
    // TableExtDesc methods
    // ============================================================================

    private void initSrcExt() throws IOException {
        this.srcExtMap = new CaseInsensitiveStringCache<>(config, "table_ext");
        this.srcExtCrud = new CachedCrudAssist<TableExtDesc>(getStore(), ResourceStore.TABLE_EXD_RESOURCE_ROOT,
                TableExtDesc.class, srcExtMap) {
            @Override
            protected TableExtDesc initEntityAfterReload(TableExtDesc t, String resourceName) {
                // convert old tableExt json to new one
                if (t.getIdentity() == null) {
                    t = convertOldTableExtToNewer(resourceName);
                }

                String prj = TableDesc.parseResourcePath(resourceName).getProject();
                t.init(prj);
                return t;
            }
        };
        srcExtCrud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new SrcTableExtSyncListener(), "table_ext");
    }

    private class SrcTableExtSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            try (AutoLock lock = srcExtMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    srcExtMap.removeLocal(cacheKey);
                else
                    srcExtCrud.reloadQuietly(cacheKey);
            }
        }
    }

    public void reloadTableExtQuietly(String table, String project) {
        try (AutoLock lock = srcExtMapLock.lockForWrite()) {
            srcExtCrud.reloadQuietly(TableExtDesc.concatResourcePath(table, project));
        }
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstants}
     * 
     * @param tableName
     * @return
     */
    public TableExtDesc getTableExt(String tableName, String prj) {
        TableDesc t = getTableDesc(tableName, prj);
        if (t == null)
            return null;

        return getTableExt(t);
    }

    public TableExtDesc getTableExt(TableDesc t) {
        try (AutoLock lock = srcExtMapLock.lockForRead()) {
            TableExtDesc result = srcExtMap.get(mapKey(t.getIdentity(), t.getProject()));

            if (null == result) {
                //TODO: notice the table ext is not project-specific, seems not necessary at all
                result = srcExtMap.get(mapKey(t.getIdentity(), null));
            }

            // avoid returning null, since the TableDesc exists
            if (null == result) {
                result = new TableExtDesc();
                result.setIdentity(t.getIdentity());
                result.setUuid(RandomUtil.randomUUID().toString());
                result.setLastModified(0);
                result.init(t.getProject());
                srcExtMap.putLocal(mapKey(t.getIdentity(), t.getProject()), result);
            }
            return result;
        }
    }

    public void saveTableExt(TableExtDesc tableExt, String prj) throws IOException {
        try (AutoLock lock = srcExtMapLock.lockForWrite()) {
            if (tableExt.getUuid() == null || tableExt.getIdentity() == null) {
                throw new IllegalArgumentException();
            }

            // updating a legacy global table
            if (tableExt.getProject() == null) {
                if (getTableExt(tableExt.getIdentity(), prj).getProject() != null)
                    throw new IllegalStateException(
                            "Updating a legacy global TableExtDesc while a project level version exists: "
                                    + tableExt.getIdentity() + ", " + prj);
                prj = tableExt.getProject();
            }

            tableExt.init(prj);

            // what is this doing??
            String path = TableExtDesc.concatResourcePath(tableExt.getIdentity(), prj);
            ResourceStore store = getStore();
            TableExtDesc t = store.getResource(path, TABLE_EXT_SERIALIZER);
            if (t != null && t.getIdentity() == null)
                store.deleteResource(path);

            srcExtCrud.save(tableExt);
        }
    }

    public void removeTableExt(String tableName, String prj) throws IOException {
        try (AutoLock lock = srcExtMapLock.lockForWrite()) {
            // note, here assume always delete TableExtDesc first, then TableDesc
            TableExtDesc t = getTableExt(tableName, prj);
            if (t == null)
                return;

            srcExtCrud.delete(t);
        }
    }

    private TableExtDesc convertOldTableExtToNewer(String resourceName) {
        ResourceStore store = getStore();
        Map<String, String> attrs = Maps.newHashMap();

        try {
            RawResource res = store.getResource(
                    ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + resourceName + MetadataConstants.FILE_SURFIX);

            InputStream is = res.content();
            try {
                attrs.putAll(JsonUtil.readValue(is, HashMap.class));
            } finally {
                if (is != null)
                    is.close();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        String cardinality = attrs.get(MetadataConstants.TABLE_EXD_CARDINALITY);

        // parse table identity from file name
        String tableIdentity = TableDesc.parseResourcePath(resourceName).getTable();
        TableExtDesc result = new TableExtDesc();
        result.setIdentity(tableIdentity);
        result.setUuid(RandomUtil.randomUUID().toString());
        result.setLastModified(0);
        result.setCardinality(cardinality);
        return result;
    }

    // ============================================================================
    // ExternalFilterDesc methods
    // ============================================================================

    private void initExtFilter() throws IOException {
        this.extFilterMap = new CaseInsensitiveStringCache<>(config, "external_filter");
        this.extFilterCrud = new CachedCrudAssist<ExternalFilterDesc>(getStore(),
                ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT, ExternalFilterDesc.class, extFilterMap) {
            @Override
            protected ExternalFilterDesc initEntityAfterReload(ExternalFilterDesc t, String resourceName) {
                return t; // noop
            }
        };
        extFilterCrud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new ExtFilterSyncListener(), "external_filter");
    }

    private class ExtFilterSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            try (AutoLock lock = extFilterMapLock.lockForWrite()) {
                if (event == Event.DROP)
                    extFilterMap.removeLocal(cacheKey);
                else
                    extFilterCrud.reloadQuietly(cacheKey);
            }
        }
    }

    public List<ExternalFilterDesc> listAllExternalFilters() {
        try (AutoLock lock = extFilterMapLock.lockForRead()) {
            return Lists.newArrayList(extFilterMap.values());
        }
    }

    public ExternalFilterDesc getExtFilterDesc(String filterTableName) {
        try (AutoLock lock = extFilterMapLock.lockForRead()) {
            ExternalFilterDesc result = extFilterMap.get(filterTableName);
            return result;
        }
    }

    public void saveExternalFilter(ExternalFilterDesc desc) throws IOException {
        try (AutoLock lock = extFilterMapLock.lockForWrite()) {
            extFilterCrud.save(desc);
        }
    }

    public void removeExternalFilter(String name) throws IOException {
        try (AutoLock lock = extFilterMapLock.lockForWrite()) {
            name = name.replaceAll("[./]", "");
            extFilterCrud.delete(name);
        }
    }

}
