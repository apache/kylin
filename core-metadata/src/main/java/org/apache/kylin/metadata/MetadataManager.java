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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Serves (and caches) metadata for Kylin instance.
 * <p/>
 * Also provides a ResourceStore for general purpose data persistence. 
 * Metadata is serialized as JSON and stored in ResourceStore.
 * 
 * @author yangli9
 */
public class MetadataManager {

    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);

    public static final Serializer<TableDesc> TABLE_SERIALIZER = new JsonSerializer<TableDesc>(TableDesc.class);
    public static final Serializer<TableExtDesc> TABLE_EXT_SERIALIZER = new JsonSerializer<TableExtDesc>(TableExtDesc.class);
    public static final Serializer<DataModelDesc> MODELDESC_SERIALIZER = new JsonSerializer<DataModelDesc>(DataModelDesc.class);
    public static final Serializer<ExternalFilterDesc> EXTERNAL_FILTER_DESC_SERIALIZER = new JsonSerializer<ExternalFilterDesc>(ExternalFilterDesc.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, MetadataManager> CACHE = new ConcurrentHashMap<KylinConfig, MetadataManager>();

    public static MetadataManager getInstance(KylinConfig config) {
        MetadataManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (MetadataManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new MetadataManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }

                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init MetadataManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // table name ==> SourceTable
    private CaseInsensitiveStringCache<TableDesc> srcTableMap;
    // name => value
    private CaseInsensitiveStringCache<TableExtDesc> srcTableExdMap;
    // name => DataModelDesc
    private CaseInsensitiveStringCache<DataModelDesc> dataModelDescMap;
    // name => External Filter Desc
    private CaseInsensitiveStringCache<ExternalFilterDesc> extFilterMap;

    private MetadataManager(KylinConfig config) throws IOException {
        init(config);
    }

    /**
     * Tell MetadataManager that the instance has changed. The cube info will
     * be stored Reload the cube desc and source table A broadcast must be sent
     * out
     * 
     * @return
     * @throws IOException
     */
    public void reload() {
        clearCache();
        getInstance(config);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public List<DataModelDesc> listDataModels() {
        return Lists.newArrayList(this.dataModelDescMap.values());
    }

    public List<TableDesc> listAllTables() {
        return Lists.newArrayList(srcTableMap.values());
    }

    public List<ExternalFilterDesc> listAllExternalFilters() {
        return Lists.newArrayList(extFilterMap.values());
    }

    public Map<String, TableDesc> getAllTablesMap() {
        return Collections.unmodifiableMap(srcTableMap.getMap());
    }

    public Map<String, TableExtDesc> listAllTableExdMap() {
        return srcTableExdMap.getMap();
    }

    /**
     * Get ColumnDesc by name, like "table.column"
     */
    public ColumnDesc getColumnDesc(String tableDotColumnName) {
        int cut = tableDotColumnName.lastIndexOf('.');
        if (cut < 0)
            throw new IllegalArgumentException();

        String tableName = tableDotColumnName.substring(0, cut);
        String columnName = tableDotColumnName.substring(cut + 1);

        TableDesc table = getTableDesc(tableName);
        if (table == null)
            return null;

        return table.findColumnByName(columnName);
    }

    /**
     * Get TableDesc by name
     */
    public TableDesc getTableDesc(String tableName) {
        if (tableName.indexOf(".") < 0)
            tableName = "DEFAULT." + tableName;

        TableDesc result = srcTableMap.get(tableName.toUpperCase());
        return result;
    }

    public ExternalFilterDesc getExtFilterDesc(String filterTableName) {
        ExternalFilterDesc result = extFilterMap.get(filterTableName);
        return result;
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstants}
     * 
     * @param tableName
     * @return
     */
    public TableExtDesc getTableExt(String tableName) {
        if (tableName.indexOf(".") < 0)
            tableName = "DEFAULT." + tableName;

        TableExtDesc result = srcTableExdMap.get(tableName.toUpperCase());

        // create new
        if (null == result) {
            result = new TableExtDesc();
            result.setName(tableName);
            result.setUuid(UUID.randomUUID().toString());
            result.setLastModified(0);
            result.init();
            try {
                saveTableExt(result);
            } catch (IOException ex) {
                logger.warn("Failed to save TableExt", ex);
            }
        }
        return result;
    }

    public void saveTableExt(TableExtDesc tableExt) throws IOException {
        if (tableExt.getUuid() == null || tableExt.getName() == null) {
            throw new IllegalArgumentException();
        }

        tableExt.init();

        String path = tableExt.getResourcePath();
        getStore().putResource(path, tableExt, TABLE_EXT_SERIALIZER);

        srcTableExdMap.put(tableExt.getName(), tableExt);
    }

    public void removeTableExt(String tableName) throws IOException {
        String path = TableExtDesc.concatResourcePath(tableName);
        getStore().deleteResource(path);
        srcTableExdMap.remove(tableName);
    }

    public void saveSourceTable(TableDesc srcTable) throws IOException {
        if (srcTable.getUuid() == null || srcTable.getIdentity() == null) {
            throw new IllegalArgumentException();
        }

        srcTable.init();

        String path = srcTable.getResourcePath();
        getStore().putResource(path, srcTable, TABLE_SERIALIZER);

        srcTableMap.put(srcTable.getIdentity(), srcTable);
    }

    public void removeSourceTable(String tableIdentity) throws IOException {
        String path = TableDesc.concatResourcePath(tableIdentity);
        getStore().deleteResource(path);
        srcTableMap.remove(tableIdentity);
    }

    public void saveExternalFilter(ExternalFilterDesc desc) throws IOException {
        if (desc.getUuid() == null) {
            throw new IllegalArgumentException("UUID not set.");
        }
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, EXTERNAL_FILTER_DESC_SERIALIZER);
        desc = reloadExternalFilterAt(path);
        extFilterMap.put(desc.getName(), desc);

    }

    public void removeExternalFilter(String name) throws IOException {
        String path = ExternalFilterDesc.concatResourcePath(name);
        getStore().deleteResource(path);
        extFilterMap.remove(name);

    }

    private void init(KylinConfig config) throws IOException {
        this.config = config;
        this.srcTableMap = new CaseInsensitiveStringCache<>(config, "table");
        this.srcTableExdMap = new CaseInsensitiveStringCache<>(config, "table_ext");
        this.dataModelDescMap = new CaseInsensitiveStringCache<>(config, "data_model");
        this.extFilterMap = new CaseInsensitiveStringCache<>(config, "external_filter");

        reloadAllSourceTable();
        reloadAllTableExt();
        reloadAllDataModel();
        reloadAllExternalFilter();

        // touch lower level metadata before registering my listener
        Broadcaster.getInstance(config).registerListener(new SrcTableSyncListener(), "table");
        Broadcaster.getInstance(config).registerListener(new SrcTableExtSyncListener(), "table_ext");
        Broadcaster.getInstance(config).registerListener(new DataModelSyncListener(), "data_model");
        Broadcaster.getInstance(config).registerListener(new ExtFilterSyncListener(), "external_filter");
    }

    private class SrcTableSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) throws IOException {
            if (event == Event.DROP)
                srcTableMap.removeLocal(cacheKey);
            else
                reloadSourceTable(cacheKey);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByTable(cacheKey)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
    }

    private class SrcTableExtSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) throws IOException {
            if (event == Event.DROP)
                srcTableExdMap.removeLocal(cacheKey);
            else
                reloadSourceTableExt(cacheKey);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByTable(cacheKey)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
    }

    private class DataModelSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (String model : ProjectManager.getInstance(config).getProject(project).getModels()) {
                reloadDataModelDesc(model);
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) throws IOException {
            if (event == Event.DROP)
                dataModelDescMap.removeLocal(cacheKey);
            else
                reloadDataModelDesc(cacheKey);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByModel(cacheKey)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
    }

    private class ExtFilterSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) throws IOException {
            if (event == Event.DROP)
                extFilterMap.removeLocal(cacheKey);
            else
                reloadExtFilter(cacheKey);
        }
    }

    private void reloadAllTableExt() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading Table_exd info from folder " + store.getReadableResourcePath(ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        srcTableExdMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_EXD_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadTableExtAt(path);
        }

        logger.debug("Loaded " + srcTableExdMap.size() + " SourceTable EXD(s)");
    }

    private TableExtDesc reloadTableExtAt(String path) throws IOException {
        ResourceStore store = getStore();
        TableExtDesc t = store.getResource(path, TableExtDesc.class, TABLE_EXT_SERIALIZER);
        if (t == null) {
            return null;
        }
        t.init();

        String name = t.getName();

        // remove old json
        if (name == null) {
            getStore().deleteResource(path);
            return null;
        }

        srcTableExdMap.putLocal(name, t);

        return t;
    }

    private void reloadAllExternalFilter() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading ExternalFilter from folder " + store.getReadableResourcePath(ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT));

        extFilterMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadExternalFilterAt(path);
        }

        logger.debug("Loaded " + extFilterMap.size() + " ExternalFilter(s)");
    }

    private void reloadAllSourceTable() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable from folder " + store.getReadableResourcePath(ResourceStore.TABLE_RESOURCE_ROOT));

        srcTableMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadSourceTableAt(path);
        }

        logger.debug("Loaded " + srcTableMap.size() + " SourceTable(s)");
    }

    private TableDesc reloadSourceTableAt(String path) throws IOException {
        ResourceStore store = getStore();
        TableDesc t = store.getResource(path, TableDesc.class, TABLE_SERIALIZER);
        if (t == null) {
            return null;
        }
        t.init();

        String tableIdentity = t.getIdentity();

        srcTableMap.putLocal(tableIdentity, t);

        return t;
    }

    private ExternalFilterDesc reloadExternalFilterAt(String path) throws IOException {
        ResourceStore store = getStore();
        ExternalFilterDesc t = store.getResource(path, ExternalFilterDesc.class, EXTERNAL_FILTER_DESC_SERIALIZER);
        if (t == null) {
            return null;
        }
        extFilterMap.putLocal(t.getName(), t);

        return t;
    }

    public void reloadExtFilter(String extFilterName) throws IOException {
        reloadExternalFilterAt(ExternalFilterDesc.concatResourcePath(extFilterName));
    }

    public void reloadSourceTableExt(String tableIdentity) throws IOException {
        reloadTableExtAt(TableExtDesc.concatResourcePath(tableIdentity));
    }

    public void reloadSourceTable(String tableIdentity) throws IOException {
        reloadSourceTableAt(TableDesc.concatResourcePath(tableIdentity));
    }

    public DataModelDesc getDataModelDesc(String name) {
        return dataModelDescMap.get(name);
    }

    public List<DataModelDesc> getModels() {
        return new ArrayList<>(dataModelDescMap.values());
    }

    public List<DataModelDesc> getModels(String projectName) throws IOException {
        ProjectInstance projectInstance = ProjectManager.getInstance(config).getProject(projectName);
        HashSet<DataModelDesc> ret = new HashSet<>();

        if (projectInstance != null && projectInstance.getModels() != null) {
            for (String modelName : projectInstance.getModels()) {
                DataModelDesc model = getDataModelDesc(modelName);
                if (null != model) {
                    ret.add(model);
                } else {
                    logger.error("Failed to load model " + modelName);
                }
            }
        }

        return new ArrayList<>(ret);
    }

    public boolean isTableInModel(String tableName, String projectName) throws IOException {
        return getModelsUsingTable(tableName, projectName).size() > 0;
    }

    public List<String> getModelsUsingTable(String tableName, String projectName) throws IOException {
        List<String> models = new ArrayList<>();
        for (DataModelDesc modelDesc : getModels(projectName)) {
            for (TableRef tableRef : modelDesc.getAllTables()) {
                if (tableRef.getTableIdentity().equalsIgnoreCase(tableName)) {
                    models.add(modelDesc.getName());
                }
            }
        }
        return models;
    }

    public boolean isTableInAnyModel(String tableName) {
        for (DataModelDesc modelDesc : getModels()) {
            for (TableRef tableRef : modelDesc.getAllTables()) {
                if (tableRef.getTableIdentity().equalsIgnoreCase(tableName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void reloadAllDataModel() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading DataModel from folder " + store.getReadableResourcePath(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT));

        dataModelDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            try {
                reloadDataModelDescAt(path);
            } catch (IllegalStateException e) {
                logger.error("Error to load DataModel at " + path, e);
                continue;
            }
        }

        logger.debug("Loaded " + dataModelDescMap.size() + " DataModel(s)");
    }

    public DataModelDesc reloadDataModelDesc(String name) {
        return reloadDataModelDescAt(DataModelDesc.concatResourcePath(name));
    }

    private DataModelDesc reloadDataModelDescAt(String path) {
        ResourceStore store = getStore();
        try {
            DataModelDesc dataModelDesc = store.getResource(path, DataModelDesc.class, MODELDESC_SERIALIZER);
            dataModelDesc.init(config, this.getAllTablesMap());
            dataModelDescMap.putLocal(dataModelDesc.getName(), dataModelDesc);
            return dataModelDesc;
        } catch (Exception e) {
            throw new IllegalStateException("Error to load " + path, e);
        }
    }

    // sync on update
    public DataModelDesc dropModel(DataModelDesc desc) throws IOException {
        logger.info("Dropping model '" + desc.getName() + "'");
        ResourceStore store = getStore();
        store.deleteResource(desc.getResourcePath());
        // delete model from project
        ProjectManager.getInstance(config).removeModelFromProjects(desc.getName());
        // clean model cache
        this.afterModelDropped(desc);
        return desc;
    }

    private void afterModelDropped(DataModelDesc desc) {
        removeModelCache(desc.getName());
    }

    public void removeModelCache(String modelName) {
        dataModelDescMap.remove(modelName);
    }

    public DataModelDesc createDataModelDesc(DataModelDesc desc, String projectName, String owner) throws IOException {
        String name = desc.getName();
        if (dataModelDescMap.containsKey(name))
            throw new IllegalArgumentException("DataModelDesc '" + name + "' already exists");
        desc.setOwner(owner);
        desc = saveDataModelDesc(desc);
        ProjectManager.getInstance(config).updateModelToProject(name, projectName);
        return desc;
    }

    public DataModelDesc updateDataModelDesc(DataModelDesc desc) throws IOException {
        String name = desc.getName();
        if (!dataModelDescMap.containsKey(name)) {
            throw new IllegalArgumentException("DataModelDesc '" + name + "' does not exist.");
        }

        return saveDataModelDesc(desc);
    }

    private DataModelDesc saveDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        dataModelDesc.init(config, this.getAllTablesMap());

        String path = dataModelDesc.getResourcePath();
        getStore().putResource(path, dataModelDesc, MODELDESC_SERIALIZER);
        dataModelDescMap.put(dataModelDesc.getName(), dataModelDesc);

        return dataModelDesc;
    }
}
