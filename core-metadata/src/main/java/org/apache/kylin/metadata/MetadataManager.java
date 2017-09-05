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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
    public static final Serializer<TableExtDesc> TABLE_EXT_SERIALIZER = new JsonSerializer<TableExtDesc>(
            TableExtDesc.class);
    public static final Serializer<DataModelDesc> MODELDESC_SERIALIZER = new JsonSerializer<DataModelDesc>(
            DataModelDesc.class);
    public static final Serializer<ExternalFilterDesc> EXTERNAL_FILTER_DESC_SERIALIZER = new JsonSerializer<ExternalFilterDesc>(
            ExternalFilterDesc.class);

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
                    logger.warn("More than one singleton exist, current keys: {}", StringUtils
                            .join(Iterators.transform(CACHE.keySet().iterator(), new Function<KylinConfig, String>() {
                                @Nullable
                                @Override
                                public String apply(@Nullable KylinConfig input) {
                                    return String.valueOf(System.identityHashCode(input));
                                }
                            }), ","));
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
    private CaseInsensitiveStringCache<TableExtDesc> srcTableExtMap;
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

    public List<TableDesc> listAllTables(String prj) {
        return Lists.newArrayList(getAllTablesMap(prj).values());
    }

    public List<ExternalFilterDesc> listAllExternalFilters() {
        return Lists.newArrayList(extFilterMap.values());
    }

    public Map<String, TableDesc> getAllTablesMap(String prj) {
        Map<String, TableDesc> globalTables = new LinkedHashMap<>();
        Map<String, TableDesc> projectTables = new LinkedHashMap<>();

        for (TableDesc t : srcTableMap.values()) {
            if (t.getProject() == null)
                globalTables.put(t.getIdentity(), t);
            else if (t.getProject().equals(prj))
                projectTables.put(t.getIdentity(), t);
        }

        Map<String, TableDesc> result = globalTables;
        result.putAll(projectTables);
        return result;
    }

    /**
     * Get TableDesc by name
     */
    public TableDesc getTableDesc(String tableName, String prj) {
        if (tableName.indexOf(".") < 0)
            tableName = "DEFAULT." + tableName;

        tableName.toUpperCase();

        TableDesc result = srcTableMap.get(mapKey(tableName, prj));
        if (result == null)
            result = srcTableMap.get(mapKey(tableName, null));

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
    public TableExtDesc getTableExt(String tableName, String prj) {
        TableDesc t = getTableDesc(tableName, prj);
        if (t == null)
            return null;

        return getTableExt(t);
    }

    public TableExtDesc getTableExt(TableDesc t) {
        TableExtDesc result = srcTableExtMap.get(mapKey(t.getIdentity(), t.getProject()));

        // avoid returning null, since the TableDesc exists
        if (null == result) {
            result = new TableExtDesc();
            result.setIdentity(t.getIdentity());
            result.setUuid(UUID.randomUUID().toString());
            result.setLastModified(0);
            result.init(t.getProject());
            srcTableExtMap.put(mapKey(t.getIdentity(), t.getProject()), result);
        }
        return result;
    }

    public void saveTableExt(TableExtDesc tableExt, String prj) throws IOException {
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

        String path = TableExtDesc.concatResourcePath(tableExt.getIdentity(), prj);

        ResourceStore store = getStore();

        TableExtDesc t = store.getResource(path, TableExtDesc.class, TABLE_EXT_SERIALIZER);
        if (t != null && t.getIdentity() == null)
            store.deleteResource(path);

        store.putResource(path, tableExt, TABLE_EXT_SERIALIZER);
        srcTableExtMap.put(mapKey(tableExt.getIdentity(), tableExt.getProject()), tableExt);
    }

    public void removeTableExt(String tableName, String prj) throws IOException {
        // note, here assume always delete TableExtDesc first, then TableDesc
        TableExtDesc t = getTableExt(tableName, prj);
        if (t == null)
            return;

        String path = TableExtDesc.concatResourcePath(t.getIdentity(), t.getProject());
        getStore().deleteResource(path);
        srcTableExtMap.remove(mapKey(t.getIdentity(), t.getProject()));
    }

    public void saveSourceTable(TableDesc srcTable, String prj) throws IOException {
        if (srcTable.getUuid() == null || srcTable.getIdentity() == null) {
            throw new IllegalArgumentException();
        }

        srcTable.init(prj);

        String path = TableDesc.concatResourcePath(srcTable.getIdentity(), prj);
        getStore().putResource(path, srcTable, TABLE_SERIALIZER);

        srcTableMap.put(mapKey(srcTable.getIdentity(), prj), srcTable);
    }

    public void removeSourceTable(String tableIdentity, String prj) throws IOException {
        TableDesc t = getTableDesc(tableIdentity, prj);
        if (t == null)
            return;

        String path = TableDesc.concatResourcePath(t.getIdentity(), t.getProject());
        getStore().deleteResource(path);
        srcTableMap.remove(mapKey(t.getIdentity(), t.getProject()));
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
        this.srcTableExtMap = new CaseInsensitiveStringCache<>(config, "table_ext");
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
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            if (event == Event.DROP)
                srcTableMap.removeLocal(cacheKey);
            else
                reloadSourceTableAt(TableDesc.concatRawResourcePath(cacheKey));

            Pair<String, String> pair = TableDesc.parseResourcePath(cacheKey);
            String table = pair.getFirst();
            String prj = pair.getSecond();

            if (prj == null) {
                for (ProjectInstance p : ProjectManager.getInstance(config).findProjectsByTable(table)) {
                    broadcaster.notifyProjectSchemaUpdate(p.getName());
                }
            } else {
                broadcaster.notifyProjectSchemaUpdate(prj);
            }
        }
    }

    private class SrcTableExtSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            if (event == Event.DROP)
                srcTableExtMap.removeLocal(cacheKey);
            else
                reloadTableExtAt(TableExtDesc.concatRawResourcePath(cacheKey));
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
                reloadDataModelDescAt(DataModelDesc.concatResourcePath(model));
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            if (event == Event.DROP)
                dataModelDescMap.removeLocal(cacheKey);
            else
                reloadDataModelDescAt(DataModelDesc.concatResourcePath(cacheKey));

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
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            if (event == Event.DROP)
                extFilterMap.removeLocal(cacheKey);
            else
                reloadExtFilter(cacheKey);
        }
    }

    private void reloadAllTableExt() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading Table_exd info from folder "
                + store.getReadableResourcePath(ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        srcTableExtMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_EXD_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadTableExtAt(path);
        }

        logger.debug("Loaded " + srcTableExtMap.size() + " SourceTable EXD(s)");
    }

    private TableExtDesc reloadTableExtAt(String path) throws IOException {
        ResourceStore store = getStore();
        String prj = TableExtDesc.parseResourcePath(path).getSecond();

        TableExtDesc t = store.getResource(path, TableExtDesc.class, TABLE_EXT_SERIALIZER);

        if (t == null) {
            return null;
        }

        // convert old tableExt json to new one
        if (t.getIdentity() == null) {
            t = convertOldTableExtToNewer(path);
        }

        t.init(prj);

        srcTableExtMap.putLocal(mapKey(t.getIdentity(), prj), t);
        return t;
    }

    private String mapKey(String identity, String prj) {
        return prj == null ? identity : identity + "--" + prj;
    }

    private TableExtDesc convertOldTableExtToNewer(String path) throws IOException {
        Map<String, String> attrs = Maps.newHashMap();

        ResourceStore store = getStore();
        RawResource res = store.getResource(path);

        InputStream is = res.inputStream;

        try {
            attrs.putAll(JsonUtil.readValue(is, HashMap.class));
        } finally {
            if (is != null)
                is.close();
        }

        String cardinality = attrs.get(MetadataConstants.TABLE_EXD_CARDINALITY);

        // parse table identity from file name
        String file = path;
        if (file.indexOf("/") > -1) {
            file = file.substring(file.lastIndexOf("/") + 1);
        }
        String tableIdentity = file.substring(0, file.length() - MetadataConstants.FILE_SURFIX.length()).toUpperCase();
        TableExtDesc result = new TableExtDesc();
        result.setIdentity(tableIdentity);
        result.setUuid(UUID.randomUUID().toString());
        result.setLastModified(0);
        result.setCardinality(cardinality);
        return result;
    }

    private void reloadAllExternalFilter() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading ExternalFilter from folder "
                + store.getReadableResourcePath(ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT));

        extFilterMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadExternalFilterAt(path);
        }

        logger.debug("Loaded " + extFilterMap.size() + " ExternalFilter(s)");
    }

    private void reloadAllSourceTable() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable from folder "
                + store.getReadableResourcePath(ResourceStore.TABLE_RESOURCE_ROOT));

        srcTableMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadSourceTableAt(path);
        }

        logger.debug("Loaded " + srcTableMap.size() + " SourceTable(s)");
    }

    private TableDesc reloadSourceTableAt(String path) throws IOException {
        ResourceStore store = getStore();
        String prj = TableDesc.parseResourcePath(path).getSecond();

        TableDesc t = store.getResource(path, TableDesc.class, TABLE_SERIALIZER);
        if (t == null) {
            return null;
        }
        t.init(prj);

        srcTableMap.putLocal(mapKey(t.getIdentity(), prj), t);

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

    public DataModelDesc getDataModelDesc(String name) {
        return dataModelDescMap.get(name);
    }

    public List<DataModelDesc> getModels() {
        return new ArrayList<>(dataModelDescMap.values());
    }

    public List<DataModelDesc> getModels(String projectName) {
        ProjectInstance projectInstance = ProjectManager.getInstance(config).getProject(projectName);
        ArrayList<DataModelDesc> ret = new ArrayList<>();

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

        return ret;
    }

    // within a project, find models that use the specified table
    public List<String> getModelsUsingTable(TableDesc table, String project) throws IOException {
        List<String> models = new ArrayList<>();
        for (DataModelDesc modelDesc : getModels(project)) {
            if (modelDesc.containsTable(table))
                models.add(modelDesc.getName());
        }
        return models;
    }

    public boolean isTableInAnyModel(TableDesc table) {
        for (DataModelDesc modelDesc : getModels()) {
            if (modelDesc.containsTable(table))
                return true;
        }
        return false;
    }

    private void reloadAllDataModel() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading DataModel from folder "
                + store.getReadableResourcePath(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT));

        dataModelDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {

            try {
                logger.info("Reloading data model at " + path);
                reloadDataModelDescAt(path);
            } catch (IllegalStateException e) {
                logger.error("Error to load DataModel at " + path, e);
                continue;
            }
        }

        logger.debug("Loaded " + dataModelDescMap.size() + " DataModel(s)");
    }

    public DataModelDesc reloadDataModelDescAt(String path) {
        ResourceStore store = getStore();
        try {
            DataModelDesc dataModelDesc = store.getResource(path, DataModelDesc.class, MODELDESC_SERIALIZER);
            String prj = ProjectManager.getInstance(config).getProjectOfModel(dataModelDesc.getName()).getName();

            if (!dataModelDesc.isDraft())
                dataModelDesc.init(config, this.getAllTablesMap(prj), listDataModels());

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

        ProjectManager prjMgr = ProjectManager.getInstance(config);
        ProjectInstance prj = prjMgr.getProject(projectName);
        if (prj.containsModel(name))
            throw new IllegalStateException("project " + projectName + " already contains model " + name);

        try {
            // Temporarily register model under project, because we want to 
            // update project formally after model is saved.
            prj.getModels().add(name);

            desc.setOwner(owner);
            desc = saveDataModelDesc(desc);

        } finally {
            prj.getModels().remove(name);
        }

        // now that model is saved, update project formally
        prjMgr.updateModelToProject(name, projectName);

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

        String prj = ProjectManager.getInstance(config).getProjectOfModel(dataModelDesc.getName()).getName();

        if (!dataModelDesc.isDraft())
            dataModelDesc.init(config, this.getAllTablesMap(prj), listDataModels());

        String path = dataModelDesc.getResourcePath();
        getStore().putResource(path, dataModelDesc, MODELDESC_SERIALIZER);
        dataModelDescMap.put(dataModelDesc.getName(), dataModelDesc);

        return dataModelDesc;
    }
}
