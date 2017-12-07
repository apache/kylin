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
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class TableMetadataManager {

    private static final Logger logger = LoggerFactory.getLogger(TableMetadataManager.class);

    public static final Serializer<TableDesc> TABLE_SERIALIZER = new JsonSerializer<TableDesc>(TableDesc.class);
    public static final Serializer<TableExtDesc> TABLE_EXT_SERIALIZER = new JsonSerializer<TableExtDesc>(
            TableExtDesc.class);
    public static final Serializer<ExternalFilterDesc> EXTERNAL_FILTER_DESC_SERIALIZER = new JsonSerializer<ExternalFilterDesc>(
            ExternalFilterDesc.class);

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
    // name => SourceTableExt
    private CaseInsensitiveStringCache<TableExtDesc> srcTableExtMap;
    // name => External Filter Desc
    private CaseInsensitiveStringCache<ExternalFilterDesc> extFilterMap;

    private TableMetadataManager(KylinConfig config) throws IOException {
        init(config);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public List<TableDesc> listAllTables(String prj) {
        return Lists.newArrayList(getAllTablesMap(prj).values());
    }

    public List<ExternalFilterDesc> listAllExternalFilters() {
        return Lists.newArrayList(extFilterMap.values());
    }

    public Map<String, TableDesc> getAllTablesMap(String prj) {
        //TODO prj == null case is now only used by test case and CubeMetaIngester
        //should refactor these test case and tool ASAP and stop supporting null case
        if (prj == null) {
            Map<String, TableDesc> globalTables = new LinkedHashMap<>();

            for (TableDesc t : srcTableMap.values()) {
                globalTables.put(t.getIdentity(), t);
            }
            return globalTables;
        }
        
        ProjectInstance project = ProjectManager.getInstance(config).getProject(prj);
        Set<String> prjTableNames = project.getTables();

        Map<String, TableDesc> ret = new LinkedHashMap<>();
        for (String tableName : prjTableNames) {
            String tableIdentity = getTableIdentity(tableName);
            ret.put(tableIdentity, getProjectSpecificTableDesc(tableIdentity, prj));
        }
        return ret;
    }

    /**
     * Get TableDesc by name
     */
    public TableDesc getTableDesc(String tableName, String prj) {
        return getProjectSpecificTableDesc(getTableIdentity(tableName), prj);
    }

    /**
     * some legacy table name may not have DB prefix
     */
    private String getTableIdentity(String tableName) {
        if (!tableName.contains("."))
            return "DEFAULT." + tableName.toUpperCase();
        else
            return tableName.toUpperCase();
    }

    /**
     * the project-specific table desc will be expand by computed columns from the projects' models
     * when the projects' model list changed, project-specific table should be reset and get expanded
     * again
     */
    public void resetProjectSpecificTableDesc(String prj) throws IOException {
        ProjectInstance project = ProjectManager.getInstance(config).getProject(prj);
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
                String s = originTableDesc.getResourcePath();
                TableDesc tableDesc = reloadSourceTableAt(s);
                srcTableMap.putLocal(key, tableDesc);
            }
        }
    }

    /**
     * make sure the returned table desc is project-specific
     */
    private TableDesc getProjectSpecificTableDesc(String fullTableName, String prj) {
        String key = mapKey(fullTableName, prj);
        TableDesc result = srcTableMap.get(key);

        if (result == null) {
            result = srcTableMap.get(mapKey(fullTableName, null));
            if (result != null) {
                result = new TableDesc(result);// deep copy of global tabledesc

                result.setProject(prj);
                result.setBorrowedFromGlobal(true);

                srcTableMap.putLocal(key, result);
            }
        }
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

        if (null == result) {
            //TODO: notice the table ext is not project-specific, seems not necessary at all
            result = srcTableExtMap.get(mapKey(t.getIdentity(), null));
        }

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

        String path = srcTable.getResourcePath();
        getStore().putResource(path, srcTable, TABLE_SERIALIZER);

        srcTableMap.put(mapKey(srcTable.getIdentity(), prj), srcTable);
    }

    public void removeSourceTable(String tableIdentity, String prj) throws IOException {
        TableDesc t = getTableDesc(tableIdentity, prj);
        if (t == null)
            return;

        String path = t.getResourcePath();
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
        this.extFilterMap = new CaseInsensitiveStringCache<>(config, "external_filter");

        reloadAllSourceTable();
        reloadAllTableExt();
        reloadAllExternalFilter();

        // touch lower level metadata before registering my listener
        Broadcaster.getInstance(config).registerListener(new SrcTableSyncListener(), "table");
        Broadcaster.getInstance(config).registerListener(new SrcTableExtSyncListener(), "table_ext");
        Broadcaster.getInstance(config).registerListener(new ExtFilterSyncListener(), "external_filter");
    }

    private class SrcTableSyncListener extends Broadcaster.Listener {

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
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            if (event == Event.DROP)
                srcTableExtMap.removeLocal(cacheKey);
            else
                reloadTableExtAt(TableExtDesc.concatRawResourcePath(cacheKey));
        }
    }

    private class ExtFilterSyncListener extends Broadcaster.Listener {

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

}
