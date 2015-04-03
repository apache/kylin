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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.DataModelDesc;

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
    public static final Serializer<DataModelDesc> MODELDESC_SERIALIZER = new JsonSerializer<DataModelDesc>(DataModelDesc.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, MetadataManager> CACHE = new ConcurrentHashMap<KylinConfig, MetadataManager>();

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
    private CaseInsensitiveStringCache<TableDesc> srcTableMap = new CaseInsensitiveStringCache<TableDesc>(Broadcaster.TYPE.TABLE);
    // name => value
    private CaseInsensitiveStringCache<Map<String, String>> srcTableExdMap = new CaseInsensitiveStringCache<Map<String, String>>(Broadcaster.TYPE.TABLE);
    // name => DataModelDesc
    private CaseInsensitiveStringCache<DataModelDesc> dataModelDescMap = new CaseInsensitiveStringCache<DataModelDesc>(Broadcaster.TYPE.DATA_MODEL);

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

    public List<TableDesc> listAllTables() {
        return Lists.newArrayList(srcTableMap.values());
    }

    public Map<String, TableDesc> getAllTablesMap() {
        return Collections.unmodifiableMap(srcTableMap.getMap());
    }

    public Map<String, Map<String, String>> listAllTableExdMap() {
        return srcTableExdMap.getMap();
    }

    /**
     * Get Table Desc object
     * 
     * @param tableName
     * @return
     */
    public TableDesc getTableDesc(String tableName) {
        if (tableName != null && tableName.indexOf(".") < 0)
            tableName = "DEFAULT." + tableName;
        
        TableDesc result = srcTableMap.get(tableName.toUpperCase());
        return result;
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstants}
     * 
     * @param tableName
     * @return
     */
    public Map<String, String> getTableDescExd(String tableName) {
        String tableIdentity = tableName;
        Map<String, String> result = new HashMap<String, String>();
        if (srcTableExdMap.containsKey(tableIdentity)) {
            Map<String, String> tmp = srcTableExdMap.get(tableIdentity);
            Iterator<Entry<String, String>> it = tmp.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, String> entry = it.next();
                result.put(entry.getKey(), entry.getValue());
            }
            result.put(MetadataConstants.TABLE_EXD_STATUS_KEY, "true");
        } else {
            result.put(MetadataConstants.TABLE_EXD_STATUS_KEY, "false");
        }
        return result;
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

    private void init(KylinConfig config) throws IOException {
        this.config = config;
        reloadAllSourceTable();
        reloadAllSourceTableExd();
        reloadAllDataModel();
    }

    private void reloadAllSourceTableExd() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable exd info from folder " + store.getReadableResourcePath(ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        srcTableExdMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_EXD_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadSourceTableExdAt(path);
        }

        logger.debug("Loaded " + srcTableExdMap.size() + " SourceTable EXD(s)");
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> reloadSourceTableExdAt(String path) throws IOException {
        Map<String, String> attrs = Maps.newHashMap();

        ResourceStore store = getStore();
        InputStream is = store.getResource(path);
        if (is == null) {
            logger.warn("Failed to get table exd info from " + path);
            return null;
        }

        try {
            attrs.putAll(JsonUtil.readValue(is, HashMap.class));
        } finally {
            if (is != null)
                is.close();
        }

        // parse table identity from file name
        String file = path;
        if (file.indexOf("/") > -1) {
            file = file.substring(file.lastIndexOf("/") + 1);
        }
        String tableIdentity = file.substring(0, file.length() - MetadataConstants.FILE_SURFIX.length()).toUpperCase();

        srcTableExdMap.putLocal(tableIdentity, attrs);
        return attrs;
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
            logger.error("Didn't load table at " + path);
            return null;
        }
        t.init();

        String tableIdentity = t.getIdentity();

        srcTableMap.putLocal(tableIdentity, t);

        return t;
    }

    public void reloadSourceTableExt(String tableIdentity) throws IOException {
        reloadSourceTableExdAt(TableDesc.concatExdResourcePath(tableIdentity));
    }

    public void reloadSourceTable(String tableIdentity) throws IOException {
        reloadSourceTableAt(TableDesc.concatResourcePath(tableIdentity));
    }

    public void reloadTableCache(String tableIdentity) throws IOException {
        reloadSourceTableExt(tableIdentity);
        reloadSourceTable(tableIdentity);
    }

    public DataModelDesc getDataModelDesc(String name) {
        return dataModelDescMap.get(name);
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
            dataModelDesc.init(this.getAllTablesMap());
            dataModelDescMap.putLocal(dataModelDesc.getName(), dataModelDesc);
            return dataModelDesc;
        } catch (IOException e) {
            throw new IllegalStateException("Error to load" + path, e);
        }
    }

    public DataModelDesc createDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        String name = dataModelDesc.getName();
        if (dataModelDescMap.containsKey(name))
            throw new IllegalArgumentException("DataModelDesc '" + name + "' already exists");

        return saveDataModelDesc(dataModelDesc);
    }

    public DataModelDesc updateDataModelDesc(DataModelDesc desc) throws IOException {
        String name = desc.getName();
        if (!dataModelDescMap.containsKey(name)) {
            throw new IllegalArgumentException("DataModelDesc '" + name + "' does not exist.");
        }

        return saveDataModelDesc(desc);
    }

    private DataModelDesc saveDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        dataModelDesc.init(this.getAllTablesMap());

        String path = dataModelDesc.getResourcePath();
        getStore().putResource(path, dataModelDesc, MODELDESC_SERIALIZER);
        dataModelDescMap.put(dataModelDesc.getName(), dataModelDesc);

        return dataModelDesc;
    }

    public void saveTableExd(String tableId, Map<String, String> tableExdProperties) throws IOException {
        if (tableId == null) {
            throw new IllegalArgumentException("tableId couldn't be null");
        }
        TableDesc srcTable = srcTableMap.get(tableId);
        if (srcTable == null) {
            throw new IllegalArgumentException("Couldn't find Source Table with identifier: " + tableId);
        }

        String path = TableDesc.concatExdResourcePath(tableId);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        JsonUtil.writeValueIndent(os, tableExdProperties);
        os.flush();
        InputStream is = new ByteArrayInputStream(os.toByteArray());
        getStore().putResource(path, is, System.currentTimeMillis());
        os.close();
        is.close();

        srcTableExdMap.putLocal(tableId, tableExdProperties);
    }

    public String appendDBName(String table) {

        if (table.indexOf(".") > 0)
            return table;

        Map<String, TableDesc> map = getAllTablesMap();

        int count = 0;
        String result = null;
        for (TableDesc t : map.values()) {
            if (t.getName().equalsIgnoreCase(table)) {
                result = t.getIdentity();
                count++;
            }
        }

        if (count == 1)
            return result;

        if (count > 1) {
            logger.warn("There are more than 1 table named with '" + table + "' in different database; The program couldn't determine, randomly pick '" + result + "'");
        }
        return result;
    }

}
