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
package com.kylinolap.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.common.restclient.SingleValueCache;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.metadata.model.DataModelDesc;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;

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

    private static final Serializer<TableDesc> TABLE_SERIALIZER = new JsonSerializer<TableDesc>(TableDesc.class);
    private static final Serializer<InvertedIndexDesc> IIDESC_SERIALIZER = new JsonSerializer<InvertedIndexDesc>(InvertedIndexDesc.class);
    private static final Serializer<DataModelDesc> MODELDESC_SERIALIZER = new JsonSerializer<DataModelDesc>(DataModelDesc.class);

    TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
    };

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

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    public static void dropCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // table name ==> SourceTable
    private SingleValueCache<String, TableDesc> srcTableMap = new SingleValueCache<String, TableDesc>(Broadcaster.TYPE.METADATA);
    // name ==> InvertedIndexDesc
    private SingleValueCache<String, InvertedIndexDesc> iiDescMap = new SingleValueCache<String, InvertedIndexDesc>(Broadcaster.TYPE.METADATA);
    // name => value
    private SingleValueCache<String, Map<String, String>> srcTableExdMap = new SingleValueCache<String, Map<String, String>>(Broadcaster.TYPE.METADATA);
    // name => DataModelDesc
    private SingleValueCache<String, DataModelDesc> dataModelDescMap = new SingleValueCache<String, DataModelDesc>(Broadcaster.TYPE.METADATA);

    private MetadataManager(KylinConfig config) throws IOException {
        init(config);
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
        return srcTableMap.get(TableDesc.getTableIdentity(tableName));
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstances}
     * 
     * @param tableName
     * @return
     */
    public Map<String, String> getTableDescExd(String tableName) {
        String tableIdentity = TableDesc.getTableIdentity(tableName);
        Map<String, String> result = new HashMap<String, String>();
        if (srcTableExdMap.containsKey(tableIdentity)) {
            Map<String, String> tmp = srcTableExdMap.get(tableIdentity);
            Iterator<Entry<String, String>> it = tmp.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, String> entry = it.next();
                result.put(entry.getKey(), entry.getValue());
            }
            result.put(MetadataConstances.TABLE_EXD_STATUS_KEY, "true");
        } else {
            result.put(MetadataConstances.TABLE_EXD_STATUS_KEY, "false");
        }
        return result;
    }

    public void createSourceTable(TableDesc srcTable) throws IOException {
        if (srcTable.getUuid() == null || srcTable.getIdentity() == null) {
            throw new IllegalArgumentException();
        }
        String tableIdentity = TableDesc.getTableIdentity(srcTable);
        if (srcTableMap.containsKey(tableIdentity)) {
            throw new IllegalArgumentException("SourceTable '" + srcTable.getIdentity() + "' already exists");
        }

        String path = srcTable.getResourcePath();
        getStore().putResource(path, srcTable, TABLE_SERIALIZER);

        srcTableMap.put(tableIdentity, srcTable);
    }

    public InvertedIndexDesc getInvertedIndexDesc(String name) {
        return iiDescMap.get(name);
    }

    // sync on update
    private void init(KylinConfig config) throws IOException {
        this.config = config;
        reloadAllSourceTable();
        reloadAllSourceTableExd();
        reloadAllInvertedIndexDesc();
        reloadAllDataModel();
    }

    private void reloadAllSourceTableExd() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable exd info from folder " + store.getReadableResourcePath(ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        srcTableExdMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_EXD_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            Map<String, String> attrContainer = new HashMap<String, String>();
            String tableName = loadSourceTableExd(getStore(), path, attrContainer);
            srcTableExdMap.putLocal(TableDesc.getTableIdentity(tableName), attrContainer);
        }
        logger.debug("Loaded " + paths.size() + " SourceTable EXD(s)");
    }

    private void reloadAllSourceTable() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable from folder " + store.getReadableResourcePath(ResourceStore.TABLE_RESOURCE_ROOT));

        srcTableMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            loadSourceTable(path);
        }

        logger.debug("Loaded " + paths.size() + " SourceTable(s)");
    }

    @SuppressWarnings("unchecked")
    /**
     * return table name
     */
    public static String loadSourceTableExd(ResourceStore store, String path, Map<String, String> attrContainer) throws IOException {

        logger.debug("Loading SourceTable exd " + path);
        InputStream is = store.getResource(path);
        if (is != null) {
            attrContainer.putAll(JsonUtil.readValue(is, HashMap.class));
            String file = path;
            if (file.indexOf("/") > -1) {
                file = file.substring(file.lastIndexOf("/") + 1);
            }
            return file.substring(0, file.length() - MetadataConstances.FILE_SURFIX.length());
        } else {
            logger.debug("Failed to get table exd info from " + path);
            return null;
        }
    }

    private TableDesc loadSourceTable(String path) throws IOException {
        ResourceStore store = getStore();
        logger.debug("Loading SourceTable " + store.getReadableResourcePath(path));

        TableDesc t = store.getResource(path, TableDesc.class, TABLE_SERIALIZER);
        t.init();

        String tableIdentity = TableDesc.getTableIdentity(t);
        if (StringUtils.isBlank(tableIdentity)) {
            throw new IllegalStateException("SourceTable name must not be blank");
        }
        if (srcTableMap.containsKey(tableIdentity)) {
            throw new IllegalStateException("Dup SourceTable name '" + tableIdentity + "'");
        }

        srcTableMap.putLocal(tableIdentity, t);

        return t;
    }

    private void reloadAllInvertedIndexDesc() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading Inverted Index Desc from folder " + store.getReadableResourcePath(ResourceStore.IIDESC_RESOURCE_ROOT));

        iiDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.IIDESC_RESOURCE_ROOT, ".json");
        for (String path : paths) {
            InvertedIndexDesc desc;
            try {
                desc = loadInvertedIndexDesc(path);
            } catch (Exception e) {
                logger.error("Error loading inverted index desc " + path, e);
                continue;
            }
            if (path.equals(desc.getResourcePath()) == false) {
                logger.error("Skip suspicious desc at " + path + ", " + desc + " should be at " + desc.getResourcePath());
                continue;
            }
            if (iiDescMap.containsKey(desc.getName())) {
                logger.error("Dup InvertedIndexDesc name '" + desc.getName() + "' on path " + path);
                continue;
            }

            iiDescMap.putLocal(desc.getName(), desc);
        }

        logger.debug("Loaded " + iiDescMap.size() + " Inverted Index Desc(s)");
    }

    private InvertedIndexDesc loadInvertedIndexDesc(String path) throws IOException {
        ResourceStore store = getStore();
        logger.debug("Loading InvertedIndexDesc " + store.getReadableResourcePath(path));

        InvertedIndexDesc desc = store.getResource(path, InvertedIndexDesc.class, IIDESC_SERIALIZER);
        if (StringUtils.isBlank(desc.getName())) {
            throw new IllegalStateException("InvertedIndexDesc name must not be blank");
        }

        desc.init(this);

        return desc;
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
        removeInstance(config);
        getInstance(config);
    }

    public DataModelDesc getDataModelDesc(String name) {
        return dataModelDescMap.get(name);
    }
    

    private void reloadAllDataModel() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading DataModel from folder " + store.getReadableResourcePath(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT));

        this.dataModelDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            DataModelDesc modelDesc = this.loadDataModelDesc(path);
            dataModelDescMap.putLocal(modelDesc.getName(), modelDesc);
        }

        logger.debug("Loaded " + paths.size() + " DataModel(s)");
    }

    public DataModelDesc createDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        if (dataModelDescMap.containsKey(dataModelDesc.getName()))
            throw new IllegalArgumentException("DataModelDesc '" + dataModelDesc.getName() + "' already exists");

        try {
            dataModelDesc.init(this.getAllTablesMap());
        } catch (IllegalStateException e) {
            dataModelDesc.addError(e.getMessage(), true);
        }
        // Check base validation
        if (!dataModelDesc.getError().isEmpty()) {
            return dataModelDesc;
        }

        String path = dataModelDesc.getResourcePath();
        getStore().putResource(path, dataModelDesc, MODELDESC_SERIALIZER);
        dataModelDescMap.put(dataModelDesc.getName(), dataModelDesc);

        return dataModelDesc;
    }
    
    /**
     * Update DataModelDesc with the input. Broadcast the event into cluster
     * 
     * @param desc
     * @return
     * @throws IOException
     */
    public DataModelDesc updateDataModelDesc(DataModelDesc desc) throws IOException {
        String name = desc.getName();
        if (!dataModelDescMap.containsKey(name)) {
            throw new IllegalArgumentException("DataModelDesc '" + name + "' does not exist.");
        }

        try {
            desc.init(this.getAllTablesMap());
        } catch (IllegalStateException e) {
            desc.addError(e.getMessage(), true);
            return desc;
        } catch (IllegalArgumentException e) {
            desc.addError(e.getMessage(), true);
            return desc;
        }


        // Save Source
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, MODELDESC_SERIALIZER);

        // Reload the DataModelDesc
        DataModelDesc ndesc = loadDataModelDesc(path);
        // Here replace the old one
        dataModelDescMap.put(ndesc.getName(), desc);

        return ndesc;
    }

    private DataModelDesc loadDataModelDesc(String path) throws IOException {
        ResourceStore store = getStore();
        logger.debug("Loading DataModelDesc " + store.getReadableResourcePath(path));
        DataModelDesc ndesc = null;
        try {
            ndesc = store.getResource(path, DataModelDesc.class, MODELDESC_SERIALIZER);

        } catch (IOException e) {
            System.err.println("Error to load" + path + ", exception is " + e.toString());
            throw e;
        }
        if (StringUtils.isBlank(ndesc.getName())) {
            throw new IllegalStateException("DataModelDesc name must not be blank");
        }

        ndesc.init(this.getAllTablesMap());

        if (ndesc.getError().isEmpty() == false) {
            throw new IllegalStateException("DataModelDesc at " + path + " has issues: " + ndesc.getError());
        }

        return ndesc;
    }


    public void deleteDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        // remove dataModelDesc
        String path = dataModelDesc.getResourcePath();
        getStore().deleteResource(path);
        dataModelDescMap.remove(dataModelDesc.getName());
    }

}
