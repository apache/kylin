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
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;
import com.kylinolap.metadata.model.schema.TableDesc;
import com.kylinolap.metadata.validation.CubeMetadataValidator;
import com.kylinolap.metadata.validation.ValidateContext;

/**
 * Serves (and caches) cube metadata for Kylin instance.
 * <p/>
 * Also provides a ResourceStore for general purpose data persistence. Cube
 * metadata is serialized as JSON and stored in ResourceStore.
 * 
 * @author yangli9
 */
public class MetadataManager {

    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);

    private static final Serializer<CubeDesc> CUBE_SERIALIZER = new JsonSerializer<CubeDesc>(CubeDesc.class);
    private static final Serializer<TableDesc> TABLE_SERIALIZER = new JsonSerializer<TableDesc>(TableDesc.class);
    private static final Serializer<InvertedIndexDesc> IIDESC_SERIALIZER = new JsonSerializer<InvertedIndexDesc>(InvertedIndexDesc.class);

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
                throw new IllegalStateException("Failed to init CubeManager from " + config, e);
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
    // name ==> CubeDesc
    private SingleValueCache<String, CubeDesc> cubeDescMap = new SingleValueCache<String, CubeDesc>(Broadcaster.TYPE.METADATA);
    // name ==> InvertedIndexDesc
    private SingleValueCache<String, InvertedIndexDesc> iiDescMap = new SingleValueCache<String, InvertedIndexDesc>(Broadcaster.TYPE.METADATA);
    // name => value
    private SingleValueCache<String, Map<String, String>> srcTableExdMap = new SingleValueCache<String, Map<String, String>>(Broadcaster.TYPE.METADATA);

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
        tableName = tableName.toUpperCase();
        return srcTableMap.get(tableName);
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstances}
     * 
     * @param tableName
     * @return
     */
    public Map<String, String> getTableDescExd(String tableName) {
        tableName = tableName.toUpperCase();
        Map<String, String> result = new HashMap<String, String>();
        if (srcTableExdMap.containsKey(tableName)) {
            Map<String, String> tmp = srcTableExdMap.get(tableName);
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
        if (srcTable.getUuid() == null || srcTable.getName() == null)
            throw new IllegalArgumentException();
        if (srcTableMap.containsKey(srcTable.getName()))
            throw new IllegalArgumentException("SourceTable '" + srcTable.getName() + "' already exists");

        String path = srcTable.getResourcePath();
        getStore().putResource(path, srcTable, TABLE_SERIALIZER);

        srcTableMap.put(srcTable.getName(), srcTable);
    }

    public InvertedIndexDesc getInvertedIndexDesc(String name) {
        return iiDescMap.get(name);
    }

    public CubeDesc getCubeDesc(String name) {
        return cubeDescMap.get(name);
    }

    /**
     * Create a new CubeDesc
     * 
     * @param cubeDesc
     * @return
     * @throws IOException
     */
    public CubeDesc createCubeDesc(CubeDesc cubeDesc) throws IOException {
        if (cubeDesc.getUuid() == null || cubeDesc.getName() == null)
            throw new IllegalArgumentException();
        if (cubeDescMap.containsKey(cubeDesc.getName()))
            throw new IllegalArgumentException("CubeDesc '" + cubeDesc.getName() + "' already exists");

        try {
            cubeDesc.init(config, srcTableMap.getMap());
        } catch (IllegalStateException e) {
            cubeDesc.addError(e.getMessage(), true);
        }
        // Check base validation
        if (!cubeDesc.getError().isEmpty()) {
            return cubeDesc;
        }
        // Semantic validation
        CubeMetadataValidator validator = new CubeMetadataValidator();
        ValidateContext context = validator.validate(cubeDesc, true);
        if (!context.ifPass()) {
            return cubeDesc;
        }

        cubeDesc.setSignature(cubeDesc.calculateSignature());

        String path = cubeDesc.getResourcePath();
        getStore().putResource(path, cubeDesc, CUBE_SERIALIZER);
        cubeDescMap.put(cubeDesc.getName(), cubeDesc);

        return cubeDesc;
    }
    
    // remove cubeDesc
    public void removeCubeDesc(CubeDesc cubeDesc) throws IOException{
        String path = cubeDesc.getResourcePath();
        getStore().deleteResource(path);
        cubeDescMap.remove(cubeDesc.getName());
    }

    // sync on update
    private void init(KylinConfig config) throws IOException {
        this.config = config;
        reloadAllSourceTable();
        reloadAllSourceTableExd();
        reloadAllCubeDesc();
        reloadAllInvertedIndexDesc();
    }

    private void reloadAllSourceTableExd() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable exd info from folder " + store.getReadableResourcePath(ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        srcTableExdMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_EXD_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            Map<String, String> attrContainer = new HashMap<String, String>();
            String tableName = loadSourceTableExd(getStore(), path, attrContainer);
            srcTableExdMap.putLocal(tableName.toUpperCase(), attrContainer);
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

        if (StringUtils.isBlank(t.getName()))
            throw new IllegalStateException("SourceTable name must not be blank");
        if (srcTableMap.containsKey(t.getName()))
            throw new IllegalStateException("Dup SourceTable name '" + t.getName() + "'");

        srcTableMap.putLocal(t.getName(), t);

        return t;
    }

    private void reloadAllCubeDesc() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading Cube Metadata from folder " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));

        cubeDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.CUBE_DESC_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            CubeDesc desc;
            try {
                desc = loadCubeDesc(path);
            } catch (Exception e) {
                logger.error("Error loading cube desc " + path, e);
                continue;
            }
            if (path.equals(desc.getResourcePath()) == false) {
                logger.error("Skip suspicious desc at " + path + ", " + desc + " should be at " + desc.getResourcePath());
                continue;
            }
            if (cubeDescMap.containsKey(desc.getName())) {
                logger.error("Dup CubeDesc name '" + desc.getName() + "' on path " + path);
                continue;
            }

            cubeDescMap.putLocal(desc.getName(), desc);
        }

        logger.debug("Loaded " + cubeDescMap.size() + " Cube(s)");
    }

    private CubeDesc loadCubeDesc(String path) throws IOException {
        ResourceStore store = getStore();
        logger.debug("Loading CubeDesc " + store.getReadableResourcePath(path));

        CubeDesc ndesc = store.getResource(path, CubeDesc.class, CUBE_SERIALIZER);

        if (StringUtils.isBlank(ndesc.getName())) {
            throw new IllegalStateException("CubeDesc name must not be blank");
        }

        ndesc.init(config, srcTableMap.getMap());

        if (ndesc.getError().isEmpty() == false) {
            throw new IllegalStateException("Cube desc at " + path + " has issues: " + ndesc.getError());
        }

        return ndesc;
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
     * Update CubeDesc with the input. Broadcast the event into cluster
     * 
     * @param desc
     * @return
     * @throws IOException
     */
    public CubeDesc updateCubeDesc(CubeDesc desc) throws IOException {
        // Validate CubeDesc
        if (desc.getUuid() == null || desc.getName() == null) {
            throw new IllegalArgumentException();
        }
        String name = desc.getName();
        if (!cubeDescMap.containsKey(name)) {
            throw new IllegalArgumentException("CubeDesc '" + name + "' does not exist.");
        }

        try {
            desc.init(config, srcTableMap.getMap());
        } catch (IllegalStateException e) {
            desc.addError(e.getMessage(), true);
            return desc;
        } catch (IllegalArgumentException e) {
            desc.addError(e.getMessage(), true);
            return desc;
        }

        // Semantic validation
        CubeMetadataValidator validator = new CubeMetadataValidator();
        ValidateContext context = validator.validate(desc, true);
        if (!context.ifPass()) {
            return desc;
        }

        desc.setSignature(desc.calculateSignature());

        // Save Source
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, CUBE_SERIALIZER);

        // Reload the CubeDesc
        CubeDesc ndesc = loadCubeDesc(path);
        // Here replace the old one
        cubeDescMap.put(ndesc.getName(), desc);

        return ndesc;
    }

    /**
     * Reload CubeDesc from resource store It will be triggered by an desc
     * update event.
     * 
     * @param name
     * @throws IOException
     */
    public CubeDesc reloadCubeDesc(String name) throws IOException {

        // Save Source
        String path = CubeDesc.getCubeDescResourcePath(name);

        // Reload the CubeDesc
        CubeDesc ndesc = loadCubeDesc(path);

        // Here replace the old one
        cubeDescMap.put(ndesc.getName(), ndesc);
        return ndesc;
    }

    /**
     * Tell CubeManager that the cube instance has changed. The cube info will
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

}
