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

package org.apache.kylin.metadata.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

/**
 */
public class DataModelManager {

    private static final Logger logger = LoggerFactory.getLogger(DataModelManager.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, DataModelManager> CACHE = new ConcurrentHashMap<KylinConfig, DataModelManager>();

    public static DataModelManager getInstance(KylinConfig config) {
        DataModelManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (DataModelManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            r = newInstance(config);
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
        }
    }
    
    private static DataModelManager newInstance(KylinConfig conf) {
        try {
            String cls = StringUtil.noBlank(conf.getDataModelManagerImpl(), DataModelManager.class.getName());
            Class<? extends DataModelManager> clz = ClassUtil.forName(cls, DataModelManager.class);
            return clz.getConstructor(KylinConfig.class).newInstance(conf);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DataModelManager from " + conf, e);
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    private Serializer<DataModelDesc> serializer;
    
    // name => DataModelDesc
    private CaseInsensitiveStringCache<DataModelDesc> dataModelDescMap;

    public DataModelManager(KylinConfig config) throws IOException {
        init(config);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }
    
    public Serializer<DataModelDesc> getDataModelSerializer() {
        if (serializer == null) {
            try {
                String cls = StringUtil.noBlank(config.getDataModelImpl(), DataModelDesc.class.getName());
                Class<? extends DataModelDesc> clz = ClassUtil.forName(cls, DataModelDesc.class);
                serializer = new JsonSerializer(clz);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return serializer;
    }

    public List<DataModelDesc> listDataModels() {
        return Lists.newArrayList(this.dataModelDescMap.values());
    }

    protected void init(KylinConfig config) throws IOException {
        this.config = config;
        this.dataModelDescMap = new CaseInsensitiveStringCache<>(config, "data_model");

        // touch lower level metadata before registering model listener
        TableMetadataManager.getInstance(config);
        
        reloadAllDataModel();
        Broadcaster.getInstance(config).registerListener(new DataModelSyncListener(), "data_model");
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
            DataModelDesc dataModelDesc = store.getResource(path, DataModelDesc.class, getDataModelSerializer());
            String prj = ProjectManager.getInstance(config).getProjectOfModel(dataModelDesc.getName()).getName();

            if (!dataModelDesc.isDraft()) {
                dataModelDesc.init(config, this.getAllTablesMap(prj), listDataModels());
            }

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
        dataModelDescMap.remove(desc.getName());
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
        getStore().putResource(path, dataModelDesc, getDataModelSerializer());
        dataModelDescMap.put(dataModelDesc.getName(), dataModelDesc);

        return dataModelDesc;
    }

    private Map<String, TableDesc> getAllTablesMap(String prj) {
        return TableMetadataManager.getInstance(config).getAllTablesMap(prj);
    }
}
