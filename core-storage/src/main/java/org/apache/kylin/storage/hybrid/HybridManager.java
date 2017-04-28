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
package org.apache.kylin.storage.hybrid;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class HybridManager implements IRealizationProvider {
    public static final Serializer<HybridInstance> HYBRID_SERIALIZER = new JsonSerializer<HybridInstance>(HybridInstance.class);

    private static final Logger logger = LoggerFactory.getLogger(HybridManager.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, HybridManager> CACHE = new ConcurrentHashMap<KylinConfig, HybridManager>();

    public static HybridManager getInstance(KylinConfig config) {
        HybridManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (HybridManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new HybridManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init Hybrid Manager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;

    private CaseInsensitiveStringCache<HybridInstance> hybridMap;

    private HybridManager(KylinConfig config) throws IOException {
        logger.info("Initializing HybridManager with config " + config);
        this.config = config;
        this.hybridMap = new CaseInsensitiveStringCache<HybridInstance>(config, "hybrid");

        // touch lower level metadata before registering my listener
        reloadAllHybridInstance();
        Broadcaster.getInstance(config).registerListener(new HybridSyncListener(), "hybrid", "cube");
    }

    private class HybridSyncListener extends Broadcaster.Listener {

        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof HybridInstance) {
                    reloadHybridInstance(real.getName());
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) throws IOException {
            if ("hybrid".equals(entity)) {
                String hybridName = cacheKey;

                if (event == Event.DROP)
                    hybridMap.removeLocal(hybridName);
                else
                    reloadHybridInstance(hybridName);

                for (ProjectInstance prj : ProjectManager.getInstance(config).findProjects(RealizationType.HYBRID, hybridName)) {
                    broadcaster.notifyProjectSchemaUpdate(prj.getName());
                }
            } else if ("cube".equals(entity)) {
                String cubeName = cacheKey;
                for (HybridInstance hybrid : getHybridInstancesByChild(RealizationType.CUBE, cubeName)) {
                    reloadHybridInstance(hybrid.getName());
                }
            }
        }
    }

    public void reloadAllHybridInstance() throws IOException {
        ResourceStore store = getStore();
        List<String> paths = store.collectResourceRecursively(ResourceStore.HYBRID_RESOURCE_ROOT, ".json");

        hybridMap.clear();
        logger.debug("Loading Hybrid from folder " + store.getReadableResourcePath(ResourceStore.HYBRID_RESOURCE_ROOT));

        for (String path : paths) {
            reloadHybridInstanceAt(path);
        }

        logger.debug("Loaded " + paths.size() + " Hybrid(s)");
    }

    public List<HybridInstance> getHybridInstancesByChild(RealizationType type, String realizationName) {
        List<HybridInstance> result = Lists.newArrayList();
        for (HybridInstance hybridInstance : hybridMap.values()) {
            for (RealizationEntry realizationEntry : hybridInstance.getRealizationEntries()) {
                if (realizationEntry.getType() == type && realizationEntry.getRealization().equalsIgnoreCase(realizationName)) {
                    result.add(hybridInstance);
                }
            }

        }

        return result;
    }

    public void reloadHybridInstance(String name) {
        reloadHybridInstanceAt(HybridInstance.concatResourcePath(name));
    }

    private synchronized HybridInstance reloadHybridInstanceAt(String path) {
        ResourceStore store = getStore();

        HybridInstance hybridInstance = null;
        try {
            hybridInstance = store.getResource(path, HybridInstance.class, HYBRID_SERIALIZER);
            hybridInstance.setConfig(config);

            if (hybridInstance.getRealizationEntries() == null || hybridInstance.getRealizationEntries().size() == 0) {
                throw new IllegalStateException("HybridInstance must have realization entries, " + path);
            }

            if (StringUtils.isBlank(hybridInstance.getName()))
                throw new IllegalStateException("HybridInstance name must not be blank, at " + path);

            final String name = hybridInstance.getName();
            hybridMap.putLocal(name, hybridInstance);

            return hybridInstance;
        } catch (Exception e) {
            logger.error("Error during load hybrid instance " + path, e);
            return null;
        }
    }

    @Override
    public RealizationType getRealizationType() {
        return RealizationType.HYBRID;
    }

    @Override
    public IRealization getRealization(String name) {
        return getHybridInstance(name);
    }

    public Collection<HybridInstance> listHybridInstances() {
        return hybridMap.values();
    }

    public HybridInstance getHybridInstance(String name) {
        return hybridMap.get(name);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }
}
