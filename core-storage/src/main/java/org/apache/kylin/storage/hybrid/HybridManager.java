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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 */
public class HybridManager implements IRealizationProvider {

    private static final Logger logger = LoggerFactory.getLogger(HybridManager.class);
    
    public static final Serializer<HybridInstance> HYBRID_SERIALIZER = new JsonSerializer<>(HybridInstance.class);

    public static HybridManager getInstance(KylinConfig config) {
        return config.getManager(HybridManager.class);
    }

    // called by reflection
    static HybridManager newInstance(KylinConfig config) throws IOException {
        return new HybridManager(config);
    }

    // ============================================================================

    private KylinConfig config;

    private CaseInsensitiveStringCache<HybridInstance> hybridMap;
    private CachedCrudAssist<HybridInstance> crud;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    private HybridManager(KylinConfig cfg) throws IOException {
        logger.info("Initializing HybridManager with config " + cfg);
        this.config = cfg;
        this.hybridMap = new CaseInsensitiveStringCache<HybridInstance>(config, "hybrid");
        this.crud = new CachedCrudAssist<HybridInstance>(getStore(), ResourceStore.HYBRID_RESOURCE_ROOT,
                HybridInstance.class, hybridMap) {
            @Override
            protected HybridInstance initEntityAfterReload(HybridInstance hybridInstance, String resourceName) {
                hybridInstance.setConfig(config);
                return hybridInstance; // noop
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new HybridSyncListener(), "hybrid", "cube");
    }

    private class HybridSyncListener extends Broadcaster.Listener {

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            try (AutoLock l = lock.lockForWrite()) {
                for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                    if (real instanceof HybridInstance) {
                        crud.reloadQuietly(real.getName());
                    }
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            if ("hybrid".equals(entity)) {
                String hybridName = cacheKey;

                try (AutoLock l = lock.lockForWrite()) {
                    if (event == Event.DROP)
                        hybridMap.removeLocal(hybridName);
                    else
                        crud.reloadQuietly(hybridName);
                }

                for (ProjectInstance prj : ProjectManager.getInstance(config).findProjects(RealizationType.HYBRID,
                        hybridName)) {
                    broadcaster.notifyProjectSchemaUpdate(prj.getName());
                }
            } else if ("cube".equals(entity)) {
                String cubeName = cacheKey;
                try (AutoLock l = lock.lockForWrite()) {
                    for (HybridInstance hybrid : getHybridInstancesByChild(RealizationType.CUBE, cubeName)) {
                        crud.reloadQuietly(hybrid.getName());
                    }
                }
            }
        }
    }

    public List<HybridInstance> getHybridInstancesByChild(RealizationType type, String realizationName) {
        try (AutoLock l = lock.lockForRead()) {
            List<HybridInstance> result = Lists.newArrayList();
            for (HybridInstance hybridInstance : hybridMap.values()) {
                for (RealizationEntry realizationEntry : hybridInstance.getRealizationEntries()) {
                    if (realizationEntry.getType() == type
                            && realizationEntry.getRealization().equalsIgnoreCase(realizationName)) {
                        result.add(hybridInstance);
                    }
                }

            }

            return result;
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
        try (AutoLock l = lock.lockForRead()) {
            return hybridMap.values();
        }
    }

    public HybridInstance getHybridInstance(String name) {
        try (AutoLock l = lock.lockForRead()) {
            return hybridMap.get(name);
        }
    }
    
    public HybridInstance reloadHybridInstance(String name) {
        try (AutoLock l = lock.lockForWrite()) {
            return crud.reload(name);
        }
    }
    
    public void reloadAllHybridInstance() throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            crud.reloadAll();
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }
}
