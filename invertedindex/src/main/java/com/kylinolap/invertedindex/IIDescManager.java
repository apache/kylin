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

package com.kylinolap.invertedindex;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.common.restclient.CaseInsensitiveStringCache;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.metadata.MetadataConstances;
import com.kylinolap.metadata.MetadataManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * copied from CubeDescManager
 * 
 * @author honma
 */
public class IIDescManager {

    private static final Logger logger = LoggerFactory.getLogger(IIDescManager.class);

    private static final Serializer<IIDesc> II_DESC_SERIALIZER = new JsonSerializer<IIDesc>(IIDesc.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, IIDescManager> CACHE = new ConcurrentHashMap<KylinConfig, IIDescManager>();

    // ============================================================================

    private KylinConfig config;
    // name ==> IIDesc
    private CaseInsensitiveStringCache<IIDesc> iiDescMap = new CaseInsensitiveStringCache<IIDesc>(Broadcaster.TYPE.INVERTED_INDEX_DESC);

    public static IIDescManager getInstance(KylinConfig config) {
        IIDescManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (IIDescManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new IIDescManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init IIDescManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    private IIDescManager(KylinConfig config) throws IOException {
        logger.info("Initializing IIDescManager with config " + config);
        this.config = config;
        reloadAllIIDesc();
    }

    public IIDesc getIIDesc(String name) {
        return iiDescMap.get(name);
    }

    /**
     * Reload IIDesc from resource store It will be triggered by an desc update
     * event.
     * 
     * @param name
     * @throws IOException
     */
    public IIDesc reloadIIDesc(String name) throws IOException {

        // Save Source
        String path = IIDesc.getIIDescResourcePath(name);

        // Reload the IIDesc
        IIDesc ndesc = loadIIDesc(path);

        // Here replace the old one
        iiDescMap.putLocal(ndesc.getName(), ndesc);
        return ndesc;
    }

    private IIDesc loadIIDesc(String path) throws IOException {
        ResourceStore store = getStore();
        logger.info("Loading IIDesc " + store.getReadableResourcePath(path));

        IIDesc ndesc = store.getResource(path, IIDesc.class, II_DESC_SERIALIZER);

        if (StringUtils.isBlank(ndesc.getName())) {
            throw new IllegalStateException("IIDesc name must not be blank");
        }

        ndesc.init(getMetadataManager());

        return ndesc;
    }

    /**
     * Create a new IIDesc
     * 
     * @param iiDesc
     * @return
     * @throws IOException
     */
    public IIDesc createIIDesc(IIDesc iiDesc) throws IOException {
        if (iiDesc.getUuid() == null || iiDesc.getName() == null)
            throw new IllegalArgumentException();

        if (iiDescMap.containsKey(iiDesc.getName()))
            throw new IllegalArgumentException("IIDesc '" + iiDesc.getName() + "' already exists");

        iiDesc.init(getMetadataManager());

        // Check base validation
        // Semantic validation
        // TODO

        iiDesc.setSignature(iiDesc.calculateSignature());

        String path = iiDesc.getResourcePath();
        getStore().putResource(path, iiDesc, II_DESC_SERIALIZER);
        iiDescMap.put(iiDesc.getName(), iiDesc);

        return iiDesc;
    }

    // remove iiDesc
    public void removeIIDesc(IIDesc iiDesc) throws IOException {
        String path = iiDesc.getResourcePath();
        getStore().deleteResource(path);
        iiDescMap.remove(iiDesc.getName());
    }

    public void removeIIDescLocal(String name) throws IOException {
        iiDescMap.remove(name);
    }

    private void reloadAllIIDesc() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading all II desc from folder " + store.getReadableResourcePath(ResourceStore.II_DESC_RESOURCE_ROOT));

        iiDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.II_DESC_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            logger.info("loading II Desc from path" + path);
            IIDesc desc;
            try {
                desc = loadIIDesc(path);
            } catch (Exception e) {
                logger.error("Error loading II desc " + path, e);
                continue;
            }
            if (!path.equals(desc.getResourcePath())) {
                logger.error("Skip suspicious desc at " + path + ", " + desc + " should be at " + desc.getResourcePath());
                continue;
            }
            if (iiDescMap.containsKey(desc.getName())) {
                logger.error("Dup IIDesc name '" + desc.getName() + "' on path " + path);
                continue;
            }

            iiDescMap.putLocal(desc.getName(), desc);
        }

        logger.debug("Loaded " + iiDescMap.size() + " II desc(s)");
    }

    /**
     * Update IIDesc with the input. Broadcast the event into cluster
     * 
     * @param desc
     * @return
     * @throws IOException
     */
    public IIDesc updateIIDesc(IIDesc desc) throws IOException {
        // Validate IIDesc
        if (desc.getUuid() == null || desc.getName() == null) {
            throw new IllegalArgumentException();
        }
        String name = desc.getName();
        if (!iiDescMap.containsKey(name)) {
            throw new IllegalArgumentException("IIDesc '" + name + "' does not exist.");
        }

        desc.init(getMetadataManager());

        // TODO: Semantic validation

        desc.setSignature(desc.calculateSignature());

        // Save Source
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, II_DESC_SERIALIZER);

        // Reload the IIDesc
        IIDesc ndesc = loadIIDesc(path);
        // Here replace the old one
        iiDescMap.put(ndesc.getName(), desc);

        return ndesc;
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

}
