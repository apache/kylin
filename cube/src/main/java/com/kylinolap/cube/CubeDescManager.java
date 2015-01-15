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

package com.kylinolap.cube;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.kylinolap.common.restclient.CaseInsensitiveStringCache;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.validation.CubeMetadataValidator;
import com.kylinolap.cube.model.validation.ValidateContext;
import com.kylinolap.metadata.MetadataConstances;
import com.kylinolap.metadata.MetadataManager;

/**
 * Manager class for CubeDesc; extracted from #CubeManager
 * @author shaoshi
 *
 */
public class CubeDescManager {

    private static final Logger logger = LoggerFactory.getLogger(CubeDescManager.class);

    private static final Serializer<CubeDesc> CUBE_DESC_SERIALIZER = new JsonSerializer<CubeDesc>(CubeDesc.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, CubeDescManager> CACHE = new ConcurrentHashMap<KylinConfig, CubeDescManager>();

    // ============================================================================

    private KylinConfig config;
    // name ==> CubeDesc
    private CaseInsensitiveStringCache<CubeDesc> cubeDescMap = new CaseInsensitiveStringCache<CubeDesc>(Broadcaster.TYPE.CUBE_DESC);

    public static CubeDescManager getInstance(KylinConfig config) {
        CubeDescManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (CubeDescManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new CubeDescManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init CubeDescManager from " + config, e);
            }
        }
    }

    public static void clearCache() {
        CACHE.clear();
    }

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    private CubeDescManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeDescManager with config " + config);
        this.config = config;
        reloadAllCubeDesc();
    }

    public CubeDesc getCubeDesc(String name) {
        return cubeDescMap.get(name);
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

    private CubeDesc loadCubeDesc(String path) throws IOException {
        ResourceStore store = getStore();

        CubeDesc ndesc = store.getResource(path, CubeDesc.class, CUBE_DESC_SERIALIZER);

        if (StringUtils.isBlank(ndesc.getName())) {
            throw new IllegalStateException("CubeDesc name must not be blank");
        }

        ndesc.init(config, getMetadataManager().getAllTablesMap());

        if (ndesc.getError().isEmpty() == false) {
            throw new IllegalStateException("Cube desc at " + path + " has issues: " + ndesc.getError());
        }

        return ndesc;
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
            cubeDesc.init(config, getMetadataManager().getAllTablesMap());
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
        getStore().putResource(path, cubeDesc, CUBE_DESC_SERIALIZER);
        cubeDescMap.put(cubeDesc.getName(), cubeDesc);

        return cubeDesc;
    }

    // remove cubeDesc
    public void removeCubeDesc(CubeDesc cubeDesc) throws IOException {
        String path = cubeDesc.getResourcePath();
        getStore().deleteResource(path);
        cubeDescMap.remove(cubeDesc.getName());
    }

    // remove cubeDesc
    public void removeLocalCubeDesc(String name) throws IOException {
        cubeDescMap.removeLocal(name);
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
            desc.init(config, getMetadataManager().getAllTablesMap());
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
        getStore().putResource(path, desc, CUBE_DESC_SERIALIZER);

        // Reload the CubeDesc
        CubeDesc ndesc = loadCubeDesc(path);
        // Here replace the old one
        cubeDescMap.put(ndesc.getName(), desc);

        return ndesc;
    }

    private MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(config);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

}
