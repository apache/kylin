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

package org.apache.kylin.cube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.CubeMetadataValidator;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager class for CubeDesc; extracted from #CubeManager
 * 
 * @author shaoshi
 */
public class CubeDescManager {

    private static final Logger logger = LoggerFactory.getLogger(CubeDescManager.class);

    public static final Serializer<CubeDesc> CUBE_DESC_SERIALIZER = new JsonSerializer<CubeDesc>(CubeDesc.class);

    // static cached instances
    private static final ConcurrentMap<KylinConfig, CubeDescManager> CACHE = new ConcurrentHashMap<KylinConfig, CubeDescManager>();

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

    // ============================================================================

    private KylinConfig config;
    // name ==> CubeDesc
    private CaseInsensitiveStringCache<CubeDesc> cubeDescMap;

    private CubeDescManager(KylinConfig config) throws IOException {
        logger.info("Initializing CubeDescManager with config " + config);
        this.config = config;
        this.cubeDescMap = new CaseInsensitiveStringCache<CubeDesc>(config, "cube_desc");

        // touch lower level metadata before registering my listener
        reloadAllCubeDesc();
        Broadcaster.getInstance(config).registerListener(new CubeDescSyncListener(), "cube_desc");
    }

    private class CubeDescSyncListener extends Broadcaster.Listener {

        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            clearCache();
            Cuboid.clearCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof CubeInstance) {
                    String descName = ((CubeInstance) real).getDescName();
                    reloadCubeDescLocal(descName);
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) throws IOException {
            String cubeDescName = cacheKey;
            CubeDesc cubeDesc = getCubeDesc(cubeDescName);
            String modelName = cubeDesc == null ? null : cubeDesc.getModel().getName();

            if (event == Event.DROP)
                removeLocalCubeDesc(cubeDescName);
            else
                reloadCubeDescLocal(cubeDescName);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByModel(modelName)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
    }

    public CubeDesc getCubeDesc(String name) {
        return cubeDescMap.get(name);
    }

    public List<CubeDesc> listAllDesc() {
        return new ArrayList<CubeDesc>(cubeDescMap.values());
    }

    /**
     * Reload CubeDesc from resource store It will be triggered by an desc
     * update event.
     * 
     * @param name
     * @throws IOException
     */
    public CubeDesc reloadCubeDescLocal(String name) throws IOException {
        // Broken CubeDesc is not allowed to be saved and broadcast.
        CubeDesc ndesc = loadCubeDesc(CubeDesc.concatResourcePath(name), false);

        cubeDescMap.putLocal(ndesc.getName(), ndesc);
        Cuboid.reloadCache(name);

        // if related cube is in DESCBROKEN state before, change it back to DISABLED
        CubeManager cubeManager = CubeManager.getInstance(config);
        for (CubeInstance cube : cubeManager.getCubesByDesc(name)) {
            if (cube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
                cubeManager.reloadCubeLocal(cube.getName());
            }
        }

        return ndesc;
    }

    private CubeDesc loadCubeDesc(String path, boolean allowBroken) throws IOException {
        ResourceStore store = getStore();
        CubeDesc ndesc = store.getResource(path, CubeDesc.class, CUBE_DESC_SERIALIZER);
        if (ndesc == null)
            throw new IllegalArgumentException("No cube desc found at " + path);

        try {
            ndesc.init(config);
        } catch (Exception e) {
            logger.warn("Broken cube desc " + path, e);
            ndesc.addError(e.getMessage());
        }

        if (!allowBroken && !ndesc.getError().isEmpty()) {
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
            cubeDesc.init(config);
        } catch (Exception e) {
            logger.warn("Broken cube desc " + cubeDesc, e);
            cubeDesc.addError(e.getMessage());
        }
        postProcessCubeDesc(cubeDesc);
        // Check base validation
        if (!cubeDesc.getError().isEmpty()) {
            return cubeDesc;
        }
        // Semantic validation
        CubeMetadataValidator validator = new CubeMetadataValidator();
        ValidateContext context = validator.validate(cubeDesc);
        if (!context.ifPass()) {
            return cubeDesc;
        }

        cubeDesc.setSignature(cubeDesc.calculateSignature());

        String path = cubeDesc.getResourcePath();
        getStore().putResource(path, cubeDesc, CUBE_DESC_SERIALIZER);
        cubeDescMap.put(cubeDesc.getName(), cubeDesc);

        return cubeDesc;
    }

    /**
     * if there is some change need be applied after getting a cubeDesc from front-end, do it here
     * @param cubeDesc
     */
    private void postProcessCubeDesc(CubeDesc cubeDesc) {
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            if (TopNMeasureType.FUNC_TOP_N.equalsIgnoreCase(measureDesc.getFunction().getExpression())) {
                // update return type scale with the estimated key length
                Map<String, String> configuration = measureDesc.getFunction().getConfiguration();
                ParameterDesc parameter = measureDesc.getFunction().getParameter();
                parameter = parameter.getNextParameter();
                int keyLength = 0;
                while (parameter != null) {
                    String encoding = configuration.get(TopNMeasureType.CONFIG_ENCODING_PREFIX + parameter.getValue());
                    String encodingVersionStr = configuration.get(TopNMeasureType.CONFIG_ENCODING_VERSION_PREFIX + parameter.getValue());
                    if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
                        keyLength += DictionaryDimEnc.MAX_ENCODING_LENGTH; // estimation for dict encoding
                    } else {
                        // non-dict encoding
                        int encodingVersion = 1;
                        if (!StringUtils.isEmpty(encodingVersionStr)) {
                            try {
                                encodingVersion = Integer.parseInt(encodingVersionStr);
                            } catch (NumberFormatException e) {
                                throw new RuntimeException("invalid encoding version: " + encodingVersionStr);
                            }
                        }
                        Object[] encodingConf = DimensionEncoding.parseEncodingConf(encoding);
                        DimensionEncoding dimensionEncoding = DimensionEncodingFactory.create((String) encodingConf[0], (String[]) encodingConf[1], encodingVersion);
                        keyLength += dimensionEncoding.getLengthOfEncoding();
                    }

                    parameter = parameter.getNextParameter();
                }

                DataType returnType = DataType.getType(measureDesc.getFunction().getReturnType());
                DataType newReturnType = new DataType(returnType.getName(), returnType.getPrecision(), keyLength);
                measureDesc.getFunction().setReturnType(newReturnType.toString());
            }
        }
    }

    // remove cubeDesc
    public void removeCubeDesc(CubeDesc cubeDesc) throws IOException {
        String path = cubeDesc.getResourcePath();
        getStore().deleteResource(path);
        cubeDescMap.remove(cubeDesc.getName());
        Cuboid.reloadCache(cubeDesc.getName());
    }

    // remove cubeDesc
    public void removeLocalCubeDesc(String name) throws IOException {
        cubeDescMap.removeLocal(name);
        Cuboid.reloadCache(name);
    }

    private void reloadAllCubeDesc() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading Cube Metadata from folder " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));

        cubeDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.CUBE_DESC_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            CubeDesc desc = loadCubeDesc(path, true);

            if (!path.equals(desc.getResourcePath())) {
                logger.error("Skip suspicious desc at " + path + ", " + desc + " should be at " + desc.getResourcePath());
                continue;
            }
            if (cubeDescMap.containsKey(desc.getName())) {
                logger.error("Dup CubeDesc name '" + desc.getName() + "' on path " + path);
                continue;
            }

            cubeDescMap.putLocal(desc.getName(), desc);
        }

        logger.info("Loaded " + cubeDescMap.size() + " Cube(s)");
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
            desc.init(config);
        } catch (Exception e) {
            logger.warn("Broken cube desc " + desc, e);
            desc.addError(e.getMessage());
            return desc;
        }

        postProcessCubeDesc(desc);
        // Semantic validation
        CubeMetadataValidator validator = new CubeMetadataValidator();
        ValidateContext context = validator.validate(desc);
        if (!context.ifPass()) {
            return desc;
        }

        desc.setSignature(desc.calculateSignature());

        // Save Source
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, CUBE_DESC_SERIALIZER);

        // Reload the CubeDesc
        CubeDesc ndesc = loadCubeDesc(path, false);
        // Here replace the old one
        cubeDescMap.put(ndesc.getName(), desc);

        return ndesc;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

}
