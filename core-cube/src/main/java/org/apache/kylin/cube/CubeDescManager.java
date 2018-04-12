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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.cube.cuboid.CuboidManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.CubeMetadataValidator;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
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
    
    public static CubeDescManager getInstance(KylinConfig config) {
        return config.getManager(CubeDescManager.class);
    }

    // called by reflection
    static CubeDescManager newInstance(KylinConfig config) throws IOException {
        return new CubeDescManager(config);
    }
    
    // ============================================================================

    private KylinConfig config;
    
    // name ==> CubeDesc
    private CaseInsensitiveStringCache<CubeDesc> cubeDescMap;
    private CachedCrudAssist<CubeDesc> crud;

    // protects concurrent operations around the cached map, to avoid for example
    // writing an entity in the middle of reloading it (dirty read)
    private AutoReadWriteLock descMapLock = new AutoReadWriteLock();

    private CubeDescManager(KylinConfig cfg) throws IOException {
        logger.info("Initializing CubeDescManager with config " + cfg);
        this.config = cfg;
        this.cubeDescMap = new CaseInsensitiveStringCache<CubeDesc>(config, "cube_desc");
        this.crud = new CachedCrudAssist<CubeDesc>(getStore(), ResourceStore.CUBE_DESC_RESOURCE_ROOT, CubeDesc.class,
                cubeDescMap) {
            @Override
            protected CubeDesc initEntityAfterReload(CubeDesc cubeDesc, String resourceName) {
                if (cubeDesc.isDraft())
                    throw new IllegalArgumentException("CubeDesc '" + cubeDesc.getName() + "' must not be a draft");

                try {
                    cubeDesc.init(config);
                } catch (Exception e) {
                    logger.warn("Broken cube desc " + cubeDesc.resourceName(), e);
                    cubeDesc.addError(e.toString());
                }
                return cubeDesc;
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new CubeDescSyncListener(), "cube_desc");
    }

    private class CubeDescSyncListener extends Broadcaster.Listener {

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            for (IRealization real : ProjectManager.getInstance(config).listAllRealizations(project)) {
                if (real instanceof CubeInstance) {
                    String descName = ((CubeInstance) real).getDescName();
                    reloadCubeDescQuietly(descName);
                }
            }
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String cubeDescName = cacheKey;
            CubeDesc cubeDesc = getCubeDesc(cubeDescName);
            String modelName = cubeDesc == null ? null : cubeDesc.getModelName();

            if (event == Event.DROP)
                removeLocalCubeDesc(cubeDescName);
            else
                reloadCubeDescQuietly(cubeDescName);

            for (ProjectInstance prj : ProjectManager.getInstance(config).findProjectsByModel(modelName)) {
                broadcaster.notifyProjectSchemaUpdate(prj.getName());
            }
        }
    }

    public CubeDesc getCubeDesc(String name) {
        try (AutoLock lock = descMapLock.lockForRead()) {
            return cubeDescMap.get(name);
        }
    }

    public List<CubeDesc> listAllDesc() {
        try (AutoLock lock = descMapLock.lockForRead()) {
            return new ArrayList<CubeDesc>(cubeDescMap.values());
        }
    }
    
    public CubeDesc reloadCubeDescQuietly(String name) {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            return reloadCubeDescLocal(name);
        } catch (Exception e) {
            logger.error("Failed to reload CubeDesc " + name, e);
            return null;
        }
    }

    public CubeDesc reloadCubeDescLocal(String name) throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            CubeDesc ndesc = crud.reload(name);
            clearCuboidCache(name);
            
            // Broken CubeDesc is not allowed to be saved and broadcast.
            if (ndesc.isBroken())
                throw new IllegalStateException("CubeDesc " + name + " is broken");
    
            return ndesc;
        }
    }

    /**
     * Create a new CubeDesc
     * 
     * @param cubeDesc
     * @return
     * @throws IOException
     */
    public CubeDesc createCubeDesc(CubeDesc cubeDesc) throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            if (cubeDesc.getUuid() == null || cubeDesc.getName() == null)
                throw new IllegalArgumentException();
            if (cubeDescMap.containsKey(cubeDesc.getName()))
                throw new IllegalArgumentException("CubeDesc '" + cubeDesc.getName() + "' already exists");
            if (cubeDesc.isDraft())
                throw new IllegalArgumentException("CubeDesc '" + cubeDesc.getName() + "' must not be a draft");

            try {
                cubeDesc.init(config);
            } catch (Exception e) {
                logger.warn("Broken cube desc " + cubeDesc, e);
                cubeDesc.addError(e.toString());
            }
            
            postProcessCubeDesc(cubeDesc);
            // Check base validation
            if (cubeDesc.isBroken()) {
                return cubeDesc;
            }
            // Semantic validation
            CubeMetadataValidator validator = new CubeMetadataValidator();
            ValidateContext context = validator.validate(cubeDesc);
            if (!context.ifPass()) {
                return cubeDesc;
            }

            cubeDesc.setSignature(cubeDesc.calculateSignature());

            // save resource
            crud.save(cubeDesc);
            
            return cubeDesc;
        }
    }

    /**
     * Update CubeDesc with the input. Broadcast the event into cluster
     * 
     * @param desc
     * @return
     * @throws IOException
     */
    public CubeDesc updateCubeDesc(CubeDesc desc) throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            // Validate CubeDesc
            if (desc.getUuid() == null || desc.getName() == null)
                throw new IllegalArgumentException();
            String name = desc.getName();
            if (!cubeDescMap.containsKey(name))
                throw new IllegalArgumentException("CubeDesc '" + name + "' does not exist.");
            if (desc.isDraft())
                throw new IllegalArgumentException("CubeDesc '" + desc.getName() + "' must not be a draft");

            try {
                desc.init(config);
            } catch (Exception e) {
                logger.warn("Broken cube desc " + desc, e);
                desc.addError(e.toString());
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

            // save resource
            crud.save(desc);

            return desc;
        }
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
                    String encodingVersionStr = configuration
                            .get(TopNMeasureType.CONFIG_ENCODING_VERSION_PREFIX + parameter.getValue());
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
                        DimensionEncoding dimensionEncoding = DimensionEncodingFactory.create((String) encodingConf[0],
                                (String[]) encodingConf[1], encodingVersion);
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
        try (AutoLock lock = descMapLock.lockForWrite()) {
            crud.delete(cubeDesc);
            clearCuboidCache(cubeDesc.getName());
        }
    }

    // remove cubeDesc
    public void removeLocalCubeDesc(String name) throws IOException {
        try (AutoLock lock = descMapLock.lockForWrite()) {
            cubeDescMap.removeLocal(name);
            clearCuboidCache(name);
        }
    }
    
    private void clearCuboidCache(String descName) {
        // avoid calling CubeDesc.getInitialCuboidScheduler() for late initializing CuboidScheduler
        CuboidManager.getInstance(config).clearCache(descName);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

}
