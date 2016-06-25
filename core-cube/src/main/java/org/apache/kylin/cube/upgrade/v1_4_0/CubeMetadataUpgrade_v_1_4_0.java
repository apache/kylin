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

package org.apache.kylin.cube.upgrade.v1_4_0;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.cube.model.v1_4_0.CubeDesc;
import org.apache.kylin.cube.model.v1_4_0.DimensionDesc;
import org.apache.kylin.cube.upgrade.common.CubeMetadataUpgrade;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * please check doc on CubeMetadataUpgrade_v_1_5_1
 */
public class CubeMetadataUpgrade_v_1_4_0 extends CubeMetadataUpgrade {

    private static final Logger logger = LoggerFactory.getLogger(CubeMetadataUpgrade_v_1_4_0.class);
    private static final Serializer<CubeDesc> oldCubeDescSerializer = new JsonSerializer<>(CubeDesc.class);

    private List<String> updatedResources = Lists.newArrayList();

    public CubeMetadataUpgrade_v_1_4_0(String newMetadataUrl) {
        super(newMetadataUrl);
    }

    public void upgradeNonCompatibleMeta() {
        dowork();
    }

    public void upgradeCompatibleMeta() {
        //do nothing
    }

    private CubeDesc loadOldCubeDesc(String path) {
        CubeDesc ndesc = null;
        try {
            ndesc = store.getResource(path, CubeDesc.class, oldCubeDescSerializer);
        } catch (IOException e) {
            throw new RuntimeException("failed to load old cubedesc at " + path);
        }

        if (StringUtils.isBlank(ndesc.getName())) {
            throw new IllegalStateException("CubeDesc name must not be blank");
        }

        return ndesc;
    }

    private DataModelDesc getDataModelDesc(String modelName) {
        MetadataManager.clearCache();
        return MetadataManager.getInstance(config).getDataModelDesc(modelName);
    }

    public void dowork() {
        List<String> paths = listResourceStore(ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        for (String path : paths) {
            logger.info("CubeMetadataUpgrade_v_1_4_0 handling in dowork {}", path);
            CubeDesc cubeDesc = loadOldCubeDesc(path);
            cubeDesc.init(config, MetadataManager.getInstance(config).getAllTablesMap());

            upgradeDataModelDesc(cubeDesc);
            upgradeCubeDesc(cubeDesc);
        }
    }

    private void upgradeDataModelDesc(CubeDesc cubeDesc) {
        boolean upgrade = false;

        DataModelDesc modelDesc = getDataModelDesc(cubeDesc.getModelName());

        try {
            if (modelDesc != null && modelDesc.getDimensions() == null && modelDesc.getMetrics() == null) {
                List<DimensionDesc> cubeDimDescList = cubeDesc.getDimensions();
                if (!CollectionUtils.isEmpty(cubeDimDescList)) {
                    Map<String, HashSet<String>> modelDimMap = Maps.newHashMap();
                    for (DimensionDesc cubeDimDesc : cubeDimDescList) {
                        if (!modelDimMap.containsKey(cubeDimDesc.getTable())) {
                            modelDimMap.put(cubeDimDesc.getTable(), new HashSet<String>());
                        }
                        modelDimMap.get(cubeDimDesc.getTable()).addAll(Lists.newArrayList(cubeDimDesc.getDerived() != null ? cubeDimDesc.getDerived() : cubeDimDesc.getColumn()));
                    }

                    List<ModelDimensionDesc> modelDimDescList = Lists.newArrayListWithCapacity(modelDimMap.size());
                    for (Map.Entry<String, HashSet<String>> modelDimEntry : modelDimMap.entrySet()) {
                        ModelDimensionDesc dimDesc = new ModelDimensionDesc();
                        dimDesc.setTable(modelDimEntry.getKey());
                        String[] columns = new String[modelDimEntry.getValue().size()];
                        columns = modelDimEntry.getValue().toArray(columns);
                        dimDesc.setColumns(columns);
                        modelDimDescList.add(dimDesc);
                    }
                    ModelDimensionDesc.capicalizeStrings(modelDimDescList);
                    modelDesc.setDimensions(modelDimDescList);
                    upgrade = true;
                }

                List<MeasureDesc> cubeMeasDescList = cubeDesc.getMeasures();
                if (!CollectionUtils.isEmpty(cubeDimDescList)) {
                    ArrayList<String> metrics = Lists.newArrayListWithExpectedSize(cubeMeasDescList.size());
                    for (MeasureDesc cubeMeasDesc : cubeMeasDescList) {
                        for (TblColRef tblColRef : cubeMeasDesc.getFunction().getParameter().getColRefs()) {
                            metrics.add(tblColRef.getName());
                        }
                    }
                    String[] metricsArray = new String[metrics.size()];
                    modelDesc.setMetrics(metrics.toArray(metricsArray));
                    upgrade = true;
                }
            }

            if (upgrade) {
                store.putResource(modelDesc.getResourcePath(), modelDesc, MetadataManager.MODELDESC_SERIALIZER);
                updatedResources.add(modelDesc.getResourcePath());
            }
        } catch (Exception e) {
            logger.error("error", e);

            errorMsgs.add("upgradeDataModelDesc [" + modelDesc.getName() + "] failed: " + e.getLocalizedMessage());
        }
    }

    @SuppressWarnings("deprecation")
    private void upgradeCubeDesc(CubeDesc cubeDesc) {
        try {

            DataModelDesc modelDesc = getDataModelDesc(cubeDesc.getModelName());

            PartitionDesc modelPartDesc = modelDesc.getPartitionDesc();
            if (cubeDesc.getPartitionDateStart() == 0 && modelPartDesc.getPartitionDateStart() != 0) {
                cubeDesc.setPartitionDateStart(modelPartDesc.getPartitionDateStart());
            }

            //            String calculatedSign = cubeDesc.calculateSignature();
            //            cubeDesc.setSignature(calculatedSign);

            store.putResource(cubeDesc.getResourcePath(), cubeDesc, oldCubeDescSerializer);
            updatedResources.add(cubeDesc.getResourcePath());

        } catch (Exception e) {
            logger.error("error", e);
            errorMsgs.add("upgradeCubeDesc [" + cubeDesc.getName() + "] failed: " + e.getLocalizedMessage());
        }
    }

}
