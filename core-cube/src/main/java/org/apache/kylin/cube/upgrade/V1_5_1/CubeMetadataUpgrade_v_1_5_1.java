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

package org.apache.kylin.cube.upgrade.V1_5_1;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.upgrade.common.CubeMetadataUpgrade;
import org.apache.kylin.cube.upgrade.common.MetadataVersionRefresher;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In 1.0, 1.1, 1.2 and 1.3 release there was a CubeMetadataUpgrade which is actually CubeMetadataUpgradeV1,
 * that upgrades metadata store from kylin-0.7 compatible to 1.0 ~ 1.3 compatible. The major difference is that we split
 * cube desc to cube desc + model desc
 * 
 * Metadata are backward compatible from 1.0 ~ 1.3.
 * 
 * In 1.4.x there is a CubeMetadataUpgradeV2 which is responsible for upgrading the metadata store
 * from 1.0 ~ 1.3 compatible to 1.4.x compatible. The major actions in that is revising cube desc signature and upgrade model desc.
 * Since the Apache Kylin community never officially make a 1.4.x release. Normal users can ignore it.
 * 
 * This CubeMetadataUpgrade_v_1_5 upgrades metadata store from 1.0 ~ 1.3 compatible to 1.5 compatible.
 * the major differences are:
 * 1) changes from 1.0 ~ 1.3 compatible to 1.4.x compatible
 * 2) brand new definition of partial cubes to allow users to select cuboids more flexibly. https://issues.apache.org/jira/browse/KYLIN-242
 * 
 * Notice:
 * 1) From Kylin 1.5.0, every metadata store entity will bind with release version number. See RootPersistentEntity.version
 * 2) From Kylin 1.5.1, the CubeMetadataUpgrade class will be named after version number. As with CubeMetadataUpgrade_v_1_5_1
 * 3) For details on how to upgrade from prior 1.5.0 to 1.5 compatible please visit http://kylin.apache.org/docs15/howto/howto_upgrade.html.
 */
public class CubeMetadataUpgrade_v_1_5_1 extends CubeMetadataUpgrade {
    private static final Logger logger = LoggerFactory.getLogger(CubeMetadataUpgrade_v_1_5_1.class);

    public CubeMetadataUpgrade_v_1_5_1(String newMetadataUrl) {
        super(newMetadataUrl);
    }

    public void upgradeNonCompatibleMeta() {
        upgradeCubeDesc();
    }

    public void upgradeCompatibleMeta() {
        upgradeVersion();
        clear();
        upgradeEngineTypeStorageType();
        upgradeSignature();
    }

    private void upgradeVersion() {
        MetadataVersionRefresher refresher = new MetadataVersionRefresher(this.store, "1.5.1");
        try {
            refresher.refresh();
        } catch (IOException e) {
            throw new RuntimeException("Failed to upgrade version number", e);
        }
    }

    private void upgradeCubeDesc() {
        List<String> paths = listResourceStore(ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        for (String path : paths) {
            logger.info("CubeMetadataUpgrade_v_1_5_1 handling in upgradeCubeDesc {}", path);

            try {
                CubeDescUpgrade_v_1_5_1 upgrade = new CubeDescUpgrade_v_1_5_1(path, store);
                CubeDesc ndesc = upgrade.upgrade();

                ResourceStore.getStore(config).putResource(ndesc.getResourcePath(), ndesc, CubeDescManager.CUBE_DESC_SERIALIZER);
                updatedResources.add(ndesc.getResourcePath());
            } catch (Exception e) {
                logger.error("error", e);
                errorMsgs.add("upgradeCubeDesc at '" + path + "' failed: " + e.getLocalizedMessage());
            }
        }
    }

    private void upgradeSignature() {
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(config);
        List<CubeDesc> cubeDescs = cubeDescManager.listAllDesc();
        for (CubeDesc cubeDesc : cubeDescs) {
            logger.info("CubeMetadataUpgrade_v_1_5_1 handling in upgradeSignature {}", cubeDesc.getName());
            upgradeSignature(cubeDesc);
        }
    }

    private void upgradeSignature(CubeDesc cubeDesc) {
        try {
            String calculatedSign = cubeDesc.calculateSignature();
            if (cubeDesc.getSignature() == null || (!cubeDesc.getSignature().equals(calculatedSign))) {
                cubeDesc.setSignature(calculatedSign);
                store.putResource(cubeDesc.getResourcePath(), cubeDesc, CubeDescManager.CUBE_DESC_SERIALIZER);
                updatedResources.add(cubeDesc.getResourcePath());
            }
        } catch (Exception e) {
            logger.error("error", e);
            errorMsgs.add("upgradeSignature [" + cubeDesc.getName() + "] failed: " + e.getLocalizedMessage());
        }
    }

    // Update engine_type and storage_type to v2, if the cube has no segments.
    private void upgradeEngineTypeStorageType() {
        CubeManager cubeManager = CubeManager.getInstance(config);
        List<CubeInstance> cubes = cubeManager.listAllCubes();
        for (CubeInstance cube : cubes) {
            try {
                org.apache.kylin.cube.model.CubeDesc cubeDesc = cube.getDescriptor();
                if (cube.getFirstSegment() == null && cubeDesc != null && cubeDesc.getStorageType() == IStorageAware.ID_HBASE && cubeDesc.getEngineType() == IEngineAware.ID_MR_V1) {
                    logger.info("CubeMetadataUpgrade_v_1_5_1 handling in upgradeEngineTypeStorageType {}", cube.getName());

                    cubeDesc.setEngineType(IEngineAware.ID_MR_V2);
                    cubeDesc.setStorageType(IStorageAware.ID_SHARDED_HBASE);

                    store.putResource(cubeDesc.getResourcePath(), cubeDesc, CubeDescManager.CUBE_DESC_SERIALIZER);
                    updatedResources.add(cubeDesc.getResourcePath());
                } else {
                    logger.info("CubeDesc {}'s storage type and engine type will not be upgraded because they're not empty", cubeDesc.getName());
                }
            } catch (Exception e) {
                logger.error("error", e);
                errorMsgs.add("upgradeEngineTypeStorageType [" + cube.getName() + "] failed: " + e.getLocalizedMessage());
            }
        }
    }

}
