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

package org.apache.kylin.cube.cli;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * used to bulk refresh the cube's signature in metadata store.
 * won't be useful unless something went wrong.
 */
public class CubeSignatureRefresher {
    private static final Logger logger = LoggerFactory.getLogger(CubeSignatureRefresher.class);

    private KylinConfig config = null;
    private ResourceStore store;
    private String[] cubeNames;
    private List<String> updatedResources = Lists.newArrayList();
    private List<String> errorMsgs = Lists.newArrayList();

    public CubeSignatureRefresher(String[] cubes) {
        config = KylinConfig.getInstanceFromEnv();
        store = ResourceStore.getStore(config);
        cubeNames = cubes;
    }

    public void update() {
        logger.info("Reloading Cube Metadata from store: " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(config);
        List<CubeDesc> cubeDescs;
        if (ArrayUtils.isEmpty(cubeNames)) {
            cubeDescs = cubeDescManager.listAllDesc();
        } else {
            String[] names = StringUtil.splitByComma(cubeNames[0]);
            if (ArrayUtils.isEmpty(names))
                return;
            cubeDescs = Lists.newArrayListWithCapacity(names.length);
            for (String name : names) {
                cubeDescs.add(cubeDescManager.getCubeDesc(name));
            }
        }
        for (CubeDesc cubeDesc : cubeDescs) {
            updateCubeDesc(cubeDesc);
        }

        verify();
    }

    private void verify() {
        try {
            Broadcaster.getInstance(config).notifyClearAll();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        DataModelManager.getInstance(config);
        CubeDescManager.getInstance(config);
        CubeManager.getInstance(config);
        ProjectManager.getInstance(config);
    }

    public List<String> getErrorMsgs() {
        return errorMsgs;
    }

    private void updateCubeDesc(CubeDesc cubeDesc) {
        try {
            String calculatedSign = cubeDesc.calculateSignature();
            if (cubeDesc.getSignature() == null || (!cubeDesc.getSignature().equals(calculatedSign))) {
                cubeDesc.setSignature(calculatedSign);
                store.checkAndPutResource(cubeDesc.getResourcePath(), cubeDesc, CubeDesc.newSerializerForLowLevelAccess());
                updatedResources.add(cubeDesc.getResourcePath());
            }
        } catch (Exception e) {
            logger.error("error", e);
            errorMsgs.add("Update CubeDesc[" + cubeDesc.getName() + "] failed: " + e.getLocalizedMessage());
        }
    }

    public static void main(String[] args) {
        if (args != null && args.length > 1) {
            System.out.println("Usage: java CubeSignatureRefresher [Cubes]; e.g, cube1,cube2 ");
            return;
        }

        CubeSignatureRefresher metadataUpgrade = new CubeSignatureRefresher(args);
        metadataUpgrade.update();

        logger.info("=================================================================");
        logger.info("Run CubeSignatureRefresher completed;");

        if (!metadataUpgrade.updatedResources.isEmpty()) {
            logger.info("Following resources are updated successfully:");
            for (String s : metadataUpgrade.updatedResources) {
                logger.info(s);
            }
        } else {
            logger.warn("No resource updated.");
        }

        if (!metadataUpgrade.errorMsgs.isEmpty()) {
            logger.info("Here are the error/warning messages, you may need to check:");
            for (String s : metadataUpgrade.errorMsgs) {
                logger.warn(s);
            }
        } else {
            logger.info("No error or warning messages; The update succeeds.");
        }

        logger.info("=================================================================");
    }

}
