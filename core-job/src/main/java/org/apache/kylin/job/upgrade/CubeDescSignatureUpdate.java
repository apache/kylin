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

package org.apache.kylin.job.upgrade;

import com.google.common.collect.Lists;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;

import java.util.List;

/**
 * Created by dongli on 11/17/15.
 */
public class CubeDescSignatureUpdate {
    private KylinConfig config = null;
    private ResourceStore store;
    private String[] cubeNames;
    private List<String> updatedResources = Lists.newArrayList();
    private List<String> errorMsgs = Lists.newArrayList();

    private static final Log logger = LogFactory.getLog(CubeDescSignatureUpdate.class);

    public CubeDescSignatureUpdate(String[] cubes) {
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
            String[] names = cubeNames[0].split(",");
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
    }

    public static void main(String args[]) {
        if (args != null && args.length != 0 && args.length != 1) {
            System.out.println("Usage: java CubeDescSignatureUpdate [Cubes]; e.g, cube1,cube2 ");
            return;
        }

        CubeDescSignatureUpdate metadataUpgrade = new CubeDescSignatureUpdate(args);
        metadataUpgrade.update();

        logger.info("=================================================================");
        logger.info("Run CubeDescSignatureUpdate completed;");

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

    private void updateCubeDesc(CubeDesc cubeDesc) {
        try {
            String calculatedSign = cubeDesc.calculateSignature();
            if (!cubeDesc.getSignature().equals(calculatedSign))
            {
                cubeDesc.setSignature(calculatedSign);
                store.putResource(cubeDesc.getResourcePath(), cubeDesc, CubeDescManager.CUBE_DESC_SERIALIZER);
                updatedResources.add(cubeDesc.getResourcePath());
            }
        } catch (Exception e) {
            e.printStackTrace();
            errorMsgs.add("Update CubeDesc[" + cubeDesc.getName() + "] failed: " + e.getLocalizedMessage());
        }
    }
}
