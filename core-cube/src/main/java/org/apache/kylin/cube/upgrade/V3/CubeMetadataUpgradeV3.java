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

package org.apache.kylin.cube.upgrade.V3;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectManager;

import com.google.common.collect.Lists;

/**
 * back in 1.x branch there was a CubeMetadataUpgrade which is actually CubeMetadataUpgradeV1,
 * that upgrades metadata store from v1(prior kylin 0.7) to v2.
 * the major difference is that we split cube desc to cube desc + model desc
 * 
 * In 2.0 there is a CubeMetadataUpgradeV2 which is responsible for upgrading the metadata store
 * from 1.x to 2.0. The major actions in that is updating cube desc signature and upgrade model desc.
 * 
 * this CubeMetadataUpgradeV3 upgrades metadata store from v2(prior kylin 2.1) to v3
 * the major different is a brand new definition of partial cubes to allow users to select 
 * cuboids more flexibly. https://issues.apache.org/jira/browse/KYLIN-242
 */
public class CubeMetadataUpgradeV3 {

    private KylinConfig config = null;
    private ResourceStore store;

    private List<String> updatedResources = Lists.newArrayList();
    private List<String> errorMsgs = Lists.newArrayList();

    private static final Log logger = LogFactory.getLog(CubeMetadataUpgradeV3.class);

    public CubeMetadataUpgradeV3(String newMetadataUrl) {
        KylinConfig.destoryInstance();
        System.setProperty(KylinConfig.KYLIN_CONF, newMetadataUrl);
        KylinConfig.getInstanceFromEnv().setMetadataUrl(newMetadataUrl);

        config = KylinConfig.getInstanceFromEnv();
        store = getStore();
    }

    public void upgrade() {

        upgradeCubeDesc();
        verify();
    }

    public void verify() {
        MetadataManager.clearCache();
        MetadataManager.getInstance(config);
        CubeDescManager.clearCache();
        CubeDescManager.getInstance(config);
        CubeManager.clearCache();
        CubeManager.getInstance(config);
        ProjectManager.clearCache();
        ProjectManager.getInstance(config);
        //cleanup();
    }

    private List<String> listResourceStore(String pathRoot) {
        List<String> paths = null;
        try {
            paths = store.collectResourceRecursively(pathRoot, MetadataConstants.FILE_SURFIX);
        } catch (IOException e1) {
            e1.printStackTrace();
            errorMsgs.add("Get IOException when scan resource store at: " + ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        }

        return paths;
    }

    private void upgradeCubeDesc() {
        logger.info("Reloading Cube Metadata from folder " + store.getReadableResourcePath(ResourceStore.CUBE_DESC_RESOURCE_ROOT));

        List<String> paths = listResourceStore(ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        for (String path : paths) {

            try {
                CubeDescUpgraderV3 upgrade = new CubeDescUpgraderV3(path);
                CubeDesc ndesc = upgrade.upgrade();
                ndesc.setSignature(ndesc.calculateSignature());

                getStore().putResource(ndesc.getResourcePath(), ndesc, CubeDescManager.CUBE_DESC_SERIALIZER);
                updatedResources.add(ndesc.getResourcePath());
            } catch (IOException e) {
                e.printStackTrace();
                errorMsgs.add("Upgrade CubeDesc at '" + path + "' failed: " + e.getLocalizedMessage());
            }
        }

    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(config);
    }

    public static void main(String[] args) {

        if (!(args != null && (args.length == 1 || args.length == 2))) {
            System.out.println("Usage: java CubeMetadataUpgrade <metadata_export_folder> <verify>; e.g, /export/kylin/meta ");
            return;
        }

        String exportFolder = args[0];
        boolean verify = false;
        if (args.length == 2 && "verify".equals(args[1])) {
            System.out.println("Only verify the metadata in folder " + exportFolder);
            verify = true;
        }

        CubeMetadataUpgradeV3 instance = null;
        if (verify) {
            instance = new CubeMetadataUpgradeV3(exportFolder);
            instance.verify();
        } else {
            File oldMetaFolder = new File(exportFolder);
            if (!oldMetaFolder.exists()) {
                System.out.println("Provided folder doesn't exist: '" + exportFolder + "'");
                return;
            }

            if (!oldMetaFolder.isDirectory()) {
                System.out.println("Provided folder is not a directory: '" + exportFolder + "'");
                return;
            }

            String newMetadataUrl = oldMetaFolder.getAbsolutePath() + "_v3";//upgrades metadata store to v3 format
            try {
                FileUtils.deleteDirectory(new File(newMetadataUrl));
                FileUtils.copyDirectory(oldMetaFolder, new File(newMetadataUrl));
            } catch (IOException e) {
                e.printStackTrace();
            }

            instance = new CubeMetadataUpgradeV3(newMetadataUrl);
            instance.upgrade();
            logger.info("=================================================================");
            logger.info("Run CubeMetadataUpgrade completed;");

        }

        logger.info("=================================================================");
        if (instance.errorMsgs.size() > 0) {
            logger.info("Here are the error/warning messages, you may need check:");
            for (String s : instance.errorMsgs) {
                logger.warn(s);
            }
        } else {
            logger.info("No error or warning messages; The migration is success.");
        }
    }
}
