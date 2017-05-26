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

package org.apache.kylin.cube.upgrade.common;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public abstract class CubeMetadataUpgrade {
    private static final Logger logger = LoggerFactory.getLogger(CubeMetadataUpgrade.class);

    protected KylinConfig config = null;
    protected ResourceStore store;
    protected List<String> updatedResources = Lists.newArrayList();
    protected List<String> errorMsgs = Lists.newArrayList();

    public CubeMetadataUpgrade(String newMetadataUrl) {
        KylinConfig.destroyInstance();
        config = KylinConfig.createInstanceFromUri(newMetadataUrl);
        store = ResourceStore.getStore(config);
    }

    protected List<String> listResourceStore(String pathRoot) {
        List<String> paths = null;
        try {
            paths = store.collectResourceRecursively(pathRoot, MetadataConstants.FILE_SURFIX);
        } catch (IOException e1) {
            logger.error("error", e1);
            errorMsgs.add("Get IOException when scan resource store at: " + ResourceStore.CUBE_DESC_RESOURCE_ROOT);
        }

        return paths;
    }

    public void clear() {
        MetadataManager.clearCache();
        CubeDescManager.clearCache();
        CubeManager.clearCache();
        ProjectManager.clearCache();
    }

    public void verify() {
        logger.info("=================================================================");
        logger.info("The changes are applied, now it's time to verify the new metadata store by reloading all metadata:");
        logger.info("=================================================================");
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

    public abstract void upgradeNonCompatibleMeta();

    public abstract void upgradeCompatibleMeta();

    public static void upgradeOrVerify(Class upgradeClass, String[] args, boolean firstStepInChain, boolean lastStepInChain) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

        if (!(args != null && (args.length == 1))) {
            System.out.println("Usage: java CubeMetadataUpgrade <metadata_export_folder>");
            System.out.println(", where metadata_export_folder is the folder containing your current metadata's dump (Upgrade program will not modify it directly, relax.");
            return;
        }

        String currentMetaDumpFolderPath = args[0];
        CubeMetadataUpgrade instance;

        File currentMetaDumpFolder = new File(currentMetaDumpFolderPath);
        if (!currentMetaDumpFolder.exists()) {
            System.out.println("Provided folder doesn't exist: '" + currentMetaDumpFolderPath + "'");
            return;
        }

        if (!currentMetaDumpFolder.isDirectory()) {
            System.out.println("Provided folder is not a directory: '" + currentMetaDumpFolderPath + "'");
            return;
        }

        String newMetadataUrl;
        if (firstStepInChain) {
            newMetadataUrl = currentMetaDumpFolder.getAbsolutePath() + "_workspace";//upgrades metadata store in a copy named xx_workspace
            try {
                FileUtils.deleteDirectory(new File(newMetadataUrl));
                FileUtils.copyDirectory(currentMetaDumpFolder, new File(newMetadataUrl));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            newMetadataUrl = currentMetaDumpFolder.getAbsolutePath();
        }

        instance = (CubeMetadataUpgrade) upgradeClass.getConstructor(String.class).newInstance(newMetadataUrl);
        instance.upgradeNonCompatibleMeta();
        logger.info("=================================================================");
        logger.info("Run {} completed", upgradeClass.toString());
        logger.info("=================================================================");
        if (instance.errorMsgs.size() > 0) {
            logger.info("Here are the error/warning messages, you may need check:");
            for (String s : instance.errorMsgs) {
                logger.error(s);
            }
        } else {
            logger.info("No error or warning messages; The migration is success.");
        }

        if (lastStepInChain) {
            instance.upgradeCompatibleMeta();
            instance.verify();
        }
    }

}
