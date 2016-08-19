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

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * companion tool for CubeMetaExtractor, ingest the extracted cube meta into another metadata store
 * 
 * TODO: only support ingest cube now
 * TODO: ingest job history
 */
public class CubeMetaIngester extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(CubeMetaIngester.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_SRC = OptionBuilder.withArgName("srcPath").hasArg().isRequired(true).withDescription("specify the path to the extracted cube metadata zip file").create("srcPath");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(true).withDescription("specify the target project for the new cubes").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_OVERWRITE_TABLES = OptionBuilder.withArgName("overwriteTables").hasArg().isRequired(false).withDescription("If table meta conflicts, overwrite the one in metadata store with the one in srcPath. Use in caution because it might break existing cubes! Suggest to backup metadata store first").create("overwriteTables");

    private KylinConfig kylinConfig;
    private MetadataManager metadataManager;
    private ProjectManager projectManager;
    private CubeManager cubeManager;
    private CubeDescManager cubeDescManager;
    private RealizationRegistry realizationRegistry;

    Set<String> requiredResources = Sets.newLinkedHashSet();
    private String targetProjectName;
    private boolean overwriteTables = false;

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_SRC);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_OVERWRITE_TABLES);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        metadataManager = MetadataManager.getInstance(kylinConfig);
        projectManager = ProjectManager.getInstance(kylinConfig);
        cubeManager = CubeManager.getInstance(kylinConfig);
        cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        realizationRegistry = RealizationRegistry.getInstance(kylinConfig);

        if (optionsHelper.hasOption(OPTION_OVERWRITE_TABLES)) {
            overwriteTables = Boolean.valueOf(optionsHelper.getOptionValue(OPTION_OVERWRITE_TABLES));
        }
        targetProjectName = optionsHelper.getOptionValue(OPTION_PROJECT);

        String srcPath = optionsHelper.getOptionValue(OPTION_SRC);
        if (!srcPath.endsWith(".zip")) {
            throw new IllegalArgumentException(OPTION_SRC.getArgName() + " has to be a zip file");
        }
        File zipFile = new File(srcPath);
        if (zipFile.isDirectory() || !zipFile.exists()) {
            throw new IllegalArgumentException(OPTION_SRC.getArgName() + " file does does exist");
        }

        File tempFolder = File.createTempFile("_unzip", "folder");
        tempFolder.deleteOnExit();
        tempFolder.delete();
        tempFolder.mkdir();
        ZipFileUtils.decompressZipfileToDirectory(srcPath, tempFolder);
        if (tempFolder.list().length != 1) {
            throw new IllegalStateException(tempFolder.list().toString());
        }

        injest(tempFolder.listFiles()[0].getAbsoluteFile());
    }

    private void injest(File metaRoot) throws IOException {
        KylinConfig srcConfig = KylinConfig.createInstanceFromUri(metaRoot.getAbsolutePath());
        MetadataManager srcMetadataManager = MetadataManager.getInstance(srcConfig);
        HybridManager srcHybridManager = HybridManager.getInstance(srcConfig);
        CubeManager srcCubeManager = CubeManager.getInstance(srcConfig);
        CubeDescManager srcCubeDescManager = CubeDescManager.getInstance(srcConfig);

        checkAndMark(srcMetadataManager, srcHybridManager, srcCubeManager, srcCubeDescManager);
        ResourceTool.copy(srcConfig, kylinConfig, Lists.newArrayList(requiredResources));

        for (TableDesc tableDesc : srcMetadataManager.listAllTables()) {
            projectManager.addTableDescToProject(Lists.newArrayList(tableDesc.getIdentity()).toArray(new String[0]), targetProjectName);
        }

        for (CubeInstance cube : srcCubeManager.listAllCubes()) {
            projectManager.updateModelToProject(cube.getDataModelDesc().getName(), targetProjectName);
            projectManager.moveRealizationToProject(RealizationType.CUBE, cube.getName(), targetProjectName, null);
        }

    }

    private void checkAndMark(MetadataManager srcMetadataManager, HybridManager srcHybridManager, CubeManager srcCubeManager, CubeDescManager srcCubeDescManager) {
        if (srcHybridManager.listHybridInstances().size() > 0) {
            throw new IllegalStateException("Does not support ingest hybrid yet");
        }

        ProjectInstance targetProject = projectManager.getProject(targetProjectName);
        if (targetProject == null) {
            throw new IllegalStateException("Target project does not exist in target metadata: " + targetProjectName);
        }

        for (TableDesc tableDesc : srcMetadataManager.listAllTables()) {
            TableDesc existing = metadataManager.getTableDesc(tableDesc.getIdentity());
            if (existing != null && !existing.equals(tableDesc)) {
                logger.info("Table {} already has a different version in target metadata store", tableDesc.getIdentity());
                logger.info("Existing version: " + existing);
                logger.info("New version: " + tableDesc);

                if (!overwriteTables) {
                    throw new IllegalStateException("table already exists with a different version: " + tableDesc.getIdentity() + ". Consider adding -overwriteTables option to force overwriting (with caution)");
                } else {
                    logger.warn("Overwriting the old table desc: " + tableDesc.getIdentity());
                }
            }
            requiredResources.add(TableDesc.concatResourcePath(tableDesc.getIdentity()));
        }

        for (DataModelDesc dataModelDesc : srcMetadataManager.listDataModels()) {
            DataModelDesc existing = metadataManager.getDataModelDesc(dataModelDesc.getName());
            if (existing != null) {
                throw new IllegalStateException("Already exist a model called " + dataModelDesc.getName());
            }
            requiredResources.add(DataModelDesc.concatResourcePath(dataModelDesc.getName()));
        }

        for (CubeDesc cubeDesc : srcCubeDescManager.listAllDesc()) {
            CubeDesc existing = cubeDescManager.getCubeDesc(cubeDesc.getName());
            if (existing != null) {
                throw new IllegalStateException("Already exist a cube desc called " + cubeDesc.getName());
            }
            requiredResources.add(CubeDesc.concatResourcePath(cubeDesc.getName()));
        }

        for (CubeInstance cube : srcCubeManager.listAllCubes()) {
            CubeInstance existing = cubeManager.getCube(cube.getName());
            if (existing != null) {
                throw new IllegalStateException("Already exist a cube desc called " + cube.getName());
            }
            requiredResources.add(CubeInstance.concatResourcePath(cube.getName()));
        }

      
    }

    public static void main(String[] args) {
        CubeMetaIngester extractor = new CubeMetaIngester();
        extractor.execute(args);
    }

}
