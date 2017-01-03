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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 1. Create new HybridCube
 * bin/kylin.sh org.apache.kylin.tool.HybridCubeCLI -action create -name hybrid_name -project project_name -model model_name -cubes cube1,cube2
 * 2. Update existing HybridCube
 * bin/kylin.sh org.apache.kylin.tool.HybridCubeCLI -action update -name hybrid_name -project project_name -model model_name -cubes cube1,cube2,cube3
 * 3. Delete the HybridCube
 * bin/kylin.sh org.apache.kylin.tool.HybridCubeCLI -action delete -name hybrid_name -project project_name -model model_name
 */
public class HybridCubeCLI extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(HybridCubeCLI.class);

    private static final Option OPTION_ACTION = OptionBuilder.withArgName("action").hasArg().isRequired(true).withDescription("create/update/delete").create("action");

    private static final Option OPTION_HYBRID_NAME = OptionBuilder.withArgName("name").hasArg().isRequired(true).withDescription("HybridCube name").create("name");

    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(true).withDescription("the target project for the hybrid cube").create("project");

    private static final Option OPTION_MODEL = OptionBuilder.withArgName("model").hasArg().isRequired(true).withDescription("the target model for the hybrid cube").create("model");

    private static final Option OPTION_CUBES = OptionBuilder.withArgName("cubes").hasArg().isRequired(false).withDescription("the cubes used in HybridCube, seperated by comma, empty if to delete HybridCube").create("cubes");

    private final Options options;

    private KylinConfig kylinConfig;
    private CubeManager cubeManager;
    private HybridManager hybridManager;
    private MetadataManager metadataManager;
    private ResourceStore store;

    public HybridCubeCLI() {
        options = new Options();
        options.addOption(OPTION_ACTION);
        options.addOption(OPTION_HYBRID_NAME);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_MODEL);
        options.addOption(OPTION_CUBES);

        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.store = ResourceStore.getStore(kylinConfig);
        this.cubeManager = CubeManager.getInstance(kylinConfig);
        this.hybridManager = HybridManager.getInstance(kylinConfig);
        this.metadataManager = MetadataManager.getInstance(kylinConfig);
    }

    public static void main(String[] args) {
        HybridCubeCLI cli = new HybridCubeCLI();
        cli.execute(args);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String action = optionsHelper.getOptionValue(OPTION_ACTION);
        String hybridName = optionsHelper.getOptionValue(OPTION_HYBRID_NAME);
        String projectName = optionsHelper.getOptionValue(OPTION_PROJECT);
        String modelName = optionsHelper.getOptionValue(OPTION_MODEL);
        String cubeNamesStr = optionsHelper.getOptionValue(OPTION_CUBES);
        String[] cubeNames = new String[] {};
        if (cubeNamesStr != null)
            cubeNames = cubeNamesStr.split(",");
        String owner = null;

        DataModelDesc modelDesc = metadataManager.getDataModelDesc(modelName);
        if (modelDesc == null) {
            throw new RuntimeException("Could not find model: " + modelName);
        }

        List<RealizationEntry> realizationEntries = new ArrayList<RealizationEntry>();
        for (String cubeName : cubeNames) {
            if (StringUtils.isEmpty(cubeName))
                continue;
            CubeInstance cube = cubeManager.getCube(cubeName);
            if (cube == null) {
                throw new RuntimeException("Could not find cube: " + cubeName);
            }
            if (owner == null) {
                owner = cube.getOwner();
            }
            realizationEntries.add(RealizationEntry.create(RealizationType.CUBE, cube.getName()));
        }

        HybridInstance hybridInstance = hybridManager.getHybridInstance(hybridName);
        if ("create".equals(action)) {
            if (hybridInstance != null) {
                throw new RuntimeException("The Hybrid Cube does exist, could not create: " + hybridName);
            }
            //Create new Hybrid
            create(hybridName, realizationEntries, projectName, owner);
        } else if ("update".equals(action)) {
            if (hybridInstance == null) {
                throw new RuntimeException("The Hybrid Cube doesn't exist, could not update: " + hybridName);
            }
            // Update the Hybrid
            update(hybridInstance, realizationEntries, projectName, owner);
        } else if ("delete".equals(action)) {
            if (hybridInstance == null) {
                throw new RuntimeException("The Hybrid Cube doesn't exist, could not delete: " + hybridName);
            }
            // Delete the Hybrid
            delete(hybridInstance);
        }

    }

    private HybridInstance create(String hybridName, List<RealizationEntry> realizationEntries, String projectName, String owner) throws IOException {
        checkSegmentOffset(realizationEntries);
        HybridInstance hybridInstance = HybridInstance.create(kylinConfig, hybridName, realizationEntries);
        store.putResource(hybridInstance.getResourcePath(), hybridInstance, HybridManager.HYBRID_SERIALIZER);
        ProjectManager.getInstance(kylinConfig).moveRealizationToProject(RealizationType.HYBRID, hybridInstance.getName(), projectName, owner);
        hybridManager.reloadHybridInstance(hybridName);
        logger.info("HybridInstance was created at: " + hybridInstance.getResourcePath());
        return hybridInstance;
    }

    private void update(HybridInstance hybridInstance, List<RealizationEntry> realizationEntries, String projectName, String owner) throws IOException {
        checkSegmentOffset(realizationEntries);
        hybridInstance.setRealizationEntries(realizationEntries);
        store.putResource(hybridInstance.getResourcePath(), hybridInstance, HybridManager.HYBRID_SERIALIZER);
        ProjectManager.getInstance(kylinConfig).moveRealizationToProject(RealizationType.HYBRID, hybridInstance.getName(), projectName, owner);
        hybridManager.reloadHybridInstance(hybridInstance.getName());
        logger.info("HybridInstance was updated at: " + hybridInstance.getResourcePath());
    }

    private void delete(HybridInstance hybridInstance) throws IOException {
        ProjectManager.getInstance(kylinConfig).removeRealizationsFromProjects(RealizationType.HYBRID, hybridInstance.getName());
        store.deleteResource(hybridInstance.getResourcePath());
        hybridManager.reloadAllHybridInstance();
        logger.info("HybridInstance was deleted at: " + hybridInstance.getResourcePath());
    }

    private void checkSegmentOffset(List<RealizationEntry> realizationEntries) {
        if (realizationEntries == null || realizationEntries.size() == 0)
            throw new RuntimeException("No realization found");
        if (realizationEntries.size() == 1)
            throw new RuntimeException("Hybrid needs at least 2 cubes");
        long lastOffset = -1;
        for (RealizationEntry entry : realizationEntries) {
            if (entry.getType() != RealizationType.CUBE) {
                throw new RuntimeException("Wrong realization type: " + entry.getType() + ", only cube supported. ");
            }

            CubeInstance cubeInstance = cubeManager.getCube(entry.getRealization());
            CubeSegment segment = cubeInstance.getLastSegment();
            if (segment == null)
                continue;
            if (lastOffset == -1) {
                lastOffset = segment.getSourceOffsetEnd();
            } else {
                if (lastOffset > segment.getSourceOffsetStart()) {
                    throw new RuntimeException("Segments has overlap, could not hybrid. Last Segment End: " + lastOffset + ", Next Segment Start: " + segment.getSourceOffsetStart());
                }
                lastOffset = segment.getSourceOffsetEnd();
            }
        }
    }
}
