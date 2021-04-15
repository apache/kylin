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

package org.apache.kylin.rest.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

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

    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("the target project for the hybrid cube").create("project");

    private static final Option OPTION_MODEL = OptionBuilder.withArgName("model").hasArg().isRequired(false).withDescription("the target model for the hybrid cube").create("model");

    private static final Option OPTION_CUBES = OptionBuilder.withArgName("cubes").hasArg().isRequired(false).withDescription("the cubes used in HybridCube, seperated by comma, empty if to delete HybridCube").create("cubes");

    private static final Option OPTION_CHECK = OptionBuilder.withArgName("check").hasArg().isRequired(false).withDescription("whether to check cube size, default true.").create("check");

    private final Options options;

    private KylinConfig kylinConfig;
    private CubeManager cubeManager;
    private HybridManager hybridManager;
    private DataModelManager metadataManager;
    private ResourceStore store;

    public HybridCubeCLI() {
        options = new Options();
        options.addOption(OPTION_ACTION);
        options.addOption(OPTION_HYBRID_NAME);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_MODEL);
        options.addOption(OPTION_CUBES);
        options.addOption(OPTION_CHECK);

        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.store = ResourceStore.getStore(kylinConfig);
        this.cubeManager = CubeManager.getInstance(kylinConfig);
        this.hybridManager = HybridManager.getInstance(kylinConfig);
        this.metadataManager = DataModelManager.getInstance(kylinConfig);
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
        boolean checkCubeSize = optionsHelper.hasOption(OPTION_CHECK) ? Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_CHECK)) : true;

        HybridInstance hybridInstance = hybridManager.getHybridInstance(hybridName);

        if ("delete".equals(action)) {
            if (hybridInstance == null) {
                throw new IllegalArgumentException("The Hybrid Cube doesn't exist, could not delete: " + hybridName);
            }
            // Delete the Hybrid
            delete(hybridInstance);
            return;
        }

        String[] cubeNames = new String[] {};
        if (cubeNamesStr != null)
            cubeNames = cubeNamesStr.split(",");
        String owner = null;

        DataModelDesc modelDesc = metadataManager.getDataModelDesc(modelName);
        if (modelDesc == null) {
            throw new IllegalArgumentException("Could not find model: " + modelName);
        }

        List<RealizationEntry> realizationEntries = new ArrayList<RealizationEntry>();
        for (String cubeName : cubeNames) {
            if (StringUtils.isEmpty(cubeName))
                continue;
            CubeInstance cube = cubeManager.getCube(cubeName);
            if (cube == null) {
                throw new IllegalArgumentException("Could not find cube: " + cubeName);
            }
            if (owner == null) {
                owner = cube.getOwner();
            }
            realizationEntries.add(RealizationEntry.create(RealizationType.CUBE, cube.getName()));
        }

        int realizationEntriesLen = realizationEntries.size();
        HashSet<RealizationEntry> hashSet = new HashSet<>();
        for (int i = 0; i < realizationEntriesLen; i++) {
            hashSet.add(realizationEntries.get(i));
        }
        int hashSetLen = hashSet.size();
        if (realizationEntriesLen != hashSetLen) {
            Collection<RealizationEntry> duplicateCubes = CollectionUtils.subtract(realizationEntries, hashSet);
            throw new IllegalArgumentException("The Cubes name does duplicate, could not create: " + duplicateCubes);
        }

        if ("create".equals(action)) {
            if (hybridInstance != null) {
                throw new IllegalArgumentException("The Hybrid Cube does exist, could not create: " + hybridName);
            }
            //Create new Hybrid
            create(hybridName, realizationEntries, projectName, owner);
        } else if ("update".equals(action)) {
            if (hybridInstance == null) {
                throw new IllegalArgumentException("The Hybrid Cube doesn't exist, could not update: " + hybridName);
            }
            // Update the Hybrid
            update(hybridInstance, realizationEntries, projectName, owner, checkCubeSize);
        }
    }

    private HybridInstance create(String hybridName, List<RealizationEntry> realizationEntries, String projectName, String owner) throws IOException {
        checkSegmentOffset(realizationEntries);
        HybridInstance hybridInstance = HybridInstance.create(kylinConfig, hybridName, realizationEntries);
        store.checkAndPutResource(hybridInstance.getResourcePath(), hybridInstance, HybridManager.HYBRID_SERIALIZER);
        ProjectManager.getInstance(kylinConfig).moveRealizationToProject(RealizationType.HYBRID, hybridInstance.getName(), projectName, owner);
        hybridManager.reloadHybridInstance(hybridName);
        logger.info("HybridInstance was created at: " + hybridInstance.getResourcePath());
        return hybridInstance;
    }

    private void update(HybridInstance hybridInstance, List<RealizationEntry> realizationEntries, String projectName, String owner, boolean checkCubeSize) throws IOException {
        if (checkCubeSize)
            checkSegmentOffset(realizationEntries);
        hybridInstance.setRealizationEntries(realizationEntries);
        store.checkAndPutResource(hybridInstance.getResourcePath(), hybridInstance, HybridManager.HYBRID_SERIALIZER);
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
        List<SegmentRange> segmentRanges = Lists.newArrayList();

        for (RealizationEntry entry : realizationEntries) {
            if (entry.getType() != RealizationType.CUBE) {
                throw new IllegalArgumentException("Wrong realization type: " + entry.getType() + ", only cube supported. ");
            }

            CubeInstance cubeInstance = cubeManager.getCube(entry.getRealization());
            Segments<CubeSegment> segments = cubeInstance.getSegments();

            for (CubeSegment segment : segments) {
                segmentRanges.add(segment.getSegRange());
            }
        }

        if (segmentRanges.size() >= 2) {
            Collections.sort(segmentRanges);

            for (int i = 0; i < segmentRanges.size() - 1; i++) {
                if (segmentRanges.get(i).overlaps(segmentRanges.get(i + 1))) {
                    throw new IllegalArgumentException("Segments has overlap, could not hybrid. First Segment Range: [" + segmentRanges.get(i).start.v + "," + segmentRanges.get(i).end.v + "], Second Segment Range: [" + segmentRanges.get(i + 1).start.v + "," + segmentRanges.get(i + 1).end.v + "]");
                }
            }
        }
    }
}
