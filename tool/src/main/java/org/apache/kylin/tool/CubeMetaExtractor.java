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
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.badquery.BadQueryHistoryManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.metadata.streaming.StreamingManager;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * extract cube related info for debugging/distributing purpose
 */
public class CubeMetaExtractor extends AbstractInfoExtractor {

    private static final Logger logger = LoggerFactory.getLogger(CubeMetaExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(false).withDescription("Specify which cube to extract").create("cube");
    @SuppressWarnings("static-access")
    private static final Option OPTION_HYBRID = OptionBuilder.withArgName("hybrid").hasArg().isRequired(false).withDescription("Specify which hybrid to extract").create("hybrid");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");
    @SuppressWarnings("static-access")
    private static final Option OPTION_All_PROJECT = OptionBuilder.withArgName("allProjects").hasArg(false).isRequired(false).withDescription("Specify realizations in all projects to extract").create("allProjects");

    @SuppressWarnings("static-access")
    private static final Option OPTION_STORAGE_TYPE = OptionBuilder.withArgName("storageType").hasArg().isRequired(false).withDescription("Specify the storage type to overwrite. Default is empty, keep origin.").create("storageType");

    @SuppressWarnings("static-access")
    private static final Option OPTION_ENGINE_TYPE = OptionBuilder.withArgName("engineType").hasArg().isRequired(false).withDescription("Specify the engine type to overwrite. Default is empty, keep origin.").create("engineType");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_SEGMENTS = OptionBuilder.withArgName("includeSegments").hasArg().isRequired(false).withDescription("set this to true if want extract the segments info. Default true").create("includeSegments");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_JOB = OptionBuilder.withArgName("includeJobs").hasArg().isRequired(false).withDescription("set this to true if want to extract job info/outputs too. Default false").create("includeJobs");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_ONLY_JOB_OUTPUT = OptionBuilder.withArgName("onlyOutput").hasArg().isRequired(false).withDescription("when include jobs, onlt extract output of job. Default true").create("onlyOutput");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_SEGMENT_DETAILS = OptionBuilder.withArgName("includeSegmentDetails").hasArg().isRequired(false).withDescription("set this to true if want to extract segment details too, such as dict, tablesnapshot. Default false").create("includeSegmentDetails");

    private KylinConfig kylinConfig;
    private MetadataManager metadataManager;
    private ProjectManager projectManager;
    private HybridManager hybridManager;
    private CubeManager cubeManager;
    private StreamingManager streamingManager;
    private CubeDescManager cubeDescManager;
    private ExecutableDao executableDao;
    private RealizationRegistry realizationRegistry;
    private BadQueryHistoryManager badQueryHistoryManager;

    private boolean includeSegments;
    private boolean includeJobs;
    private boolean includeSegmentDetails;
    private boolean onlyJobOutput;
    private String storageType = null;
    private String engineType = null;

    private Set<String> requiredResources = Sets.newLinkedHashSet();
    private Set<String> optionalResources = Sets.newLinkedHashSet();
    private Set<CubeInstance> cubesToTrimAndSave = Sets.newLinkedHashSet();//these cubes needs to be saved skipping segments

    public CubeMetaExtractor() {
        super();

        packageType = "cubemeta";

        OptionGroup realizationOrProject = new OptionGroup();
        realizationOrProject.addOption(OPTION_CUBE);
        realizationOrProject.addOption(OPTION_PROJECT);
        realizationOrProject.addOption(OPTION_HYBRID);
        realizationOrProject.addOption(OPTION_All_PROJECT);
        realizationOrProject.setRequired(true);

        options.addOptionGroup(realizationOrProject);
        options.addOption(OPTION_INCLUDE_SEGMENTS);
        options.addOption(OPTION_INCLUDE_JOB);
        options.addOption(OPTION_INCLUDE_SEGMENT_DETAILS);
        options.addOption(OPTION_INCLUDE_ONLY_JOB_OUTPUT);
        options.addOption(OPTION_STORAGE_TYPE);
        options.addOption(OPTION_ENGINE_TYPE);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        includeSegments = optionsHelper.hasOption(OPTION_INCLUDE_SEGMENTS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_SEGMENTS)) : true;
        includeJobs = optionsHelper.hasOption(OPTION_INCLUDE_JOB) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_JOB)) : false;
        includeSegmentDetails = optionsHelper.hasOption(OPTION_INCLUDE_SEGMENT_DETAILS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_SEGMENT_DETAILS)) : false;
        onlyJobOutput = optionsHelper.hasOption(OPTION_INCLUDE_ONLY_JOB_OUTPUT) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_ONLY_JOB_OUTPUT)) : true;
        storageType = optionsHelper.hasOption(OPTION_STORAGE_TYPE) ? optionsHelper.getOptionValue(OPTION_STORAGE_TYPE) : null;
        engineType = optionsHelper.hasOption(OPTION_ENGINE_TYPE) ? optionsHelper.getOptionValue(OPTION_ENGINE_TYPE) : null;

        kylinConfig = KylinConfig.getInstanceFromEnv();
        metadataManager = MetadataManager.getInstance(kylinConfig);
        projectManager = ProjectManager.getInstance(kylinConfig);
        hybridManager = HybridManager.getInstance(kylinConfig);
        cubeManager = CubeManager.getInstance(kylinConfig);
        cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        executableDao = ExecutableDao.getInstance(kylinConfig);
        realizationRegistry = RealizationRegistry.getInstance(kylinConfig);
        badQueryHistoryManager = BadQueryHistoryManager.getInstance(kylinConfig);

        if (optionsHelper.hasOption(OPTION_All_PROJECT)) {
            for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
                requireProject(projectInstance);
            }
        } else if (optionsHelper.hasOption(OPTION_PROJECT)) {
            String projectNames = optionsHelper.getOptionValue(OPTION_PROJECT);
            for (String projectName : projectNames.split(",")) {
                ProjectInstance projectInstance = projectManager.getProject(projectName);
                Preconditions.checkNotNull(projectInstance, "Project " + projectName + " does not exist.");
                requireProject(projectInstance);
            }
        } else if (optionsHelper.hasOption(OPTION_CUBE)) {
            String cubeNames = optionsHelper.getOptionValue(OPTION_CUBE);
            for (String cubeName : cubeNames.split(",")) {
                IRealization realization = cubeManager.getRealization(cubeName);
                if (realization == null) {
                    throw new IllegalArgumentException("No cube found with name of " + cubeName);
                } else {
                    retrieveResourcePath(realization);
                }
            }
        } else if (optionsHelper.hasOption(OPTION_HYBRID)) {
            String hybridNames = optionsHelper.getOptionValue(OPTION_HYBRID);
            for (String hybridName : hybridNames.split(",")) {
                IRealization realization = hybridManager.getRealization(hybridName);

                if (realization != null) {
                    retrieveResourcePath(realization);
                } else {
                    throw new IllegalArgumentException("No hybrid found with name of" + hybridName);
                }
            }
        }

        executeExtraction(exportDir.getAbsolutePath());
        engineOverwrite(new File(exportDir.getAbsolutePath()));
    }

    private void requireProject(ProjectInstance projectInstance) throws IOException {
        addRequired(projectInstance.getResourcePath());
        List<RealizationEntry> realizationEntries = projectInstance.getRealizationEntries();
        for (RealizationEntry realizationEntry : realizationEntries) {
            retrieveResourcePath(getRealization(realizationEntry));
        }
        List<DataModelDesc> modelDescs = metadataManager.getModels(projectInstance.getName());
        for (DataModelDesc modelDesc : modelDescs) {
            addRequired(DataModelDesc.concatResourcePath(modelDesc.getName()));
        }
        addOptional(badQueryHistoryManager.getBadQueriesForProject(projectInstance.getName()).getResourcePath());
    }

    private void executeExtraction(String dest) {
        logger.info("The resource paths going to be extracted:");
        for (String s : requiredResources) {
            logger.info(s + "(required)");
        }
        for (String s : optionalResources) {
            logger.info(s + "(optional)");
        }
        for (CubeInstance cube : cubesToTrimAndSave) {
            logger.info("Cube {} will be trimmed and extracted", cube);
        }

        try {
            KylinConfig srcConfig = KylinConfig.getInstanceFromEnv();
            KylinConfig dstConfig = KylinConfig.createInstanceFromUri(dest);

            ResourceTool.copy(srcConfig, dstConfig, Lists.newArrayList(requiredResources));

            for (String r : optionalResources) {
                try {
                    ResourceTool.copy(srcConfig, dstConfig, Lists.newArrayList(r));
                } catch (Exception e) {
                    logger.warn("Exception when copying optional resource {}. May be caused by resource missing. skip it.", r);
                }
            }

            ResourceStore dstStore = ResourceStore.getStore(dstConfig);
            for (CubeInstance cube : cubesToTrimAndSave) {
                CubeInstance trimmedCube = CubeInstance.getCopyOf(cube);
                trimmedCube.getSegments().clear();
                trimmedCube.setUuid(cube.getUuid());
                dstStore.putResource(trimmedCube.getResourcePath(), trimmedCube, CubeManager.CUBE_SERIALIZER);
            }

        } catch (Exception e) {
            throw new RuntimeException("Exception", e);
        }
    }

    private void engineOverwrite(File dest) throws IOException {
        if (engineType != null || storageType != null) {
            for (File f : dest.listFiles()) {
                if (f.isDirectory()) {
                    engineOverwrite(f);
                } else {
                    engineOverwriteInternal(f);
                }
            }
        }
    }

    private void engineOverwriteInternal(File f) throws IOException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(f);
            boolean replaced = false;
            if (engineType != null && rootNode.get("engine_type") != null) {
                ((ObjectNode) rootNode).put("engine_type", Integer.parseInt(engineType));
                replaced = true;
            }
            if (storageType != null && rootNode.get("storage_type") != null) {
                ((ObjectNode) rootNode).put("storage_type", Integer.parseInt(storageType));
                replaced = true;
            }
            if (replaced) {
                objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
                objectMapper.writeValue(f, rootNode);
            }
        } catch (JsonProcessingException ex) {
            logger.info("cannot parse file {}", f);
        }
    }

    private IRealization getRealization(RealizationEntry realizationEntry) {
        return realizationRegistry.getRealization(realizationEntry.getType(), realizationEntry.getRealization());
    }

    private void dealWithStreaming(CubeInstance cube) {
        streamingManager = StreamingManager.getInstance(kylinConfig);
        for (StreamingConfig streamingConfig : streamingManager.listAllStreaming()) {
            if (streamingConfig.getName() != null && streamingConfig.getName().equalsIgnoreCase(cube.getRootFactTable())) {
                addRequired(StreamingConfig.concatResourcePath(streamingConfig.getName()));
                addRequired(KafkaConfig.concatResourcePath(streamingConfig.getName()));
            }
        }
    }

    private void retrieveResourcePath(IRealization realization) {
        if (realization == null) {
            return;
        }

        logger.info("Deal with realization {} of type {}", realization.getName(), realization.getType());

        if (realization instanceof CubeInstance) {
            CubeInstance cube = (CubeInstance) realization;
            String descName = cube.getDescName();
            CubeDesc cubeDesc = cubeDescManager.getCubeDesc(descName);
            String modelName = cubeDesc.getModelName();
            DataModelDesc modelDesc = metadataManager.getDataModelDesc(modelName);

            dealWithStreaming(cube);

            for (TableRef table : modelDesc.getAllTables()) {
                String tableName = table.getTableIdentity();
                addRequired(TableDesc.concatResourcePath(tableName));
                addOptional(TableDesc.concatExdResourcePath(tableName));
            }

            addRequired(DataModelDesc.concatResourcePath(modelDesc.getName()));
            addRequired(CubeDesc.concatResourcePath(cubeDesc.getName()));

            if (includeSegments) {
                addRequired(CubeInstance.concatResourcePath(cube.getName()));
                for (CubeSegment segment : cube.getSegments(SegmentStatusEnum.READY)) {
                    addRequired(CubeSegment.getStatisticsResourcePath(cube.getName(), segment.getUuid()));
                    if (includeSegmentDetails) {
                        for (String dictPat : segment.getDictionaryPaths()) {
                            addRequired(dictPat);
                        }
                        for (String snapshotPath : segment.getSnapshotPaths()) {
                            addRequired(snapshotPath);
                        }
                    }

                    if (includeJobs) {
                        String lastJobId = segment.getLastBuildJobID();
                        if (StringUtils.isEmpty(lastJobId)) {
                            throw new RuntimeException("No job exist for segment :" + segment);
                        } else {
                            try {
                                if (onlyJobOutput) {
                                    ExecutablePO executablePO = executableDao.getJob(lastJobId);
                                    addRequired(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + lastJobId);
                                } else {
                                    ExecutablePO executablePO = executableDao.getJob(lastJobId);
                                    addRequired(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + lastJobId);
                                    addRequired(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + lastJobId);
                                    for (ExecutablePO task : executablePO.getTasks()) {
                                        addRequired(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + task.getUuid());
                                        addRequired(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + task.getUuid());
                                    }
                                }
                            } catch (PersistentException e) {
                                throw new RuntimeException("PersistentException", e);
                            }
                        }
                    }
                }
            } else {
                if (includeJobs) {
                    logger.warn("It's useless to set includeJobs to true when includeSegments is set to false");
                }

                cube.setStatus(RealizationStatusEnum.DISABLED);
                cubesToTrimAndSave.add(cube);
            }
        } else if (realization instanceof HybridInstance) {
            HybridInstance hybridInstance = (HybridInstance) realization;
            addRequired(HybridInstance.concatResourcePath(hybridInstance.getName()));
            for (IRealization iRealization : hybridInstance.getRealizations()) {
                if (iRealization.getType() != RealizationType.CUBE) {
                    throw new RuntimeException("Hybrid " + iRealization.getName() + " contains non cube child " + iRealization.getName() + " with type " + iRealization.getType());
                }
                retrieveResourcePath(iRealization);
            }
        } else {
            logger.warn("Unknown realization type: " + realization.getType());
        }
    }

    private void addRequired(String record) {
        logger.info("adding required resource {}", record);
        requiredResources.add(record);
    }

    private void addOptional(String record) {
        logger.info("adding optional resource {}", record);
        optionalResources.add(record);
    }

    public static void main(String[] args) {
        CubeMetaExtractor extractor = new CubeMetaExtractor();
        extractor.execute(args);
    }
}
