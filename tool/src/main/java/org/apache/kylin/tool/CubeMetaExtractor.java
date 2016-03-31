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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * extract cube related info for debugging/distributing purpose
 * TODO: deal with II case
 */
public class CubeMetaExtractor extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(CubeMetaExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(false).withDescription("Specify which cube to extract").create("cube");
    @SuppressWarnings("static-access")
    private static final Option OPTION_HYBRID = OptionBuilder.withArgName("hybrid").hasArg().isRequired(false).withDescription("Specify which hybrid to extract").create("hybrid");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_SEGMENTS = OptionBuilder.withArgName("includeSegments").hasArg().isRequired(false).withDescription("set this to true if want extract the segments info. Default true").create("includeSegments");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_JOB = OptionBuilder.withArgName("includeJobs").hasArg().isRequired(false).withDescription("set this to true if want to extract job info/outputs too. Default false").create("includeJobs");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_SEGMENT_DETAILS = OptionBuilder.withArgName("includeSegmentDetails").hasArg().isRequired(false).withDescription("set this to true if want to extract segment details too, such as dict, tablesnapshot. Default false").create("includeSegmentDetails");

    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(false).withDescription("specify the dest dir to save the related metadata").create("destDir");

    private Options options = null;
    private KylinConfig kylinConfig;
    private MetadataManager metadataManager;
    private ProjectManager projectManager;
    private HybridManager hybridManager;
    private CubeManager cubeManager;
    private StreamingManager streamingManager;
    private KafkaConfigManager kafkaConfigManager;
    private CubeDescManager cubeDescManager;
    private ExecutableDao executableDao;
    private RealizationRegistry realizationRegistry;

    boolean includeSegments;
    boolean includeJobs;
    boolean includeSegmentDetails;

    List<String> requiredResources = Lists.newArrayList();
    List<String> optionalResources = Lists.newArrayList();
    List<CubeInstance> cubesToTrimAndSave = Lists.newArrayList();//these cubes needs to be saved skipping segments

    public CubeMetaExtractor() {
        options = new Options();

        OptionGroup realizationOrProject = new OptionGroup();
        realizationOrProject.addOption(OPTION_CUBE);
        realizationOrProject.addOption(OPTION_PROJECT);
        realizationOrProject.addOption(OPTION_HYBRID);
        realizationOrProject.setRequired(true);

        options.addOptionGroup(realizationOrProject);
        options.addOption(OPTION_INCLUDE_SEGMENTS);
        options.addOption(OPTION_INCLUDE_JOB);
        options.addOption(OPTION_INCLUDE_SEGMENT_DETAILS);
        options.addOption(OPTION_DEST);

    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        includeSegments = optionsHelper.hasOption(OPTION_INCLUDE_SEGMENTS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_SEGMENTS)) : true;
        includeJobs = optionsHelper.hasOption(OPTION_INCLUDE_JOB) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_JOB)) : false;
        includeSegmentDetails = optionsHelper.hasOption(OPTION_INCLUDE_SEGMENT_DETAILS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_SEGMENT_DETAILS)) : false;

        String dest = null;
        if (optionsHelper.hasOption(OPTION_DEST)) {
            dest = optionsHelper.getOptionValue(OPTION_DEST);
        }

        if (StringUtils.isEmpty(dest)) {
            throw new RuntimeException("destDir is not set, exit directly without extracting");
        }

        if (!dest.endsWith("/")) {
            dest = dest + "/";
        }

        kylinConfig = KylinConfig.getInstanceFromEnv();
        metadataManager = MetadataManager.getInstance(kylinConfig);
        projectManager = ProjectManager.getInstance(kylinConfig);
        hybridManager = HybridManager.getInstance(kylinConfig);
        cubeManager = CubeManager.getInstance(kylinConfig);
        cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        streamingManager = StreamingManager.getInstance(kylinConfig);
        kafkaConfigManager = KafkaConfigManager.getInstance(kylinConfig);
        executableDao = ExecutableDao.getInstance(kylinConfig);
        realizationRegistry = RealizationRegistry.getInstance(kylinConfig);

        if (optionsHelper.hasOption(OPTION_PROJECT)) {
            ProjectInstance projectInstance = projectManager.getProject(optionsHelper.getOptionValue(OPTION_PROJECT));
            if (projectInstance == null) {
                throw new IllegalArgumentException("Project " + optionsHelper.getOptionValue(OPTION_PROJECT) + " does not exist");
            }
            addRequired(ProjectInstance.concatResourcePath(projectInstance.getName()));
            List<RealizationEntry> realizationEntries = projectInstance.getRealizationEntries();
            for (RealizationEntry realizationEntry : realizationEntries) {
                retrieveResourcePath(getRealization(realizationEntry));
            }
        } else if (optionsHelper.hasOption(OPTION_CUBE)) {
            String cubeName = optionsHelper.getOptionValue(OPTION_CUBE);
            IRealization realization;

            if ((realization = cubeManager.getRealization(cubeName)) != null) {
                retrieveResourcePath(realization);
            } else {
                throw new IllegalArgumentException("No cube found with name of " + cubeName);
            }
        } else if (optionsHelper.hasOption(OPTION_HYBRID)) {
            String hybridName = optionsHelper.getOptionValue(OPTION_HYBRID);
            IRealization realization;

            if ((realization = hybridManager.getRealization(hybridName)) != null) {
                retrieveResourcePath(realization);
            } else {
                throw new IllegalArgumentException("No hybrid found with name of" + hybridName);
            }
        }

        executeExtraction(dest);

        logger.info("Extracted metadata files located at: " + new File(dest).getAbsolutePath());
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
            ResourceStore src = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
            ResourceStore dst = ResourceStore.getStore(KylinConfig.createInstanceFromUri(dest));

            for (String path : requiredResources) {
                ResourceTool.copyR(src, dst, path);
            }

            for (String path : optionalResources) {
                try {
                    ResourceTool.copyR(src, dst, path);
                } catch (Exception e) {
                    logger.warn("Exception when copying optional resource {}. May be caused by resource missing. Ignore it.");
                }
            }

            for (CubeInstance cube : cubesToTrimAndSave) {
                CubeInstance trimmedCube = CubeInstance.getCopyOf(cube);
                trimmedCube.getSegments().clear();
                trimmedCube.setUuid(cube.getUuid());
                dst.putResource(trimmedCube.getResourcePath(), trimmedCube, CubeManager.CUBE_SERIALIZER);
            }

        } catch (IOException e) {
            throw new RuntimeException("IOException", e);
        }
    }

    private IRealization getRealization(RealizationEntry realizationEntry) {
        return realizationRegistry.getRealization(realizationEntry.getType(), realizationEntry.getRealization());
    }

    private void dealWithStreaming(CubeInstance cube) {
        for (StreamingConfig streamingConfig : streamingManager.listAllStreaming()) {
            if (streamingConfig.getName() != null && streamingConfig.getName().equalsIgnoreCase(cube.getFactTable())) {
                addRequired(StreamingConfig.concatResourcePath(streamingConfig.getName()));
                addRequired(KafkaConfig.concatResourcePath(streamingConfig.getName()));
            }
        }
    }

    private void retrieveResourcePath(IRealization realization) {

        logger.info("Deal with realization {} of type {}", realization.getName(), realization.getType());

        if (realization instanceof CubeInstance) {
            CubeInstance cube = (CubeInstance) realization;
            String descName = cube.getDescName();
            CubeDesc cubeDesc = cubeDescManager.getCubeDesc(descName);
            String modelName = cubeDesc.getModelName();
            DataModelDesc modelDesc = metadataManager.getDataModelDesc(modelName);

            dealWithStreaming(cube);

            for (String tableName : modelDesc.getAllTables()) {
                addRequired(TableDesc.concatResourcePath(tableName));
                addOptional(TableDesc.concatExdResourcePath(tableName));
            }

            addRequired(DataModelDesc.concatResourcePath(modelDesc.getName()));
            addRequired(CubeDesc.concatResourcePath(cubeDesc.getName()));

            if (includeSegments) {
                addRequired(CubeInstance.concatResourcePath(cube.getName()));
                for (CubeSegment segment : cube.getSegments(SegmentStatusEnum.READY)) {
                    if (includeSegmentDetails) {
                        for (String dictPat : segment.getDictionaryPaths()) {
                            addRequired(dictPat);
                        }
                        for (String snapshotPath : segment.getSnapshotPaths()) {
                            addRequired(snapshotPath);
                        }
                        addRequired(segment.getStatisticsResourcePath());
                    }

                    if (includeJobs) {
                        String lastJobId = segment.getLastBuildJobID();
                        if (StringUtils.isEmpty(lastJobId)) {
                            throw new RuntimeException("No job exist for segment :" + segment);
                        } else {
                            try {
                                ExecutablePO executablePO = executableDao.getJob(lastJobId);
                                addRequired(ExecutableDao.pathOfJob(lastJobId));
                                addRequired(ExecutableDao.pathOfJobOutput(lastJobId));
                                for (ExecutablePO task : executablePO.getTasks()) {
                                    addRequired(ExecutableDao.pathOfJob(task.getUuid()));
                                    addRequired(ExecutableDao.pathOfJobOutput(task.getUuid()));
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
        } else if (realization instanceof IIInstance) {
            throw new IllegalStateException("Does not support extract II instance or hybrid that contains II");
        } else {
            throw new IllegalStateException("Unknown realization type: " + realization.getType());
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
