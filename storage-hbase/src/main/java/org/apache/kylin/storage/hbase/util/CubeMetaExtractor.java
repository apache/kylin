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

package org.apache.kylin.storage.hbase.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * extract cube related info for debugging/distributing purpose
 * TODO: deal with II case, deal with Streaming case
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
    private static final Option OPTION_INCLUDE_SEGMENTS = OptionBuilder.withArgName("includeSegments").hasArg().isRequired(false).withDescription("set this to true if want extract the segments info, related dicts, etc.").create("includeSegments");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_JOB = OptionBuilder.withArgName("includeJobs").hasArg().isRequired(false).withDescription("set this to true if want to extract job info/outputs too").create("includeJobs");

    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(false).withDescription("specify the dest dir to save the related metadata").create("destDir");

    private Options options = null;
    private KylinConfig kylinConfig;
    private MetadataManager metadataManager;
    private ProjectManager projectManager;
    private HybridManager hybridManager;
    private CubeManager cubeManager;
    private CubeDescManager cubeDescManager;
    private IIManager iiManager;
    private IIDescManager iiDescManager;
    private ExecutableDao executableDao;
    RealizationRegistry realizationRegistry;

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
        options.addOption(OPTION_DEST);

    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        boolean includeSegments = optionsHelper.hasOption(OPTION_INCLUDE_SEGMENTS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_SEGMENTS)) : true;
        boolean includeJobs = optionsHelper.hasOption(OPTION_INCLUDE_JOB) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_JOB)) : true;
        String dest = null;
        if (optionsHelper.hasOption(OPTION_DEST)) {
            dest = optionsHelper.getOptionValue(OPTION_DEST);
        }

        if (!includeSegments) {
            throw new RuntimeException("Does not support skip segments for now");
        }

        kylinConfig = KylinConfig.getInstanceFromEnv();
        metadataManager = MetadataManager.getInstance(kylinConfig);
        projectManager = ProjectManager.getInstance(kylinConfig);
        hybridManager = HybridManager.getInstance(kylinConfig);
        cubeManager = CubeManager.getInstance(kylinConfig);
        cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        iiManager = IIManager.getInstance(kylinConfig);
        iiDescManager = IIDescManager.getInstance(kylinConfig);
        executableDao = ExecutableDao.getInstance(kylinConfig);
        realizationRegistry = RealizationRegistry.getInstance(kylinConfig);

        List<String> requiredResources = Lists.newArrayList();
        List<String> optionalResources = Lists.newArrayList();

        if (optionsHelper.hasOption(OPTION_PROJECT)) {
            ProjectInstance projectInstance = projectManager.getProject(optionsHelper.getOptionValue(OPTION_PROJECT));
            if (projectInstance == null) {
                throw new IllegalArgumentException("Project " + optionsHelper.getOptionValue(OPTION_PROJECT) + " does not exist");
            }
            addRequired(requiredResources, ProjectInstance.concatResourcePath(projectInstance.getName()));
            List<RealizationEntry> realizationEntries = projectInstance.getRealizationEntries();
            for (RealizationEntry realizationEntry : realizationEntries) {
                retrieveResourcePath(getRealization(realizationEntry), includeSegments, includeJobs, requiredResources, optionalResources);
            }
        } else if (optionsHelper.hasOption(OPTION_CUBE)) {
            String cubeName = optionsHelper.getOptionValue(OPTION_CUBE);
            IRealization realization;

            if ((realization = cubeManager.getRealization(cubeName)) != null) {
                retrieveResourcePath(realization, includeSegments, includeJobs, requiredResources, optionalResources);
            } else {
                throw new IllegalArgumentException("No cube found with name of " + cubeName);
            }
        } else if (optionsHelper.hasOption(OPTION_HYBRID)) {
            String hybridName = optionsHelper.getOptionValue(OPTION_HYBRID);
            IRealization realization;

            if ((realization = hybridManager.getRealization(hybridName)) != null) {
                retrieveResourcePath(realization, includeSegments, includeJobs, requiredResources, optionalResources);
            } else {
                throw new IllegalArgumentException("No hybrid found with name of" + hybridName);
            }
        }

        executeExtraction(requiredResources, optionalResources, dest);
    }

    private void executeExtraction(List<String> requiredPaths, List<String> optionalPaths, String dest) {
        logger.info("The resource paths going to be extracted:");
        for (String s : requiredPaths) {
            logger.info(s + "(required)");
        }
        for (String s : optionalPaths) {
            logger.info(s + "(optional)");
        }

        if (dest == null) {
            logger.info("Dest is not set, exit directly without extracting");
        } else {
            try {
                ResourceTool.copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(dest));
            } catch (IOException e) {
                throw new RuntimeException("IOException", e);
            }
        }
    }

    private IRealization getRealization(RealizationEntry realizationEntry) {
        return realizationRegistry.getRealization(realizationEntry.getType(), realizationEntry.getRealization());
    }

    private void retrieveResourcePath(IRealization realization, boolean includeSegments, boolean includeJobs, List<String> requiredResources, List<String> optionalResources) {

        logger.info("Deal with realization {} of type {}", realization.getName(), realization.getType());

        if (realization instanceof CubeInstance) {
            CubeInstance cube = (CubeInstance) realization;
            String descName = cube.getDescName();
            CubeDesc cubeDesc = cubeDescManager.getCubeDesc(descName);
            String modelName = cubeDesc.getModelName();
            DataModelDesc modelDesc = metadataManager.getDataModelDesc(modelName);

            for (String tableName : modelDesc.getAllTables()) {
                addRequired(requiredResources, TableDesc.concatResourcePath(tableName));
                addOptional(optionalResources, TableDesc.concatExdResourcePath(tableName));
            }

            addRequired(requiredResources, DataModelDesc.concatResourcePath(modelDesc.getName()));
            addRequired(requiredResources, CubeDesc.concatResourcePath(cubeDesc.getName()));

            if (includeSegments) {
                addRequired(requiredResources, CubeInstance.concatResourcePath(cube.getName()));
                for (CubeSegment segment : cube.getSegments()) {
                    for (String dictPat : segment.getDictionaryPaths()) {
                        addRequired(requiredResources, dictPat);
                    }
                    for (String snapshotPath : segment.getSnapshotPaths()) {
                        addRequired(requiredResources, snapshotPath);
                    }
                    addRequired(requiredResources, segment.getStatisticsResourcePath());

                    if (includeJobs) {
                        String lastJobId = segment.getLastBuildJobID();
                        if (!StringUtils.isEmpty(lastJobId)) {
                            logger.warn("No job exist for segment {}", segment);
                        } else {
                            try {
                                ExecutablePO executablePO = executableDao.getJob(lastJobId);
                                addRequired(requiredResources, ExecutableDao.pathOfJob(lastJobId));
                                addRequired(requiredResources, ExecutableDao.pathOfJobOutput(lastJobId));
                                for (ExecutablePO task : executablePO.getTasks()) {
                                    addRequired(requiredResources, ExecutableDao.pathOfJob(task.getUuid()));
                                    addRequired(requiredResources, ExecutableDao.pathOfJobOutput(task.getUuid()));
                                }
                            } catch (PersistentException e) {
                                throw new RuntimeException("PersistentException", e);
                            }
                        }
                    } else {
                        logger.info("Job info will not be extracted");
                    }
                }
            } else {
                if (includeJobs) {
                    logger.warn("It's useless to set includeJobs to true when includeSegments is set to false");
                }

                throw new IllegalStateException("Does not support skip segments now");
            }
        } else if (realization instanceof HybridInstance) {
            HybridInstance hybridInstance = (HybridInstance) realization;
            addRequired(requiredResources, HybridInstance.concatResourcePath(hybridInstance.getName()));
            for (IRealization iRealization : hybridInstance.getRealizations()) {
                retrieveResourcePath(iRealization, includeSegments, includeJobs, requiredResources, optionalResources);
            }
        } else if (realization instanceof IIInstance) {
            throw new IllegalStateException("Does not support extract II instance or hybrid that contains II");
        } else {
            throw new IllegalStateException("Unknown realization type: " + realization.getType());
        }
    }

    private void addRequired(List<String> resourcePaths, String record) {
        logger.info("adding required resource {}", record);
        resourcePaths.add(record);
    }

    private void addOptional(List<String> optionalPaths, String record) {
        logger.info("adding optional resource {}", record);
        optionalPaths.add(record);
    }

    public static void main(String[] args) {
        CubeMetaExtractor extractor = new CubeMetaExtractor();
        extractor.execute(args);
    }
}
