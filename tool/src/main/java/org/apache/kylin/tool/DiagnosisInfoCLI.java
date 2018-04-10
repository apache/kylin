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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DiagnosisInfoCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosisInfoCLI.class);
    private static final int DEFAULT_PERIOD = 3;

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false)
            .withDescription("Specify realizations in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg()
            .isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.")
            .create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_HBASE = OptionBuilder.withArgName("includeHBase").hasArg()
            .isRequired(false).withDescription("Specify whether to include hbase files to extract. Default true.")
            .create("includeHBase");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.withArgName("includeClient").hasArg()
            .isRequired(false).withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_JOB = OptionBuilder.withArgName("includeJobs").hasArg().isRequired(false)
            .withDescription("Specify whether to include job info to extract. Default true.").create("includeJobs");

    @SuppressWarnings("static-access")
    private static final Option OPTION_THREADS = OptionBuilder.withArgName("threads").hasArg().isRequired(false)
            .withDescription("Specify number of threads for parallel extraction.").create("threads");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PERIOD = OptionBuilder.withArgName("period").hasArg().isRequired(false)
            .withDescription("specify how many days of kylin info to extract. Default " + DEFAULT_PERIOD + ".")
            .create("period");

    private static final int DEFAULT_PARALLEL_SIZE = 4;

    private ExecutorService executorService;

    public DiagnosisInfoCLI() {
        super();

        packageType = "project";

        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_INCLUDE_CONF);
        options.addOption(OPTION_INCLUDE_HBASE);
        options.addOption(OPTION_INCLUDE_CLIENT);
        options.addOption(OPTION_INCLUDE_JOB);
        options.addOption(OPTION_PERIOD);

    }

    public static void main(String[] args) {
        DiagnosisInfoCLI diagnosisInfoCLI = new DiagnosisInfoCLI();
        diagnosisInfoCLI.execute(args);
    }

    private List<String> getProjects(String projectSeed) {
        List<String> result = Lists.newLinkedList();
        if (projectSeed.equalsIgnoreCase("-all")) {
            ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
                result.add(projectInstance.getName());
            }
        } else {
            result.add(projectSeed);
        }
        if (result.isEmpty()) {
            throw new RuntimeException("No project to extract.");
        }
        return result;
    }

    @Override
    protected void executeExtract(final OptionsHelper optionsHelper, final File exportDir) throws IOException {
        final String projectInput = optionsHelper.getOptionValue(options.getOption("project"));
        final boolean includeConf = optionsHelper.hasOption(OPTION_INCLUDE_CONF)
                ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CONF))
                : true;
        final boolean includeHBase = optionsHelper.hasOption(OPTION_INCLUDE_HBASE)
                ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_HBASE))
                : true;
        final boolean includeClient = optionsHelper.hasOption(OPTION_INCLUDE_CLIENT)
                ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CLIENT))
                : true;
        final boolean includeJob = optionsHelper.hasOption(OPTION_INCLUDE_JOB)
                ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_JOB))
                : true;
        final int threadsNum = optionsHelper.hasOption(OPTION_THREADS)
                ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_THREADS))
                : DEFAULT_PARALLEL_SIZE;
        final String projectNames = StringUtils.join(getProjects(projectInput), ",");
        final int period = optionsHelper.hasOption(OPTION_PERIOD)
                ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_PERIOD))
                : DEFAULT_PERIOD;

        logger.info("Start diagnosis info extraction in {} threads.", threadsNum);
        executorService = Executors.newFixedThreadPool(threadsNum);

        // export cube metadata
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Start to extract metadata.");
                try {
                    String[] cubeMetaArgs = { "-packagetype", "cubemeta", "-destDir",
                            new File(exportDir, "metadata").getAbsolutePath(), "-project", projectNames, "-compress",
                            "false", "-includeJobs", "false", "-submodule", "true" };
                    CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
                    logger.info("CubeMetaExtractor args: " + Arrays.toString(cubeMetaArgs));
                    cubeMetaExtractor.execute(cubeMetaArgs);
                } catch (Exception e) {
                    logger.error("Error in export metadata.", e);
                }
            }
        });

        // extract all job instances
        if (includeJob) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    logger.info("Start to extract jobs.");
                    try {
                        String[] jobArgs = { "-destDir", new File(exportDir, "jobs").getAbsolutePath(), "-period",
                                Integer.toString(period), "-compress", "false", "-submodule", "true" };
                        JobInstanceExtractor jobInstanceExtractor = new JobInstanceExtractor();
                        jobInstanceExtractor.execute(jobArgs);
                    } catch (Exception e) {
                        logger.error("Error in export jobs.", e);
                    }
                }
            });
        }

        // export HBase
        if (includeHBase) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    logger.info("Start to extract HBase usage.");
                    try {
                        // use reflection to isolate NoClassDef errors when HBase is not available
                        String[] hbaseArgs = { "-destDir", new File(exportDir, "hbase").getAbsolutePath(), "-project",
                                projectNames, "-compress", "false", "-submodule", "true" };
                        logger.info("HBaseUsageExtractor args: " + Arrays.toString(hbaseArgs));
                        Object extractor = ClassUtil.newInstance("org.apache.kylin.tool.HBaseUsageExtractor");
                        Method execute = extractor.getClass().getMethod("execute", String[].class);
                        execute.invoke(extractor, (Object) hbaseArgs);
                    } catch (Throwable e) {
                        logger.error("Error in export HBase usage.", e);
                    }
                }
            });
        }

        // export conf
        if (includeConf) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    logger.info("Start to extract kylin conf files.");
                    try {
                        File destConfDir = new File(exportDir, "conf");
                        FileUtils.forceMkdir(destConfDir);
                        File srcConfDir = new File(ToolUtil.getConfFolder());
                        Preconditions.checkState(srcConfDir.exists(),
                                "Cannot find config dir: " + srcConfDir.getAbsolutePath());
                        File[] confFiles = srcConfDir.listFiles();
                        if (confFiles != null) {
                            for (File confFile : confFiles) {
                                FileUtils.copyFileToDirectory(confFile, destConfDir);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Error in export conf.", e);
                    }
                }
            });
        }

        // export client
        if (includeClient) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        String[] clientArgs = { "-destDir", new File(exportDir, "client").getAbsolutePath(),
                                "-compress", "false", "-submodule", "true" };
                        ClientEnvExtractor clientEnvExtractor = new ClientEnvExtractor();
                        logger.info("ClientEnvExtractor args: " + Arrays.toString(clientArgs));
                        clientEnvExtractor.execute(clientArgs);
                    } catch (Exception e) {
                        logger.error("Error in export client info.", e);
                    }
                }
            });
        }

        // export logs
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Start to extract logs.");
                try {
                    String[] logsArgs = { "-destDir", new File(exportDir, "logs").getAbsolutePath(), "-logPeriod",
                            Integer.toString(period), "-compress", "false", "-submodule", "true" };
                    KylinLogExtractor logExtractor = new KylinLogExtractor();
                    logger.info("KylinLogExtractor args: " + Arrays.toString(logsArgs));
                    logExtractor.execute(logsArgs);
                } catch (Exception e) {
                    logger.error("Error in export logs.", e);
                }
            }
        });

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Diagnosis info dump interrupted.", e);
        }
    }
}
