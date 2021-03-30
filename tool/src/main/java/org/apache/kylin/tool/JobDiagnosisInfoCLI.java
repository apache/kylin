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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.common.util.ToolUtil;
import org.apache.kylin.tool.extractor.AbstractInfoExtractor;
import org.apache.kylin.tool.extractor.ClientEnvExtractor;
import org.apache.kylin.tool.extractor.CubeMetaExtractor;
import org.apache.kylin.tool.extractor.JStackExtractor;
import org.apache.kylin.tool.extractor.KylinLogExtractor;
import org.apache.kylin.tool.extractor.SparkEnvInfoExtractor;
import org.apache.kylin.tool.extractor.YarnLogExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;


public class JobDiagnosisInfoCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(JobDiagnosisInfoCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_JOB_ID = OptionBuilder.withArgName("jobId").hasArg().isRequired(true)
            .withDescription("specify the Job ID to extract information. ").create("jobId");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CUBE = OptionBuilder.withArgName("includeCube").hasArg()
            .isRequired(false)
            .withDescription("set this to true if want to extract related cube info too. Default true")
            .create("includeCube");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_YARN_LOGS = OptionBuilder.withArgName("includeYarnLogs").hasArg()
            .isRequired(false)
            .withDescription("set this to true if want to extract related yarn logs too. Default true")
            .create("includeYarnLogs");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.withArgName("includeClient").hasArg()
            .isRequired(false).withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg()
            .isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.")
            .create("includeConf");

    List<String> requiredResources = Lists.newArrayList();
    private KylinConfig kylinConfig;
    private ExecutableDao executableDao;

    public JobDiagnosisInfoCLI() {
        super();

        packageType = "job";

        options.addOption(OPTION_JOB_ID);
        options.addOption(OPTION_INCLUDE_CUBE);
        options.addOption(OPTION_INCLUDE_CLIENT);
        options.addOption(OPTION_INCLUDE_YARN_LOGS);
        options.addOption(OPTION_INCLUDE_CONF);

        kylinConfig = KylinConfig.getInstanceFromEnv();
        executableDao = ExecutableDao.getInstance(kylinConfig);
    }

    public static void main(String[] args) {
        JobDiagnosisInfoCLI extractor = new JobDiagnosisInfoCLI();
        extractor.execute(args);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        String kylinJobId = optionsHelper.getOptionValue(OPTION_JOB_ID);
        boolean includeCube = optionsHelper.hasOption(OPTION_INCLUDE_CUBE)
                ? Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_INCLUDE_CUBE))
                : true;
        boolean includeYarnLogs = optionsHelper.hasOption(OPTION_INCLUDE_YARN_LOGS)
                ? Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_INCLUDE_YARN_LOGS))
                : true;
        boolean includeClient = optionsHelper.hasOption(OPTION_INCLUDE_CLIENT)
                ? Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_INCLUDE_CLIENT))
                : true;
        boolean includeConf = optionsHelper.hasOption(OPTION_INCLUDE_CONF)
                ? Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_INCLUDE_CONF))
                : true;

        // dump job output
        logger.info("Start to dump job output");
        ExecutablePO executablePO = executableDao.getJob(kylinJobId);
        addRequired(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + kylinJobId);
        addRequired(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + kylinJobId);
        for (ExecutablePO kylinTask : executablePO.getTasks()) {
            addRequired(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + kylinTask.getUuid());
            addRequired(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + kylinTask.getUuid());
        }
        extractResources(exportDir);

        // dump cube metadata
        if (includeCube) {
            String cubeName = executablePO.getParams().get("cubeName");
            if (!StringUtils.isEmpty(cubeName)) {
                File metaDir = new File(exportDir, "cube");
                FileUtils.forceMkdir(metaDir);
                String[] cubeMetaArgs = {"-packagetype", "cubemeta", "-cube", cubeName, "-destDir",
                        new File(metaDir, cubeName).getAbsolutePath(), "-includeJobs", "false", "-compress", "false",
                        "-submodule", "true"};
                logger.info("Start to extract related cube: {}", StringUtils.join(cubeMetaArgs));
                CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
                logger.info("CubeMetaExtractor args: {}", Arrays.toString(cubeMetaArgs));
                cubeMetaExtractor.execute(cubeMetaArgs);
            }
        }

        // dump mr job info
        if (includeYarnLogs) {
            YarnLogExtractor yarnLogExtractor = new YarnLogExtractor();
            logger.info("Start to dump mr job info: {}", kylinJobId);
            File yarnDir = new File(exportDir, "yarn");
            FileUtils.forceMkdir(yarnDir);
            yarnLogExtractor.extractYarnLogAndMRJob(kylinJobId, new File(yarnDir, kylinJobId));
        }

        // host info
        if (includeClient) {
            logger.info("Start to extract client info.");
            String[] clientArgs = {"-destDir", new File(exportDir, "client").getAbsolutePath(), "-compress", "false",
                    "-submodule", "true"};
            ClientEnvExtractor clientEnvExtractor = new ClientEnvExtractor();
            logger.info("ClientEnvExtractor args: " + Arrays.toString(clientArgs));
            clientEnvExtractor.execute(clientArgs);
        }

        // export conf
        if (includeConf) {
            logger.info("Start to extract kylin conf files.");
            try {
                FileUtils.copyDirectoryToDirectory(new File(ToolUtil.getConfFolder()), exportDir);
            } catch (Exception e) {
                logger.warn("Error in export conf.", e);
            }
        }

        // dump jstack
        String[] jstackDumpArgs = {"-destDir", exportDir.getAbsolutePath(), "-compress", "false", "-submodule",
                "true"};
        logger.info("JStackExtractor args: {}", Arrays.toString(jstackDumpArgs));
        try {
            new JStackExtractor().execute(jstackDumpArgs);
        } catch (Exception e) {
            logger.error("Error execute jstack dump extractor");
        }

        // export spark conf
        String[] sparkEnvArgs = {"-destDir", exportDir.getAbsolutePath(), "-compress", "false", "-submodule",
                "true"};
        try {
            new SparkEnvInfoExtractor().execute(sparkEnvArgs);
        } catch (Exception e) {
            logger.error("Error execute spark extractor");
        }

        // export kylin logs
        String[] logsArgs = {"-destDir", new File(exportDir, "logs").getAbsolutePath(), "-compress", "false",
                "-submodule", "true"};
        KylinLogExtractor logExtractor = new KylinLogExtractor();
        logger.info("KylinLogExtractor args: " + Arrays.toString(logsArgs));
        logExtractor.execute(logsArgs);
    }

    private void extractResources(File destDir) {
        logger.info("The resource paths going to be extracted:");
        for (String s : requiredResources) {
            logger.info(s + "(required)");
        }

        try {
            KylinConfig srcConfig = KylinConfig.getInstanceFromEnv();
            KylinConfig dstConfig = KylinConfig.createInstanceFromUri(destDir.getAbsolutePath());
            new ResourceTool().copy(srcConfig, dstConfig, requiredResources);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract job resources. ", e);
        }
    }

    private void addRequired(String record) {
        logger.info("adding required resource {}", record);
        requiredResources.add(record);
    }
}
