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
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JobDiagnosisInfoCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(JobDiagnosisInfoCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_JOB_ID = OptionBuilder.withArgName("jobId").hasArg().isRequired(true).withDescription("specify the Job ID to extract information. ").create("jobId");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CUBE = OptionBuilder.withArgName("includeCube").hasArg().isRequired(false).withDescription("set this to true if want to extract related cube info too. Default true").create("includeCube");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_YARN_LOGS = OptionBuilder.withArgName("includeYarnLogs").hasArg().isRequired(false).withDescription("set this to true if want to extract related yarn logs too. Default true").create("includeYarnLogs");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.withArgName("includeClient").hasArg().isRequired(false).withDescription("Specify whether to include client info to extract. Default true.").create("includeClient");

    private KylinConfig kylinConfig;
    private ExecutableDao executableDao;

    List<String> requiredResources = Lists.newArrayList();
    List<String> yarnLogsResources = Lists.newArrayList();

    public JobDiagnosisInfoCLI() {
        super();

        packageType = "job";

        options.addOption(OPTION_JOB_ID);
        options.addOption(OPTION_INCLUDE_CUBE);
        options.addOption(OPTION_INCLUDE_CLIENT);
        options.addOption(OPTION_INCLUDE_YARN_LOGS);

        kylinConfig = KylinConfig.getInstanceFromEnv();
        executableDao = ExecutableDao.getInstance(kylinConfig);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        String jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);
        boolean includeCube = optionsHelper.hasOption(OPTION_INCLUDE_CUBE) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CUBE)) : true;
        boolean includeYarnLogs = optionsHelper.hasOption(OPTION_INCLUDE_YARN_LOGS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_YARN_LOGS)) : true;
        boolean includeClient = optionsHelper.hasOption(OPTION_INCLUDE_CLIENT) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CLIENT)) : true;

        // dump job output
        ExecutablePO executablePO = executableDao.getJob(jobId);
        addRequired(ExecutableDao.pathOfJob(jobId));
        addRequired(ExecutableDao.pathOfJobOutput(jobId));
        for (ExecutablePO task : executablePO.getTasks()) {
            addRequired(ExecutableDao.pathOfJob(task.getUuid()));
            addRequired(ExecutableDao.pathOfJobOutput(task.getUuid()));
            if (includeYarnLogs) {
                yarnLogsResources.add(task.getUuid());
            }
        }
        extractResources(exportDir);

        // dump cube metadata
        if (includeCube) {
            String cubeName = executablePO.getParams().get("cubeName");
            if (!StringUtils.isEmpty(cubeName)) {
                File metaDir = new File(exportDir, "cube");
                FileUtils.forceMkdir(metaDir);
                String[] cubeMetaArgs = { "-cube", cubeName, "-destDir", new File(metaDir, cubeName).getAbsolutePath(), "-includeJobs", "false", "-compress", "false", "-submodule", "true" };

                logger.info("Start to extract related cube: " + StringUtils.join(cubeMetaArgs));
                CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
                cubeMetaExtractor.execute(cubeMetaArgs);
            }
        }

        // dump yarn logs
        if (includeYarnLogs) {
            logger.info("Start to related yarn job logs: " + jobId);
            File yarnLogDir = new File(exportDir, "yarn");
            FileUtils.forceMkdir(yarnLogDir);
            for (String taskId : yarnLogsResources) {
                extractYarnLog(taskId, new File(yarnLogDir, jobId), true);
            }
        }

        if (includeClient) {
            String[] clientArgs = { "-destDir", new File(exportDir, "client").getAbsolutePath(), "-compress", "false", "-submodule", "true" };
            ClientEnvExtractor clientEnvExtractor = new ClientEnvExtractor();
            clientEnvExtractor.execute(clientArgs);
        }

        // export kylin logs
        String[] logsArgs = { "-destDir", new File(exportDir, "logs").getAbsolutePath(), "-compress", "false", "-submodule", "true" };
        KylinLogExtractor logExtractor = new KylinLogExtractor();
        logExtractor.execute(logsArgs);
    }

    private void extractResources(File destDir) {
        logger.info("The resource paths going to be extracted:");
        for (String s : requiredResources) {
            logger.info(s + "(required)");
        }

        try {
            ResourceStore src = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
            ResourceStore dst = ResourceStore.getStore(KylinConfig.createInstanceFromUri(destDir.getAbsolutePath()));

            for (String path : requiredResources) {
                ResourceTool.copyR(src, dst, path);
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to extract job resources. ", e);
        }
    }

    private void extractYarnLog(String taskId, File destDir, boolean onlyFail) throws Exception {
        final Map<String, String> jobInfo = executableDao.getJobOutput(taskId).getInfo();
        FileUtils.forceMkdir(destDir);
        if (jobInfo.containsKey(ExecutableConstants.MR_JOB_ID)) {
            String applicationId = jobInfo.get(ExecutableConstants.MR_JOB_ID).replace("job", "application");
            if (!onlyFail || !isYarnAppSucc(applicationId)) {
                File destFile = new File(destDir, applicationId + ".log");
                String yarnCmd = "yarn logs -applicationId " + applicationId + " > " + destFile.getAbsolutePath();
                logger.debug(yarnCmd);
                kylinConfig.getCliCommandExecutor().execute(yarnCmd);
            }
        }
    }

    private boolean isYarnAppSucc(String applicationId) throws IOException {
        final String yarnCmd = "yarn application -status " + applicationId;
        final String cmdOutput = kylinConfig.getCliCommandExecutor().execute(yarnCmd).getSecond();
        final Map<String, String> params = Maps.newHashMap();
        final String[] cmdOutputLines = cmdOutput.split("\n");
        for (String cmdOutputLine : cmdOutputLines) {
            String[] pair = cmdOutputLine.split(":");
            params.put(pair[0].trim(), pair[1].trim());
        }
        for (Map.Entry<String, String> e : params.entrySet()) {
            logger.info(e.getKey() + ":" + e.getValue());
        }
        if (params.containsKey("Final-State") && params.get("Final-State").equals("SUCCEEDED")) {
            return true;
        }

        return false;
    }

    private void addRequired(String record) {
        logger.info("adding required resource {}", record);
        requiredResources.add(record);
    }

    public static void main(String args[]) {
        JobDiagnosisInfoCLI extractor = new JobDiagnosisInfoCLI();
        extractor.execute(args);
    }
}
