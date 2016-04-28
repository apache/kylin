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
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class JobInfoExtractor extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(JobInfoExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_JOB_ID = OptionBuilder.withArgName("jobId").hasArg().isRequired(true).withDescription("specify the Job ID to extract information. ").create("jobId");

    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(true).withDescription("specify the dest dir to save the related information").create("destDir");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CUBE = OptionBuilder.withArgName("includeCube").hasArg().isRequired(false).withDescription("set this to true if want to extract related cube info too. Default true").create("includeCube");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_YARN_LOGS = OptionBuilder.withArgName("includeYarnLogs").hasArg().isRequired(false).withDescription("set this to true if want to extract related yarn logs too. Default true").create("includeYarnLogs");

    private Options options;

    private KylinConfig kylinConfig;
    private CubeMetaExtractor cubeMetaExtractor;

    private ExecutableDao executableDao;

    List<String> requiredResources = Lists.newArrayList();
    List<String> yarnLogsResources = Lists.newArrayList();

    public JobInfoExtractor() {
        cubeMetaExtractor = new CubeMetaExtractor();

        options = new Options();
        options.addOption(OPTION_JOB_ID);
        options.addOption(OPTION_DEST);
        options.addOption(OPTION_INCLUDE_CUBE);
        options.addOption(OPTION_INCLUDE_YARN_LOGS);

        kylinConfig = KylinConfig.getInstanceFromEnv();
        executableDao = ExecutableDao.getInstance(kylinConfig);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);
        String dest = optionsHelper.getOptionValue(OPTION_DEST);
        boolean includeCube = optionsHelper.hasOption(OPTION_INCLUDE_CUBE) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CUBE)) : true;
        boolean includeYarnLogs = optionsHelper.hasOption(OPTION_INCLUDE_YARN_LOGS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_YARN_LOGS)) : true;

        if (StringUtils.isEmpty(dest)) {
            throw new RuntimeException("destDir is not set, exit directly without extracting");
        }

        if (!dest.endsWith("/")) {
            dest = dest + "/";
        }

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
        executeExtraction(dest);

        if (includeCube) {
            String cubeName = executablePO.getParams().get("cubename");
            String[] cubeMetaArgs = { "-cube", cubeName, "-destDir", dest + "cube_" + cubeName + "/", "-includeJobs", "false" };
            logger.info("Start to extract related cube: " + StringUtils.join(cubeMetaArgs));
            cubeMetaExtractor.execute(cubeMetaArgs);
        }

        if (includeYarnLogs) {
            logger.info("Start to related yarn job logs: " + jobId);
            for (String taskId : yarnLogsResources) {
                extractYarnLog(taskId, dest + "yarn_" + jobId + "/", true);
            }
        }

        logger.info("Extracted kylin jobs located at: " + new File(dest).getAbsolutePath());
    }

    private void executeExtraction(String dest) {
        logger.info("The resource paths going to be extracted:");
        for (String s : requiredResources) {
            logger.info(s + "(required)");
        }

        try {
            ResourceStore src = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
            ResourceStore dst = ResourceStore.getStore(KylinConfig.createInstanceFromUri(dest));

            for (String path : requiredResources) {
                ResourceTool.copyR(src, dst, path);
            }

        } catch (IOException e) {
            throw new RuntimeException("IOException", e);
        }
    }

    private void extractYarnLog(String taskId, String dest, boolean onlySucc) throws Exception {
        final Map<String, String> jobInfo = executableDao.getJobOutput(taskId).getInfo();
        if (jobInfo.containsKey(ExecutableConstants.MR_JOB_ID)) {
            String applicationId = jobInfo.get(ExecutableConstants.MR_JOB_ID).replace("job", "application");
            if (!onlySucc || isYarnAppSucc(applicationId)) {
                File destFile = new File(dest + applicationId + ".log");

                String yarnCmd = "yarn logs -applicationId " + applicationId + " > " + destFile.getAbsolutePath();
                logger.info(yarnCmd);
                kylinConfig.getCliCommandExecutor().execute(yarnCmd);
            }
        }
    }

    private boolean isYarnAppSucc(String applicationId) throws IOException {
        final String yarnCmd = "yarn application -status " + applicationId;
        final String cmdOutput = kylinConfig.getCliCommandExecutor().execute(yarnCmd).getSecond();
        final String[] cmdOutputLines = cmdOutput.split("\n");
        for (String cmdOutputLine : cmdOutputLines) {
            if (cmdOutputLine.equals("Final-State : SUCCEEDED")) {
                return true;
            }
        }
        return false;
    }

    private void addRequired(String record) {
        logger.info("adding required resource {}", record);
        requiredResources.add(record);
    }

    public static void main(String args[]) {
        JobInfoExtractor extractor = new JobInfoExtractor();
        extractor.execute(args);
    }
}
