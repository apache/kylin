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
 *
 */

package org.apache.kylin.tool.extractor;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * http://hadoop.apache.org/docs/r2.7.3/hadoop-yarn/hadoop-yarn-site/YarnCommands.html#logs
 */
public class YarnLogExtractor {
    private static final Logger logger = LoggerFactory.getLogger(YarnLogExtractor.class);
    List<String> yarnLogsResources = Lists.newArrayList();
    private KylinConfig kylinConfig;
    private ExecutableDao executableDao;

    public void extractYarnLogAndMRJob(String jobId, File yarnLogDir) throws Exception {
        logger.info("Collecting Yarn logs and MR counters for the Job {}", jobId);
        kylinConfig = KylinConfig.getInstanceFromEnv();
        executableDao = ExecutableDao.getInstance(kylinConfig);
        ExecutablePO executablePO = null;
        executablePO = executableDao.getJob(jobId);

        if (executablePO == null) {
            logger.error("Can not find executablePO.");
            return;
        }

        for (ExecutablePO task : executablePO.getTasks()) {
            yarnLogsResources.add(task.getUuid());
        }

        for (String stepId : yarnLogsResources) {
            logger.info("Checking step {}", stepId);
            extractYarnLog(stepId, new File(yarnLogDir, stepId));
            extractMRJob(stepId, new File(yarnLogDir, stepId));
        }
    }


    protected void extractMRJob(String taskId, File destDir) {
        try {
            final Map<String, String> jobInfo = executableDao.getJobOutput(taskId).getInfo();
            String jobId = null;
            if (jobInfo.containsKey(ExecutableConstants.MR_JOB_ID)) {
                jobId = jobInfo.get(ExecutableConstants.MR_JOB_ID);
            } else if (taskId.endsWith("00")) {
                logger.info("Create Intermediate Flat Hive Table's taskId: " + taskId);
                final String jobContent = executableDao.getJobOutput(taskId).getContent();
                if (jobContent != null) {
                    String applicationId = extractApplicationId(jobContent);
                    if (applicationId != null) {
                        jobId = applicationId.replace("application", "job");
                        logger.info("jobId is: " + jobId);
                    }
                }
            }

            if (jobId != null) {
                FileUtils.forceMkdir(destDir);
                String[] mrJobArgs = {"-mrJobId", jobId, "-destDir", destDir.getAbsolutePath(), "-compress", "false",
                        "-submodule", "true"};
                new MrJobInfoExtractor().execute(mrJobArgs);
            }

        } catch (Exception e) {
            logger.error("Failed to extract MRJob .", e);
        }

    }

    protected void extractYarnLog(String taskId, File destDir) {
        try {
            final Map<String, String> jobInfo = executableDao.getJobOutput(taskId).getInfo();
            FileUtils.forceMkdir(destDir);
            String appId = null;
            if (jobInfo.containsKey(ExecutableConstants.MR_JOB_ID)) {
                appId = jobInfo.get(ExecutableConstants.MR_JOB_ID).replace("job", "application");
            } else if (jobInfo.containsKey(ExecutableConstants.SPARK_JOB_ID)) {
                appId = jobInfo.get(ExecutableConstants.SPARK_JOB_ID);
            }

            if (appId != null) {
                String applicationId = jobInfo.get(ExecutableConstants.MR_JOB_ID).replace("job", "application");
                extractYarnLogByApplicationId(applicationId, destDir);
            } else if (taskId.endsWith("00")) {
                extractFlatStepInfo(taskId, destDir);
            }
        } catch (Exception e) {
            logger.error("Failed to extract yarn log.", e);
        }

    }

    private void extractFlatStepInfo(String taskId, File destDir) {
        try {
            logger.info("Create Intermediate Flat Hive Table's taskId: " + taskId);
            final String jobContent = executableDao.getJobOutput(taskId).getContent();
            if (jobContent != null) {
                String applicationId = extractApplicationId(jobContent);

                logger.info("applicationId is: " + applicationId);
                if (applicationId != null && applicationId.startsWith("application")) {
                    logger.info("Create Intermediate Flat Hive Table's applicationId: " + applicationId);
                    extractYarnLogByApplicationId(applicationId, destDir);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract FlatStepInfo.", e);
        }
    }

    private String extractApplicationId(String jobContent) {
        Matcher matcher = Pattern.compile("application_[0-9]+[_][0-9]+").matcher(jobContent);

        if (matcher.find()) {
            return matcher.group(0);
        }
        return null;
    }

    private void extractYarnLogByApplicationId(String applicationId, File destDir) throws Exception {
        if (shouldDoLogCollection(applicationId, kylinConfig)) {
            File destFile = new File(destDir, applicationId + ".log");
            String yarnCmd = "yarn logs -applicationId " + applicationId + " > " + destFile.getAbsolutePath();
            logger.info(yarnCmd);
            try {
                kylinConfig.getCliCommandExecutor().execute(yarnCmd);
            } catch (Exception ex) {
                logger.warn("Failed to get yarn logs. ", ex);
            }
        } else {
            logger.info("Skip this application {}.", applicationId);
        }
    }

    /**
     * The log of application which is finished & failed should be collected
     */
    public static boolean shouldDoLogCollection(String applicationId, KylinConfig kylinConfig) throws IOException {
        final String yarnCmd = "yarn application -status " + applicationId;
        final String cmdOutput = kylinConfig.getCliCommandExecutor().execute(yarnCmd).getSecond();
        final Map<String, String> params = Maps.newHashMap();
        final String[] cmdOutputLines = cmdOutput.split("\n");
        for (String cmdOutputLine : cmdOutputLines) {
            String[] pair = cmdOutputLine.split(":");
            if (pair.length >= 2) {
                params.put(pair[0].trim(), pair[1].trim());
            }
        }
        for (Map.Entry<String, String> e : params.entrySet()) {
            logger.info("Status of {}  {} : {}", applicationId, e.getKey(), e.getValue());
        }

        // Skip running application because log agg is not completed
        if (params.containsKey("State") && params.get("State").equals("RUNNING")) {
            return false;
        }
        // Skip succeed application
        return params.containsKey("Final-State") && !params.get("Final-State").equals("SUCCEEDED");
    }
}
