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

package org.apache.kylin.admin;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 3/29/16.
 */
public class YarnLogExtractor extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(YarnLogExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_JOB_ID = OptionBuilder.withArgName("jobId").hasArg().isRequired(true).withDescription("specify the Job ID to extract information. ").create("jobId");

    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(true).withDescription("specify the dest dir to save the related information").create("destDir");

    private Options options;

    private KylinConfig kylinConfig;
    private ExecutableDao executableDao;

    List<String> requiredResources = Lists.newArrayList();

    public YarnLogExtractor() {
        options = new Options();
        options.addOption(OPTION_JOB_ID);
        options.addOption(OPTION_DEST);

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

        if (StringUtils.isEmpty(dest)) {
            throw new RuntimeException("destDir is not set, exit directly without extracting");
        }

        if (!dest.endsWith("/")) {
            dest = dest + "/";
        }

        ExecutablePO executablePO = executableDao.getJob(jobId);
        for (ExecutablePO task : executablePO.getTasks()) {
            addRequired(task.getUuid());
        }
        executeExtraction(dest);

        logger.info("Extracted yarn logs located at: " + new File(dest).getAbsolutePath());
    }

    private void extractYarnLog(String taskId, String dest) throws Exception {
        final Map<String, String> jobInfo = executableDao.getJobOutput(taskId).getInfo();
        if (jobInfo.containsKey(ExecutableConstants.MR_JOB_ID)) {
            String applicationId = jobInfo.get(ExecutableConstants.MR_JOB_ID).replace("job", "application");
            File destFile = new File(dest + applicationId + ".log");

            ShellExecutable yarnExec = new ShellExecutable();
            yarnExec.setCmd("yarn logs -applicationId " + applicationId + " > " + destFile.getAbsolutePath());
            yarnExec.setName(yarnExec.getCmd());

            logger.info(yarnExec.getCmd());
            kylinConfig.getCliCommandExecutor().execute(yarnExec.getCmd(), null);
        }
    }

    private void executeExtraction(String dest) throws Exception {
        logger.info("The resource paths going to be extracted:");
        for (String taskId : requiredResources) {
            logger.info(taskId + "(required)");
        }

        logger.info("Start to download yarn logs.");
        FileUtils.forceMkdir(new File(dest));
        for (String taskId : requiredResources) {
            extractYarnLog(taskId, dest);
        }
    }

    private void addRequired(String record) {
        logger.info("adding required resource {}", record);
        requiredResources.add(record);
    }

    public static void main(String args[]) {
        YarnLogExtractor extractor = new YarnLogExtractor();
        extractor.execute(args);
    }
}
