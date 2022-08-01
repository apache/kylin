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
import java.util.Locale;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class YarnApplicationTool extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DESTINATION_DIR")
            .withDescription("Specify the file to save yarn application id").isRequired(true).create("dir");

    private static final Option OPTION_JOB = OptionBuilder.getInstance().hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job").isRequired(true).create("job");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("OPTION_PROJECT")
            .withDescription("Specify project").isRequired(true).create("project");

    private final Options options;

    private final KylinConfig kylinConfig;

    YarnApplicationTool() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public YarnApplicationTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.options = new Options();
        initOptions();
    }

    public static void main(String[] args) {
        val tool = new YarnApplicationTool();
        tool.execute(args);
        System.out.println("Yarn application task finished.");
        Unsafe.systemExit(0);
    }

    private void initOptions() {
        options.addOption(OPTION_JOB);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_DIR);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String dir = optionsHelper.getOptionValue(OPTION_DIR);
        val jobId = optionsHelper.getOptionValue(OPTION_JOB);
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        AbstractExecutable job = NExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
        if (job instanceof ChainedExecutable) {
            FileUtils.writeLines(new File(dir), extract(project, jobId));
        }
    }

    private Set<String> extract(String project, String jobId) {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getYarnApplicationJobs(jobId);
    }

    public void extractYarnLogs(File exportDir, String project, String jobId) {
        try {
            File yarnLogsDir = new File(exportDir, "yarn_application_log");
            FileUtils.forceMkdir(yarnLogsDir);

            AbstractExecutable job = NExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
            if (!(job instanceof ChainedExecutable)) {
                logger.warn("job type is not ChainedExecutable!");
                return;
            }

            Set<String> applicationIdList = extract(project, jobId);
            if (CollectionUtils.isEmpty(applicationIdList)) {
                String message = "Yarn task submission failed, please check whether yarn is running normally.";
                logger.error(message);
                FileUtils.write(new File(yarnLogsDir, "failed_yarn_application.log"), message);
                return;
            }

            CliCommandExecutor cmdExecutor = new CliCommandExecutor();
            String cmd = "yarn logs -applicationId %s";
            for (String applicationId : applicationIdList) {
                try {
                    if (!applicationId.startsWith("application")) {
                        continue;
                    }
                    val result = cmdExecutor.execute(String.format(Locale.ROOT, cmd, applicationId), null);

                    if (result.getCode() != 0) {
                        logger.error("Failed to execute the yarn cmd: {}", cmd);
                    }

                    if (null != result.getCmd()) {
                        FileUtils.write(new File(yarnLogsDir, applicationId + ".log"), result.getCmd());
                    }
                } catch (ShellException se) {
                    logger.error("Failed to extract log by yarn job: {}", applicationId, se);
                    String detailMessage = se.getMessage()
                            + "\n For detailed error information, please see logs/diag.log or logs/kylin.log";
                    FileUtils.write(new File(yarnLogsDir, applicationId + ".log"), detailMessage);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract yarn job logs.", e);
        }
    }

}
