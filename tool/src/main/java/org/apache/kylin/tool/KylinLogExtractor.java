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

package org.apache.kylin.tool;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class KylinLogExtractor extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(KylinLogExtractor.class);

    private static final int DEFAULT_LOG_PERIOD = 3;

    @SuppressWarnings("static-access")
    private static final Option OPTION_LOG_PERIOD = OptionBuilder.withArgName("logPeriod").hasArg().isRequired(false).withDescription("specify how many days of kylin logs to extract. Default " + DEFAULT_LOG_PERIOD + ".").create("logPeriod");

    KylinConfig config;

    public KylinLogExtractor() {
        super();

        packageType = "logs";
        options.addOption(OPTION_LOG_PERIOD);

        config = KylinConfig.getInstanceFromEnv();
    }

    private void beforeExtract() {
        // reload metadata before extract diagnosis info
        logger.info("Start to reload metadata from diagnosis.");

        CubeManager.clearCache();
        CubeManager.getInstance(config);
        CubeDescManager.clearCache();
        CubeDescManager.getInstance(config);
        MetadataManager.clearCache();
        MetadataManager.getInstance(config);
        ProjectManager.clearCache();
        ProjectManager.getInstance(config);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        beforeExtract();

        int logPeriod = optionsHelper.hasOption(OPTION_LOG_PERIOD) ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_LOG_PERIOD)) : DEFAULT_LOG_PERIOD;

        if (logPeriod < 1) {
            logger.warn("No logs to extract.");
            return;
        }

        logger.info("Start to extract kylin logs in {} days", logPeriod);

        List<File> logDirs = Lists.newArrayList();
        logDirs.add(new File(KylinConfig.getKylinHome(), "logs"));
        String kylinVersion = ToolUtil.decideKylinMajorVersionFromCommitFile();
        if (kylinVersion != null && kylinVersion.equals("1.3")) {
            logDirs.add(new File(KylinConfig.getKylinHome(), "tomcat/logs"));
        }

        final ArrayList<File> requiredLogFiles = Lists.newArrayList();
        final long logThresholdTime = System.currentTimeMillis() - logPeriod * 24 * 3600 * 1000;

        for (File kylinLogDir : logDirs) {
            final File[] allLogFiles = kylinLogDir.listFiles();
            if (allLogFiles == null || allLogFiles.length == 0) {
                return;
            }

            for (File logFile : allLogFiles) {
                if (logFile.lastModified() > logThresholdTime) {
                    requiredLogFiles.add(logFile);
                }
            }
        }

        for (File logFile : requiredLogFiles) {
            logger.info("Log file:" + logFile.getAbsolutePath());
            if (logFile.exists()) {
                String cmd = String.format("cp %s %s", logFile.getAbsolutePath(), exportDir.getAbsolutePath());
                config.getCliCommandExecutor().execute(cmd);
            }
        }
    }
}
