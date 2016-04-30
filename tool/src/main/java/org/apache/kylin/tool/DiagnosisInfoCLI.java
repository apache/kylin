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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiagnosisInfoCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosisInfoCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg().isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.").create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_HBASE = OptionBuilder.withArgName("includeHBase").hasArg().isRequired(false).withDescription("Specify whether to include hbase files to extract. Default true.").create("includeHBase");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.withArgName("includeClient").hasArg().isRequired(false).withDescription("Specify whether to include client info to extract. Default true.").create("includeClient");

    public DiagnosisInfoCLI() {
        super();

        packageType = "project";

        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_INCLUDE_CONF);
        options.addOption(OPTION_INCLUDE_HBASE);
        options.addOption(OPTION_INCLUDE_CLIENT);
    }

    public static void main(String args[]) {
        DiagnosisInfoCLI diagnosisInfoCLI = new DiagnosisInfoCLI();
        diagnosisInfoCLI.execute(args);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws IOException {
        final String project = optionsHelper.getOptionValue(options.getOption("project"));
        boolean includeConf = optionsHelper.hasOption(OPTION_INCLUDE_CONF) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CONF)) : true;
        boolean includeHBase = optionsHelper.hasOption(OPTION_INCLUDE_HBASE) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_HBASE)) : true;
        boolean includeClient = optionsHelper.hasOption(OPTION_INCLUDE_CLIENT) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CLIENT)) : true;

        // export cube metadata
        String[] cubeMetaArgs = { "-destDir", new File(exportDir, "metadata").getAbsolutePath(), "-project", project, "-compress", "false", "-submodule", "true" };
        CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
        cubeMetaExtractor.execute(cubeMetaArgs);

        // export HBase
        if (includeHBase) {
            String[] hbaseArgs = { "-destDir", new File(exportDir, "hbase").getAbsolutePath(), "-project", project, "-compress", "false", "-submodule", "true" };
            HBaseUsageExtractor hBaseUsageExtractor = new HBaseUsageExtractor();
            hBaseUsageExtractor.execute(hbaseArgs);
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

        // export client
        if (includeClient) {
            String[] clientArgs = { "-destDir", new File(exportDir, "client").getAbsolutePath(), "-compress", "false", "-submodule", "true" };
            ClientEnvExtractor clientEnvExtractor = new ClientEnvExtractor();
            clientEnvExtractor.execute(clientArgs);
        }

        // export commit id
        try {
            FileUtils.copyFileToDirectory(new File(KylinConfig.getKylinHome(), "commit_SHA1"), exportDir);
        } catch (Exception e) {
            logger.warn("Error in export commit id.", e);
        }

        // export logs
        String[] logsArgs = { "-destDir", new File(exportDir, "logs").getAbsolutePath(), "-compress", "false", "-submodule", "true" };
        KylinLogExtractor logExtractor = new KylinLogExtractor();
        logExtractor.execute(logsArgs);
    }
}
