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
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class DiagnosisInfoCLI extends AbstractInfoExtractor {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosisInfoCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg().isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.").create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_HBASE = OptionBuilder.withArgName("includeHBase").hasArg().isRequired(false).withDescription("Specify whether to include hbase files to extract. Default true.").create("includeHBase");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_LINUX = OptionBuilder.withArgName("includeLinux").hasArg().isRequired(false).withDescription("Specify whether to include os and linux kernel info to extract. Default true.").create("includeLinux");

    private KylinConfig kylinConfig;

    public DiagnosisInfoCLI() {
        super();

        packagePrefix = "diagnosis";
        kylinConfig = KylinConfig.getInstanceFromEnv();

        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_INCLUDE_CONF);
        options.addOption(OPTION_INCLUDE_HBASE);
        options.addOption(OPTION_INCLUDE_LINUX);
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
        boolean includeLinux = optionsHelper.hasOption(OPTION_INCLUDE_LINUX) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_LINUX)) : true;

        // export cube metadata
        String[] cubeMetaArgs = { "-destDir", new File(exportDir, "metadata").getAbsolutePath(), "-project", project, "-compress", "false", "-quiet", "false" };
        CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
        cubeMetaExtractor.execute(cubeMetaArgs);

        // export HBase
        if (includeHBase) {
            String[] hbaseArgs = { "-destDir", new File(exportDir, "hbase").getAbsolutePath(), "-project", project, "-compress", "false", "-quiet", "false" };
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

        // export os (linux)
        if (includeLinux) {
            File linuxDir = new File(exportDir, "linux");
            FileUtils.forceMkdir(linuxDir);
            File transparentHugepageCompactionDir = new File(linuxDir, "transparent_hugepage");
            FileUtils.forceMkdir(transparentHugepageCompactionDir);
            File vmSwappinessDir = new File(linuxDir, "vm.swappiness");
            FileUtils.forceMkdir(vmSwappinessDir);
            try {
                String transparentHugepageCompactionPath = "/sys/kernel/mm/transparent_hugepage/defrag";
                Files.copy(new File(transparentHugepageCompactionPath), new File(transparentHugepageCompactionDir, "defrag"));
            } catch (Exception e) {
                logger.warn("Error in export transparent hugepage compaction status.", e);
            }

            try {
                String vmSwapinessPath = "/proc/sys/vm/swappiness";
                Files.copy(new File(vmSwapinessPath), new File(vmSwappinessDir, "swappiness"));
            } catch (Exception e) {
                logger.warn("Error in export vm swapiness.", e);
            }
        }

        // export commit id
        try {
            FileUtils.copyFileToDirectory(new File(KylinConfig.getKylinHome(), "commit_SHA1"), exportDir);
        } catch (Exception e) {
            logger.warn("Error in export commit id.", e);
        }

        // export basic info
        try {
            File basicDir = new File(exportDir, "basic");
            FileUtils.forceMkdir(basicDir);
            String output = kylinConfig.getCliCommandExecutor().execute("ps -ef|grep kylin").getSecond();
            FileUtils.writeStringToFile(new File(basicDir, "process"), output);
            output = kylinConfig.getCliCommandExecutor().execute("lsb_release -a").getSecond();
            FileUtils.writeStringToFile(new File(basicDir, "lsb_release"), output);
            output = KylinVersion.getKylinClientInformation();
            FileUtils.writeStringToFile(new File(basicDir, "client"), output + "\n");
            output = ToolUtil.getHBaseMetaStoreId();
            FileUtils.writeStringToFile(new File(basicDir, "client"), output, true);

        } catch (Exception e) {
            logger.warn("Error in export process info.", e);
        }

        // export logs
        String[] logsArgs = { "-destDir", new File(exportDir, "logs").getAbsolutePath(), "-compress", "false", "-quiet", "false" };
        KylinLogExtractor logExtractor = new KylinLogExtractor();
        logExtractor.execute(logsArgs);
    }
}
