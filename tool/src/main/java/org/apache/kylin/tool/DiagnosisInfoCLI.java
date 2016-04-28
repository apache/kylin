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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class DiagnosisInfoCLI extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosisInfoCLI.class);

    private static final int DEFAULT_LOG_PERIOD = 3;

    @SuppressWarnings("static-access")
    private static final Option OPTION_LOG_PERIOD = OptionBuilder.withArgName("logPeriod").hasArg().isRequired(false).withDescription("specify how many days of kylin logs to extract. Default 3.").create("logPeriod");

    @SuppressWarnings("static-access")
    private static final Option OPTION_COMPRESS = OptionBuilder.withArgName("compress").hasArg().isRequired(false).withDescription("specify whether to compress the output with zip. Default true.").create("compress");

    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(true).withDescription("specify the dest dir to save the related metadata").create("destDir");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.withArgName("includeConf").hasArg().isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.").create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_HBASE = OptionBuilder.withArgName("includeHBase").hasArg().isRequired(false).withDescription("Specify whether to include hbase files to extract. Default true.").create("includeHBase");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_LINUX = OptionBuilder.withArgName("includeLinux").hasArg().isRequired(false).withDescription("Specify whether to include os and linux kernel info to extract. Default true.").create("includeLinux");

    private CubeMetaExtractor cubeMetaExtractor;
    private HBaseUsageExtractor hBaseUsageExtractor;
    private KylinConfig kylinConfig;
    private Options options;
    private String exportDest;

    public DiagnosisInfoCLI() {
        cubeMetaExtractor = new CubeMetaExtractor();
        hBaseUsageExtractor = new HBaseUsageExtractor();
        kylinConfig = KylinConfig.getInstanceFromEnv();

        options = new Options();
        options.addOption(OPTION_LOG_PERIOD);
        options.addOption(OPTION_COMPRESS);
        options.addOption(OPTION_DEST);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_INCLUDE_CONF);
    }

    public static void main(String args[]) {
        DiagnosisInfoCLI diagnosisInfoCLI = new DiagnosisInfoCLI();
        diagnosisInfoCLI.execute(args);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String project = optionsHelper.getOptionValue(options.getOption("project"));
        exportDest = optionsHelper.getOptionValue(options.getOption("destDir"));

        if (StringUtils.isEmpty(exportDest)) {
            throw new RuntimeException("destDir is not set, exit directly without extracting");
        }
        if (!exportDest.endsWith("/")) {
            exportDest = exportDest + "/";
        }

        // create new folder to contain the output
        String packageName = "diagnosis_" + new SimpleDateFormat("YYYY_MM_dd_HH_mm_ss").format(new Date());
        if (new File(exportDest).exists()) {
            exportDest = exportDest + packageName + "/";
        }
        File exportDir = new File(exportDest);

        // export cube metadata
        String[] cubeMetaArgs = { "-destDir", exportDest + "metadata", "-project", project };
        cubeMetaExtractor.execute(cubeMetaArgs);

        int logPeriod = optionsHelper.hasOption(OPTION_LOG_PERIOD) ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_LOG_PERIOD)) : DEFAULT_LOG_PERIOD;
        boolean compress = optionsHelper.hasOption(OPTION_COMPRESS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_COMPRESS)) : true;
        boolean includeConf = optionsHelper.hasOption(OPTION_INCLUDE_CONF) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_CONF)) : true;
        boolean includeHBase = optionsHelper.hasOption(OPTION_INCLUDE_HBASE) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_HBASE)) : true;
        boolean includeLinux = optionsHelper.hasOption(OPTION_INCLUDE_LINUX) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_INCLUDE_LINUX)) : true;

        // export HBase
        if (includeHBase) {
            String[] hbaseArgs = { "-destDir", exportDest + "hbase", "-project", project };
            hBaseUsageExtractor.execute(hbaseArgs);
        }

        // export conf
        if (includeConf) {
            logger.info("Start to extract kylin conf files.");
            try {
                FileUtils.copyDirectoryToDirectory(new File(getConfFolder()), exportDir);
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
            FileUtils.writeStringToFile(new File(basicDir, "client"), output);
            output = getHBaseMetaStoreId();
            FileUtils.writeStringToFile(new File(basicDir, "client"), output, true);

        } catch (Exception e) {
            logger.warn("Error in export process info.", e);
        }

        // export logs
        if (logPeriod > 0) {
            logger.info("Start to extract kylin logs in {} days", logPeriod);

            final File kylinLogDir = new File(KylinConfig.getKylinHome(), "logs");
            final File exportLogsDir = new File(exportDir, "logs");
            final ArrayList<File> logFiles = Lists.newArrayList();
            final long logThresholdTime = System.currentTimeMillis() - logPeriod * 24 * 3600 * 1000;

            FileUtils.forceMkdir(exportLogsDir);
            for (File logFile : kylinLogDir.listFiles()) {
                if (logFile.lastModified() > logThresholdTime) {
                    logFiles.add(logFile);
                }
            }

            for (File logFile : logFiles) {
                logger.info("Log file:" + logFile.getAbsolutePath());
                if (logFile.exists()) {
                    FileUtils.copyFileToDirectory(logFile, exportLogsDir);
                }
            }
        }

        // compress to zip package
        if (compress) {
            File tempZipFile = File.createTempFile("diagnosis_", ".zip");
            ZipFileUtils.compressZipFile(exportDir.getAbsolutePath(), tempZipFile.getAbsolutePath());
            FileUtils.cleanDirectory(exportDir);

            File zipFile = new File(exportDir, packageName + ".zip");
            FileUtils.moveFile(tempZipFile, zipFile);
            exportDest = zipFile.getAbsolutePath();
            exportDir = new File(exportDest);
        }

        StringBuffer output = new StringBuffer();
        output.append("\n========================================");
        output.append("\nDiagnosis package locates at: \n" + exportDir.getAbsolutePath());
        output.append("\n========================================");
        logger.info(output.toString());
    }

    public String getExportDest() {
        return exportDest;
    }

    private String getConfFolder() {
        String path = System.getProperty(KylinConfig.KYLIN_CONF);
        if (StringUtils.isNotEmpty(path)) {
            return path;
        }
        path = KylinConfig.getKylinHome();
        if (StringUtils.isNotEmpty(path)) {
            return path + File.separator + "conf";
        }
        return null;
    }

    private String getHBaseMetaStoreId() throws IOException {
        HBaseAdmin hbaseAdmin = new HBaseAdmin(HBaseConfiguration.create(HadoopUtil.getCurrentConfiguration()));
        String metaStoreName = kylinConfig.getMetadataUrlPrefix();
        HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(metaStoreName));
        return "MetaStore UUID: " + desc.getValue(HBaseConnection.HTABLE_UUID_TAG);
    }
}
