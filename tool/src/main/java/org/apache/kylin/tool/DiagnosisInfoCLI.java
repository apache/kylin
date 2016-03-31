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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ZipFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class DiagnosisInfoCLI extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosisInfoCLI.class);

    private static final int DEFAULT_LOG_PERIOD = 3;

    @SuppressWarnings("static-access")
    private static final Option OPTION_LOG_PERIOD = OptionBuilder.withArgName("logPeriod").hasArg().isRequired(false).withDescription("specify how many days of kylin logs to extract. Default 3.").create("logPeriod");

    @SuppressWarnings("static-access")
    private static final Option OPTION_COMPRESS = OptionBuilder.withArgName("compress").hasArg().isRequired(false).withDescription("specify whether to compress the output with zip. Default false.").create("compress");

    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(true).withDescription("specify the dest dir to save the related metadata").create("destDir");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false).withDescription("Specify realizations in which project to extract").create("project");

    private CubeMetaExtractor cubeMetaExtractor;
    private Options options;
    private String exportDest;

    public DiagnosisInfoCLI() {
        cubeMetaExtractor = new CubeMetaExtractor();

        options = new Options();
        options.addOption(OPTION_LOG_PERIOD);
        options.addOption(OPTION_COMPRESS);
        options.addOption(OPTION_DEST);
        options.addOption(OPTION_PROJECT);
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

        // export cube metadata
        String[] cubeMetaArgs = { "-destDir", exportDest + File.pathSeparator, "-project", project };
        cubeMetaExtractor.execute(cubeMetaArgs);

        int logPeriod = optionsHelper.hasOption(OPTION_LOG_PERIOD) ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_LOG_PERIOD)) : DEFAULT_LOG_PERIOD;
        boolean compress = optionsHelper.hasOption(OPTION_COMPRESS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_COMPRESS)) : false;

        // export logs
        if (logPeriod > 0) {
            logger.info("Start to extract kylin logs in {} days", logPeriod);

            final String logFolder = KylinConfig.getKylinHome() + "/logs/";
            final String defaultLogFilename = "kylin.log";
            final File logsDir = new File(exportDest + "/logs/");
            final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

            FileUtils.forceMkdir(logsDir);

            final ArrayList<String> logFileNames = Lists.newArrayListWithCapacity(logPeriod);

            logFileNames.add(defaultLogFilename);
            for (int i = 1; i < logPeriod; i++) {
                Calendar todayCal = Calendar.getInstance();
                todayCal.add(Calendar.DAY_OF_MONTH, 0 - i);
                logFileNames.add(defaultLogFilename + "." + format.format(todayCal.getTime()));
            }

            for (String logFilename : logFileNames) {
                File logFile = new File(logFolder + logFilename);
                if (logFile.exists()) {
                    FileUtils.copyFileToDirectory(logFile, logsDir);
                }
            }
        }

        // compress to zip package
        if (compress) {
            File tempZipFile = File.createTempFile("diagnosis_", ".zip");
            ZipFileUtils.compressZipFile(exportDest, tempZipFile.getAbsolutePath());
            FileUtils.forceDelete(new File(exportDest));
            FileUtils.moveFileToDirectory(tempZipFile, new File(exportDest), true);
            exportDest = exportDest + tempZipFile.getName();
        }
        logger.info("Diagnosis info locates at: " + new File(exportDest).getAbsolutePath());
    }

    public String getExportDest() {
        return exportDest;
    }
}
