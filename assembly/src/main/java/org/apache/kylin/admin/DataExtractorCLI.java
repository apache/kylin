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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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

public class DataExtractorCLI extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(DataExtractorCLI.class);

    private static final int DEFAULT_LOG_PERIOD = 3;

    @SuppressWarnings("static-access")
    private static final Option OPTION_LOG_PERIOD = OptionBuilder.withArgName("logPeriod").hasArg().isRequired(false).withDescription("specify how many days of kylin logs to extract. Default 3.").create("logPeriod");

    @SuppressWarnings("static-access")
    private static final Option OPTION_COMPRESS = OptionBuilder.withArgName("compress").hasArg().isRequired(false).withDescription("specify whether to compress the output with zip. Default false.").create("compress");

    private CubeMetaExtractor cubeMetaExtractor;
    private JobInfoExtractor jobInfoExtractor;
    private Options options;
    private String type;

    public DataExtractorCLI(String type) {
        this.type = type;

        jobInfoExtractor = new JobInfoExtractor();
        cubeMetaExtractor = new CubeMetaExtractor();

        if (this.type.equalsIgnoreCase("job")) {
            options = jobInfoExtractor.getOptions();
        } else if (this.type.equalsIgnoreCase("metadata")) {
            options = cubeMetaExtractor.getOptions();
        } else {
            throw new RuntimeException("Only job and metadata are allowed.");
        }

        options.addOption(OPTION_LOG_PERIOD);
        options.addOption(OPTION_COMPRESS);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String dest = null;
        if (this.type.equals("job")) {
            jobInfoExtractor.execute(optionsHelper);
            dest = optionsHelper.getOptionValue(options.getOption("destDir"));
        } else if (this.type.equals("metadata")) {
            cubeMetaExtractor.execute(optionsHelper);
            dest = optionsHelper.getOptionValue(options.getOption("destDir"));
        }

        if (StringUtils.isEmpty(dest)) {
            throw new RuntimeException("destDir is not set, exit directly without extracting");
        }
        if (!dest.endsWith("/")) {
            dest = dest + "/";
        }

        int logPeriod = optionsHelper.hasOption(OPTION_LOG_PERIOD) ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_LOG_PERIOD)) : DEFAULT_LOG_PERIOD;
        boolean compress = optionsHelper.hasOption(OPTION_COMPRESS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_COMPRESS)) : false;

        if (logPeriod > 0) {
            logger.info("Start to extract kylin logs in {} days", logPeriod);

            final String logFolder = KylinConfig.getKylinHome() + "/logs/";
            final String defaultLogFilename = "kylin.log";
            final File logsDir = new File(dest + "/logs/");
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

        if (compress) {
            File tempZipFile = File.createTempFile("extraction_", ".zip");
            ZipFileUtils.compressZipFile(dest, tempZipFile.getAbsolutePath());
            FileUtils.forceDelete(new File(dest));
            FileUtils.moveFileToDirectory(tempZipFile, new File(dest), true);
        }

        logger.info("Extraction finished at: " + new File(dest).getAbsolutePath());
    }

    public static void main(String args[]) {
        DataExtractorCLI dataExtractorCLI = new DataExtractorCLI(args[0]);
        dataExtractorCLI.execute(Arrays.copyOfRange(args, 1, args.length));
    }
}
