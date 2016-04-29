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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ZipFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractInfoExtractor extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(AbstractInfoExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(true).withDescription("specify the dest dir to save the related information").create("destDir");

    @SuppressWarnings("static-access")
    private static final Option OPTION_COMPRESS = OptionBuilder.withArgName("compress").hasArg().isRequired(false).withDescription("specify whether to compress the output with zip. Default true.").create("compress");

    @SuppressWarnings("static-access")
    private static final Option OPTION_QUIET = OptionBuilder.withArgName("quiet").hasArg().isRequired(false).withDescription("specify whether to print final result").create("quiet");


    private static final String DEFAULT_PACKAGE_PREFIX = "dump";

    protected final Options options;

    protected String packagePrefix;
    protected File exportDir;

    public AbstractInfoExtractor() {
        options = new Options();
        options.addOption(OPTION_DEST);
        options.addOption(OPTION_COMPRESS);
        options.addOption(OPTION_QUIET);

        packagePrefix = DEFAULT_PACKAGE_PREFIX;
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String exportDest = optionsHelper.getOptionValue(options.getOption("destDir"));
        boolean compress = optionsHelper.hasOption(OPTION_COMPRESS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_COMPRESS)) : true;
        boolean quiet = optionsHelper.hasOption(OPTION_QUIET) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_QUIET)) : false;

        if (StringUtils.isEmpty(exportDest)) {
            throw new RuntimeException("destDir is not set, exit directly without extracting");
        }
        if (!exportDest.endsWith("/")) {
            exportDest = exportDest + "/";
        }

        // create new folder to contain the output
        String packageName = packagePrefix + "_" + new SimpleDateFormat("YYYY_MM_dd_HH_mm_ss").format(new Date());
        if (new File(exportDest).exists()) {
            exportDest = exportDest + packageName + "/";
        }
        exportDir = new File(exportDest);

        executeExtract(optionsHelper, exportDir);

        // compress to zip package
        if (compress) {
            File tempZipFile = File.createTempFile(packagePrefix + "_", ".zip");
            ZipFileUtils.compressZipFile(exportDir.getAbsolutePath(), tempZipFile.getAbsolutePath());
            FileUtils.cleanDirectory(exportDir);

            File zipFile = new File(exportDir, packageName + ".zip");
            FileUtils.moveFile(tempZipFile, zipFile);
            exportDest = zipFile.getAbsolutePath();
            exportDir = new File(exportDest);
        }

        if (!quiet) {
            StringBuffer output = new StringBuffer();
            output.append("\n========================================");
            output.append("\nDump " + packagePrefix + " package locates at: \n" + exportDir.getAbsolutePath());
            output.append("\n========================================");
            logger.info(output.toString());
        }
    }

    protected abstract void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception;

    public String getExportDest() {
        return exportDir.getAbsolutePath();
    }
}
