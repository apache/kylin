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
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractInfoExtractor extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(AbstractInfoExtractor.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_DEST = OptionBuilder.withArgName("destDir").hasArg().isRequired(true).withDescription("specify the dest dir to save the related information").create("destDir");

    @SuppressWarnings("static-access")
    private static final Option OPTION_COMPRESS = OptionBuilder.withArgName("compress").hasArg().isRequired(false).withDescription("specify whether to compress the output with zip. Default true.").create("compress");

    @SuppressWarnings("static-access")
    private static final Option OPTION_SUBMODULE = OptionBuilder.withArgName("submodule").hasArg().isRequired(false).withDescription("specify whether this is a submodule of other CLI tool").create("submodule");

    private static final String DEFAULT_PACKAGE_TYPE = "base";
    private static final String[] COMMIT_SHA1_FILES = { "commit_SHA1", "commit.sha1" };

    protected final Options options;

    protected String packageType;
    protected File exportDir;

    public AbstractInfoExtractor() {
        options = new Options();
        options.addOption(OPTION_DEST);
        options.addOption(OPTION_COMPRESS);
        options.addOption(OPTION_SUBMODULE);

        packageType = DEFAULT_PACKAGE_TYPE;
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String exportDest = optionsHelper.getOptionValue(options.getOption("destDir"));
        boolean shouldCompress = optionsHelper.hasOption(OPTION_COMPRESS) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_COMPRESS)) : true;
        boolean isSubmodule = optionsHelper.hasOption(OPTION_SUBMODULE) ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_SUBMODULE)) : false;

        if (StringUtils.isEmpty(exportDest)) {
            throw new RuntimeException("destDir is not set, exit directly without extracting");
        }
        if (!exportDest.endsWith("/")) {
            exportDest = exportDest + "/";
        }

        // create new folder to contain the output
        String packageName = packageType.toLowerCase() + "_" + new SimpleDateFormat("YYYY_MM_dd_HH_mm_ss").format(new Date());
        if (!isSubmodule && new File(exportDest).exists()) {
            exportDest = exportDest + packageName + "/";
        }

        exportDir = new File(exportDest);
        FileUtils.forceMkdir(exportDir);

        if (!isSubmodule) {
            dumpBasicDiagInfo();
        }

        executeExtract(optionsHelper, exportDir);

        // compress to zip package
        if (shouldCompress) {
            File tempZipFile = File.createTempFile(packageType + "_", ".zip");
            ZipFileUtils.compressZipFile(exportDir.getAbsolutePath(), tempZipFile.getAbsolutePath());
            FileUtils.cleanDirectory(exportDir);

            File zipFile = new File(exportDir, packageName + ".zip");
            FileUtils.moveFile(tempZipFile, zipFile);
            exportDest = zipFile.getAbsolutePath();
            exportDir = new File(exportDest);
        }

        if (!isSubmodule) {
            StringBuffer output = new StringBuffer();
            output.append("\n========================================");
            output.append("\nDump " + packageType + " package locates at: \n" + exportDir.getAbsolutePath());
            output.append("\n========================================");
            logger.info(output.toString());
            System.out.println(output.toString());
        }
    }

    private void dumpBasicDiagInfo() throws IOException {
        try {
            for (String commitSHA1File : COMMIT_SHA1_FILES) {
                File commitFile = new File(KylinConfig.getKylinHome(), commitSHA1File);
                if (commitFile.exists()) {
                    FileUtils.copyFileToDirectory(commitFile, exportDir);
                }
            }
        } catch (IOException e) {
            logger.warn("Failed to copy commit_SHA1 file.", e);
        }

        String output = KylinVersion.getKylinClientInformation() + "\n";
        FileUtils.writeStringToFile(new File(exportDir, "kylin_env"), output, Charset.defaultCharset());

        StringBuilder basicSb = new StringBuilder();
        basicSb.append("MetaStoreID: ").append(ToolUtil.getHBaseMetaStoreId()).append("\n");
        basicSb.append("PackageType: ").append(packageType.toUpperCase()).append("\n");
        SimpleDateFormat format  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
        basicSb.append("PackageTimestamp: ").append(format.format(new Date())).append("\n");
        basicSb.append("Host: ").append(ToolUtil.getHostName()).append("\n");
        FileUtils.writeStringToFile(new File(exportDir, "info"), basicSb.toString(), Charset.defaultCharset());
    }

    protected abstract void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception;

    public String getExportDest() {
        return exportDir.getAbsolutePath();
    }
}
