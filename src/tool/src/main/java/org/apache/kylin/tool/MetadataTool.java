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

import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_NOT_SPECIFY;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.helper.MetadataToolHelper;
import org.apache.kylin.tool.util.ScreenPrintUtil;
import org.apache.kylin.tool.util.ToolMainWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class MetadataTool extends ExecutableApplication {

    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    private static final Option OPERATE_BACKUP = OptionBuilder.getInstance()
            .withDescription("Backup metadata to local path or HDFS path").isRequired(false).create("backup");

    private static final Option OPERATE_COMPRESS = OptionBuilder.getInstance()
            .withDescription("Backup compressed metadata to HDFS path").isRequired(false).create("compress");

    private static final Option OPERATE_RESTORE = OptionBuilder.getInstance()
            .withDescription("Restore metadata from local path or HDFS path").isRequired(false).create("restore");

    private static final Option OPTION_AFTER_TRUNCATE = OptionBuilder.getInstance()
            .withDescription("Restore overwrite metadata from local path or HDFS path (optional)").isRequired(false)
            .withLongOpt("after-truncate").hasArg(false).create("d");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DIRECTORY_PATH")
            .withDescription("Specify the target directory for backup and restore").isRequired(false).create("dir");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("PROJECT_NAME")
            .withDescription("Specify project level backup and restore (optional)").isRequired(false).create("project");

    private static final Option FOLDER_NAME = OptionBuilder.getInstance().hasArg().withArgName("FOLDER_NAME")
            .withDescription("Specify the folder name for backup").isRequired(false).create("folder");

    private static final Option OPTION_EXCLUDE_TABLE_EXD = OptionBuilder.getInstance()
            .withDescription("Exclude metadata {project}/table_exd directory").isRequired(false)
            .create("excludeTableExd");

    private final Options options;

    private final KylinConfig kylinConfig;
    private final MetadataToolHelper helper;

    public MetadataTool() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public MetadataTool(KylinConfig kylinConfig) {
        this(kylinConfig, new MetadataToolHelper());
    }

    public MetadataTool(KylinConfig kylinConfig, MetadataToolHelper helper) {
        this.kylinConfig = kylinConfig;
        this.helper = helper;
        this.options = initOptions();
    }

    public static void backup(KylinConfig kylinConfig) throws IOException {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        String[] args = new String[] { "-backup", "-compress", "-dir", HadoopUtil.getBackupFolder(kylinConfig) };
        val backupTool = new MetadataTool(kylinConfig);
        backupTool.execute(args);
    }

    public static void backup(KylinConfig kylinConfig, String dir, String folder) throws IOException {
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        String[] args = new String[] { "-backup", "-compress", "-dir", dir, "-folder", folder };
        val backupTool = new MetadataTool(kylinConfig);
        backupTool.execute(args);
    }

    public static void restore(KylinConfig kylinConfig, String folder) throws IOException {
        val tool = new MetadataTool(kylinConfig);
        tool.execute(new String[] { "-restore", "-dir", folder, "--after-truncate" });
    }

    public static void main(String[] args) {
        ToolMainWrapper.wrap(args, () -> {
            val config = KylinConfig.getInstanceFromEnv();
            val tool = new MetadataTool(config);
            val optionsHelper = new OptionsHelper();
            optionsHelper.parseOptions(tool.getOptions(), args);
            boolean isBackup = optionsHelper.hasOption(OPERATE_BACKUP);
            if (isBackup && ScreenPrintUtil.isMainThread()) {
                config.setProperty("kylin.env.metadata.only-for-read", "true");
            }
            val resourceStore = ResourceStore.getKylinMetaStore(config);
            resourceStore.getAuditLogStore().setInstance(AddressUtil.getMockPortAddress());
            tool.execute(args);
        });
        Unsafe.systemExit(0);
    }

    private Options initOptions() {
        Options result = new Options();
        OptionGroup optionGroup = new OptionGroup();
        optionGroup.setRequired(true);
        optionGroup.addOption(OPERATE_BACKUP);
        optionGroup.addOption(OPERATE_RESTORE);

        result.addOptionGroup(optionGroup);
        result.addOption(OPTION_DIR);
        result.addOption(OPTION_PROJECT);
        result.addOption(FOLDER_NAME);
        result.addOption(OPERATE_COMPRESS);
        result.addOption(OPTION_EXCLUDE_TABLE_EXD);
        result.addOption(OPTION_AFTER_TRUNCATE);
        return result;
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("start to init ResourceStore");
        String project = optionsHelper.getOptionValue(OPTION_PROJECT);
        String path = optionsHelper.getOptionValue(OPTION_DIR);
        String folder = optionsHelper.getOptionValue(FOLDER_NAME);
        boolean compress = optionsHelper.hasOption(OPERATE_COMPRESS);
        boolean excludeTableExd = optionsHelper.hasOption(OPTION_EXCLUDE_TABLE_EXD);
        if (optionsHelper.hasOption(OPERATE_BACKUP)) {
            helper.backup(kylinConfig, project, path, folder, compress, excludeTableExd);
        } else if (optionsHelper.hasOption(OPERATE_RESTORE)) {
            boolean delete = optionsHelper.hasOption(OPTION_AFTER_TRUNCATE);
            helper.restore(kylinConfig, project, path, delete);
        } else {
            throw new KylinException(PARAMETER_NOT_SPECIFY, "-restore");
        }
    }

}
