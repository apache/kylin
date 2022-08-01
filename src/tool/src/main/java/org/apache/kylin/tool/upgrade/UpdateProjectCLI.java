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
package org.apache.kylin.tool.upgrade;

import static org.apache.kylin.tool.util.MetadataUtil.getMetadataUrl;
import static org.apache.kylin.tool.util.ScreenPrintUtil.println;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateProjectCLI extends ExecutableApplication {
    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("dir")
            .withDescription("Specify the directory to operator").isRequired(true).create("dir");

    private static final Option OPTION_HELP = OptionBuilder.getInstance().hasArg(false)
            .withDescription("print help message.").isRequired(false).withLongOpt("help").create("h");

    private static final Map<String, String> REPLACE_OVERRIDE_PROPERTIES_MAP = new HashMap<>();

    static {
        REPLACE_OVERRIDE_PROPERTIES_MAP.put("kap.metadata.semi-automatic-mode", "kylin.metadata.semi-automatic-mode");
        REPLACE_OVERRIDE_PROPERTIES_MAP.put("kap.query.metadata.expose-computed-column",
                "kylin.query.metadata.expose-computed-column");
    }

    private KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

    public static void main(String[] args) throws Exception {
        val tool = new UpdateProjectCLI();
        tool.execute(args);
        println("Update project finished.");
        Unsafe.systemExit(0);
    }

    void updateAllProjects(KylinConfig kylinConfig) {
        log.info("Start to update project...");
        val projectManager = NProjectManager.getInstance(kylinConfig);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            updateProject(project);
        }
        log.info("Update project finished!");
    }

    private void updateProject(ProjectInstance project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NProjectManager npr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            // for upgrade, set project override properties
            REPLACE_OVERRIDE_PROPERTIES_MAP.forEach(project::replaceKeyOverrideKylinProps);

            // for upgrade, set default database
            if (StringUtils.isEmpty(project.getDefaultDatabase())) {
                val schemaMap = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName())
                        .listTablesGroupBySchema();
                String defaultDatabase = DatabaseDesc.getDefaultDatabaseByMaxTables(schemaMap);
                project.setDefaultDatabase(defaultDatabase.toUpperCase(Locale.ROOT));
            }
            npr.updateProject(project);
            return 0;
        }, project.getName(), 1);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_HELP);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }
        log.info("Start to update project...");

        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_DIR));
        kylinConfig.setMetadataUrl(metadataUrl);

        UpdateProjectCLI defaultDatabaseCLI = new UpdateProjectCLI();
        defaultDatabaseCLI.updateAllProjects(kylinConfig);
        log.info("Update project finished!");
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }
}
