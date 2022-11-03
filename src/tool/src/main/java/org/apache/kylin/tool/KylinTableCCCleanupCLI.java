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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.Unsafe;
import io.kyligence.kap.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinTableCCCleanupCLI extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger(KylinTableCCCleanupCLI.class);

    private static final Option OPTION_CLEANUP;
    private static final Option OPTION_PROJECTS;
    private static final Option OPTION_HELP;

    static {
        OPTION_CLEANUP = new Option("c", "cleanup", false, "Cleanup the computed columns in table metadata.");
        OPTION_PROJECTS = new Option("p", "projects", true, "Specify projects to cleanup.");
        OPTION_HELP = new Option("h", "help", false, "Print usage of KapTableCCCleanupCLI");
    }

    private boolean cleanup = false;

    public static void main(String[] args) {
        int exit = 0;
        MaintainModeTool maintainModeTool = new MaintainModeTool("cleanup table cc");
        maintainModeTool.init();
        try {
            maintainModeTool.markEpochs();
            if (EpochManager.getInstance().isMaintenanceMode()) {
                Runtime.getRuntime().addShutdownHook(new Thread(maintainModeTool::releaseEpochs));
            }
            new KylinTableCCCleanupCLI().execute(args);
        } catch (Throwable e) {
            exit = 1;
            logger.warn("Fail to cleanup table cc.", e);
        }
        Unsafe.systemExit(exit);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_CLEANUP);
        options.addOption(OPTION_PROJECTS);
        options.addOption(OPTION_HELP);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }
        boolean hasOptionCleanup = optionsHelper.hasOption(OPTION_CLEANUP);
        if (hasOptionCleanup) {
            cleanup = true;
        }

        boolean hasOptionProjects = optionsHelper.hasOption(OPTION_PROJECTS);
        List<String> projects;
        if (hasOptionProjects) {
            projects = Arrays.asList(optionsHelper.getOptionValue(OPTION_PROJECTS).split(","));
        } else {
            projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects().stream()
                    .map(ProjectInstance::getName).collect(Collectors.toList());
        }
        logger.info("Cleanup option value: '{}', projects {}", cleanup, projects);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        KylinTableCCCleanup kapTableCCCleanup = new KylinTableCCCleanup(config, cleanup, projects);
        kapTableCCCleanup.scanAllTableCC();
        System.out.println("Done. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr");
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }
}
