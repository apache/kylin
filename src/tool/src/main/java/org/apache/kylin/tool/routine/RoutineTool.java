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
package org.apache.kylin.tool.routine;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.helper.MetadataToolHelper;
import org.apache.kylin.helper.RoutineToolHelper;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.tool.MaintainModeTool;
import org.apache.kylin.tool.garbage.CleanTaskExecutorService;
import org.apache.kylin.tool.util.ToolMainWrapper;
import org.apache.kylin.metadata.epoch.EpochManager;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class RoutineTool extends ExecutableApplication {
    private static final Option OPTION_CLEANUP_METADATA = new Option("m", "metadata", false,
            "cleanup metadata garbage after check.");
    private static final Option OPTION_CLEANUP = new Option("c", "cleanup", false, "cleanup hdfs garbage after check.");
    private static final Option OPTION_PROJECTS = new Option("p", "projects", true, "specify projects to cleanup.");
    private static final Option OPTION_REQUEST_FS_RATE = new Option("r", "rate", true, "specify request fs rate.");
    private static final Option OPTION_RETRY_TIMES = new Option("t", "retryTimes", true, "specify retry times.");
    private static final Option OPTION_HELP = new Option("h", "help", false, "print help message.");
    private boolean storageCleanup;
    private boolean metadataCleanup;
    private String[] projects = new String[0];
    private int retryTimes;
    private double requestFSRate;

    private MetadataToolHelper helper = new MetadataToolHelper();

    public static void main(String[] args) {
        ToolMainWrapper.wrap(args, () -> {
            RoutineTool tool = new RoutineTool();
            tool.execute(args);
        });
        Unsafe.systemExit(0);
    }

    public static void deleteRawRecItems() {
        RoutineToolHelper.deleteRawRecItems();
    }

    public static void cleanQueryHistories() {
        RoutineToolHelper.cleanQueryHistoriesAsync();
    }

    public static void cleanStreamingStats() {
        RoutineToolHelper.cleanStreamingStats();
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_CLEANUP_METADATA);
        options.addOption(OPTION_CLEANUP);
        options.addOption(OPTION_PROJECTS);
        options.addOption(OPTION_REQUEST_FS_RATE);
        options.addOption(OPTION_RETRY_TIMES);
        options.addOption(OPTION_HELP);
        return options;
    }

    protected final List<String> getProjectsToCleanup() {
        if (getProjects().length != 0) {
            return Arrays.asList(getProjects());
        } else {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            List<ProjectInstance> instances = NProjectManager.getInstance(kylinConfig).listAllProjects();
            return instances.stream().map(ProjectInstance::getName).collect(Collectors.toList());
        }
    }


    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }
        initOptionValues(optionsHelper);
        System.out.println("Start to cleanup metadata");
        List<String> projectsToCleanup = getProjectsToCleanup();
        MaintainModeTool maintainModeTool = new MaintainModeTool("routine tool");
        maintainModeTool.init();
        maintainModeTool.markEpochs();
        if (EpochManager.getInstance().isMaintenanceMode()) {
            Runtime.getRuntime().addShutdownHook(new Thread(maintainModeTool::releaseEpochs));
        }
        doCleanup(projectsToCleanup);
    }

    private void doCleanup(List<String> projectsToCleanup) {
        try {
            if (metadataCleanup) {
                RoutineToolHelper.cleanMeta(projectsToCleanup);
            }
            cleanStorage();
        } catch (Exception e) {
            log.error("Failed to execute routintool", e);
        }
    }

    public void cleanStorage() {
        CleanTaskExecutorService.getInstance().cleanStorageForRoutine(
            storageCleanup, Arrays.asList(projects), requestFSRate, retryTimes);
    }

    protected boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }

    protected void initOptionValues(OptionsHelper optionsHelper) {
        this.storageCleanup = optionsHelper.hasOption(OPTION_CLEANUP);
        this.metadataCleanup = optionsHelper.hasOption(OPTION_CLEANUP_METADATA);

        if (optionsHelper.hasOption(OPTION_PROJECTS)) {
            this.projects = optionsHelper.getOptionValue(OPTION_PROJECTS).split(",");
        }
        if (optionsHelper.hasOption(OPTION_REQUEST_FS_RATE)) {
            this.requestFSRate = Double.parseDouble(optionsHelper.getOptionValue(OPTION_REQUEST_FS_RATE));
        }
        if (optionsHelper.hasOption(OPTION_RETRY_TIMES)) {
            this.retryTimes = Integer.parseInt(optionsHelper.getOptionValue(OPTION_RETRY_TIMES));
        }

        log.info("RoutineTool has option metadata cleanup: " + metadataCleanup + " storage cleanup: " + storageCleanup
                + (projects.length > 0 ? " projects: " + optionsHelper.getOptionValue(OPTION_PROJECTS) : "")
                + " Request FileSystem rate: " + requestFSRate + " Retry Times: " + retryTimes);
        System.out.println(
                "RoutineTool has option metadata cleanup: " + metadataCleanup + " storage cleanup: " + storageCleanup
                        + (projects.length > 0 ? " projects: " + optionsHelper.getOptionValue(OPTION_PROJECTS) : "")
                        + " Request FileSystem rate: " + requestFSRate + " Retry Times: " + retryTimes);
    }


}
