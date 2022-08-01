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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.util.QueryHisStoreUtil;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.streaming.util.StreamingJobRecordStoreUtil;
import org.apache.kylin.metadata.streaming.util.StreamingJobStatsStoreUtil;
import org.apache.kylin.tool.MaintainModeTool;
import org.apache.kylin.tool.garbage.GarbageCleaner;
import org.apache.kylin.tool.garbage.SourceUsageCleaner;
import org.apache.kylin.tool.garbage.StorageCleaner;

import lombok.Getter;
import lombok.val;
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

    public static void main(String[] args) {
        RoutineTool tool = new RoutineTool();
        tool.execute(args);
        Unsafe.systemExit(0);
    }

    public static void deleteRawRecItems() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> projectInstances = NProjectManager.getInstance(config).listAllProjects().stream()
                .filter(projectInstance -> !projectInstance.isExpertMode()).collect(Collectors.toList());
        if (projectInstances.isEmpty()) {
            return;
        }
        try (SetThreadName ignored = new SetThreadName("DeleteRawRecItemsInDB")) {
            val jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
            jdbcRawRecStore.deleteOutdated();
        } catch (Exception e) {
            log.error("delete outdated advice fail: ", e);
        }
    }

    public static void cleanQueryHistories() {
        QueryHisStoreUtil.cleanQueryHistory();
    }

    public static void cleanStreamingStats() {
        StreamingJobStatsStoreUtil.cleanStreamingJobStats();
        StreamingJobRecordStoreUtil.cleanStreamingJobRecord();
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

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }
        initOptionValues(optionsHelper);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> instances = NProjectManager.getInstance(kylinConfig).listAllProjects();
        System.out.println("Start to cleanup metadata");
        List<String> projectsToCleanup = Arrays.asList(projects);
        if (projectsToCleanup.isEmpty()) {
            projectsToCleanup = instances.stream().map(ProjectInstance::getName).collect(Collectors.toList());
        }
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
                cleanMeta(projectsToCleanup);
            }
            cleanStorage();
        } catch (Exception e) {
            log.error("Failed to execute routintool", e);
        }
    }

    protected void cleanMeta(List<String> projectsToCleanup) throws IOException {
        try {
            cleanGlobalMeta();
            for (String projName : projectsToCleanup) {
                cleanMetaByProject(projName);
            }
            cleanQueryHistories();
            cleanStreamingStats();
            deleteRawRecItems();
            System.out.println("Metadata cleanup finished");
        } catch (Exception e) {
            log.error("Metadata cleanup failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "Metadata cleanup failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
        }

    }

    public void cleanGlobalMeta() {
        log.info("Start to clean up global meta");
        try {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                new SourceUsageCleaner().cleanup();
                return null;
            }, UnitOfWork.GLOBAL_UNIT);
        } catch (Exception e) {
            log.error("Failed to clean global meta", e);
        }
        log.info("Clean up global meta finished");

    }

    public void cleanMetaByProject(String projectName) {
        log.info("Start to clean up {} meta", projectName);
        try {
            GarbageCleaner.cleanMetadata(projectName);
        } catch (Exception e) {
            log.error("Project[{}] cleanup Metadata failed", projectName, e);
        }
        log.info("Clean up {} meta finished", projectName);
    }

    public void cleanStorage() {
        try {
            StorageCleaner storageCleaner = new StorageCleaner(storageCleanup, Arrays.asList(projects), requestFSRate,
                    retryTimes);
            System.out.println("Start to cleanup HDFS");
            storageCleaner.execute();
            System.out.println("cleanup HDFS finished");
        } catch (Exception e) {
            log.error("cleanup HDFS failed", e);
            System.out.println(StorageCleaner.ANSI_RED
                    + "cleanup HDFS failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr"
                    + StorageCleaner.ANSI_RESET);
        }
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

    public void setProjects(String[] projects) {
        this.projects = projects;
    }

    public void setStorageCleanup(boolean storageCleanup) {
        this.storageCleanup = storageCleanup;
    }
}
