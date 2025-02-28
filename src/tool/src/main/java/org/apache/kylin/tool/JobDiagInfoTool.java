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

import static org.apache.kylin.tool.constant.DiagSubTaskEnum.CANDIDATE_LOG;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.LOG;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.SOURCE_TABLE_STATS;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.YARN;

import java.io.File;
import java.util.Locale;

import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.tool.snapshot.SnapshotSourceTableStatsTool;
import org.apache.kylin.tool.util.DiagnosticFilesChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class JobDiagInfoTool extends AbstractInfoExtractorTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    private static final Option OPTION_JOB_ID = OptionBuilder.getInstance().withArgName("job").hasArg().isRequired(true)
            .withDescription("specify the Job ID to extract information. ").create("job");

    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_YARN_LOGS = OptionBuilder.getInstance().withArgName("includeYarnLogs")
            .hasArg().isRequired(false)
            .withDescription("set this to true if want to extract related yarn logs too. Default true")
            .create("includeYarnLogs");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CLIENT = OptionBuilder.getInstance().withArgName("includeClient")
            .hasArg().isRequired(false)
            .withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");
    @SuppressWarnings("static-access")
    private static final Option OPTION_INCLUDE_CONF = OptionBuilder.getInstance().withArgName("includeConf").hasArg()
            .isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.")
            .create("includeConf");

    @SuppressWarnings("static-access")
    private static final Option OPTION_META = OptionBuilder.getInstance().withArgName("includeMeta").hasArg()
            .isRequired(false).withDescription("Specify whether to include metadata to extract. Default true.")
            .create("includeMeta");

    @SuppressWarnings("static-access")
    private static final Option OPTION_AUDIT_LOG = OptionBuilder.getInstance().withArgName("includeAuditLog").hasArg()
            .isRequired(false).withDescription("Specify whether to include auditLog to extract. Default true.")
            .create("includeAuditLog");

    private static final String OPT_JOB = "-job";

    public JobDiagInfoTool() {
        super();
        setPackageType("job");

        options.addOption(OPTION_JOB_ID);

        options.addOption(OPTION_INCLUDE_CLIENT);
        options.addOption(OPTION_INCLUDE_YARN_LOGS);
        options.addOption(OPTION_INCLUDE_CONF);

        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_META);
        options.addOption(OPTION_AUDIT_LOG);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        final String jobId = optionsHelper.getOptionValue(OPTION_JOB_ID);
        final boolean includeYarnLogs = getBooleanOption(optionsHelper, OPTION_INCLUDE_YARN_LOGS, true);
        final boolean includeClient = getBooleanOption(optionsHelper, OPTION_INCLUDE_CLIENT, true);
        final boolean includeConf = getBooleanOption(optionsHelper, OPTION_INCLUDE_CONF, true);
        final boolean includeMeta = getBooleanOption(optionsHelper, OPTION_META, true);
        final boolean isCloud = getKapConfig().isCloud();
        final boolean includeAuditLog = getBooleanOption(optionsHelper, OPTION_AUDIT_LOG, false);
        final boolean includeBin = true;

        final long start = System.currentTimeMillis();
        final File recordTime = new File(exportDir, "time_used_info");

        val job = getJobByJobId(jobId);
        if (null == job) {
            logger.error("Can not find the jobId: {}", jobId);
            throw new KylinRuntimeException(String.format(Locale.ROOT, "Can not find the jobId: %s", jobId));
        }
        String project = job.getProject();
        long startTime = job.getCreateTime();
        long endTime = job.getOutput().getEndTime() != 0 ? job.getOutput().getEndTime() : System.currentTimeMillis();
        logger.info("job project : {} , startTime : {} , endTime : {}", project, startTime, endTime);

        if (includeMeta) {
            // dump job metadata
            File metaDir = new File(exportDir, "metadata");
            FileUtils.forceMkdir(metaDir);
            String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_PROJECT, project,
                    "-excludeTableExd" };
            dumpMetadata(metaToolArgs, recordTime);
        }

        if (includeAuditLog) {
            File auditLogDir = new File(exportDir, "audit_log");
            FileUtils.forceMkdir(auditLogDir);
            String[] auditLogToolArgs = { OPT_JOB, jobId, OPT_PROJECT, project, OPT_DIR,
                    auditLogDir.getAbsolutePath() };
            exportAuditLog(auditLogToolArgs, recordTime);
        }

        String modelId = job.getTargetModelId();
        ExecutableManager executableManager = ExecutableManager.getInstance(getKylinConfig(), project);
        AbstractExecutable executable = executableManager.fromPO(job);
        if (StringUtils.isNotEmpty(modelId)) {
            exportRecCandidate(project, modelId, exportDir, false, recordTime);
        }
        // extract yarn log
        if (includeYarnLogs && !isCloud) {
            val future = executorService.submit(() -> {
                recordTaskStartTime(YARN);
                new YarnApplicationTool().extractYarnLogs(exportDir, project, jobId);
                recordTaskExecutorTimeToFile(YARN, recordTime);
            });

            scheduleTimeoutTask(future, YARN);
        }

        if (includeClient) {
            exportClient(recordTime);
        }

        exportJstack(recordTime);

        exportConf(exportDir, recordTime, includeConf, includeBin);

        exportJobSparkLog(exportDir, recordTime, project, jobId, job);
        exportCandidateLog(exportDir, recordTime, project, startTime, endTime);
        exportKgLogs(exportDir, startTime, endTime, recordTime);

        exportInfluxDBMetrics(exportDir, recordTime);

        executeTimeoutTask(taskQueue);

        exportSourceTableStats(exportDir, recordTime, project, executable);

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());

        // export logs
        recordTaskStartTime(LOG);
        KylinLogTool.extractKylinLog(exportDir, jobId);
        KylinLogTool.extractOtherLogs(exportDir, startTime, endTime);
        recordTaskExecutorTimeToFile(LOG, recordTime);
        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - start, recordTime);
    }

    public Boolean exportSourceTableStats(File exportDir, final File recordTime, String project,
                                          AbstractExecutable job) {
        val projectManager = NProjectManager.getInstance(KylinConfig.readSystemKylinConfig());
        val projectConfig = projectManager.getProject(project).getConfig();
        if (!projectConfig.isSnapshotManualManagementEnabled() || !projectConfig.isSnapshotAutoRefreshEnabled()) {
            return false;
        }
        val needExtractSourceTableStatsJobTypes = Sets.<JobTypeEnum> newHashSet(JobTypeEnum.INDEX_REFRESH,
                JobTypeEnum.INDEX_BUILD, JobTypeEnum.INC_BUILD, JobTypeEnum.SUB_PARTITION_BUILD,
                JobTypeEnum.SUB_PARTITION_REFRESH, JobTypeEnum.SNAPSHOT_BUILD, JobTypeEnum.SNAPSHOT_REFRESH);
        if (needExtractSourceTableStatsJobTypes.contains(job.getJobType())) {
            val sourceTableStatsTask = executorService.submit(() -> {
                recordTaskStartTime(SOURCE_TABLE_STATS);
                SnapshotSourceTableStatsTool.extractSourceTableStats(projectConfig, exportDir, project, job);
                recordTaskExecutorTimeToFile(SOURCE_TABLE_STATS, recordTime);
            });
            scheduleTimeoutTask(sourceTableStatsTask, SOURCE_TABLE_STATS);
            return true;
        }
        return false;
    }

    private void exportCandidateLog(File exportDir, File recordTime, String project, long startTime, long endTime) {
        // candidate log
        val candidateLogTask = executorService.submit(() -> {
            recordTaskStartTime(CANDIDATE_LOG);
            KylinLogTool.extractJobTmpCandidateLog(exportDir, project, startTime, endTime);
            recordTaskExecutorTimeToFile(CANDIDATE_LOG, recordTime);
        });
        scheduleTimeoutTask(candidateLogTask, CANDIDATE_LOG);
    }


    @VisibleForTesting
    public ExecutablePO getJobByJobId(String jobId) {
        return JobContextUtil.getJobInfoDao(getKylinConfig()).getExecutablePOByUuid(jobId);
    }

    protected KylinConfig getConfigForModelOrProjectLevel(String modelId, String project) {
        KylinConfig config = null;
        IndexPlan indexPlan = NIndexPlanManager.getInstance(getKylinConfig(), project).getIndexPlan(modelId);
        if (indexPlan != null) {
            config = indexPlan.getConfig();
        }
        if (config == null) {
            config = NProjectManager.getProjectConfig(project);
        }
        return config;
    }
}
