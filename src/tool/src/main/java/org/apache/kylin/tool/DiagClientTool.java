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

import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_TIMESTAMP_COMPARE;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.CANDIDATE_LOG;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.LOG;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.tool.util.DiagnosticFilesChecker;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class DiagClientTool extends AbstractInfoExtractorTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().withArgName("project").hasArg()
            .isRequired(false).withDescription("Specify realizations in which project to extract").create("project");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CONF = OptionBuilder.getInstance().withArgName("includeConf").hasArg()
            .isRequired(false).withDescription("Specify whether to include conf files to extract. Default true.")
            .create("includeConf");
    @SuppressWarnings("static-access")
    private static final Option OPTION_META = OptionBuilder.getInstance().withArgName("includeMeta").hasArg()
            .isRequired(false).withDescription("Specify whether to include metadata to extract. Default true.")
            .create("includeMeta");
    @SuppressWarnings("static-access")
    private static final Option OPTION_LOG = OptionBuilder.getInstance().withArgName("includeLog").hasArg()
            .isRequired(false).withDescription("Specify whether to include logs to extract. Default true.")
            .create("includeLog");
    @SuppressWarnings("static-access")
    private static final Option OPTION_SPARK = OptionBuilder.getInstance().withArgName("includeSpark").hasArg()
            .isRequired(false).withDescription("Specify whether to include spark conf to extract. Default false.")
            .create("includeSpark");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CLIENT = OptionBuilder.getInstance().withArgName("includeClient").hasArg()
            .isRequired(false).withDescription("Specify whether to include client info to extract. Default true.")
            .create("includeClient");
    @SuppressWarnings("static-access")
    private static final Option OPTION_AUDIT_LOG = OptionBuilder.getInstance().withArgName("includeAuditLog").hasArg()
            .isRequired(false).withDescription("Specify whether to include auditLog to extract. Default true.")
            .create("includeAuditLog");
    @SuppressWarnings("static-access")
    private static final Option OPTION_BEFORE_DAY = OptionBuilder.getInstance().withArgName("beforeDay").hasArg()
            .isRequired(false).withDescription("Specify the range of time to extract use info. Default last 30 days.")
            .create("beforeDay");

    // Problem category
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_BASE = OptionBuilder.getInstance().withArgName("base").hasArg()
            .isRequired(false).withDescription("package components include base").create("base");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_QUERY = OptionBuilder.getInstance().withArgName("query").hasArg()
            .isRequired(false).withDescription("package components include slow and failed query").create("query");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CATE_META = OptionBuilder.getInstance().withArgName("meta").hasArg()
            .isRequired(false).withDescription("package components include wrong metadata operation").create("meta");

    public DiagClientTool() {
        super();
        setPackageType("full");

        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_CONF);
        options.addOption(OPTION_CLIENT);
        options.addOption(OPTION_SPARK);
        options.addOption(OPTION_CURRENT_TIME);
        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_CATE_BASE);
        options.addOption(OPTION_CATE_QUERY);
        options.addOption(OPTION_CATE_META);
        options.addOption(OPTION_META);
        options.addOption(OPTION_AUDIT_LOG);
        options.addOption(OPTION_BEFORE_DAY);
    }

    @Override
    protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
        final boolean includeConf = getBooleanOption(optionsHelper, OPTION_CONF,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));
        final boolean includeMeta = getBooleanOption(optionsHelper, OPTION_META,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));
        final boolean includeClient = getBooleanOption(optionsHelper, OPTION_CLIENT,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));
        final boolean includeLog = getBooleanOption(optionsHelper, OPTION_LOG,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));
        final boolean includeAuditLog = getBooleanOption(optionsHelper, OPTION_AUDIT_LOG,
                getBooleanOption(optionsHelper, OPTION_CATE_BASE, true));
        final boolean includeBin = true;

        final long startTime = getLongOption(optionsHelper, OPTION_START_TIME, getDefaultStartTime());
        final long endTime = getLongOption(optionsHelper, OPTION_END_TIME, getDefaultEndTime());
        if (startTime >= endTime) {
            throw new KylinException(PARAMETER_TIMESTAMP_COMPARE);
        }
        final int useInfoBeforeDay = getIntOption(optionsHelper, OPTION_BEFORE_DAY, 30);
        logger.info("Time range: start={}, end={}", startTime, endTime);

        // calculate time used
        final long start = System.currentTimeMillis();
        final File recordTime = new File(exportDir, "time_used_info");

        // export cube metadata
        if (includeMeta) {
            File metaDir = new File(exportDir, "metadata");
            FileUtils.forceMkdir(metaDir);
            String[] metaToolArgs = { "-backup", OPT_DIR, metaDir.getAbsolutePath(), OPT_COMPRESS, FALSE,
                    "-excludeTableExd" };
            dumpMetadata(metaToolArgs, recordTime);
        }

        if (includeAuditLog) {
            File auditLogDir = new File(exportDir, "audit_log");
            FileUtils.forceMkdir(auditLogDir);
            String[] auditLogToolArgs = { "-startTime", String.valueOf(startTime), "-endTime", String.valueOf(endTime),
                    OPT_DIR, auditLogDir.getAbsolutePath() };
            exportAuditLog(auditLogToolArgs, recordTime);
        }

        exportRecCandidate(null, null, exportDir, true, recordTime);

        if (includeClient) {
            exportClient(recordTime);
        }

        String[] sparkLogArgs = { OPT_DIR, exportDir.getAbsolutePath(), "-startTime", String.valueOf(startTime),
                "-endTime", String.valueOf(endTime) };
        dumpStreamingSparkLog(sparkLogArgs, recordTime);

        exportJstack(recordTime);

        exportConf(exportDir, recordTime, includeConf, includeBin);
        exportInfluxDBMetrics(exportDir, recordTime);

        exportSparkLog(exportDir, startTime, endTime, recordTime, null);

        exportCandidateLog(exportDir, recordTime, startTime, endTime);

        exportKgLogs(exportDir, startTime, endTime, recordTime);

        exportTieredStorage(null, exportDir, startTime, endTime, recordTime);

        exportSystemUsageInfo(recordTime, DateTime.now().minusDays(useInfoBeforeDay).withTimeAtStartOfDay().getMillis(),
                Long.MAX_VALUE);

        executeTimeoutTask(taskQueue);

        executorService.shutdown();
        awaitDiagPackageTermination(getKapConfig().getDiagPackageTimeout());

        // export logs
        if (includeLog) {
            recordTaskStartTime(LOG);
            KylinLogTool.extractKylinLog(exportDir, startTime, endTime);
            KylinLogTool.extractOtherLogs(exportDir, startTime, endTime);
            recordTaskExecutorTimeToFile(LOG, recordTime);
        }

        DiagnosticFilesChecker.writeMsgToFile("Total files", System.currentTimeMillis() - start, recordTime);
    }

    private void exportCandidateLog(File exportDir, File recordTime, long startTime, long endTime) {
        // candidate log
        val candidateLogTask = executorService.submit(() -> {
            recordTaskStartTime(CANDIDATE_LOG);
            List<ProjectInstance> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .listAllProjects();
            projects.forEach(x -> KylinLogTool.extractJobTmpCandidateLog(exportDir, x.getName(), startTime, endTime));
            recordTaskExecutorTimeToFile(CANDIDATE_LOG, recordTime);
        });
        scheduleTimeoutTask(candidateLogTask, CANDIDATE_LOG);
    }

    public long getDefaultStartTime() {
        return DateTime.now().minusDays(getKapConfig().getExtractionStartTimeDays() - 1).withTimeAtStartOfDay()
                .getMillis();
    }

    public long getDefaultEndTime() {
        return DateTime.now().plusDays(1).minus(1).withTimeAtStartOfDay().getMillis();
    }
}
