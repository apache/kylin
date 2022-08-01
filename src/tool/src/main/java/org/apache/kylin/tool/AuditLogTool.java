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

import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_NOT_SPECIFY;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_TIMESTAMP_NOT_SPECIFY;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PATH_NOT_EXISTS;
import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static org.apache.kylin.tool.garbage.StorageCleaner.ANSI_RED;
import static org.apache.kylin.tool.garbage.StorageCleaner.ANSI_RESET;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.common.constant.Constant;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Lists;

import lombok.val;

public class AuditLogTool extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final String CHARSET = Charset.defaultCharset().name();

    private static final Option OPTION_START_TIME = OptionBuilder.getInstance().hasArg().withArgName("START_TIMESTAMP")
            .withDescription("Specify the start timestamp (sec) (optional)").isRequired(false).create("startTime");

    private static final Option OPTION_END_TIME = OptionBuilder.getInstance().hasArg().withArgName("END_TIMESTAMP")
            .withDescription("Specify the end timestamp (sec) (optional)").isRequired(false).create("endTime");

    private static final Option OPTION_JOB = OptionBuilder.getInstance().hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job (optional)").isRequired(false).create("job");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("OPTION_PROJECT")
            .withDescription("Specify project (optional)").isRequired(false).create("project");

    private static final Option OPTION_RESTORE = OptionBuilder.getInstance()
            .withDescription("Restore audit log from local path").isRequired(false).create("restore");

    private static final Option OPTION_TABLE = OptionBuilder.getInstance().hasArg().withArgName("TABLE_NAME")
            .withDescription("Specify the table (optional)").isRequired(false).create("table");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DESTINATION_DIR")
            .withDescription("Specify the directory for audit log backup or restore").isRequired(true).create("dir");

    private static final String AUDIT_LOG_SUFFIX = ".jsonl";

    private static final int MAX_BATCH_SIZE = 50000;

    private final Options options;

    private final KylinConfig kylinConfig;

    AuditLogTool() {
        this(KylinConfig.newKylinConfig());
    }

    public AuditLogTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.options = new Options();
        initOptions();
    }

    public static void main(String[] args) {
        try {
            val tool = new AuditLogTool();
            tool.execute(args);
        } catch (Exception e) {
            System.out.println(ANSI_RED + "Audit log task failed." + ANSI_RESET);
            logger.error("fail execute audit log tool: ", e);
            Unsafe.systemExit(1);
        }
        System.out.println("Audit log task finished.");
        Unsafe.systemExit(0);
    }

    private void initOptions() {
        options.addOption(OPTION_JOB);
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_RESTORE);
        options.addOption(OPTION_TABLE);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String dir = optionsHelper.getOptionValue(OPTION_DIR);
        if (optionsHelper.hasOption(OPTION_RESTORE)) {
            restore(optionsHelper, dir);
        } else if (optionsHelper.hasOption(OPTION_JOB)) {
            extractJob(optionsHelper, dir);
        } else {
            extractFull(optionsHelper, dir);
        }
    }

    private void extractJob(OptionsHelper optionsHelper, final String dir) throws Exception {
        if (!optionsHelper.hasOption(OPTION_PROJECT)) {
            throw new KylinException(PARAMETER_NOT_SPECIFY, "-project");
        }
        val project = optionsHelper.getOptionValue(OPTION_PROJECT);
        if (StringUtils.isEmpty(project)) {
            throw new KylinException(PARAMETER_EMPTY, "project");
        }

        val jobId = optionsHelper.getOptionValue(OPTION_JOB);
        if (StringUtils.isEmpty(jobId)) {
            throw new KylinException(PARAMETER_EMPTY, "job");
        }
        AbstractExecutable job = NExecutableManager.getInstance(kylinConfig, project).getJob(jobId);
        long startTs = job.getStartTime();
        long endTs = job.getEndTime();

        extract(startTs, endTs,
                Paths.get(dir, String.format(Locale.ROOT, "%d-%d%s", startTs, endTs, AUDIT_LOG_SUFFIX)).toFile());
    }

    private void extractFull(OptionsHelper optionsHelper, final String dir) throws Exception {
        if (!optionsHelper.hasOption(OPTION_START_TIME)) {
            throw new KylinException(PARAMETER_TIMESTAMP_NOT_SPECIFY, "-startTime");
        }
        if (!optionsHelper.hasOption(OPTION_END_TIME)) {
            throw new KylinException(PARAMETER_TIMESTAMP_NOT_SPECIFY, "-endTime");
        }
        long startTs = Long.parseLong(optionsHelper.getOptionValue(OPTION_START_TIME));
        long endTs = Long.parseLong(optionsHelper.getOptionValue(OPTION_END_TIME));

        extract(startTs, endTs,
                Paths.get(dir, String.format(Locale.ROOT, "%d-%d%s", startTs, endTs, AUDIT_LOG_SUFFIX)).toFile());
    }

    private void restore(OptionsHelper optionsHelper, final String dir) throws Exception {
        if (!optionsHelper.hasOption(OPTION_TABLE)) {
            throw new KylinException(PARAMETER_NOT_SPECIFY, "-table");
        }
        val table = optionsHelper.getOptionValue(OPTION_TABLE);
        if (StringUtils.isEmpty(table)) {
            throw new KylinException(PARAMETER_EMPTY, "table");
        }

        File dirFile = Paths.get(dir).toFile();
        if (!dirFile.exists()) {
            throw new KylinException(PATH_NOT_EXISTS, dir);
        }

        val url = kylinConfig.getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        val transactionManager = new DataSourceTransactionManager(dataSource);
        val jdbcTemplate = new JdbcTemplate(dataSource);
        try (JdbcAuditLogStore auditLogStore = new JdbcAuditLogStore(kylinConfig, jdbcTemplate, transactionManager,
                table)) {
            for (File logFile : Objects.requireNonNull(dirFile.listFiles())) {
                if (!logFile.getName().endsWith(AUDIT_LOG_SUFFIX)) {
                    continue;
                }
                String line;
                try (InputStream fin = new FileInputStream(logFile);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fin, CHARSET))) {
                    List<AuditLog> auditLogs = Lists.newArrayList();
                    while ((line = br.readLine()) != null) {
                        try {
                            auditLogs.add(JsonUtil.readValue(line, AuditLog.class));
                        } catch (Exception e) {
                            logger.error("audit log deserialize error >>> {}", line, e);
                        }
                    }
                    auditLogStore.batchInsert(auditLogs);
                }
            }
        }
    }

    private void extract(long startTs, long endTs, File auditLogFile) throws Exception {
        auditLogFile.getParentFile().mkdirs();
        long fromId = Long.MAX_VALUE;
        int batchSize = KylinConfig.getInstanceFromEnv().getAuditLogBatchSize();
        //Prevent OOM
        batchSize = Math.min(MAX_BATCH_SIZE, batchSize);
        logger.info("Audit log batch size is {}.", batchSize);
        try (JdbcAuditLogStore auditLogStore = new JdbcAuditLogStore(kylinConfig,
                kylinConfig.getAuditLogBatchTimeout());
                OutputStream fos = new FileOutputStream(auditLogFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, CHARSET),
                        Constant.AUDIT_MAX_BUFFER_SIZE)) {
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("audit log task is interrupt");
                }
                List<AuditLog> auditLogs = auditLogStore.fetchRange(fromId, startTs, endTs, batchSize);
                if (CollectionUtils.isEmpty(auditLogs)) {
                    break;
                }
                auditLogs.forEach(x -> {
                    try {
                        bw.write(JsonUtil.writeValueAsString(x));
                        bw.newLine();
                    } catch (Exception e) {
                        logger.error("Write audit log error, id is {}", x.getId(), e);
                    }
                });

                if (auditLogs.size() < batchSize) {
                    break;
                }
                fromId = auditLogs.get(auditLogs.size() - 1).getId();
                logger.info("Audit log size is {}, id range is [{},{}].", auditLogs.size(), auditLogs.get(0).getId(),
                        fromId);
            }
        }
    }
}
