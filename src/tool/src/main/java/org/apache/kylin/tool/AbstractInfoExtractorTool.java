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

import static org.apache.kylin.tool.constant.DiagSubTaskEnum.ASYNC_TASK;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.AUDIT_LOG;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.BIN;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.CANDIDATE_LOG;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.CATALOG_INFO;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.CLIENT;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.CONF;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.FAVORITE_RULE;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.HADOOP_CONF;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.HADOOP_ENV;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.JOB_EVENTLOGS;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.JOB_INFO;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.JOB_TMP;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.JSTACK;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.KG_LOGS;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.LOG;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.LOG_ON_WORKING_DIR;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.METADATA;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.MONITOR_METRICS;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.QUERY_HISTORY_OFFSET;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.REC_CANDIDATE;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.SPARDER_HISTORY;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.SPARK_LOGS;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.SPARK_STREAMING_LOGS;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.SYSTEM_METRICS;
import static org.apache.kylin.tool.constant.DiagSubTaskEnum.SYSTEM_USAGE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.query.util.ExtractFactory;
import org.apache.kylin.query.util.ILogExtractor;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.tool.constant.DiagSubTaskEnum;
import org.apache.kylin.tool.constant.SensitiveConfigKeysConstant;
import org.apache.kylin.tool.constant.StageEnum;
import org.apache.kylin.tool.obf.IpObfuscator;
import org.apache.kylin.tool.obf.KylinConfObfuscator;
import org.apache.kylin.tool.obf.MappingRecorder;
import org.apache.kylin.tool.obf.ObfLevel;
import org.apache.kylin.tool.obf.ResultRecorder;
import org.apache.kylin.tool.restclient.RestClient;
import org.apache.kylin.tool.util.DiagnosticFilesChecker;
import org.apache.kylin.tool.util.HashFunction;
import org.apache.kylin.tool.util.ServerInfoUtil;
import org.apache.kylin.tool.util.ToolUtil;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

public abstract class AbstractInfoExtractorTool extends ExecutableApplication {
    public static final String SLASH = "/";
    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String OPT_COMPRESS = "-compress";
    public static final String OPT_PROJECT = "-project";
    public static final String OPT_DIR = "-dir";
    @SuppressWarnings("static-access")
    static final Option OPTION_DEST = OptionBuilder.getInstance().withArgName("destDir").hasArg().isRequired(true)
            .withDescription("specify the dest dir to save the related information").create("destDir");
    @SuppressWarnings("static-access")
    static final Option OPTION_START_TIME = OptionBuilder.getInstance().withArgName("startTime").hasArg()
            .isRequired(false).withDescription("specify the start of time range to extract logs. ").create("startTime");
    @SuppressWarnings("static-access")
    static final Option OPTION_END_TIME = OptionBuilder.getInstance().withArgName("endTime").hasArg().isRequired(false)
            .withDescription("specify the end of time range to extract logs. ").create("endTime");
    @SuppressWarnings("static-access")
    static final Option OPTION_CURRENT_TIME = OptionBuilder.getInstance().withArgName("currentTime").hasArg()
            .isRequired(false)
            .withDescription(
                    "specify the current of time from client to fix diff between client and server and timezone problem. ")
            .create("currentTime");
    @SuppressWarnings("static-access")
    static final Option OPTION_COMPRESS = OptionBuilder.getInstance().withArgName("compress").hasArg().isRequired(false)
            .withDescription("specify whether to compress the output with zip. Default true.").create("compress");
    @SuppressWarnings("static-access")
    static final Option OPTION_SUBMODULE = OptionBuilder.getInstance().withArgName("submodule").hasArg()
            .isRequired(false).withDescription("specify whether this is a submodule of other CLI tool")
            .create("submodule");
    @SuppressWarnings("static-access")
    static final Option OPTION_SYSTEM_ENV = OptionBuilder.getInstance().withArgName("systemProp").hasArg()
            .isRequired(false)
            .withDescription("specify whether to include system env and properties to extract. Default false.")
            .create("systemProp");
    @SuppressWarnings("static-access")
    static final Option OPTION_DIAGID = OptionBuilder.getInstance().withArgName("diagId").hasArg().isRequired(false)
            .withDescription("Specify whether diag from web").create("diagId");
    static final Option OPTION_OBF_LEVEL = OptionBuilder.getInstance().withArgName("obfLevel").hasArg()
            .isRequired(false)
            .withDescription("specify obfuscate level of the diagnostic package: \nRAW means no obfuscate,\n"
                    + "OBF means obfuscate,\nDefault obfuscate level is OBF.")
            .create("obfLevel");
    private static final Logger logger = LoggerFactory.getLogger("diag");
    @SuppressWarnings("static-access")
    private static final Option OPTION_THREADS = OptionBuilder.getInstance().withArgName("threads").hasArg()
            .isRequired(false).withDescription("Specify number of threads for parallel extraction.").create("threads");
    private static final String DEFAULT_PACKAGE_TYPE = "base";
    private static final String[] COMMIT_SHA1_FILES = { "commit_SHA1", "commit.sha1" };
    private static final int DEFAULT_PARALLEL_SIZE = 4;

    protected final Options options;
    protected ConcurrentHashMap<DiagSubTaskEnum, Long> taskStartTime;
    protected LinkedBlockingQueue<DiagSubTaskInfo> taskQueue;
    protected StageEnum stage = StageEnum.PREPARE;
    protected ScheduledExecutorService executorService;
    protected ScheduledExecutorService timerExecutorService;
    protected boolean mainTaskComplete;
    @Getter
    @Setter
    private String packageType;
    @Getter
    protected File exportDir;
    @Getter
    private KylinConfig kylinConfig;
    @Getter
    private KapConfig kapConfig;
    @Getter
    private String kylinHome;
    @Getter
    private CliCommandExecutor cmdExecutor;
    private boolean includeSystemEnv;

    public AbstractInfoExtractorTool() {
        options = new Options();
        options.addOption(OPTION_DEST);
        options.addOption(OPTION_COMPRESS);
        options.addOption(OPTION_SUBMODULE);
        options.addOption(OPTION_SYSTEM_ENV);

        options.addOption(OPTION_START_TIME);
        options.addOption(OPTION_END_TIME);
        options.addOption(OPTION_DIAGID);
        options.addOption(OPTION_OBF_LEVEL);

        packageType = DEFAULT_PACKAGE_TYPE;

        kylinConfig = KylinConfig.getInstanceFromEnv();
        kapConfig = KapConfig.wrap(kylinConfig);
        kylinHome = KapConfig.getKylinHomeAtBestEffort().getAbsolutePath();

        cmdExecutor = kylinConfig.getCliCommandExecutor();
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    /**
     * extract the diagnosis package content.
     *
     * @param optionsHelper
     * @param exportDir
     * @throws Exception
     */
    protected abstract void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception;

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        stage = StageEnum.PREPARE;
        TimeZoneUtils.setDefaultTimeZone(kylinConfig);

        String exportDest = optionsHelper.getOptionValue(OPTION_DEST);
        boolean shouldCompress = getBooleanOption(optionsHelper, OPTION_COMPRESS, true);
        boolean submodule = getBooleanOption(optionsHelper, OPTION_SUBMODULE, false);
        includeSystemEnv = getBooleanOption(optionsHelper, OPTION_SYSTEM_ENV, false);

        if (isDiag()) {
            final int threadsNum = getIntOption(optionsHelper, OPTION_THREADS, DEFAULT_PARALLEL_SIZE);
            executorService = Executors.newScheduledThreadPool(threadsNum);
            timerExecutorService = Executors.newScheduledThreadPool(2);
            taskQueue = new LinkedBlockingQueue<>();
            taskStartTime = new ConcurrentHashMap<>();
            if (isDiagFromWeb(optionsHelper)) {
                scheduleDiagProgress(optionsHelper.getOptionValue(OPTION_DIAGID));
            }
            logger.info("Start diagnosis info extraction in {} threads.", threadsNum);
        }

        Preconditions.checkArgument(!StringUtils.isEmpty(exportDest),
                "destDir is not set, exit directly without extracting");

        if (!exportDest.endsWith(SLASH)) {
            exportDest = exportDest + SLASH;
        }

        // create new folder to contain the output
        String packageName = packageType.toLowerCase(Locale.ROOT) + "_"
                + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss", Locale.getDefault(Locale.Category.FORMAT))
                        .format(new Date());
        if (!submodule) {
            exportDest = exportDest + packageName + SLASH;
        }

        exportDir = new File(exportDest);
        FileUtils.forceMkdir(exportDir);

        if (!submodule) {
            dumpBasicDiagInfo();
        }
        stage = StageEnum.EXTRACT;
        mainTaskComplete = false;
        executeExtract(optionsHelper, exportDir);
        mainTaskComplete = true;
        obfDiag(optionsHelper, exportDir);
        // compress to zip package
        if (shouldCompress) {
            stage = StageEnum.COMPRESS;
            File tempZipFile = new File(RandomUtil.randomUUIDStr() + ".zip");
            ZipFileUtils.compressZipFile(exportDir.getAbsolutePath(), tempZipFile.getAbsolutePath());
            FileUtils.cleanDirectory(exportDir);
            String sha256Sum = DatatypeConverter.printHexBinary((HashFunction.SHA256.checksum(tempZipFile)));

            String packageFilename = StringUtils.join(new String[] { ToolUtil.getHostName(),
                    getKylinConfig().getServerPort(), packageName, sha256Sum.substring(0, 6) }, '_');
            File zipFile = new File(exportDir, packageFilename + ".zip");
            FileUtils.moveFile(tempZipFile, zipFile);
            exportDest = zipFile.getAbsolutePath();
            exportDir = new File(exportDest);
        }
        stage = StageEnum.DONE;
        if (isDiagFromWeb(optionsHelper)) {
            reportDiagProgressImmediately(optionsHelper.getOptionValue(OPTION_DIAGID));
            ExecutorServiceUtil.forceShutdown(timerExecutorService);
        }
    }

    String getObfMappingPath() {
        return KylinConfig.getKylinHome() + "/logs/obfuscation-mapping.json";
    }

    protected void obfDiag(OptionsHelper optionsHelper, File rootDir) throws IOException {
        logger.info("Start obf diag file.");
        ObfLevel obfLevel = ObfLevel.valueOf(kylinConfig.getDiagObfLevel());
        if (optionsHelper.hasOption(OPTION_OBF_LEVEL)) {
            obfLevel = ObfLevel.valueOf(optionsHelper.getOptionValue(OPTION_OBF_LEVEL));
        }
        logger.info("Obf level is {}.", obfLevel);
        try (MappingRecorder recorder = new MappingRecorder(null)) {
            ResultRecorder resultRecorder = new ResultRecorder();
            KylinConfObfuscator kylinConfObfuscator = new KylinConfObfuscator(obfLevel, recorder, resultRecorder);
            kylinConfObfuscator.obfuscate(new File(rootDir, SensitiveConfigKeysConstant.CONF_DIR),
                    file -> (file.isFile() && file.getName().startsWith(SensitiveConfigKeysConstant.KYLIN_PROPERTIES)));
        }

        obfIpDiag(rootDir, obfLevel);
    }

    private void obfIpDiag(File rootDir, ObfLevel obfLevel) throws IOException {
        if (!(obfLevel == ObfLevel.OBF && kylinConfig.isDiagIpObfEnabled())) {
            return;
        }
        logger.info("diag start obf ip");
        try (MappingRecorder recorder = new MappingRecorder(null)) {
            ResultRecorder resultRecorder = new ResultRecorder();
            IpObfuscator ipObfuscator = new IpObfuscator(obfLevel, recorder, resultRecorder);
            ipObfuscator.obfuscate(rootDir, null);
        }
        logger.info("diag end obf ip");
    }

    private boolean isDiag() {
        return this instanceof DiagClientTool || this instanceof JobDiagInfoTool
                || this instanceof StreamingJobDiagInfoTool || this instanceof QueryDiagInfoTool
                || this instanceof DiagK8sTool;
    }

    private boolean isDiagFromWeb(OptionsHelper optionsHelper) {
        return isDiag() && optionsHelper.hasOption(OPTION_DIAGID);
    }

    private void scheduleDiagProgress(String diagId) {
        long interval = 3;
        int serverPort = Integer.parseInt(getKylinConfig().getServerPort());
        RestClient restClient = new RestClient("127.0.0.1", serverPort, null, null);
        timerExecutorService.scheduleWithFixedDelay(
                () -> restClient.updateDiagProgress(diagId, getStage(), getProgress(), System.currentTimeMillis()), 0,
                interval, TimeUnit.SECONDS);
    }

    private void reportDiagProgressImmediately(String diagId) {
        int retry = 3;
        int serverPort = Integer.parseInt(getKylinConfig().getServerPort());
        RestClient restClient = new RestClient("127.0.0.1", serverPort, null, null);
        boolean updateSuccess = false;
        while (retry-- > 0 && !updateSuccess) {
            updateSuccess = restClient.updateDiagProgress(diagId, getStage(), getProgress(),
                    System.currentTimeMillis());
        }
    }

    protected void exportSparkLog(File exportDir, long startTime, long endTime, File recordTime, String queryId) {
        QueryHistory query = new QueryDiagInfoTool().getQueryByQueryId(queryId);
        String hostName = query == null ? null
                : AddressUtil.getServerInfo(query.getQueryHistoryInfo().getHostName(),
                        query.getQueryHistoryInfo().getPort());

        // job spark log
        Future sparkLogTask = executorService.submit(() -> {
            recordTaskStartTime(SPARK_LOGS);
            if (StringUtils.isEmpty(queryId)) {
                KylinLogTool.extractFullDiagSparderLog(exportDir, startTime, endTime);
            } else {
                KylinLogTool.extractQueryDiagSparderLog(exportDir, startTime, endTime);
            }
            recordTaskExecutorTimeToFile(SPARK_LOGS, recordTime);
        });

        scheduleTimeoutTask(sparkLogTask, SPARK_LOGS);

        // sparder history rolling eventlog
        Future sparderHistoryTask = executorService.submit(() -> {
            recordTaskStartTime(SPARDER_HISTORY);
            ILogExtractor extractTool = ExtractFactory.create();
            tryRollUpEventLog(query);
            KylinLogTool.extractSparderEventLog(exportDir, startTime, endTime, getKapConfig().getSparkConf(),
                    extractTool, hostName);
            recordTaskExecutorTimeToFile(SPARDER_HISTORY, recordTime);
        });

        scheduleTimeoutTask(sparderHistoryTask, SPARDER_HISTORY);
    }

    public void tryRollUpEventLog(QueryHistory queryHistory) {
        int retry = 3;
        List<RestClient> restClients = Lists.newArrayList();
        if (KylinConfig.getInstanceFromEnv().getMicroServiceMode() != null && queryHistory == null) {
            ClusterManager clusterManager = SpringContext.getApplicationContext().getBean(ClusterManager.class);
            for (ServerInfoResponse queryServer : clusterManager.getQueryServers()) {
                RestClient restClient = new RestClient(queryServer.getHost());
                restClients.add(restClient);
            }
        } else if (queryHistory != null) {
            RestClient restClient = new RestClient(queryHistory.getHostName());
            restClients.add(restClient);
        } else {
            int serverPort = Integer.parseInt(getKylinConfig().getServerPort());
            RestClient restClient = new RestClient("127.0.0.1", serverPort, null, null);
            restClients.add(restClient);
        }

        while (retry-- > 0 && !restClients.isEmpty()) {
            List<RestClient> successClient = Lists.newArrayList();
            for (RestClient restClient : restClients) {
                if (restClient.rollUpEventLog()) {
                    successClient.add(restClient);
                }
            }
            restClients.removeAll(successClient);
        }
    }

    public void extractCommitFile(File exportDir) {
        try {
            for (String commitSHA1File : COMMIT_SHA1_FILES) {
                File commitFile = new File(kylinHome, commitSHA1File);
                if (commitFile.exists()) {
                    Files.copy(commitFile.toPath(), new File(exportDir, commitFile.getName()).toPath());
                }
            }
        } catch (IOException e) {
            logger.warn("Failed to copy commit_SHA1 file.", e);
        }
    }

    public void dumpSystemEnv() throws IOException {
        StringBuilder sb = new StringBuilder("System env:").append("\n");
        Map<String, String> systemEnv = System.getenv();
        for (Map.Entry<String, String> entry : systemEnv.entrySet()) {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        sb.append("System properties:").append("\n");

        Properties systemProperties = System.getProperties();
        for (String key : systemProperties.stringPropertyNames()) {
            sb.append(key).append("=").append(systemProperties.getProperty(key)).append("\n");
        }
        FileUtils.writeStringToFile(new File(exportDir, "system_env"), sb.toString());
    }

    private void dumpBasicDiagInfo() throws Exception {
        // dump commit file
        extractCommitFile(exportDir);

        // dump kylin env
        File kylinEnv = new File(exportDir, "kylin_env");
        FileUtils.writeStringToFile(kylinEnv, ServerInfoUtil.getKylinClientInformation());

        // dump system env and properties
        if (includeSystemEnv) {
            dumpSystemEnv();
        }
    }

    protected void addFile(File srcFile, File destDir) {
        logger.info("copy file: {}", srcFile.getName());

        try {
            FileUtils.forceMkdir(destDir);
        } catch (IOException e) {
            logger.error("Can not create" + destDir, e);
        }

        File destFile = new File(destDir, srcFile.getName());
        String copyCmd = String.format(Locale.ROOT, "cp -r %s %s", srcFile.getAbsolutePath(),
                destFile.getAbsolutePath());
        logger.info("The command is: {}", copyCmd);

        try {
            cmdExecutor.execute(copyCmd, null);
        } catch (Exception e) {
            logger.debug("Failed to execute copyCmd", e);
        }
    }

    protected void addShellOutput(String cmd, File destDir, String filename) {
        addShellOutput(cmd, destDir, filename, false);
    }

    protected void addShellOutput(String cmd, File destDir, String filename, boolean append) {
        addShellOutput(cmd, destDir, filename, append, false);
    }

    protected void addShellOutput(String cmd, File destDir, String filename, boolean append, boolean errorDebug) {
        if (null == cmdExecutor) {
            logger.error("Failed to run cmd because cmdExecutor is null: {}", cmd);
            return;
        }

        try {
            if (null == destDir) {
                destDir = exportDir;
            }

            FileUtils.forceMkdir(destDir);
            String output = cmdExecutor.execute(cmd, null).getCmd();

            FileUtils.writeStringToFile(new File(destDir, filename), output, append);
        } catch (Exception e) {
            if (errorDebug) {
                logger.debug("Failed to run command: {}", cmd, e);
            } else {
                logger.error("Failed to run command: {}", cmd, e);
            }
        }
    }

    public String getStringOption(OptionsHelper optionsHelper, Option option, String defaultVal) {
        return optionsHelper.hasOption(option) ? optionsHelper.getOptionValue(option) : defaultVal;
    }

    public boolean getBooleanOption(OptionsHelper optionsHelper, Option option, boolean defaultVal) {
        return optionsHelper.hasOption(option) ? Boolean.parseBoolean(optionsHelper.getOptionValue(option))
                : defaultVal;
    }

    public int getIntOption(OptionsHelper optionsHelper, Option option, int defaultVal) {
        return optionsHelper.hasOption(option) ? Integer.parseInt(optionsHelper.getOptionValue(option)) : defaultVal;
    }

    public long getLongOption(OptionsHelper optionsHelper, Option option, long defaultVal) {
        return optionsHelper.hasOption(option) ? Long.parseLong(optionsHelper.getOptionValue(option)) : defaultVal;
    }

    public String getStage() {
        return stage.toString();
    }

    public float getProgress() {
        if (executorService == null || getStage().equals("PREPARE")) {
            return 0.0f;
        }
        if (getStage().equals("DONE")) {
            return 1.0f;
        }
        long totalTaskCount = ((ThreadPoolExecutor) executorService).getTaskCount() + 1;
        long completedTaskCount = ((ThreadPoolExecutor) executorService).getCompletedTaskCount()
                + (mainTaskComplete ? 1 : 0);
        return (float) completedTaskCount / totalTaskCount * 0.9f;
    }

    protected void awaitDiagPackageTermination(long timeout) throws InterruptedException {
        try {
            if (executorService != null && !executorService.awaitTermination(timeout, TimeUnit.SECONDS)) {
                ExecutorServiceUtil.forceShutdown(executorService);
                throw new KylinTimeoutException("The query exceeds the set time limit of "
                        + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds()
                        + "s. Current step: Diagnosis packaging. ");
            }
        } catch (InterruptedException e) {
            ExecutorServiceUtil.forceShutdown(executorService);
            logger.debug("diagnosis main wait for all sub task exit...");
            long start = System.currentTimeMillis();
            boolean allSubTaskExit = executorService.awaitTermination(600, TimeUnit.SECONDS);
            logger.warn("diagnosis main task quit by interrupt , all sub task exit ? {} , waiting for {} ms ",
                    allSubTaskExit, System.currentTimeMillis() - start);
            throw e;
        }
    }

    protected void dumpMetadata(String[] metaToolArgs, File recordTime) {
        Future metadataTask = executorService.submit(() -> {
            recordTaskStartTime(METADATA);
            try {
                File metaDir = new File(exportDir, "metadata");
                FileUtils.forceMkdir(metaDir);
                new MetadataTool().execute(metaToolArgs);
            } catch (Exception e) {
                logger.error("Failed to extract metadata.", e);
            }
            recordTaskExecutorTimeToFile(METADATA, recordTime);
        });

        scheduleTimeoutTask(metadataTask, METADATA);
    }

    protected void dumpStreamingSparkLog(String[] sparkToolArgs, File recordTime) {
        Future metadataTask = executorService.submit(() -> {
            recordTaskStartTime(SPARK_STREAMING_LOGS);
            try {
                new StreamingSparkLogTool().execute(sparkToolArgs);
            } catch (Exception e) {
                logger.error("Failed to extract streaming spark log.", e);
            }
            recordTaskExecutorTimeToFile(SPARK_STREAMING_LOGS, recordTime);
        });

        scheduleTimeoutTask(metadataTask, SPARK_STREAMING_LOGS);
    }

    protected void exportRecCandidate(String project, String modelId, File exportDir, boolean full, File recordTime) {
        val recTask = executorService.submit(() -> {
            recordTaskStartTime(REC_CANDIDATE);
            try {
                File recDir = new File(exportDir, "rec_candidate");
                FileUtils.forceMkdir(recDir);
                if (full) {
                    new RecCandidateTool().extractFull(recDir);
                } else {
                    new RecCandidateTool().extractModel(project, modelId, recDir);
                }
            } catch (Exception e) {
                logger.error("Failed to extract rec candidate.", e);
            }
            recordTaskExecutorTimeToFile(REC_CANDIDATE, recordTime);
        });

        scheduleTimeoutTask(recTask, REC_CANDIDATE);
    }

    protected void exportJobInfo(String project, String jobId, File recordTime) {
        exportJobInfo(project, jobId, -1, -1, recordTime);
    }

    protected void exportJobInfo(long startTime, long endTime, File recordTime) {
        exportJobInfo(null, null, startTime, endTime, recordTime);
    }

    private void exportJobInfo(String project, String jobId, long startTime, long endTime, File recordTime) {
        val jobTask = executorService.submit(() -> {
            recordTaskStartTime(JOB_INFO);
            try {
                File jobDir = new File(exportDir, "job_info");
                FileUtils.forceMkdir(jobDir);
                JobInfoTool tool = new JobInfoTool();
                if (jobId != null) {
                    tool.extractJob(jobDir, project, jobId);
                } else {
                    tool.extractFull(jobDir, startTime, endTime);
                }
                tool.extractJobLock(jobDir);
            } catch (Exception e) {
                logger.error("Failed to extract job info.", e);
            }
            recordTaskExecutorTimeToFile(JOB_INFO, recordTime);
        });

        scheduleTimeoutTask(jobTask, JOB_INFO);
    }

    protected void exportFavoriteRule(String project, File recordTime) {
        val favoriteRuleTask = executorService.submit(() -> {
            recordTaskStartTime(FAVORITE_RULE);
            try {
                File favoriteRuleDir = new File(exportDir, "favorite_rule");
                FileUtils.forceMkdir(favoriteRuleDir);
                FavoriteRuleTool tool = new FavoriteRuleTool();
                if (project != null) {
                    tool.extractProject(favoriteRuleDir, project);
                } else {
                    tool.extractFull(favoriteRuleDir);
                }
            } catch (Exception e) {
                logger.error("Failed to extract favorite rules.", e);
            }
            recordTaskExecutorTimeToFile(FAVORITE_RULE, recordTime);
        });

        scheduleTimeoutTask(favoriteRuleTask, FAVORITE_RULE);
    }

    protected void exportAsyncTask(String project, File recordTime) {
        val asyncTaskTask = executorService.submit(() -> {
            recordTaskStartTime(ASYNC_TASK);
            try {
                File asyncTaskDir = new File(exportDir, "async_task");
                FileUtils.forceMkdir(asyncTaskDir);
                AsyncTaskTool tool = new AsyncTaskTool();
                if (project != null) {
                    tool.extractProject(asyncTaskDir, project);
                } else {
                    tool.extractFull(asyncTaskDir);
                }
            } catch (Exception e) {
                logger.error("Failed to extract async task.", e);
            }
            recordTaskExecutorTimeToFile(ASYNC_TASK, recordTime);
        });

        scheduleTimeoutTask(asyncTaskTask, ASYNC_TASK);
    }

    public void exportQueryHistoryOffset(String project, File recordTime) {
        val queryHistoryOffsetTask = executorService.submit(() -> {
            recordTaskStartTime(QUERY_HISTORY_OFFSET);
            try {
                File queryHistoryOffsetDir = new File(exportDir, "query_history_offset");
                FileUtils.forceMkdir(queryHistoryOffsetDir);
                QueryHistoryOffsetTool tool = new QueryHistoryOffsetTool();
                if (project != null) {
                    tool.extractProject(queryHistoryOffsetDir, project);
                } else {
                    tool.extractFull(queryHistoryOffsetDir);
                }
            } catch (Exception e) {
                logger.error("Failed to extract query history offset.", e);
            }
            recordTaskExecutorTimeToFile(QUERY_HISTORY_OFFSET, recordTime);
        });

        scheduleTimeoutTask(queryHistoryOffsetTask, QUERY_HISTORY_OFFSET);
    }

    protected void exportKgLogs(File exportDir, long startTime, long endTime, File recordTime) {
        Future kgLogTask = executorService.submit(() -> {
            recordTaskStartTime(KG_LOGS);
            KylinLogTool.extractKGLogs(exportDir, startTime, endTime);
            recordTaskExecutorTimeToFile(KG_LOGS, recordTime);
        });

        scheduleTimeoutTask(kgLogTask, KG_LOGS);
    }

    protected void exportAuditLog(String[] auditLogToolArgs, File recordTime) {
        Future auditTask = executorService.submit(() -> {
            recordTaskStartTime(AUDIT_LOG);
            try {
                new AuditLogTool(KylinConfig.getInstanceFromEnv()).execute(auditLogToolArgs);
            } catch (Exception e) {
                logger.error("Failed to extract audit log.", e);
            }
            recordTaskExecutorTimeToFile(AUDIT_LOG, recordTime);
        });

        scheduleTimeoutTask(auditTask, AUDIT_LOG);
    }

    protected void exportInfluxDBMetrics(File exportDir, File recordTime) {
        // influxdb metrics
        Future metricsTask = executorService.submit(() -> {
            recordTaskStartTime(SYSTEM_METRICS);
            InfluxDBTool.dumpInfluxDBMetrics(exportDir);
            recordTaskExecutorTimeToFile(SYSTEM_METRICS, recordTime);
        });

        scheduleTimeoutTask(metricsTask, SYSTEM_METRICS);

        // influxdb sla monitor metrics
        Future monitorTask = executorService.submit(() -> {
            recordTaskStartTime(MONITOR_METRICS);
            InfluxDBTool.dumpInfluxDBMonitorMetrics(exportDir);
            recordTaskExecutorTimeToFile(MONITOR_METRICS, recordTime);
        });

        scheduleTimeoutTask(monitorTask, MONITOR_METRICS);
    }

    protected void exportClient(File recordTime) {
        Future clientTask = executorService.submit(() -> {
            recordTaskStartTime(CLIENT);
            CommonInfoTool.exportClientInfo(exportDir);
            recordTaskExecutorTimeToFile(CLIENT, recordTime);
        });

        scheduleTimeoutTask(clientTask, CLIENT);
    }

    protected void exportJstack(File recordTime) {
        Future<?> jstackTask = executorService.submit(() -> {
            recordTaskStartTime(JSTACK);
            JStackTool.extractJstack(exportDir);
            recordTaskExecutorTimeToFile(JSTACK, recordTime);
        });

        scheduleTimeoutTask(jstackTask, JSTACK);

    }

    protected void exportSystemUsageInfo(File recordTime, long startTime, long endTime) {
        Future<?> confTask = executorService.submit(() -> {
            recordTaskStartTime(SYSTEM_USAGE);
            SystemUsageTool.extractUseInfo(exportDir, startTime, endTime);
            recordTaskExecutorTimeToFile(SYSTEM_USAGE, recordTime);
        });

        scheduleTimeoutTask(confTask, SYSTEM_USAGE);
    }

    protected void exportLogFromLoki(File exportDir, Long startTime, Long endTime, List<String> instances,
            File recordTime) {
        Future<?> logTask = executorService.submit(() -> {
            recordTaskStartTime(LOG);
            KylinLogTool.extractKylinLogFromLoki(exportDir, startTime, endTime, instances);
            recordTaskExecutorTimeToFile(LOG, recordTime);
        });

        scheduleTimeoutTask(logTask, LOG);
    }

    protected void exportLogFromWorkingDir(File exportDir, List<String> instances, File recordTime) {
        Future<?> logTask = executorService.submit(() -> {
            recordTaskStartTime(LOG_ON_WORKING_DIR);
            KylinLogTool.extractLogFromWorkingDir(exportDir, instances);
            recordTaskExecutorTimeToFile(LOG_ON_WORKING_DIR, recordTime);
        });

        scheduleTimeoutTask(logTask, LOG_ON_WORKING_DIR);
    }

    protected void exportK8sConf(HttpHeaders headers, File exportDir, final File recordTime, String serverId) {
        Future confTask = executorService.submit(() -> {
            recordTaskStartTime(CONF);
            ConfTool.extractK8sConf(headers, exportDir, serverId);
            recordTaskExecutorTimeToFile(CONF, recordTime);
        });

        scheduleTimeoutTask(confTask, CONF);
    }

    protected void exportConf(File exportDir, final File recordTime, final boolean includeConf,
            final boolean includeBin) {
        // export conf
        if (includeConf) {
            Future confTask = executorService.submit(() -> {
                recordTaskStartTime(CONF);
                ConfTool.extractConf(exportDir);
                recordTaskExecutorTimeToFile(CONF, recordTime);
            });

            scheduleTimeoutTask(confTask, CONF);
        }

        // hadoop conf
        Future hadoopConfTask = executorService.submit(() -> {
            recordTaskStartTime(HADOOP_CONF);
            ConfTool.extractHadoopConf(exportDir);
            recordTaskExecutorTimeToFile(HADOOP_CONF, recordTime);
        });

        scheduleTimeoutTask(hadoopConfTask, HADOOP_CONF);

        // export bin
        if (includeBin) {
            Future binTask = executorService.submit(() -> {
                recordTaskStartTime(BIN);
                ConfTool.extractBin(exportDir);
                recordTaskExecutorTimeToFile(BIN, recordTime);
            });

            scheduleTimeoutTask(binTask, BIN);
        }

        // export hadoop env
        Future hadoopEnvTask = executorService.submit(() -> {
            recordTaskStartTime(HADOOP_ENV);
            CommonInfoTool.exportHadoopEnv(exportDir);
            recordTaskExecutorTimeToFile(HADOOP_ENV, recordTime);
        });

        scheduleTimeoutTask(hadoopEnvTask, HADOOP_ENV);

        // export KYLIN_HOME dir
        Future catcalogTask = executorService.submit(() -> {
            recordTaskStartTime(CATALOG_INFO);
            CommonInfoTool.exportKylinHomeDir(exportDir);
            recordTaskExecutorTimeToFile(CATALOG_INFO, recordTime);
        });

        scheduleTimeoutTask(catcalogTask, CATALOG_INFO);
    }

    protected void scheduleTimeoutTask(Future task, DiagSubTaskEnum taskEnum) {
        if (!KylinConfig.getInstanceFromEnv().getDiagTaskTimeoutBlackList().contains(taskEnum.name())) {
            taskQueue.add(new DiagSubTaskInfo(task, taskEnum));
            logger.info("Add {} to task queue.", taskEnum);
        }
    }

    protected void executeTimeoutTask(LinkedBlockingQueue<DiagSubTaskInfo> tasks) {
        timerExecutorService.submit(() -> {
            while (!tasks.isEmpty()) {
                val info = tasks.poll();
                Future task = info.getTask();
                DiagSubTaskEnum taskEnum = info.getTaskEnum();
                logger.info("Timeout task {} start at {}.", taskEnum, System.currentTimeMillis());
                Long startTime = taskStartTime.get(taskEnum);
                if (startTime == null) {
                    startTime = System.currentTimeMillis();
                    logger.info("Task {} start time is not set now, choose current time {} as task start time.",
                            taskEnum, startTime);
                }
                long endTime = startTime + KylinConfig.getInstanceFromEnv().getDiagTaskTimeout() * 1000;
                long waitTime = endTime - System.currentTimeMillis();
                logger.info("Timeout task {} wait time is {}ms.", taskEnum, waitTime);
                if (waitTime > 0) {
                    try {
                        task.get(waitTime, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        logger.warn(String.format(Locale.ROOT, "Task %s call get function.", task), e);
                    }
                }
                if (task.cancel(true)) {
                    logger.error("Cancel '{}' task.", taskEnum);
                }
                logger.info("Timeout task {} exit at {}.", taskEnum, System.currentTimeMillis());
            }
        });
    }

    protected void recordTaskStartTime(DiagSubTaskEnum subTask) {
        logger.info("Start to dump {}.", subTask);
        taskStartTime.put(subTask, System.currentTimeMillis());
    }

    protected void recordTaskExecutorTimeToFile(DiagSubTaskEnum subTask, File file) {
        long startTime = taskStartTime.get(subTask);
        DiagnosticFilesChecker.writeMsgToFile(subTask.name(), System.currentTimeMillis() - startTime, file);
    }

    public long getDefaultStartTime() {
        return DateTime.now().minusDays(getKapConfig().getExtractionStartTimeDays() - 1).withTimeAtStartOfDay()
                .getMillis();
    }

    public long getDefaultEndTime() {
        return DateTime.now().plusDays(1).minus(1).withTimeAtStartOfDay().getMillis();
    }

    public void exportJobSparkLog(File exportDir, final File recordTime, String project, String jobId,
            ExecutablePO job) {
        // job spark log
        Future sparkLogTask = executorService.submit(() -> {
            recordTaskStartTime(SPARK_LOGS);
            KylinLogTool.extractSparkLog(exportDir, project, jobId);
            recordTaskExecutorTimeToFile(SPARK_LOGS, recordTime);
        });

        scheduleTimeoutTask(sparkLogTask, SPARK_LOGS);

        // extract job step eventLogs
        Future eventLogTask = executorService.submit(() -> {
            try {
                if (ClassUtil.forName(job.getType(), AbstractExecutable.class)
                        .newInstance() instanceof DefaultExecutable) {
                    recordTaskStartTime(JOB_EVENTLOGS);
                    YarnApplicationTool yarnApplicationTool = new YarnApplicationTool();
                    val appIds = yarnApplicationTool.extract(project, jobId);
                    KylinConfig config = getConfigForModelOrProjectLevel(job.getTargetModelId(), project);
                    Map<String, String> sparkConf = config.getSparkConfigOverride();
                    KylinLogTool.extractJobEventLogs(exportDir, appIds, sparkConf);
                    recordTaskExecutorTimeToFile(JOB_EVENTLOGS, recordTime);
                }
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                logger.error("Class <{}> not found or cannot be created.", job.getType(), e);
            }
        });

        scheduleTimeoutTask(eventLogTask, JOB_EVENTLOGS);

        // job tmp
        Future jobTmpTask = executorService.submit(() -> {
            recordTaskStartTime(JOB_TMP);
            KylinLogTool.extractJobTmp(exportDir, project, jobId);
            recordTaskExecutorTimeToFile(JOB_TMP, recordTime);
        });

        scheduleTimeoutTask(jobTmpTask, JOB_TMP);
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

    protected void exportCandidateLog(File exportDir, File recordTime, long startTime, long endTime) {
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
}

@Getter
@AllArgsConstructor
class DiagSubTaskInfo {
    private final Future task;
    private final DiagSubTaskEnum taskEnum;
}
