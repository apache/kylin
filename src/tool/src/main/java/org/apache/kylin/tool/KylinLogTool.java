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

import static org.apache.kylin.common.constant.Constants.KE_LICENSE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.query.util.ILogExtractor;
import org.apache.kylin.tool.util.ToolUtil;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import lombok.val;
import scala.collection.JavaConversions;

public class KylinLogTool {
    public static final long DAY = 24 * 3600 * 1000L;
    public static final String SECOND_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    private static final Logger logger = LoggerFactory.getLogger("diag");
    private static final String CHARSET_NAME = Charset.defaultCharset().name();
    private static final String LOG_TIME_PATTERN = "^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})";

    // 2019-11-11 09:30:04,628 INFO  [FetchJobWorker(project:test_fact)-p-94-t-94] threadpool.NDefaultScheduler : start check project test_fact
    private static final String LOG_PATTERN = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}) ([^ ]*)[ ]+\\[(.*)\\] ([^: ]*) :([\\n\\r. ]*)";

    private static final String QUERY_LOG_PATTERN = "Query ([0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})";

    private static final int EXTRA_LINES = 100;

    private static final String ROLL_LOG_FILE_NAME_PREFIX = "events";

    private static final String SYSTEM_PROPERTIES = "System Properties";

    private static final Set<String> kylinLogPrefix = Sets.newHashSet("kylin.log", "kylin.schedule.log",
            "kylin.query.log", "kylin.smart.log", "kylin.build.log");

    private static final Set<String> queryDiagExcludedLogs = Sets.newHashSet("kylin.log", "kylin.schedule.log",
            "kylin.smart.log", "kylin.build.log");

    private static ExtractLogByRangeTool DEFAULT_EXTRACT_LOG_BY_RANGE = new ExtractLogByRangeTool(LOG_PATTERN,
            LOG_TIME_PATTERN, SECOND_DATE_FORMAT);

    private KylinLogTool() {
    }

    // 2019-11-11 03:24:52,342 DEBUG [JobWorker(prj:doc_smart,jobid:8a13964c)-965] job.NSparkExecutable : Copied metadata to the target metaUrl, delete the temp dir: /tmp/kylin_job_meta204633716010108932
    @VisibleForTesting
    public static String getJobLogPattern(String jobId) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(jobId));
        return String.format(Locale.ROOT, "%s(.*JobWorker.*jobid:%s.*)|%s.*%s", LOG_TIME_PATTERN, jobId.substring(0, 8),
                LOG_TIME_PATTERN, jobId);
    }

    // group(1) is first  ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})
    // group(2) is (.*JobWorker.*jobid:%s.*)
    // group(3) is second ([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})
    @VisibleForTesting
    public static String getJobTimeString(Matcher matcher) {
        String result = matcher.group(1);
        if (StringUtils.isEmpty(result)) {
            result = matcher.group(3);
        }
        return result;
    }

    // 2019-11-11 09:30:04,004 INFO  [Query 4e3350d5-1cd9-450f-ac7e-5859939bedf1-125] service.QueryService : The original query: select * from ssb.SUPPLIER
    private static String getQueryLogPattern(String queryId) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(queryId));
        return String.format(Locale.ROOT, "%s(.*Query %s.*)", LOG_TIME_PATTERN, queryId);
    }

    private static boolean checkTimeoutTask(long timeout, String task) {
        return !KylinConfig.getInstanceFromEnv().getDiagTaskTimeoutBlackList().contains(task)
                && System.currentTimeMillis() > timeout;
    }

    private static void extractAllIncludeLogs(File[] logFiles, File destDir, long timeout) throws IOException {
        String[] allIncludeLogs = { "kylin.gc.", "shell.", "kylin.out", "diag.log" };
        for (File logFile : logFiles) {
            for (String includeLog : allIncludeLogs) {
                if (checkTimeoutTask(timeout, "LOG")) {
                    logger.error("Cancel 'LOG:all' task.");
                    return;
                }
                if (logFile.getName().startsWith(includeLog)) {
                    Files.copy(logFile.toPath(), new File(destDir, logFile.getName()).toPath());
                }
            }
        }
    }

    private static void extractPartIncludeLogByDay(File[] logFiles, String startDate, String endDate, File destDir,
            long timeout) throws IOException {
        String[] partIncludeLogByDay = { "access_log." };
        for (File logFile : logFiles) {
            for (String includeLog : partIncludeLogByDay) {
                if (checkTimeoutTask(timeout, "LOG")) {
                    logger.error("Cancel 'LOG:partByDay' task.");
                    return;
                }
                if (logFile.getName().startsWith(includeLog)) {
                    String date = logFile.getName().split("\\.")[1];
                    if (date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0) {
                        Files.copy(logFile.toPath(), new File(destDir, logFile.getName()).toPath());
                    }
                }
            }
        }
    }

    private static void extractPartIncludeLogByMs(File[] logFiles, long start, long end, File destDir, long timeout)
            throws IOException {
        String[] partIncludeLogByMs = { "jstack.timed.log" };
        for (File logFile : logFiles) {
            for (String includeLog : partIncludeLogByMs) {
                if (checkTimeoutTask(timeout, "LOG")) {
                    logger.error("Cancel 'LOG:partByMs' task.");
                    return;
                }
                if (logFile.getName().startsWith(includeLog)) {
                    long time = Long.parseLong(logFile.getName().split("\\.")[3]);
                    if (time >= start && time <= end) {
                        Files.copy(logFile.toPath(), new File(destDir, logFile.getName()).toPath());
                    }
                }
            }
        }
    }

    /**
     * extract the specified log from kylin logs dir.
     *
     * @param exportDir
     */

    public static void extractOtherLogs(File exportDir, long start, long end) {
        File destDir = new File(exportDir, "logs");
        SimpleDateFormat logFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        String startDate = logFormat.format(new Date(start));
        String endDate = logFormat.format(new Date(end));
        logger.debug("logs startDate : {}, endDate : {}", startDate, endDate);
        long duration = KylinConfig.getInstanceFromEnv().getDiagTaskTimeout() * 1000L;
        long timeout = System.currentTimeMillis() + duration;
        try {
            FileUtils.forceMkdir(destDir);
            File kylinLogDir = new File(ToolUtil.getLogFolder());
            if (kylinLogDir.exists()) {
                File[] logFiles = kylinLogDir.listFiles();
                if (null == logFiles) {
                    logger.error("Failed to list kylin logs dir: {}", kylinLogDir);
                    return;
                }
                extractAllIncludeLogs(logFiles, destDir, timeout);
                extractPartIncludeLogByDay(logFiles, startDate, endDate, destDir, timeout);
                extractPartIncludeLogByMs(logFiles, start, end, destDir, timeout);
            }
        } catch (Exception e) {
            logger.error("Failed to extract the logs from kylin logs dir, ", e);
        }
    }

    /**
     * extract kylin log by jobId
     * for job diagnosis
     *
     * @param exportDir
     * @param jobId
     */
    public static void extractKylinLog(File exportDir, String jobId) {
        extractKylinLog(exportDir, jobId, null, null, null);
    }

    /**
     * extract kylin log by time range
     * for full diagnosis
     *
     * @param exportDir
     * @param startTime
     * @param endTime
     */
    public static void extractKylinLog(File exportDir, long startTime, long endTime) {
        extractKylinLog(exportDir, null, startTime, endTime, null);
    }

    /**
     * extract kylin log by time range
     * for query diagnosis
     *
     * @param exportDir
     * @param startTime
     * @param endTime
     * @param queryId
     */
    public static void extractKylinLog(File exportDir, long startTime, long endTime, String queryId) {
        extractKylinLog(exportDir, null, startTime, endTime, queryId);
    }

    private static Pair<String, String> getTimeRangeFromLogFileByJobId(String jobId, File logFile) {
        return getTimeRangeFromLogFileByJobId(jobId, logFile, false);
    }

    private static Pair<String, String> getTimeRangeFromLogFileByJobId(String jobId, File logFile,
            boolean onlyStartTime) {
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(logFile);

        String dateStart = null;
        String dateEnd = null;
        try (InputStream in = new FileInputStream(logFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, CHARSET_NAME))) {
            Pattern pattern = Pattern.compile(getJobLogPattern(jobId));

            String log;
            while ((log = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(log);
                if (matcher.find()) {
                    if (Objects.isNull(dateStart)) {
                        dateStart = getJobTimeString(matcher);
                        if (onlyStartTime) {
                            return new Pair<>(dateStart, dateStart);
                        }
                    }
                    dateEnd = getJobTimeString(matcher);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get time range from logFile:{}, jobId:{}, onlyStartTime: {}",
                    logFile.getAbsolutePath(), jobId, onlyStartTime, e);
        }
        return new Pair<>(dateStart, dateEnd);
    }

    private static Pair<String, String> getTimeRangeFromLogFileByJobId(String jobId, File[] kylinLogs) {
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(kylinLogs);

        Pair<String, String> timeRangeResult = new Pair<>();
        if (0 == kylinLogs.length) {
            return timeRangeResult;
        }

        List<File> logFiles = Stream.of(kylinLogs).sorted(Comparator.comparing(File::getName))
                .collect(Collectors.toList());

        int i = 0;
        while (i < logFiles.size()) {
            Pair<String, String> timeRange = getTimeRangeFromLogFileByJobId(jobId, logFiles.get(i++));
            if (null != timeRange.getFirst() && null != timeRange.getSecond()) {
                timeRangeResult.setFirst(timeRange.getFirst());
                timeRangeResult.setSecond(timeRange.getSecond());
                break;
            }
        }

        while (i < logFiles.size()) {
            String dateStart = getTimeRangeFromLogFileByJobId(jobId, logFiles.get(i++), true).getFirst();
            if (null == dateStart) {
                break;
            }
            if (dateStart.compareTo(timeRangeResult.getFirst()) < 0) {
                timeRangeResult.setFirst(dateStart);
            }
        }

        return timeRangeResult;
    }

    private static boolean isKylinLogFile(String fileName) {
        for (String name : kylinLogPrefix) {
            if (StringUtils.startsWith(fileName, name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isQueryDiagExcludedLogs(String fileName) {
        for (String name : queryDiagExcludedLogs) {
            if (StringUtils.startsWith(fileName, name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * extract kylin query log by queryId
     *
     * @param queryLogFile
     * @param queryId
     * @param distFile
     */
    private static void extractQueryLogByQueryId(File queryLogFile, String queryId, File distFile) {
        Preconditions.checkNotNull(queryLogFile);
        Preconditions.checkNotNull(queryId);
        Preconditions.checkNotNull(distFile);
        final String charsetName = Charset.defaultCharset().name();
        try (InputStream in = new FileInputStream(queryLogFile);
                OutputStream out = new FileOutputStream(distFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, charsetName));
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charsetName))) {

            boolean extract = false;
            boolean stdLogNotFound = true;
            Pattern pattern = Pattern.compile(QUERY_LOG_PATTERN);
            String queryLog;
            while ((queryLog = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(queryLog);

                if (matcher.find()) {
                    stdLogNotFound = false;
                    if (queryId.equals(matcher.group(1))) {
                        extract = true;
                        bw.write(queryLog);
                        bw.write('\n');
                    } else {
                        extract = false;
                    }
                } else if (extract || stdLogNotFound) {
                    bw.write(queryLog);
                    bw.write('\n');
                }
            }
        } catch (IOException e) {
            logger.error("Failed to extract query log from {} to {}", queryLogFile.getAbsolutePath(),
                    distFile.getAbsolutePath(), e);
        }
    }

    /**
     * extract sparder log by time range
     * for query diagnosis
     *
     * @param exportDir
     * @param startTime
     * @param endTime
     */
    public static void extractQueryDiagSparderLog(File exportDir, long startTime, long endTime) {
        File sparkLogsDir = new File(exportDir, "spark_logs");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        try {
            FileUtils.forceMkdir(sparkLogsDir);
            if (!checkTimeRange(startTime, endTime)) {
                return;
            }

            DateTime date = new DateTime(startTime);
            while (date.getMillis() <= endTime) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Sparder log task is interrupt");
                }
                String sourceLogsPath = getSourceLogPath(kylinConfig, date);
                FileSystem sourceFileSystem = HadoopUtil.getFileSystem(sourceLogsPath);
                if (sourceFileSystem.exists(new Path(sourceLogsPath))) {
                    File sparkLogsDateDir = new File(sparkLogsDir, date.toString("yyyy-MM-dd"));
                    FileUtils.forceMkdir(sparkLogsDateDir);

                    FileStatus[] sourceAppFiles = sourceFileSystem.listStatus(new Path(sourceLogsPath));
                    for (FileStatus sourceAppFile : sourceAppFiles) {
                        extractAppDirSparderLog(sourceAppFile, sparkLogsDateDir, startTime, endTime);
                    }
                }
                if (kylinConfig.cleanDiagTmpFile()) {
                    sourceFileSystem.delete(new Path(sourceLogsPath), true);
                    logger.info("Clean tmp spark logs {}", sourceLogsPath);
                }
                date = date.plusDays(1);
            }
        } catch (Exception e) {
            logger.error("Failed to extract query sparder log, ", e);
        }
    }

    private static void extractAppDirSparderLog(FileStatus fileStatus, File sparkLogsDateDir, long startTime,
            long endTime) {
        File exportAppDir = new File(sparkLogsDateDir, fileStatus.getPath().getName());

        try {
            FileUtils.forceMkdir(exportAppDir);
            FileSystem sourceExecutorFiles = HadoopUtil.getFileSystem(fileStatus.getPath());
            if (sourceExecutorFiles.exists(fileStatus.getPath())) {
                FileStatus[] executors = sourceExecutorFiles.listStatus(fileStatus.getPath());
                for (FileStatus sourceExecutorFile : executors) {
                    FileSystem executorFileSystem = HadoopUtil.getFileSystem(sourceExecutorFile.getPath());
                    Pair<String, String> timeRange = new Pair<>(new DateTime(startTime).toString(SECOND_DATE_FORMAT),
                            new DateTime(endTime).toString(SECOND_DATE_FORMAT));
                    File exportExecutorsDir = new File(exportAppDir, sourceExecutorFile.getPath().getName());
                    extractExecutorByTimeRange(executorFileSystem, timeRange, exportExecutorsDir,
                            sourceExecutorFile.getPath());

                    if (FileUtils.sizeOf(exportExecutorsDir) == 0) {
                        FileUtils.deleteQuietly(exportExecutorsDir);
                    }
                }
            }
            if (FileUtils.sizeOf(exportAppDir) == 0) {
                FileUtils.deleteQuietly(exportAppDir);
            }
        } catch (Exception e) {
            logger.error("Failed to extract sparder log in application directory, ", e);
        }
    }

    private static void extractExecutorByTimeRange(FileSystem logFile, Pair<String, String> timeRange, File distFile,
            Path path) {
        Preconditions.checkNotNull(logFile);
        Preconditions.checkNotNull(path);

        final String charsetName = Charset.defaultCharset().name();
        try (FSDataInputStream in = logFile.open(path);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, charsetName));
                OutputStream out = new FileOutputStream(distFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charsetName))) {
            boolean extract = false;
            boolean stdLogNotFound = true;
            Pattern pattern = Pattern.compile(LOG_PATTERN);
            String log;
            while ((log = br.readLine()) != null) {
                Matcher matcher = pattern.matcher(log);

                if (matcher.find()) {
                    stdLogNotFound = false;
                    String logDate = matcher.group(1);
                    if (logDate.compareTo(timeRange.getSecond()) > 0) {
                        break;
                    }

                    if (extract || logDate.compareTo(timeRange.getFirst()) >= 0) {
                        extract = true;
                        bw.write(log);
                        bw.write('\n');
                    }
                } else if (extract || stdLogNotFound) {
                    bw.write(log);
                    bw.write('\n');
                }
            }
        } catch (IOException e) {
            logger.error("Failed to extract executor from {} to {}", path, distFile.getAbsolutePath(), e);
        }
    }

    /**
     * extract kylin log
     *
     * @param exportDir
     * @param jobId
     * @param startTime
     * @param endTime
     */
    private static void extractKylinLog(File exportDir, String jobId, Long startTime, Long endTime, String queryId) {
        File destLogDir = new File(exportDir, "logs");

        try {
            FileUtils.forceMkdir(destLogDir);

            File logsDir = new File(ToolUtil.getKylinHome(), "logs");
            if (!logsDir.exists()) {
                logger.error("Can not find the logs dir: {}", logsDir);
                return;
            }

            File[] kylinLogs;
            if (null != queryId) {
                kylinLogs = logsDir.listFiles(pathname -> isQueryDiagExcludedLogs(pathname.getName()));
            } else {
                kylinLogs = logsDir.listFiles(pathname -> isKylinLogFile(pathname.getName()));
            }
            if (null == kylinLogs || 0 == kylinLogs.length) {
                logger.error("Can not find the kylin.log file!");
                return;
            }

            Pair<String, String> timeRange;
            if (null != jobId) {
                timeRange = getTimeRangeFromLogFileByJobId(jobId, kylinLogs);
                Preconditions.checkArgument(
                        null != timeRange.getFirst() && null != timeRange.getSecond()
                                && timeRange.getFirst().compareTo(timeRange.getSecond()) <= 0,
                        "Can not get time range from log files by jobId: {}", jobId);
            } else {
                timeRange = new Pair<>(new DateTime(startTime).toString(SECOND_DATE_FORMAT),
                        new DateTime(endTime).toString(SECOND_DATE_FORMAT));
            }

            logger.info("Extract kylin log from {} to {} .", timeRange.getFirst(), timeRange.getSecond());
            long start = System.currentTimeMillis();
            long duration = KylinConfig.getInstanceFromEnv().getDiagTaskTimeout() * 1000L;
            long timeout = start + duration;
            for (File logFile : kylinLogs) {
                if (checkTimeoutTask(timeout, "LOG")) {
                    logger.error("Cancel 'LOG:kylin.log' task.");
                    break;
                }
                DEFAULT_EXTRACT_LOG_BY_RANGE.extractLogByRange(logFile, timeRange, destLogDir);
            }
        } catch (Exception e) {
            logger.error("Failed to extract kylin.log, ", e);
        }
    }

    /**
     * extract kylin query log
     *
     * @param exportDir
     * @param queryId
     */
    public static void extractKylinQueryLog(File exportDir, String queryId) {
        File destLogDir = new File(exportDir, "logs");

        try {
            FileUtils.forceMkdir(destLogDir);

            File logsDir = new File(ToolUtil.getKylinHome(), "logs");
            File[] kylinQueryLogs = logsDir
                    .listFiles(pathname -> StringUtils.startsWith(pathname.getName(), "kylin.query.log"));

            if (kylinQueryLogs == null || kylinQueryLogs.length == 0) {
                logger.error("Can not fond the kylin.query.log file!");
            }

            for (File kylinQueryLog : kylinQueryLogs) {
                extractQueryLogByQueryId(kylinQueryLog, queryId, new File(destLogDir, kylinQueryLog.getName()));
            }
        } catch (Exception e) {
            logger.error("Failed to extract kylin.query.log, ", e);
        }
    }

    /**
     * extract the spark executor log by project and jobId.
     * for job diagnosis
     *
     * @param exportDir
     * @param project
     * @param jobId
     */
    public static void extractSparkLog(File exportDir, String project, String jobId) {
        File sparkLogsDir = new File(exportDir, "spark_logs");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        try {
            FileUtils.forceMkdir(sparkLogsDir);
            String sourceLogsPath = SparkLogExtractorFactory.create(kylinConfig).getSparkLogsDir(project, kylinConfig);
            FileSystem fs = HadoopUtil.getFileSystem(sourceLogsPath);
            String jobPath = sourceLogsPath + "/*/" + jobId;
            FileStatus[] fileStatuses = fs.globStatus(new Path(jobPath));
            if (null == fileStatuses || fileStatuses.length == 0) {
                logger.error("Can not find the spark logs: {}", jobPath);
                return;
            }
            for (FileStatus fileStatus : fileStatuses) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("spark log task is interrupt");
                }
                fs.copyToLocalFile(false, fileStatus.getPath(), new Path(sparkLogsDir.getAbsolutePath()), true);
            }
            if (kylinConfig.cleanDiagTmpFile()) {
                logger.info("Clean tmp spark logs {}", sourceLogsPath);
                fs.delete(new Path(sourceLogsPath), true);
            }
        } catch (Exception e) {
            logger.error("Failed to extract spark log, ", e);
        }
    }

    /**
     * Extract the sparder history event log for job diagnosis. Sparder conf must set "spark.eventLog.rolling.enabled=true"
     * otherwise it always increases.
     */
    public static void extractSparderEventLog(File exportDir, long startTime, long endTime,
            Map<String, String> sparderConf, ILogExtractor extractTool) {
        val sparkLogsDir = new File(exportDir, "sparder_history");
        val fs = HadoopUtil.getFileSystem(extractTool.getSparderEvenLogDir());
        val validApps = extractTool.getValidSparderApps(startTime, endTime);
        JavaConversions.asJavaCollection(validApps).forEach(app -> {
            try {
                if (!sparkLogsDir.exists()) {
                    FileUtils.forceMkdir(sparkLogsDir);
                }
                String fileAppId = app.getPath().getName().split("#")[0].replace(extractTool.ROLL_LOG_DIR_NAME_PREFIX(),
                        "");
                File localFile = new File(sparkLogsDir, fileAppId);
                copyValidLog(fileAppId, startTime, endTime, app, fs, localFile);
            } catch (Exception e) {
                logger.error("Failed to extract sparder eventLog.", e);
            }
        });
    }

    public static void hideLicenseString(File file) throws IOException {
        if (!file.exists() || !file.isFile()) {
            logger.error("file {} is not exist", file.getAbsolutePath());
            return;
        }
        logger.info("hide license file {}", file.getAbsolutePath());
        File tempFile = new File(file.getParent(), RandomUtil.randomUUIDStr());
        try {
            FileUtils.moveFile(file, tempFile);
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(tempFile), Charset.defaultCharset().name()));
                    BufferedWriter bw = new BufferedWriter(
                            new OutputStreamWriter(new FileOutputStream(file), Charset.defaultCharset().name()))) {
                String line;
                while ((line = br.readLine()) != null) {
                    val map = JsonUtil.readValue(line, HashMap.class);
                    if (map.containsKey(SYSTEM_PROPERTIES)) {
                        Map<String, String> systemProperties = (Map) map.get(SYSTEM_PROPERTIES);
                        systemProperties.put(KE_LICENSE, "***");
                        line = JsonUtil.writeValueAsString(map);
                    }
                    bw.write(line);
                    bw.newLine();
                }
            }
        } finally {
            FileUtils.deleteQuietly(tempFile);
        }
    }

    private static void copyValidLog(String appId, long startTime, long endTime, FileStatus fileStatus, FileSystem fs,
            File localFile) throws IOException, InterruptedException {
        FileStatus[] eventStatuses = fs.listStatus(new Path(fileStatus.getPath().toUri()));
        for (FileStatus status : eventStatuses) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Event log task is interrupt");
            }
            boolean valid = false;
            boolean isFirstLogFile = false;
            String[] att = status.getPath().getName().replace("_" + appId, "").split("_");
            if (att.length >= 3 && ROLL_LOG_FILE_NAME_PREFIX.equals(att[0])) {

                long begin = Long.parseLong(att[2]);
                long end = System.currentTimeMillis();
                if (att.length == 4) {
                    end = Long.parseLong(att[3]);
                }

                boolean isTimeValid = begin <= endTime && end >= startTime;
                isFirstLogFile = "1".equals(att[1]);
                if (isTimeValid || isFirstLogFile) {
                    valid = true;
                }
            }
            copyToLocalFile(valid, localFile, status, fs, isFirstLogFile);
        }
    }

    private static void copyToLocalFile(boolean valid, File localFile, FileStatus status, FileSystem fs,
            boolean isFirstLogFile) throws IOException {
        if (valid) {
            if (!localFile.exists()) {
                FileUtils.forceMkdir(localFile);
            }
            fs.copyToLocalFile(false, status.getPath(), new Path(localFile.getAbsolutePath()), true);
            if (isFirstLogFile) {
                hideLicenseString(new File(localFile, status.getPath().getName()));
            }
        }
    }

    public static void extractJobEventLogs(File exportDir, Set<String> appIds, Map<String, String> sparkConf) {
        try {
            String logDir = sparkConf.get("spark.eventLog.dir").trim();
            boolean eventEnabled = Boolean.parseBoolean(sparkConf.get("spark.eventLog.enabled").trim());
            if (!eventEnabled || StringUtils.isBlank(logDir)) {
                return;
            }

            File jobLogsDir = new File(exportDir, "job_history");
            FileUtils.forceMkdir(jobLogsDir);
            FileSystem fs = HadoopUtil.getFileSystem(logDir);

            for (String appId : appIds) {
                if (StringUtils.isNotEmpty(appId)) {
                    copyJobEventLog(fs, appId, logDir, jobLogsDir);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract job eventLog.", e);
        }
    }

    private static void copyJobEventLog(FileSystem fs, String appId, String logDir, File exportDir) throws Exception {
        if (StringUtils.isBlank(appId)) {
            logger.warn("Failed to extract step eventLog due to the appId is empty.");
            return;
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Job eventLog task is interrupt");
        }
        String eventPath = logDir + "/" + appId + "*";

        FileStatus[] eventLogsStatus = fs.globStatus(new Path(eventPath));
        Path[] listedPaths = FileUtil.stat2Paths(eventLogsStatus);

        logger.info("Copy appId {}.", appId);
        for (Path path : listedPaths) {
            FileStatus fileStatus = fs.getFileStatus(path);
            if (fileStatus != null) {
                fs.copyToLocalFile(false, fileStatus.getPath(), new Path(exportDir.getAbsolutePath()), true);
            }
        }
    }

    private static File getJobTmpDir(File exportDir) throws IOException {
        File jobTmpDir = new File(exportDir, "job_tmp");
        FileUtils.forceMkdir(jobTmpDir);
        return jobTmpDir;
    }

    private static boolean notExistHdfsPath(FileSystem fs, String hdfsPath) throws IOException {
        if (!fs.exists(new Path(hdfsPath))) {
            logger.error("Can not find the hdfs path: {}", hdfsPath);
            return true;
        }
        return false;
    }

    /**
     * extract the job tmp by project and jobId.
     * for job diagnosis.
     *
     * @param exportDir
     * @param project
     * @param jobId
     */
    public static void extractJobTmp(File exportDir, String project, String jobId) {
        try {
            File jobTmpDir = getJobTmpDir(exportDir);
            String hdfsPath = ToolUtil.getJobTmpDir(project, jobId);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (notExistHdfsPath(fs, hdfsPath)) {
                return;
            }

            fs.copyToLocalFile(false, new Path(hdfsPath), new Path(jobTmpDir.getAbsolutePath()), true);
        } catch (Exception e) {
            logger.error("Failed to extract job_tmp, ", e);
        }
    }

    public static void extractJobTmpCandidateLog(File exportDir, String project, long startTime, long endTime) {
        logger.info("extract job tmp candidate log for {}", project);
        try {
            File jobTmpDir = getJobTmpDir(exportDir);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            String hdfsPath = ToolUtil.getHdfsJobTmpDir(project);
            if (notExistHdfsPath(fs, hdfsPath)) {
                return;
            }
            FileStatus[] fileStatuses = fs.listStatus(new Path(hdfsPath));
            for (FileStatus fileStatus : fileStatuses) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Candidate log task is interrupt");
                }
                String fileName = fileStatus.getPath().getName();
                if (fileName.startsWith(project) && fileName.endsWith(".zip")) {
                    String dateString = fileName.substring(project.length() + 1, fileName.length() - 4);
                    long date = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS",
                            Locale.getDefault(Locale.Category.FORMAT)).parse(dateString).getTime();
                    if (startTime <= date && date <= endTime) {
                        fs.copyToLocalFile(false, fileStatus.getPath(), new Path(jobTmpDir.getAbsolutePath()), true);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to extract job_tmp candidate log", e);
        }
    }

    /**
     * extract the sparder log range by startime and endtime
     * for full diagnosis
     *
     * @param exportDir
     * @param startTime
     * @param endTime
     */
    public static void extractFullDiagSparderLog(File exportDir, long startTime, long endTime) {
        File sparkLogsDir = new File(exportDir, "spark_logs");
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        try {
            FileUtils.forceMkdir(sparkLogsDir);
            if (!checkTimeRange(startTime, endTime)) {
                return;
            }

            long days = (endTime - startTime) / DAY;
            if (days > 31) {
                logger.error("time range is too large, startTime: {}, endTime: {}, days: {}", startTime, endTime, days);
                return;
            }

            DateTime date = new DateTime(startTime).withTimeAtStartOfDay();
            while (date.getMillis() <= (endTime + DAY - 1)) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("sparder log task is interrupt");
                }
                String sourceLogsPath = getSourceLogPath(kylinConfig, date);
                FileSystem fs = HadoopUtil.getFileSystem(sourceLogsPath);
                if (fs.exists(new Path(sourceLogsPath))) {
                    fs.copyToLocalFile(false, new Path(sourceLogsPath), new Path(sparkLogsDir.getAbsolutePath()), true);
                }
                if (kylinConfig.cleanDiagTmpFile()) {
                    fs.delete(new Path(sourceLogsPath), true);
                    logger.info("Clean tmp spark logs {}", sourceLogsPath);
                }
                date = date.plusDays(1);
            }

        } catch (Exception e) {
            logger.error("Failed to extract sparder log, ", e);
        }
    }

    private static String getSourceLogPath(KylinConfig kylinConfig, DateTime date) {
        return SparkLogExtractorFactory.create(kylinConfig).getSparderLogsDir(kylinConfig) + File.separator
                + date.toString("yyyy-MM-dd");
    }

    private static boolean checkTimeRange(long startTime, long endTime) {
        if (endTime < startTime) {
            logger.error("Time range is error, endTime: {} < startTime: {}", endTime, startTime);
            return false;
        }
        return true;
    }

    public static void extractKGLogs(File exportDir, long startTime, long endTime) {
        File kgLogsDir = new File(exportDir, "logs");
        try {
            FileUtils.forceMkdir(kgLogsDir);

            if (endTime < startTime) {
                logger.error("endTime: {} < startTime: {}", endTime, startTime);
                return;
            }

            File logsDir = new File(ToolUtil.getKylinHome(), "logs");
            if (!logsDir.exists()) {
                logger.error("Can not find the logs dir: {}", logsDir);
                return;
            }

            File[] kgLogs = logsDir.listFiles(pathname -> pathname.getName().startsWith("guardian.log"));
            if (null == kgLogs || 0 == kgLogs.length) {
                logger.error("Can not find the guardian.log file!");
                return;
            }

            Pair<String, String> timeRange = new Pair<>(new DateTime(startTime).toString(SECOND_DATE_FORMAT),
                    new DateTime(endTime).toString(SECOND_DATE_FORMAT));

            logger.info("Extract guardian log from {} to {} .", timeRange.getFirst(), timeRange.getSecond());

            for (File logFile : kgLogs) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Kg log task is interrupt");
                }
                DEFAULT_EXTRACT_LOG_BY_RANGE.extractLogByRange(logFile, timeRange, kgLogsDir);
            }
        } catch (Exception e) {
            logger.error("Failed to extract kg log!", e);
        }
    }

    public static class ExtractLogByRangeTool {
        private String logPattern;
        private String logTimePattern;
        private String secondDateFormat;

        public ExtractLogByRangeTool(String logPattern, String logTimePattern, String secondDateFormat) {
            this.logPattern = logPattern;
            this.logTimePattern = logTimePattern;
            this.secondDateFormat = secondDateFormat;
        }

        public ExtractLogByRangeTool(String logPattern, String secondDateFormat) {
            this.logPattern = logPattern;
            this.logTimePattern = logPattern;
            this.secondDateFormat = secondDateFormat;
        }

        public String getFirstTimeByLogFile(File logFile) {
            try (InputStream in = new FileInputStream(logFile);
                    BufferedReader br = new BufferedReader(new InputStreamReader(in, CHARSET_NAME))) {
                Pattern pattern = Pattern.compile(logTimePattern);
                String log;
                while ((log = br.readLine()) != null) {
                    Matcher matcher = pattern.matcher(log);
                    if (matcher.find()) {
                        return matcher.group(1);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to get first time by log file: {}", logFile.getAbsolutePath(), e);
            }
            return null;
        }

        public void extractLogByRange(File logFile, Pair<String, String> timeRange, File destLogDir)
                throws IOException {
            String lastDate = new DateTime(logFile.lastModified()).toString(secondDateFormat);
            if (lastDate.compareTo(timeRange.getFirst()) < 0) {
                return;
            }

            String logFirstTime = getFirstTimeByLogFile(logFile);
            if (Objects.isNull(logFirstTime) || logFirstTime.compareTo(timeRange.getSecond()) > 0) {
                return;
            }

            if (logFirstTime.compareTo(timeRange.getFirst()) >= 0 && lastDate.compareTo(timeRange.getSecond()) <= 0) {
                Files.copy(logFile.toPath(), new File(destLogDir, logFile.getName()).toPath());
            } else {
                extractLogByTimeRange(logFile, timeRange, new File(destLogDir, logFile.getName()));
            }
        }

        public void extractLogByTimeRange(File logFile, Pair<String, String> timeRange, File distFile) {
            Preconditions.checkNotNull(logFile);
            Preconditions.checkNotNull(timeRange);
            Preconditions.checkNotNull(distFile);
            Preconditions.checkArgument(timeRange.getFirst().compareTo(timeRange.getSecond()) <= 0);

            final String charsetName = Charset.defaultCharset().name();
            try (InputStream in = new FileInputStream(logFile);
                    OutputStream out = new FileOutputStream(distFile);
                    BufferedReader br = new BufferedReader(new InputStreamReader(in, charsetName));
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, charsetName))) {

                boolean extract = false;
                boolean stdLogNotFound = true;
                Pattern pattern = Pattern.compile(logPattern);
                int extraLines = EXTRA_LINES;
                String log;
                while ((log = br.readLine()) != null) {
                    Matcher matcher = pattern.matcher(log);

                    if (matcher.find()) {
                        stdLogNotFound = false;
                        String logDate = matcher.group(1);
                        if (logDate.compareTo(timeRange.getSecond()) > 0 && --extraLines < 1) {
                            break;
                        }

                        if (extract || logDate.compareTo(timeRange.getFirst()) >= 0) {
                            extract = true;
                            bw.write(log);
                            bw.write('\n');
                        }
                    } else if (extract || stdLogNotFound) {
                        bw.write(log);
                        bw.write('\n');
                    }
                }
            } catch (IOException e) {
                logger.error("Failed to extract log from {} to {}", logFile.getAbsolutePath(),
                        distFile.getAbsolutePath(), e);
            }
        }
    }
}
