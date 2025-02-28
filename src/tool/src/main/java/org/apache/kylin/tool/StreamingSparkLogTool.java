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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;

import lombok.extern.slf4j.Slf4j;

@Slf4j(topic = "diag")
public class StreamingSparkLogTool extends ExecutableApplication {

    private static final Option OPTION_STREAMING_DIR = OptionBuilder.getInstance().hasArg()
            .withArgName("DESTINATION_DIR").withDescription("Specify the file to save yarn application id")
            .isRequired(true).create("dir");

    private static final Option OPTION_STREAMING_JOB = OptionBuilder.getInstance().hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job").isRequired(false).create("job");

    private static final Option OPTION_STREAMING_PROJECT = OptionBuilder.getInstance().hasArg()
            .withArgName("OPTION_PROJECT").withDescription("Specify project").isRequired(false).create("project");

    private static final Option OPTION_STREAMING_START_TIME = OptionBuilder.getInstance().withArgName("startTime")
            .hasArg().isRequired(false).withDescription("specify the start of time range to extract logs. ")
            .create("startTime");

    private static final Option OPTION_STREAMING_END_TIME = OptionBuilder.getInstance().withArgName("endTime").hasArg()
            .isRequired(false).withDescription("specify the end of time range to extract logs. ").create("endTime");

    private static final long DAY = 24 * 3600 * 1000L;
    private static final long MAX_DAY = 31L;
    private static final int JOB_LOG_COUNT = 3;
    private static final String STREAMING_LOG_ROOT_DIR = "streaming_spark_logs";
    private static final String STREAMING_SPARK_DRIVER_DIR = "spark_driver";
    private static final String STREAMING_SPARK_EXECUTOR_DIR = "spark_executor";
    private static final String STREAMING_SPARK_CHECKPOINT_DIR = "spark_checkpoint";

    private final Options options;

    private final KylinConfig kylinConfig;

    StreamingSparkLogTool() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public StreamingSparkLogTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.options = new Options();
        initOptions();
    }

    private static <T> List<T> lastN(Stream<T> stream) {
        Deque<T> result = new ArrayDeque<>(JOB_LOG_COUNT);
        stream.forEach(x -> {
            if (result.size() == JOB_LOG_COUNT) {
                result.pop();
            }
            result.add(x);
        });
        return new ArrayList<>(result);
    }

    private void initOptions() {
        options.addOption(OPTION_STREAMING_JOB);
        options.addOption(OPTION_STREAMING_PROJECT);
        options.addOption(OPTION_STREAMING_DIR);

        options.addOption(OPTION_STREAMING_END_TIME);
        options.addOption(OPTION_STREAMING_START_TIME);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String dir = optionsHelper.getOptionValue(OPTION_STREAMING_DIR);
        String jobId = optionsHelper.getOptionValue(OPTION_STREAMING_JOB);
        String project = optionsHelper.getOptionValue(OPTION_STREAMING_PROJECT);

        String startTimeStr = optionsHelper.getOptionValue(OPTION_STREAMING_START_TIME);
        String endTimeStr = optionsHelper.getOptionValue(OPTION_STREAMING_END_TIME);

        // The acquisition of executor and checkpoint logs depends on the job start time returned by the driver
        // streaming job
        if (StringUtils.isNotEmpty(project) && StringUtils.isNotEmpty(jobId)) {
            log.info("start dump streaming spark driver/executor/checkpoint job log, project: {}, jobId: {}", project,
                    jobId);
            Map<String, Map<String, Set<String>>> projectJobMap = dumpJobDriverLog(project, jobId, dir, null, null);
            dumpExecutorLog(projectJobMap, dir);
            if (jobId.contains("_merge")) {
                log.warn("Only build job have checkpoint, current job: {}", jobId);
                return;
            }
            dumpCheckPoint(project, StringUtils.split(jobId, "_")[0], dir);
            return;
        }

        // full
        if (StringUtils.isNotEmpty(startTimeStr) && StringUtils.isNotEmpty(endTimeStr)) {
            Long startTime = Long.parseLong(startTimeStr);
            Long endTime = Long.parseLong(endTimeStr);

            long days = (endTime - startTime) / DAY;
            if (days > MAX_DAY) {
                log.error("time range is too large, startTime: {}, endTime: {}, days: {}", startTime, endTime, days);
                return;
            }

            log.info("start dump streaming spark driver/executor/checkpoint full log, startTime: {}, endTime: {}",
                    startTimeStr, endTimeStr);
            Map<String, Map<String, Set<String>>> projectJobMap = dumpAllDriverLog(dir, startTimeStr, endTimeStr);
            if (ObjectUtils.isEmpty(projectJobMap)) {
                return;
            }
            dumpExecutorLog(projectJobMap, dir);
            dumpAllCheckPoint(dir, projectJobMap);
        }

    }

    /**
     * dump spark driver logs for a single job
     * single job -> latest 3 logs
     * full       -> all log with time filter
     * @return Map<project, Map<jobId, Set<JobStartedTime>>>
     */
    private Map<String, Map<String, Set<String>>> dumpJobDriverLog(String project, String jobId, String exportDir,
            String startTime, String endTime) {

        Map<String, Map<String, Set<String>>> projectJobMap = new HashMap<>();
        Map<String, Set<String>> jobTimeMap = new HashMap<>();

        // streaming job driver log in hdfs directory
        String outputStoreDirPath = kylinConfig.getStreamingJobTmpOutputStorePath(project, jobId);
        ExecutableManager executableManager = ExecutableManager.getInstance(kylinConfig, project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        if (!executableManager.isHdfsPathExists(outputStoreDirPath)) {
            log.warn("The job driver log file on HDFS has not been generated yet, jobId: {}, filePath: {}", jobId,
                    outputStoreDirPath);
            return projectJobMap;
        }

        List<String> logFilePathList = executableManager.getFilePathsFromHDFSDir(outputStoreDirPath, true);
        if (CollectionUtils.isEmpty(logFilePathList)) {
            log.warn("There is no file in the current job HDFS directory: {}", outputStoreDirPath);
            return projectJobMap;
        }

        File driverDir = new File(exportDir, String.format(Locale.ROOT, "/%s/%s/%s/%s", STREAMING_LOG_ROOT_DIR,
                STREAMING_SPARK_DRIVER_DIR, project, jobId));

        List<Path> needCopyLogPathList = new ArrayList<>();
        List<Path> logPathFullList = new ArrayList<>();
        Set<String> needCopyJobStartedSet = new HashSet<>();
        Set<String> jobStartedFullSet = new HashSet<>();
        boolean isJob = StringUtils.isEmpty(startTime) && StringUtils.isEmpty(endTime);

        // copy hdfs file to local
        logFilePathList.stream().map(Path::new).filter(path -> {
            logPathFullList.add(path);
            jobStartedFullSet.add(path.getParent().getName());
            if (isJob) {
                return true;
            }
            // Time Filter
            Long logTimeStamp = Long.parseLong(StringUtils.split(path.getName(), "\\.")[1]);
            return logTimeStamp.compareTo(Long.parseLong(startTime)) >= 0
                    && logTimeStamp.compareTo(Long.parseLong(endTime)) <= 0;
        }).forEach(path -> {
            needCopyLogPathList.add(path);
            String jobStartedTime = path.getParent().getName();
            needCopyJobStartedSet.add(jobStartedTime);
        });
        if (CollectionUtils.isEmpty(logPathFullList)) {
            return projectJobMap;
        }

        try {
            FileUtils.forceMkdir(driverDir);
            if (isJob) {
                // log get latest 3 log
                List<String> needCopyPathLimitedList = lastN(needCopyJobStartedSet.stream().sorted());
                Set<String> startedTimeSet = new HashSet<>();
                for (Path path : needCopyLogPathList) {
                    String jobStartTime = path.getParent().getName();
                    if (!needCopyPathLimitedList.contains(jobStartTime)) {
                        continue;
                    }
                    File jobStartedTimeDir = new File(driverDir, jobStartTime);
                    FileUtils.forceMkdir(jobStartedTimeDir);
                    fs.copyToLocalFile(path, new Path(jobStartedTimeDir.getAbsolutePath()));
                    startedTimeSet.add(jobStartTime);
                }
                jobTimeMap.put(jobId, startedTimeSet);
                projectJobMap.put(project, jobTimeMap);
                return projectJobMap;
            }
            // full
            // In any case, ensure that each job in the full diagnostic package 
            // has the oldest driver log that can be returned
            if (needCopyJobStartedSet.isEmpty()) {
                needCopyJobStartedSet.add(Collections.max(jobStartedFullSet));
            }
            for (Path path : logPathFullList) {
                if (!needCopyJobStartedSet.contains(path.getParent().getName())) {
                    continue;
                }
                String jobStartTime = path.getParent().getName();
                File jobStartedTimeDir = new File(driverDir, jobStartTime);
                FileUtils.forceMkdir(jobStartedTimeDir);
                fs.copyToLocalFile(path, new Path(jobStartedTimeDir.getAbsolutePath()));
            }
            jobTimeMap.put(jobId, needCopyJobStartedSet);
            projectJobMap.put(project, jobTimeMap);
        } catch (IOException e) {
            log.error("dump streaming driver log failed. ", e);
        }
        return projectJobMap;
    }

    /**
     * dump spark driver All logs
     * @return Map<project, Map<jobId, Set<JobStartedTime>>>
     */
    private Map<String, Map<String, Set<String>>> dumpAllDriverLog(String exportDir, String startTime, String endTime) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        Map<String, Map<String, Set<String>>> projectJobMap = new HashMap<>();
        projectManager.listAllProjects().forEach(projectInstance -> {
            Map<String, Set<String>> jobTimeMap = new HashMap<>();
            String project = projectInstance.getName();

            StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(kylinConfig, project);
            streamingJobManager.listAllStreamingJobMeta().stream().map(StreamingJobMeta::getId).forEach(jobId -> {
                Map<String, Map<String, Set<String>>> map = dumpJobDriverLog(project, jobId, exportDir, startTime,
                        endTime);
                if (ObjectUtils.isEmpty(map)) {
                    return;
                }
                for (Map.Entry<String, Map<String, Set<String>>> entry : map.entrySet()) {
                    for (Map.Entry<String, Set<String>> setEntry : entry.getValue().entrySet()) {
                        jobTimeMap.put(jobId, setEntry.getValue());
                    }
                }
            });
            if (ObjectUtils.isEmpty(jobTimeMap)) {
                return;
            }
            projectJobMap.put(project, jobTimeMap);
        });
        return projectJobMap;
    }

    /**
     * dump spark All checkpoint
     */
    private void dumpAllCheckPoint(String exportDir, Map<String, Map<String, Set<String>>> projectJobMap) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);

        projectManager.listAllProjects().stream().map(ProjectInstance::getName).filter(projectJobMap.keySet()::contains)
                .forEach(project -> {
                    Set<String> jobIdSet = projectJobMap.get(project).keySet();
                    StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(kylinConfig, project);
                    streamingJobManager.listAllStreamingJobMeta().stream().map(StreamingJobMeta::getModelId).distinct()
                            .filter(modelId -> jobIdSet.contains(modelId.concat("_build")))
                            .forEach(modelId -> dumpCheckPoint(project, modelId, exportDir));
                });
    }

    /**
     * dump spark checkpoint for a single job
     */
    private void dumpCheckPoint(String project, String modelId, String exportDir) {
        String hdfsStreamLogRootPath = kylinConfig.getHdfsWorkingDirectoryWithoutScheme();
        String hdfsStreamJobCheckPointPath = String.format(Locale.ROOT, "%s%s%s", hdfsStreamLogRootPath,
                "streaming/checkpoint/", modelId);
        ExecutableManager executableManager = ExecutableManager.getInstance(kylinConfig, project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        if (!executableManager.isHdfsPathExists(hdfsStreamJobCheckPointPath)) {
            log.warn("The job checkpoint file on HDFS has not been generated yet, modelId: {}, filePath: {}", modelId,
                    hdfsStreamJobCheckPointPath);
            return;
        }
        List<String> executorLogPath = executableManager.getFilePathsFromHDFSDir(hdfsStreamJobCheckPointPath, true);
        if (CollectionUtils.isEmpty(executorLogPath)) {
            log.warn("There is no file in the current job HDFS directory: {}", hdfsStreamJobCheckPointPath);
            return;
        }

        File checkpointDir = new File(exportDir, String.format(Locale.ROOT, "/%s/%s/%s", STREAMING_LOG_ROOT_DIR,
                STREAMING_SPARK_CHECKPOINT_DIR, project));
        try {
            FileUtils.forceMkdir(checkpointDir);
            fs.copyToLocalFile(new Path(hdfsStreamJobCheckPointPath), new Path(checkpointDir.getAbsolutePath()));
        } catch (IOException e) {
            log.error("dump streaming checkpoint failed. ", e);
        }
    }

    private void dumpExecutorLog(Map<String, Map<String, Set<String>>> projectJobMap, String exportDir) {
        if (ObjectUtils.isEmpty(projectJobMap)) {
            return;
        }
        for (Map.Entry<String, Map<String, Set<String>>> entryOut : projectJobMap.entrySet()) {
            String project = entryOut.getKey();
            Map<String, Set<String>> jobTimeMap = entryOut.getValue();
            for (Map.Entry<String, Set<String>> entryInner : jobTimeMap.entrySet()) {
                String jobId = entryInner.getKey();
                Set<String> jobStartedSet = entryInner.getValue();
                dumpSingleExecutorLog(project, jobId, exportDir, jobStartedSet);
            }
        }
    }

    /**
     * dump spark executor log for a single job
     */
    private void dumpSingleExecutorLog(String project, String jobId, String exportDir, Set<String> jobStartedSet) {
        String hdfsStreamLogRootPath = kylinConfig.getHdfsWorkingDirectoryWithoutScheme();
        String hdfsStreamLogProjectPath = String.format(Locale.ROOT, "%s%s%s", hdfsStreamLogRootPath,
                "streaming/spark_logs/", project);
        ExecutableManager executableManager = ExecutableManager.getInstance(kylinConfig, project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        if (!executableManager.isHdfsPathExists(hdfsStreamLogProjectPath)) {
            log.warn("The job executor log file on HDFS has not been generated yet, jobId: {}, filePath: {}", jobId,
                    hdfsStreamLogProjectPath);
            return;
        }
        List<String> executorLogPath = executableManager.getFilePathsFromHDFSDir(hdfsStreamLogProjectPath, true);
        if (CollectionUtils.isEmpty(executorLogPath)) {
            log.warn("There is no file in the current job HDFS directory: {}", hdfsStreamLogProjectPath);
            return;
        }

        executorLogPath.stream().filter(StringUtils::isNotEmpty).map(Path::new).filter(
                path -> StringUtils.isEmpty(jobId) || StringUtils.equals(path.getParent().getParent().getName(), jobId))
                .filter(path -> {
                    String executorTimeStamp = path.getParent().getName();
                    return jobStartedSet.contains(executorTimeStamp);
                }).forEach(logPath -> {
                    String logJobId = logPath.getParent().getParent().getName();
                    String executorDateTime = logPath.getParent().getName();
                    File executorDir = new File(exportDir, String.format(Locale.ROOT, "/%s/%s/%s/%s/%s",
                            STREAMING_LOG_ROOT_DIR, STREAMING_SPARK_EXECUTOR_DIR, project, logJobId, executorDateTime));
                    try {
                        FileUtils.forceMkdir(executorDir);
                        fs.copyToLocalFile(logPath, new Path(executorDir.getAbsolutePath()));
                    } catch (IOException e) {
                        log.error("dump streaming executor log failed. ", e);
                    }
                });
    }

}
