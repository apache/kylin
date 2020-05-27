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
package org.apache.kylin.engine.flink;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.JobRelatedMetaUtil;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The Flink executable contains job submission specific business logic.
 */
public class FlinkExecutable extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(FlinkExecutable.class);

    private static final String CLASS_NAME = "className";
    private static final String JARS = "jars";
    private static final String JOB_ID = "jobId";
    private static final String COUNTER_SAVE_AS = "CounterSaveAs";
    private static final String CONFIG_NAME = "configName";

    public void setClassName(String className) {
        this.setParam(CLASS_NAME, className);
    }

    public void setJobId(String jobId) {
        this.setParam(JOB_ID, jobId);
    }

    public void setJars(String jars) {
        this.setParam(JARS, jars);
    }

    public void setCounterSaveAs(String value) {
        this.setParam(COUNTER_SAVE_AS, value);
    }

    public void setCounterSaveAs(String value, String counterOutputPath) {
        this.setParam(COUNTER_SAVE_AS, value);
        this.setParam(BatchConstants.ARG_COUNTER_OUTPUT, counterOutputPath);
    }

    public String getCounterSaveAs() {
        return getParam(COUNTER_SAVE_AS);
    }

    public void setFlinkConfigName(String configName) {
        this.setParam(CONFIG_NAME, configName);
    }

    public String getFlinkConfigName() {
        return getParam(CONFIG_NAME);
    }

    private String formatArgs() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : getParams().entrySet()) {
            StringBuilder tmp = new StringBuilder();
            tmp.append("-").append(entry.getKey()).append(" ").append(entry.getValue()).append(" ");
            if (entry.getKey().equals(CLASS_NAME)) {
                stringBuilder.insert(0, tmp);
            } else if (entry.getKey().equals(JARS) || entry.getKey().equals(JOB_ID)
                    || entry.getKey().equals(COUNTER_SAVE_AS) || entry.getKey().equals(CONFIG_NAME)) {
                // JARS is for flink-submit, not for app
                continue;
            } else {
                stringBuilder.append(tmp);
            }
        }
        if (stringBuilder.length() > 0) {
            return stringBuilder.substring(0, stringBuilder.length() - 1).toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    @SuppressWarnings("checkstyle:methodlength")
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException, PersistentException {
        ExecutableManager manager = getManager();
        Map<String, String> extra = manager.getOutput(getId()).getExtra();
        String flinkJobId = extra.get(ExecutableConstants.FLINK_JOB_ID);
        if (!StringUtils.isEmpty(flinkJobId)) {
            return onResumed(flinkJobId, manager);
        } else {
            String cubeName = this.getParam(FlinkCubingByLayer.OPTION_CUBE_NAME.getOpt());
            CubeInstance cube = CubeManager.getInstance(context.getConfig()).getCube(cubeName);

            final KylinConfig config = cube.getConfig();

            setAlgorithmLayer();

            if (KylinConfig.getFlinkHome() == null) {
                throw new NullPointerException();
            }
            if (config.getKylinJobJarPath() == null) {
                throw new NullPointerException();
            }

            String jars = this.getParam(JARS);

            String hadoopConf = System.getProperty("kylin.hadoop.conf.dir");

            if (StringUtils.isEmpty(hadoopConf)) {
                throw new RuntimeException(
                        "kylin_hadoop_conf_dir is empty, check if there's error in the output of 'kylin.sh start'");
            }

            logger.info("Using " + hadoopConf + " as HADOOP_CONF_DIR");

            String hadoopClasspathEnv = new File(hadoopConf).getParentFile().getAbsolutePath();

            String jobJar = config.getKylinJobJarPath();
            if (StringUtils.isEmpty(jars)) {
                jars = jobJar;
            }

            String segmentID = this.getParam(FlinkCubingByLayer.OPTION_SEGMENT_ID.getOpt());
            CubeSegment segment = cube.getSegmentById(segmentID);
            Segments<CubeSegment> mergingSeg = cube.getMergingSegments(segment);
            dumpMetadata(segment, mergingSeg);

            StringBuilder sb = new StringBuilder();
            if (Shell.osType == Shell.OSType.OS_TYPE_WIN) {
                sb.append("set HADOOP_CONF_DIR=%s && set HADOOP_CLASSPATH=%s && %s/bin/flink run -m yarn-cluster ");
            } else {
                sb.append("export HADOOP_CONF_DIR=%s && export HADOOP_CLASSPATH=%s && %s/bin/flink run -m yarn-cluster ");
            }

            Map<String, String> flinkConfs = config.getFlinkConfigOverride();

            String flinkConfigName = getFlinkConfigName();
            if (flinkConfigName != null) {
                Map<String, String> flinkSpecificConfs = config.getFlinkConfigOverrideWithSpecificName(flinkConfigName);
                flinkSpecificConfs.putAll(flinkConfs);
            }

            int parallelism = 1;
            for (Map.Entry<String, String> entry : flinkConfs.entrySet()) {
                if (!(FlinkOnYarnConfigMapping.flinkOnYarnConfigMap.containsKey(entry.getKey())
                        || entry.getKey().startsWith("program") || entry.getKey().startsWith("job"))) {
                    logger.error("Unsupported Flink configuration pair : key[%s], value[%s]", entry.getKey(), entry.getValue());
                    throw new IllegalArgumentException("Unsupported Flink configuration pair : key["
                            + entry.getKey() + "], value[" + entry.getValue() + "]");
                }

                if (entry.getKey().equals("job.parallelism")) {
                    parallelism = Integer.parseInt(entry.getValue());
                } else if (entry.getKey().startsWith("program.")) {
                    getParams().put(entry.getKey().replaceAll("program.", ""), entry.getValue());
                } else {
                    String configOptionKey = FlinkOnYarnConfigMapping.flinkOnYarnConfigMap.get(entry.getKey());
                    //flink on yarn specific option (pattern : -yn 1)
                    if (configOptionKey.startsWith("-y") && !entry.getValue().isEmpty()) {
                        sb.append(" ").append(configOptionKey).append(" ").append(entry.getValue());
                    } else if(!configOptionKey.startsWith("-y")){
                        //flink on yarn specific option (pattern : -yD taskmanager.network.memory.min=536346624)
                        sb.append(" ").append(configOptionKey).append("=").append(entry.getValue());
                    }
                }
            }

            sb.append(" -c org.apache.kylin.common.util.FlinkEntry -p %s %s %s ");
            final String cmd = String.format(Locale.ROOT, sb.toString(), hadoopConf, hadoopClasspathEnv,
                    KylinConfig.getFlinkHome(), parallelism, jars, formatArgs());
            logger.info("cmd: " + cmd);
            final ExecutorService executorService = Executors.newSingleThreadExecutor();
            final CliCommandExecutor exec = new CliCommandExecutor();
            final PatternedLogger patternedLogger = new PatternedLogger(logger, (String infoKey, Map<String, String> info) -> {
                // only care three properties here
                if (ExecutableConstants.FLINK_JOB_ID.equals(infoKey)
                        || ExecutableConstants.YARN_APP_ID.equals(infoKey)
                        || ExecutableConstants.YARN_APP_URL.equals(infoKey)) {
                    getManager().addJobInfo(getId(), info);
                }
            });

            try {
                Future<Pair<Integer, String>> future = executorService.submit(() -> {
                    Pair<Integer, String> result;
                    try {
                        result = exec.execute(cmd, patternedLogger, null);
                    } catch (Exception e) {
                        logger.error("error run Flink job:", e);
                        result = new Pair<>(-1, e.getMessage());
                    }
                    return result;
                });
                Pair<Integer, String> result = null;
                while (!isDiscarded() && !isPaused()) {
                    if (future.isDone()) {
                        result = future.get();
                        break;
                    } else {
                        Thread.sleep(5000);
                    }
                }

                if (future.isDone() == false) { // user cancelled
                    executorService.shutdownNow(); // interrupt
                    extra = manager.getOutput(getId()).getExtra();
                    if (extra != null && extra.get(ExecutableConstants.FLINK_JOB_ID) != null) {
                        killAppRetry(extra.get(ExecutableConstants.FLINK_JOB_ID));
                    }

                    if (isDiscarded()) {
                        return new ExecuteResult(ExecuteResult.State.DISCARDED, "Discarded");
                    }
                    if (isPaused()) {
                        return new ExecuteResult(ExecuteResult.State.STOPPED, "Stopped");
                    }

                    throw new IllegalStateException();
                }

                if (result == null) {
                    result = future.get();
                }
                if (result != null && result.getFirst() == 0) {
                    // done, update all properties
                    Map<String, String> joblogInfo = patternedLogger.getInfo();
                    // read counter from hdfs
                    String counterOutput = getParam(BatchConstants.ARG_COUNTER_OUTPUT);
                    if (counterOutput != null) {
                        if (HadoopUtil.getWorkingFileSystem().exists(new Path(counterOutput))) {
                            Map<String, String> counterMap = HadoopUtil.readFromSequenceFile(counterOutput);
                            joblogInfo.putAll(counterMap);
                        } else {
                            logger.warn("Flink counter output path not exists: " + counterOutput);
                        }
                    }
                    readCounters(joblogInfo);
                    getManager().addJobInfo(getId(), joblogInfo);
                    return new ExecuteResult(ExecuteResult.State.SUCCEED, patternedLogger.getBufferedLog());
                }
                // clear FLINK_JOB_ID on job failure.
                extra = manager.getOutput(getId()).getExtra();
                extra.put(ExecutableConstants.FLINK_JOB_ID, "");
                getManager().addJobInfo(getId(), extra);
                return new ExecuteResult(ExecuteResult.State.ERROR, result != null ? result.getSecond() : "");
            } catch (Exception e) {
                logger.error("error run Flink job:", e);
                return ExecuteResult.createError(e);
            }
        }
    }

    private ExecuteResult onResumed(String appId, ExecutableManager mgr) throws ExecuteException {
        Map<String, String> info = new HashMap<>();
        try {
            logger.info("flink_job_id:" + appId + " resumed");
            info.put(ExecutableConstants.FLINK_JOB_ID, appId);

            while (!isPaused() && !isDiscarded()) {
                String status = getAppState(appId);

                if (status.equals("FAILED") || status.equals("KILLED")) {
                    mgr.updateJobOutput(getId(), ExecutableState.ERROR, null, appId + " has failed");
                    return new ExecuteResult(ExecuteResult.State.FAILED, appId + " has failed");
                }

                if (status.equals("SUCCEEDED")) {
                    mgr.addJobInfo(getId(), info);
                    return new ExecuteResult(ExecuteResult.State.SUCCEED, appId + " has finished");
                }

                Thread.sleep(5000);

            }

            killAppRetry(appId);

            if (isDiscarded()) {
                return new ExecuteResult(ExecuteResult.State.DISCARDED, appId + " is discarded");
            } else {
                return new ExecuteResult(ExecuteResult.State.STOPPED, appId + " is stopped");
            }
        } catch (Exception e) {
            logger.error("error run spark job:", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    private String getAppState(String appId) throws IOException {
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        PatternedLogger patternedLogger = new PatternedLogger(logger);
        String stateCmd = String.format(Locale.ROOT, "yarn application -status %s", appId);
        executor.execute(stateCmd, patternedLogger, null);
        Map<String, String> info = patternedLogger.getInfo();
        return info.get(ExecutableConstants.YARN_APP_STATE);
    }

    private void setAlgorithmLayer() {
        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubingJob cubingJob = (CubingJob) execMgr.getJob(this.getParam(JOB_ID));
        cubingJob.setAlgorithm(CubingJob.AlgorithmEnum.LAYER);
    }

    private void dumpMetadata(CubeSegment segment, List<CubeSegment> mergingSeg) throws ExecuteException {
        try {
            if (mergingSeg == null || mergingSeg.size() == 0) {
                attachSegmentMetadataWithDict(segment);
            } else {
                List<CubeSegment> allRelatedSegs = new ArrayList();
                allRelatedSegs.add(segment);
                allRelatedSegs.addAll(mergingSeg);
                attachSegmentsMetadataWithDict(allRelatedSegs);
            }
        } catch (IOException e) {
            throw new ExecuteException("meta dump failed");
        }
    }

    private void attachSegmentMetadataWithDict(CubeSegment segment) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.addAll(JobRelatedMetaUtil.collectCubeMetadata(segment.getCubeInstance()));
        dumpList.addAll(segment.getDictionaryPaths());
        ResourceStore rs = ResourceStore.getStore(segment.getConfig());
        if (rs.exists(segment.getStatisticsResourcePath())) {
            // cube statistics is not available for new segment
            dumpList.add(segment.getStatisticsResourcePath());
        }

        JobRelatedMetaUtil.dumpAndUploadKylinPropsAndMetadata(dumpList, (KylinConfigExt) segment.getConfig(),
                this.getParam(FlinkCubingByLayer.OPTION_META_URL.getOpt()));
    }

    private void attachSegmentsMetadataWithDict(List<CubeSegment> segments) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.addAll(JobRelatedMetaUtil.collectCubeMetadata(segments.get(0).getCubeInstance()));
        ResourceStore rs = ResourceStore.getStore(segments.get(0).getConfig());
        for (CubeSegment segment : segments) {
            dumpList.addAll(segment.getDictionaryPaths());
            if (rs.exists(segment.getStatisticsResourcePath())) {
                // cube statistics is not available for new segment
                dumpList.add(segment.getStatisticsResourcePath());
            }
        }

        JobRelatedMetaUtil.dumpAndUploadKylinPropsAndMetadata(dumpList, (KylinConfigExt) segments.get(0).getConfig(),
                this.getParam(FlinkCubingByLayer.OPTION_META_URL.getOpt()));
    }

    private int killAppRetry(String appId) throws IOException, InterruptedException {
        String state = getAppState(appId);
        if ("SUCCEEDED".equals(state) || "FAILED".equals(state) || "KILLED".equals(state)) {
            logger.warn(appId + "is final state, no need to kill");
            return 0;
        }

        killApp(appId);

        state = getAppState(appId);
        int retry = 0;
        while (state == null || !state.equals("KILLED") && retry < 5) {
            killApp(appId);

            Thread.sleep(1000);

            state = getAppState(appId);
            retry++;
        }

        if ("KILLED".equals(state)) {
            logger.info(appId + " killed successfully");
            return 0;
        } else {
            logger.info(appId + " killed failed");
            return 1;
        }
    }

    private void killApp(String appId) throws IOException, InterruptedException {
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        String killCmd = String.format(Locale.ROOT, "yarn application -kill %s", appId);
        executor.execute(killCmd);
    }

    private void readCounters(final Map<String, String> info) {
        String counterSaveAs = getCounterSaveAs();
        if (counterSaveAs != null) {
            String[] saveAsNames = counterSaveAs.split(",");
            saveCounterAs(info.get(ExecutableConstants.SOURCE_RECORDS_COUNT), saveAsNames, 0, info);
            saveCounterAs(info.get(ExecutableConstants.SOURCE_RECORDS_SIZE), saveAsNames, 1, info);
            saveCounterAs(info.get(ExecutableConstants.HDFS_BYTES_WRITTEN), saveAsNames, 2, info);
        }
    }

    private void saveCounterAs(String counter, String[] saveAsNames, int i, Map<String, String> info) {
        if (saveAsNames.length > i && StringUtils.isBlank(saveAsNames[i]) == false) {
            info.put(saveAsNames[i].trim(), counter);
        }
    }

}
