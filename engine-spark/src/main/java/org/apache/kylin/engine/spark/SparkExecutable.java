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
package org.apache.kylin.engine.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDescTiretreeGlobalDomainDictUtil;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.JobRelatedMetaUtil;
import org.apache.kylin.engine.spark.exception.SparkException;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.LoggerFactory;

/**
 */
public class SparkExecutable extends AbstractExecutable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SparkExecutable.class);

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

    /**
     * set spark override conf for specific job
     */
    public void setSparkConfigName(String configName) {
        this.setParam(CONFIG_NAME, configName);
    }

    public String getSparkConfigName() {
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
                // JARS is for spark-submit, not for app
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

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        final Output output = getOutput();
        if (output.getExtra().containsKey(START_TIME)) {
            final String sparkJobID = output.getExtra().get(ExecutableConstants.SPARK_JOB_ID);
            if (StringUtils.isEmpty(sparkJobID)) {
                getManager().updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
                return;
            }
            try {
                String status = getAppState(sparkJobID);
                if (status == null || status.equals("FAILED") || status.equals("KILLED")) {
                    //remove previous mr job info
                    super.onExecuteStart(executableContext);
                } else {
                    getManager().updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
                }
            } catch (IOException e) {
                logger.warn("error get hadoop status");
                super.onExecuteStart(executableContext);
            }
        } else {
            super.onExecuteStart(executableContext);
        }
    }

    protected ExecuteResult onResumed(String appId, ExecutableManager mgr) throws ExecuteException {
        Map<String, String> info = new HashMap<>();
        try {
            logger.info("spark_job_id:" + appId + " resumed");
            info.put(ExecutableConstants.SPARK_JOB_ID, appId);

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

    @SuppressWarnings("checkstyle:methodlength")
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        ExecutableManager mgr = getManager();
        Map<String, String> extra = mgr.getOutput(getId()).getExtra();
        String sparkJobId = extra.get(ExecutableConstants.SPARK_JOB_ID);
        if (!StringUtils.isEmpty(sparkJobId)) {
            return onResumed(sparkJobId, mgr);
        } else {
            String cubeName = this.getParam(SparkCubingByLayer.OPTION_CUBE_NAME.getOpt());
            CubeInstance cube;
            if (cubeName != null) {
                cube = CubeManager.getInstance(context.getConfig()).getCube(cubeName);
            } else { // Cube name can't be got when loading hive table
                cube = null;
            }
            final KylinConfig config;
            if (cube != null) {
                config = cube.getConfig();
            } else {
                // when loading hive table, we can't get cube name/config, so we get config from project.
                String projectName = this.getParam(SparkColumnCardinality.OPTION_PRJ.getOpt());
                ProjectInstance projectInst = ProjectManager.getInstance(context.getConfig()).getProject(projectName);
                config = projectInst.getConfig();
            }

            if (KylinConfig.getSparkHome() == null) {
                throw new NullPointerException();
            }
            if (config.getKylinJobJarPath() == null) {
                throw new NullPointerException();
            }
            String jars = this.getParam(JARS);

            //hadoop conf dir
            String hadoopConf = null;
            hadoopConf = System.getProperty("kylin.hadoop.conf.dir");

            if (StringUtils.isEmpty(hadoopConf)) {
                throw new RuntimeException(
                        "kylin_hadoop_conf_dir is empty, check if there's error in the output of 'kylin.sh start'");
            }

            logger.info("Using " + hadoopConf + " as HADOOP_CONF_DIR");

            String jobJar = config.getKylinJobJarPath();
            if (StringUtils.isEmpty(jars)) {
                jars = jobJar;
            }

            if (cube != null && !isCreateFlatTable()) {
                setAlgorithmLayer();
                String segmentID = this.getParam(SparkCubingByLayer.OPTION_SEGMENT_ID.getOpt());
                CubeSegment segment = cube.getSegmentById(segmentID);
                Segments<CubeSegment> mergingSeg = cube.getMergingSegments(segment);
                dumpMetadata(segment, mergingSeg);
            }

            StringBuilder stringBuilder = new StringBuilder();
            if (Shell.osType == Shell.OSType.OS_TYPE_WIN) {
                stringBuilder.append(
                        "set HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry ");
            } else {
                stringBuilder.append(
                        "export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry ");
            }

            if (getName() != null) {
                stringBuilder.append("--name \"" + getName() + "\"");
            }

            Map<String, String> sparkConfs = config.getSparkConfigOverride();

            String sparkConfigName = getSparkConfigName();
            if (sparkConfigName != null) {
                Map<String, String> sparkSpecificConfs = config.getSparkConfigOverrideWithSpecificName(sparkConfigName);
                sparkConfs.putAll(sparkSpecificConfs);
            }

            // Add hbase fs to spark conf: spark.yarn.access.hadoopFileSystems
            if (StringUtils.isNotEmpty(config.getHBaseClusterFs())) {
                String fileSystems = sparkConfs.get("spark.yarn.access.hadoopFileSystems");
                if (StringUtils.isNotEmpty(fileSystems)) {
                    sparkConfs.put("spark.yarn.access.hadoopFileSystems",
                            fileSystems + "," + config.getHBaseClusterFs());
                } else {
                    sparkConfs.put("spark.yarn.access.hadoopFileSystems", config.getHBaseClusterFs());
                }
            }

            for (Map.Entry<String, String> entry : sparkConfs.entrySet()) {
                stringBuilder.append(" --conf ").append(entry.getKey()).append("=").append(entry.getValue())
                        .append(" ");
            }

            stringBuilder.append("--jars %s %s %s");
            final String cmd = String.format(Locale.ROOT, stringBuilder.toString(), hadoopConf,
                    KylinConfig.getSparkHome(), jars, jobJar, formatArgs());
            logger.info("cmd: " + cmd);
            final ExecutorService executorService = Executors.newSingleThreadExecutor();
            final CliCommandExecutor exec = new CliCommandExecutor();
            final PatternedLogger patternedLogger = new PatternedLogger(logger, new PatternedLogger.ILogListener() {
                @Override
                public void onLogEvent(String infoKey, Map<String, String> info) {
                    // only care three properties here
                    if (ExecutableConstants.SPARK_JOB_ID.equals(infoKey)
                            || ExecutableConstants.YARN_APP_ID.equals(infoKey)
                            || ExecutableConstants.YARN_APP_URL.equals(infoKey)) {
                        getManager().addJobInfo(getId(), info);
                    }
                }
            });
            Callable callable = new Callable<Pair<Integer, String>>() {
                @Override
                public Pair<Integer, String> call() throws Exception {
                    Pair<Integer, String> result;
                    try {
                        result = exec.execute(cmd, patternedLogger, null);
                    } catch (Exception e) {
                        logger.error("error run spark job:", e);
                        result = new Pair<>(-1, e.getMessage());
                    }
                    return result;
                }
            };
            try {
                Future<Pair<Integer, String>> future = executorService.submit(callable);
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
                    extra = mgr.getOutput(getId()).getExtra();
                    if (extra != null) {
                        String newJobId = extra.get(ExecutableConstants.SPARK_JOB_ID);
                        if (StringUtils.isNotEmpty(newJobId)) {
                            killAppRetry(newJobId);
                        }
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
                            logger.warn("Spark counter output path not exists: " + counterOutput);
                        }
                    }
                    readCounters(joblogInfo);
                    getManager().addJobInfo(getId(), joblogInfo);
                    if (joblogInfo.containsKey(ExecutableConstants.SPARK_DIMENSION_DIC_SEGMENT_ID)) {
                        updateSparkDimensionDicMetadata(config, cube,
                                joblogInfo.get(ExecutableConstants.SPARK_DIMENSION_DIC_SEGMENT_ID));
                        logger.info("Finished update dictionaries and snapshot info from {} to {}.",
                                this.getParam(SparkBuildDictionary.OPTION_META_URL.getOpt()), config.getMetadataUrl());
                    }
                    return new ExecuteResult(ExecuteResult.State.SUCCEED, patternedLogger.getBufferedLog());
                }
                // clear SPARK_JOB_ID on job failure.
                extra = mgr.getOutput(getId()).getExtra();
                extra.put(ExecutableConstants.SPARK_JOB_ID, "");
                getManager().addJobInfo(getId(), extra);

                // when job failure, truncate the result
                String resultLog = result != null ? result.getSecond() : "";
                if (resultLog.length() > config.getSparkOutputMaxSize()) {
                    resultLog = resultLog.substring(0, config.getSparkOutputMaxSize());
                }

                return ExecuteResult.createFailed(new SparkException(resultLog));
            } catch (Exception e) {
                logger.error("Error run spark job:", e);
                return ExecuteResult.createError(e);
            }
        }
    }

    protected void dumpMetadata(CubeSegment segment, List<CubeSegment> mergingSeg) throws ExecuteException {
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

    //to update metadata from hdfs due to the step build dimension dic using spark dump metadata to hdfs
    private void updateSparkDimensionDicMetadata(KylinConfig config, CubeInstance cube, String segmentId)
            throws IOException {
        KylinConfig hdfsConfig = AbstractHadoopJob
                .loadKylinConfigFromHdfs(this.getParam(SparkBuildDictionary.OPTION_META_URL.getOpt()));
        CubeInstance cubeInstance = CubeManager.getInstance(hdfsConfig).reloadCube(cube.getName());
        CubeSegment segment = cubeInstance.getSegmentById(segmentId);

        CubeSegment oldSeg = cube.getSegmentById(segmentId);
        oldSeg.setDictionaries((ConcurrentHashMap<String, String>) segment.getDictionaries());
        oldSeg.setSnapshots((ConcurrentHashMap) segment.getSnapshots());
        oldSeg.getRowkeyStats().addAll(segment.getRowkeyStats());
        CubeInstance cubeCopy = cube.latestCopyForWrite();
        CubeUpdate update = new CubeUpdate(cubeCopy);
        update.setToUpdateSegs(oldSeg);
        CubeManager.getInstance(config).updateCube(update);

        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.addAll(segment.getDictionaryPaths());
        dumpList.addAll(segment.getSnapshotPaths());

        JobRelatedMetaUtil.dumpAndUploadKylinPropsAndMetadata(dumpList, (KylinConfigExt) segment.getConfig(),
                config.getMetadataUrl().toString());
    }

    // Spark Cubing can only work in layer algorithm
    protected void setAlgorithmLayer() {
        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubingJob cubingJob = (CubingJob) execMgr.getJob(this.getParam(JOB_ID));
        cubingJob.setAlgorithm(CubingJob.AlgorithmEnum.LAYER);
    }

    protected String getAppState(String appId) throws IOException {
        if (StringUtils.isEmpty(appId)) {
            throw new IOException("The app is is null or empty");
        }
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        PatternedLogger patternedLogger = new PatternedLogger(logger);
        String stateCmd = String.format(Locale.ROOT, "yarn application -status %s", appId);
        executor.execute(stateCmd, patternedLogger, null);
        Map<String, String> info = patternedLogger.getInfo();
        return info.get(ExecutableConstants.YARN_APP_STATE);
    }

    protected void killApp(String appId) throws IOException, InterruptedException {
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        String killCmd = String.format(Locale.ROOT, "yarn application -kill %s", appId);
        executor.execute(killCmd);
    }

    protected int killAppRetry(String appId) throws IOException, InterruptedException {
        if (StringUtils.isEmpty(appId)) {
            logger.warn("The app is is null or empty");
            return 0;
        }

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

    private void attachSegmentMetadataWithDict(CubeSegment segment) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.addAll(JobRelatedMetaUtil.collectCubeMetadata(segment.getCubeInstance()));
        dumpList.addAll(segment.getDictionaryPaths());
        ResourceStore rs = ResourceStore.getStore(segment.getConfig());
        if (rs.exists(segment.getStatisticsResourcePath())) {
            // cube statistics is not available for new segment
            dumpList.add(segment.getStatisticsResourcePath());
        }
        //tiretree global domain dic
        CubeDescTiretreeGlobalDomainDictUtil.cuboidJob(segment.getCubeDesc(), dumpList);

        JobRelatedMetaUtil.dumpAndUploadKylinPropsAndMetadata(dumpList, (KylinConfigExt) segment.getConfig(),
                this.getParam(SparkCubingByLayer.OPTION_META_URL.getOpt()));
    }

    private void attachSegmentsMetadataWithDict(List<CubeSegment> segments) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>(
                JobRelatedMetaUtil.collectCubeMetadata(segments.get(0).getCubeInstance()));
        ResourceStore rs = ResourceStore.getStore(segments.get(0).getConfig());
        for (CubeSegment segment : segments) {
            dumpList.addAll(segment.getDictionaryPaths());
            if (rs.exists(segment.getStatisticsResourcePath())) {
                // cube statistics is not available for new segment
                dumpList.add(segment.getStatisticsResourcePath());
            }
            //tiretree global domain dic
            CubeDescTiretreeGlobalDomainDictUtil.cuboidJob(segment.getCubeDesc(), dumpList);
        }
        JobRelatedMetaUtil.dumpAndUploadKylinPropsAndMetadata(dumpList, (KylinConfigExt) segments.get(0).getConfig(),
                this.getParam(SparkCubingByLayer.OPTION_META_URL.getOpt()));
    }

    protected void readCounters(final Map<String, String> info) {
        String counter_save_as = getCounterSaveAs();
        if (counter_save_as != null) {
            String[] saveAsNames = counter_save_as.split(",");
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

    private boolean isCreateFlatTable() {
        return ExecutableConstants.STEP_NAME_CREATE_FLAT_TABLE_WITH_SPARK.equals(getName());
    }
}
