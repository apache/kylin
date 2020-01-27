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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.livy.LivyRestBuilder;
import org.apache.kylin.common.livy.LivyRestExecutor;
import org.apache.kylin.common.livy.LivyStateEnum;
import org.apache.kylin.common.livy.LivyTypeEnum;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.metadata.model.Segments;
import org.apache.parquet.Strings;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SparkExecutableLivy extends SparkExecutable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SparkExecutableLivy.class);

    private static final String CLASS_NAME = "className";
    private static final String JARS = "jars";
    private static final String JOB_ID = "jobId";
    private static final String COUNTER_SAVE_AS = "CounterSaveAs";
    private static final String CONFIG_NAME = "configName";

    public void formatArgs(List<String> args) {
        //-className must first
        for (Map.Entry<String, String> entry : getParams().entrySet()) {
            if (entry.getKey().equals(CLASS_NAME)) {
                args.add("-" + entry.getKey());
                args.add(entry.getValue());
                break;
            }
        }
        for (Map.Entry<String, String> entry : getParams().entrySet()) {
            if (entry.getKey().equals(CLASS_NAME) || entry.getKey().equals(JARS) || entry.getKey().equals(JOB_ID)
                    || entry.getKey().equals(COUNTER_SAVE_AS) || entry.getKey().equals(CONFIG_NAME)) {
                continue;
            } else {
                args.add("-" + entry.getKey());
                args.add(entry.getValue());
            }
        }
    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        final Output output = getOutput();
        if (output.getExtra().containsKey(START_TIME)) {
            final String sparkJobID = output.getExtra().get(ExecutableConstants.SPARK_JOB_ID);
            if (sparkJobID == null) {
                getManager().updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
                return;
            }
            try {
                String status = getAppState(sparkJobID);
                if (Strings.isNullOrEmpty(status) || LivyStateEnum.dead.name().equalsIgnoreCase(status)
                        || LivyStateEnum.error.name().equalsIgnoreCase(status)
                        || LivyStateEnum.shutting_down.name().equalsIgnoreCase(status)) {
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

    @Override
    protected ExecuteResult onResumed(String appId, ExecutableManager mgr) throws ExecuteException {
        Map<String, String> info = new HashMap<>();
        try {
            logger.info("livy spark_job_id:" + appId + " resumed");
            info.put(ExecutableConstants.SPARK_JOB_ID, appId);

            while (!isPaused() && !isDiscarded()) {
                String status = getAppState(appId);

                if (Strings.isNullOrEmpty(status) || LivyStateEnum.dead.name().equalsIgnoreCase(status)
                        || LivyStateEnum.error.name().equalsIgnoreCase(status)
                        || LivyStateEnum.shutting_down.name().equalsIgnoreCase(status)) {
                    mgr.updateJobOutput(getId(), ExecutableState.ERROR, null, appId + " has failed");
                    return new ExecuteResult(ExecuteResult.State.FAILED, appId + " has failed");
                }

                if (LivyStateEnum.success.name().equalsIgnoreCase(status)) {
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
            CubeInstance cube = CubeManager.getInstance(context.getConfig()).getCube(cubeName);
            final KylinConfig config = cube.getConfig();

            setAlgorithmLayer();

            LivyRestBuilder livyRestBuilder = new LivyRestBuilder();

            String segmentID = this.getParam(SparkCubingByLayer.OPTION_SEGMENT_ID.getOpt());
            CubeSegment segment = cube.getSegmentById(segmentID);
            Segments<CubeSegment> mergingSeg = cube.getMergingSegments(segment);
            dumpMetadata(segment, mergingSeg);

            Map<String, String> sparkConfs = config.getSparkConfigOverride();
            String sparkConfigName = getSparkConfigName();
            if (sparkConfigName != null) {
                Map<String, String> sparkSpecificConfs = config.getSparkConfigOverrideWithSpecificName(sparkConfigName);
                sparkConfs.putAll(sparkSpecificConfs);
            }

            for (Map.Entry<String, String> entry : sparkConfs.entrySet()) {
                if (entry.getKey().equals("spark.submit.deployMode") || entry.getKey().equals("spark.master")
                        || entry.getKey().equals("spark.yarn.archive")) {
                    continue;
                } else {
                    livyRestBuilder.addConf(entry.getKey(), entry.getValue());
                }
            }
            formatArgs(livyRestBuilder.getArgs());

            final LivyRestExecutor executor = new LivyRestExecutor();
            final PatternedLogger patternedLogger = new PatternedLogger(logger, (infoKey, info) -> {
                // only care three properties here
                if (ExecutableConstants.SPARK_JOB_ID.equals(infoKey) || ExecutableConstants.YARN_APP_ID.equals(infoKey)
                        || ExecutableConstants.YARN_APP_URL.equals(infoKey)) {
                    getManager().addJobInfo(getId(), info);
                }
            });

            try {
                livyRestBuilder.setLivyTypeEnum(LivyTypeEnum.job);
                executor.execute(livyRestBuilder, patternedLogger);
                if (isDiscarded()) {
                    return new ExecuteResult(ExecuteResult.State.DISCARDED, "Discarded");
                }
                if (isPaused()) {
                    return new ExecuteResult(ExecuteResult.State.STOPPED, "Stopped");
                }
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
                return new ExecuteResult(ExecuteResult.State.SUCCEED, patternedLogger.getBufferedLog());

            } catch (Exception e) {
                logger.error("error run spark job:", e);
                // clear SPARK_JOB_ID on job failure.
                extra = mgr.getOutput(getId()).getExtra();
                extra.put(ExecutableConstants.SPARK_JOB_ID, "");
                getManager().addJobInfo(getId(), extra);
                return new ExecuteResult(ExecuteResult.State.ERROR, e.getMessage());
            }
        }
    }

    @Override
    protected String getAppState(String appId) throws IOException {
        LivyRestExecutor executor = new LivyRestExecutor();
        return executor.state(appId);
    }

    @Override
    protected void killApp(String appId) throws IOException, InterruptedException {
        LivyRestExecutor executor = new LivyRestExecutor();
        executor.kill(appId);
    }

    @Override
    protected int killAppRetry(String appId) throws IOException, InterruptedException {
        String status = getAppState(appId);
        if (Strings.isNullOrEmpty(status) || LivyStateEnum.dead.name().equalsIgnoreCase(status)
                || LivyStateEnum.error.name().equalsIgnoreCase(status)
                || LivyStateEnum.shutting_down.name().equalsIgnoreCase(status)) {
            logger.warn(appId + "is final state, no need to kill");
            return 0;
        }

        killApp(appId);

        status = getAppState(appId);
        int retry = 0;
        while (Strings.isNullOrEmpty(status) || LivyStateEnum.dead.name().equalsIgnoreCase(status)
                || LivyStateEnum.error.name().equalsIgnoreCase(status)
                || LivyStateEnum.shutting_down.name().equalsIgnoreCase(status) && retry < 5) {
            killApp(appId);

            Thread.sleep(1000);

            status = getAppState(appId);
            retry++;
        }

        if (Strings.isNullOrEmpty(status)) {
            logger.info(appId + " killed successfully");
            return 0;
        } else {
            logger.info(appId + " killed failed");
            return 1;
        }
    }

}
