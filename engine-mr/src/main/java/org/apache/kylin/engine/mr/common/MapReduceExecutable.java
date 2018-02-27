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

package org.apache.kylin.engine.mr.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.exception.MapReduceException;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 */
public class MapReduceExecutable extends AbstractExecutable {

    public static final String MAP_REDUCE_WAIT_TIME = "mapReduceWaitTime";
    private static final String KEY_MR_JOB = "MR_JOB_CLASS";
    private static final String KEY_PARAMS = "MR_JOB_PARAMS";
    private static final String KEY_COUNTER_SAVEAS = "MR_COUNTER_SAVEAS";

    protected static final Logger logger = LoggerFactory.getLogger(MapReduceExecutable.class);

    public MapReduceExecutable() {
        super();
    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        final Output output = getOutput();
        if (output.getExtra().containsKey(START_TIME)) {
            final String mrJobId = output.getExtra().get(ExecutableConstants.MR_JOB_ID);
            if (mrJobId == null) {
                getManager().updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
                return;
            }
            try {
                Configuration conf = new Configuration(HadoopUtil.getCurrentConfiguration());
                overwriteJobConf(conf, executableContext.getConfig(), getMapReduceParams().trim().split("\\s+"));
                Job job = new Cluster(conf).getJob(JobID.forName(mrJobId));
                if (job == null || job.getJobState() == JobStatus.State.FAILED) {
                    //remove previous mr job info
                    super.onExecuteStart(executableContext);
                } else {
                    getManager().updateJobOutput(getId(), ExecutableState.RUNNING, null, null);
                }
            } catch (IOException | ParseException e) {
                logger.warn("error get hadoop status");
                super.onExecuteStart(executableContext);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("error get hadoop status");
                super.onExecuteStart(executableContext);
            }
        } else {
            super.onExecuteStart(executableContext);
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final String mapReduceJobClass = getMapReduceJobClass();
        Preconditions.checkNotNull(mapReduceJobClass);
        try {
            Job job;
            ExecutableManager mgr = getManager();
            Configuration conf = new Configuration(HadoopUtil.getCurrentConfiguration());
            String[] jobArgs = overwriteJobConf(conf, context.getConfig(), getMapReduceParams().trim().split("\\s+"));
            final Map<String, String> extra = mgr.getOutput(getId()).getExtra();
            if (extra.containsKey(ExecutableConstants.MR_JOB_ID)) {
                job = new Cluster(conf).getJob(JobID.forName(extra.get(ExecutableConstants.MR_JOB_ID)));
                logger.info("mr_job_id:" + extra.get(ExecutableConstants.MR_JOB_ID) + " resumed");
            } else {
                final Constructor<? extends AbstractHadoopJob> constructor = ClassUtil
                        .forName(mapReduceJobClass, AbstractHadoopJob.class).getConstructor();
                final AbstractHadoopJob hadoopJob = constructor.newInstance();
                hadoopJob.setConf(conf);
                hadoopJob.setAsync(true); // so the ToolRunner.run() returns right away
                logger.info("parameters of the MapReduceExecutable: {}", getMapReduceParams());
                try {

                    hadoopJob.run(jobArgs);

                    if (hadoopJob.isSkipped()) {
                        return new ExecuteResult(ExecuteResult.State.SUCCEED, "skipped");
                    }
                } catch (Exception ex) {
                    StringBuilder log = new StringBuilder();
                    logger.error("error execute " + this.toString(), ex);
                    StringWriter stringWriter = new StringWriter();
                    ex.printStackTrace(new PrintWriter(stringWriter));
                    log.append(stringWriter.toString()).append("\n");
                    log.append("result code:").append(2);
                    return new ExecuteResult(ExecuteResult.State.ERROR, log.toString(), ex);
                }
                job = hadoopJob.getJob();
            }
            final StringBuilder output = new StringBuilder();
            final HadoopCmdOutput hadoopCmdOutput = new HadoopCmdOutput(job, output);

            JobStepStatusEnum status = JobStepStatusEnum.NEW;
            while (!isDiscarded() && !isPaused()) {

                JobStepStatusEnum newStatus = HadoopJobStatusChecker.checkStatus(job, output);
                if (status == JobStepStatusEnum.KILLED) {
                    mgr.updateJobOutput(getId(), ExecutableState.ERROR, hadoopCmdOutput.getInfo(), "killed by admin");
                    return new ExecuteResult(ExecuteResult.State.FAILED, "killed by admin");
                }
                if (status == JobStepStatusEnum.WAITING && (newStatus == JobStepStatusEnum.FINISHED
                        || newStatus == JobStepStatusEnum.ERROR || newStatus == JobStepStatusEnum.RUNNING)) {
                    final long waitTime = System.currentTimeMillis() - getStartTime();
                    setMapReduceWaitTime(waitTime);
                }
                mgr.addJobInfo(getId(), hadoopCmdOutput.getInfo());
                status = newStatus;
                if (status.isComplete()) {
                    final Map<String, String> info = hadoopCmdOutput.getInfo();
                    readCounters(hadoopCmdOutput, info);
                    mgr.addJobInfo(getId(), info);

                    if (status == JobStepStatusEnum.FINISHED) {
                        return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
                    } else {
                        return ExecuteResult.createFailed(new MapReduceException(output.toString()));
                    }
                }
                Thread.sleep(context.getConfig().getYarnStatusCheckIntervalSeconds() * 1000L);
            }

            // try to kill running map-reduce job to release resources.
            if (job != null) {
                try {
                    job.killJob();
                } catch (Exception e) {
                    logger.warn("failed to kill hadoop job: " + job.getJobID(), e);
                }
            }

            if (isDiscarded()) {
                return new ExecuteResult(ExecuteResult.State.DISCARDED, output.toString());
            } else {
                return new ExecuteResult(ExecuteResult.State.STOPPED, output.toString());
            }

        } catch (ReflectiveOperationException e) {
            logger.error("error getMapReduceJobClass, class name:" + getParam(KEY_MR_JOB), e);
            return ExecuteResult.createError(e);
        } catch (Exception e) {
            logger.error("error execute " + this.toString(), e);
            return ExecuteResult.createError(e);
        }
    }

    private void readCounters(final HadoopCmdOutput hadoopCmdOutput, final Map<String, String> info) {
        hadoopCmdOutput.updateJobCounter();
        info.put(ExecutableConstants.SOURCE_RECORDS_COUNT, hadoopCmdOutput.getMapInputRecords());
        info.put(ExecutableConstants.SOURCE_RECORDS_SIZE, hadoopCmdOutput.getRawInputBytesRead());
        info.put(ExecutableConstants.HDFS_BYTES_WRITTEN, hadoopCmdOutput.getHdfsBytesWritten());

        String saveAs = getParam(KEY_COUNTER_SAVEAS);
        if (saveAs != null) {
            String[] saveAsNames = saveAs.split(",");
            saveCounterAs(hadoopCmdOutput.getMapInputRecords(), saveAsNames, 0, info);
            saveCounterAs(hadoopCmdOutput.getRawInputBytesRead(), saveAsNames, 1, info);
            saveCounterAs(hadoopCmdOutput.getHdfsBytesWritten(), saveAsNames, 2, info);
        }
    }

    private void saveCounterAs(String counter, String[] saveAsNames, int i, Map<String, String> info) {
        if (saveAsNames.length > i && StringUtils.isBlank(saveAsNames[i]) == false) {
            info.put(saveAsNames[i].trim(), counter);
        }
    }

    public long getMapReduceWaitTime() {
        return getExtraInfoAsLong(MAP_REDUCE_WAIT_TIME, 0L);
    }

    public void setMapReduceWaitTime(long t) {
        addExtraInfo(MAP_REDUCE_WAIT_TIME, t + "");
    }

    public String getMapReduceJobClass() throws ExecuteException {
        return getParam(KEY_MR_JOB);
    }

    public void setMapReduceJobClass(Class<? extends AbstractHadoopJob> clazzName) {
        setParam(KEY_MR_JOB, clazzName.getName());
    }

    public String getMapReduceParams() {
        return getParam(KEY_PARAMS);
    }

    public void setMapReduceParams(String param) {
        setParam(KEY_PARAMS, param);
    }

    public void setCounterSaveAs(String value) {
        setParam(KEY_COUNTER_SAVEAS, value);
    }

    @SuppressWarnings("static-access")
    private static final Option OPTION_JOB_CONF = OptionBuilder.withArgName(BatchConstants.ARG_CONF).hasArg()
            .isRequired(false).create(BatchConstants.ARG_CONF);

    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg()
            .isRequired(false).create(BatchConstants.ARG_CUBE_NAME);

    private String[] overwriteJobConf(Configuration conf, KylinConfig config, String[] jobParams)
            throws ParseException {
        Options options = new Options();
        options.addOption(OPTION_JOB_CONF);
        options.addOption(OPTION_CUBE_NAME);
        CustomParser parser = new CustomParser();
        CommandLine commandLine = parser.parse(options, jobParams);

        String confFile = commandLine.getOptionValue(BatchConstants.ARG_CONF);
        String cubeName = commandLine.getOptionValue(BatchConstants.ARG_CUBE_NAME);
        List<String> remainingArgs = Lists.newArrayList();

        if (StringUtils.isNotBlank(confFile)) {
            conf.addResource(new Path(confFile));
        }

        if (StringUtils.isNotBlank(cubeName)) {
            for (Map.Entry<String, String> entry : CubeManager.getInstance(config).getCube(cubeName).getConfig()
                    .getMRConfigOverride().entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
            if (conf.get("mapreduce.job.is-mem-hungry") != null
                    && Boolean.valueOf(conf.get("mapreduce.job.is-mem-hungry"))) {
                for (Map.Entry<String, String> entry : CubeManager.getInstance(config).getCube(cubeName).getConfig()
                        .getMemHungryConfigOverride().entrySet()) {
                    conf.set(entry.getKey(), entry.getValue());
                }
            }
            remainingArgs.add("-" + BatchConstants.ARG_CUBE_NAME);
            remainingArgs.add(cubeName);
        }

        remainingArgs.addAll(parser.getRemainingArgs());
        return (String[]) remainingArgs.toArray(new String[remainingArgs.size()]);
    }

    private static class CustomParser extends GnuParser {
        private List<String> remainingArgs;

        public CustomParser() {
            this.remainingArgs = Lists.newArrayList();
        }

        @Override
        protected void processOption(final String arg, final ListIterator iter) throws ParseException {
            boolean hasOption = getOptions().hasOption(arg);

            if (hasOption) {
                super.processOption(arg, iter);
            } else {
                remainingArgs.add(arg);
                remainingArgs.add(iter.next().toString());
            }
        }

        public List<String> getRemainingArgs() {
            return remainingArgs;
        }
    }
}
