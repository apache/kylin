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

package org.apache.kylin.job.execution;

import static org.apache.kylin.job.constant.ExecutableConstants.MR_JOB_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_ID;
import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_URL;
import static org.apache.kylin.job.constant.ExecutableConstants.FLINK_JOB_ID;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.JobProcessContext;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class ExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(ExecutableManager.class);

    private static final int CMD_EXEC_TIMEOUT_SEC = 60;
    private static final String KILL_PROCESS_TREE = "kill-process-tree.sh";

    public static ExecutableManager getInstance(KylinConfig config) {
        return config.getManager(ExecutableManager.class);
    }

    // called by reflection
    static ExecutableManager newInstance(KylinConfig config) throws IOException {
        return new ExecutableManager(config);
    }

    // ============================================================================

    private final KylinConfig config;
    private final ExecutableDao executableDao;

    private ExecutableManager(KylinConfig config) {
        logger.info("Using metadata url: " + config);
        this.config = config;
        this.executableDao = ExecutableDao.getInstance(config);
    }

    private static ExecutablePO parse(AbstractExecutable executable) {
        ExecutablePO result = new ExecutablePO();
        result.setName(executable.getName());
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setParams(executable.getParams());
        result.setPriority(executable.getPriority());
        if (executable instanceof ChainedExecutable) {
            List<ExecutablePO> tasks = Lists.newArrayList();
            for (AbstractExecutable task : ((ChainedExecutable) executable).getTasks()) {
                tasks.add(parse(task));
            }
            result.setTasks(tasks);
        }
        if (executable instanceof CheckpointExecutable) {
            List<ExecutablePO> tasksForCheck = Lists.newArrayList();
            for (AbstractExecutable taskForCheck : ((CheckpointExecutable) executable).getSubTasksForCheck()) {
                tasksForCheck.add(parse(taskForCheck));
            }
            result.setTasksForCheck(tasksForCheck);
        }
        return result;
    }

    public void addJob(AbstractExecutable executable) {
        try {
            executable.initConfig(config);
            if (executableDao.getJob(executable.getId()) != null) {
                throw new IllegalArgumentException("job id:" + executable.getId() + " already exists");
            }
            addJobOutput(executable);
            executableDao.addJob(parse(executable));
        } catch (PersistentException e) {
            logger.error("fail to submit job:" + executable.getId(), e);
            throw new RuntimeException(e);
        }
    }

    private void addJobOutput(AbstractExecutable executable) throws PersistentException {
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setUuid(executable.getId());
        executableDao.addJobOutput(executableOutputPO);
        if (executable instanceof DefaultChainedExecutable) {
            for (AbstractExecutable subTask : ((DefaultChainedExecutable) executable).getTasks()) {
                addJobOutput(subTask);
            }
        }
    }

    public void updateCheckpointJob(String jobId, List<AbstractExecutable> subTasksForCheck) {
        try {
            jobId = jobId.replaceAll("[./]", "");
            final ExecutablePO job = executableDao.getJob(jobId);
            Preconditions.checkArgument(job != null, "there is no related job for job id:" + jobId);

            List<ExecutablePO> tasksForCheck = Lists.newArrayListWithExpectedSize(subTasksForCheck.size());
            for (AbstractExecutable taskForCheck : subTasksForCheck) {
                tasksForCheck.add(parse(taskForCheck));
            }
            job.setTasksForCheck(tasksForCheck);
            executableDao.updateJob(job);
        } catch (PersistentException e) {
            logger.error("fail to update checkpoint job:" + jobId, e);
            throw new RuntimeException(e);
        }
    }

    //for ut
    public void deleteJob(String jobId) {
        try {
            jobId = jobId.replaceAll("[./]", "");
            executableDao.deleteJob(jobId);
        } catch (PersistentException e) {
            logger.error("fail to delete job:" + jobId, e);
            throw new RuntimeException(e);
        }
    }

    public AbstractExecutable getJob(String uuid) {
        try {
            uuid = uuid.replaceAll("[./]", "");
            return parseTo(executableDao.getJob(uuid));
        } catch (PersistentException e) {
            logger.error("fail to get job:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    public AbstractExecutable getJobDigest(String uuid) {
        return parseTo(executableDao.getJobDigest(uuid));
    }

    public void syncDigestsOfJob(String uuid) throws PersistentException {
        executableDao.syncDigestsOfJob(uuid);
    }

    public Output getOutput(String uuid) {
        try {
            uuid = uuid.replaceAll("[./]", "");
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(uuid);
            Preconditions.checkArgument(jobOutput != null, "there is no related output for job id:" + uuid);
            return parseOutput(jobOutput);
        } catch (PersistentException e) {
            logger.error("fail to get job output:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    public Output getOutputDigest(String uuid) {
        final ExecutableOutputPO jobOutput = executableDao.getJobOutputDigest(uuid);
        Preconditions.checkArgument(jobOutput != null, "there is no related output for job id:" + uuid);
        return parseOutput(jobOutput);
    }

    private DefaultOutput parseOutput(ExecutableOutputPO jobOutput) {
        final DefaultOutput result = new DefaultOutput();
        result.setExtra(jobOutput.getInfo());
        result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
        result.setVerboseMsg(jobOutput.getContent());
        result.setLastModified(jobOutput.getLastModified());
        return result;
    }

    public Map<String, Output> getAllOutputs() {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs();
            HashMap<String, Output> result = Maps.newHashMap();
            for (ExecutableOutputPO jobOutput : jobOutputs) {
                result.put(jobOutput.getId(), parseOutput(jobOutput));
            }
            return result;
        } catch (PersistentException e) {
            logger.error("fail to get all job output:", e);
            throw new RuntimeException(e);
        }
    }

    public Map<String, Output> getAllOutputs(long timeStartInMillis, long timeEndInMillis) {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs(timeStartInMillis, timeEndInMillis);
            HashMap<String, Output> result = Maps.newHashMap();
            for (ExecutableOutputPO jobOutput : jobOutputs) {
                result.put(jobOutput.getId(), parseOutput(jobOutput));
            }
            return result;
        } catch (PersistentException e) {
            logger.error("fail to get all job output:", e);
            throw new RuntimeException(e);
        }
    }

    public Map<String, ExecutableOutputPO> getAllOutputDigests(long timeStartInMillis, long timeEndInMillis) {
        final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputDigests(timeStartInMillis,
                timeEndInMillis);
        HashMap<String, ExecutableOutputPO> result = Maps.newHashMap();
        for (ExecutableOutputPO jobOutput : jobOutputs) {
            result.put(jobOutput.getId(), jobOutput);
        }
        return result;
    }

    public Output getOutputFromHDFSByJobId(String jobId, String stepId) {
        return getOutputFromHDFSByJobId(jobId, stepId, 100);
    }

    /**
     * get job output from hdfs json file;
     * if json file contains logPath,
     * the logPath is spark driver log hdfs path(*.json.log), read sample data from log file.
     *
     * @param jobId
     * @return
     */
    public Output getOutputFromHDFSByJobId(String jobId, String stepId, int nLines) {
        AbstractExecutable jobInstance = getJob(jobId);
        String outputStorePath = KylinConfig.getInstanceFromEnv().getJobOutputStorePath(jobInstance.getParam(MetadataConstants.P_PROJECT_NAME), stepId);
        ExecutableOutputPO jobOutput = getJobOutputFromHDFS(outputStorePath);
        if (jobOutput == null) {
            return null;
        }
        assertOutputNotNull(jobOutput, outputStorePath);

        if (Objects.nonNull(jobOutput.getLogPath())) {
            if (isHdfsPathExists(jobOutput.getLogPath())) {
                jobOutput.setContent(getSampleDataFromHDFS(jobOutput.getLogPath(), nLines));
            } else if (StringUtils.isEmpty(jobOutput.getContent()) && Objects.nonNull(getJob(jobId))
                    && getJob(jobId).getStatus() == ExecutableState.RUNNING) {
                jobOutput.setContent("Wait a moment ... ");
            }
        }

        return parseOutput(jobOutput);
    }

    public ExecutableOutputPO getJobOutputFromHDFS(String resPath) {
        DataInputStream din = null;
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(path)) {
                return null;
            }

            din = fs.open(path);
            return JsonUtil.readValue(din, ExecutableOutputPO.class);
        } catch (Exception e) {
            // If the output file on hdfs is corrupt, give an empty output
            logger.error("get job output [{}] from HDFS failed.", resPath, e);
            ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
            executableOutputPO.setContent("job output broken, please check kylin.log");
            return executableOutputPO;
        } finally {
            IOUtils.closeQuietly(din);
        }
    }

    private void assertOutputNotNull(ExecutableOutputPO output, String idOrPath) {
        com.google.common.base.Preconditions.checkArgument(output != null, "there is no related output for job :" + idOrPath);
    }

    /**
     * check the hdfs path exists.
     *
     * @param hdfsPath
     * @return
     */
    public boolean isHdfsPathExists(String hdfsPath) {
        if (StringUtils.isBlank(hdfsPath)) {
            return false;
        }

        Path path = new Path(hdfsPath);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        try {
            return fs.exists(path);
        } catch (IOException e) {
            logger.error("check the hdfs path [{}] exists failed, ", hdfsPath, e);
        }

        return false;
    }

    /**
     * get sample data from hdfs log file.
     * specified the lines, will get the first num lines and last num lines.
     *
     * @param resPath
     * @return
     */
    public String getSampleDataFromHDFS(String resPath, final int nLines) {
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(path)) {
                return null;
            }

            FileStatus fileStatus = fs.getFileStatus(path);
            try (FSDataInputStream din = fs.open(path);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(din, "UTF-8"))) {

                String line;
                StringBuilder sampleData = new StringBuilder();
                for (int i = 0; i < nLines && (line = reader.readLine()) != null; i++) {
                    if (sampleData.length() > 0) {
                        sampleData.append('\n');
                    }
                    sampleData.append(line);
                }

                int offset = sampleData.toString().getBytes("UTF-8").length + 1;
                if (offset < fileStatus.getLen()) {
                    sampleData.append("\n================================================================\n");
                    sampleData.append(tailHdfsFileInputStream(din, offset, fileStatus.getLen(), nLines));
                }
                return sampleData.toString();
            }
        } catch (IOException e) {
            logger.error("get sample data from hdfs log file [{}] failed!", resPath, e);
            return null;
        }
    }

    /**
     * get the last N_LINES lines from the end of hdfs file input stream;
     *
     * @param hdfsDin
     * @param startPos
     * @param endPos
     * @param nLines
     * @return
     * @throws IOException
     */
    private String tailHdfsFileInputStream(FSDataInputStream hdfsDin, final long startPos, final long endPos,
                                           final int nLines) throws IOException {
        com.google.common.base.Preconditions.checkNotNull(hdfsDin);
        com.google.common.base.Preconditions.checkArgument(startPos < endPos && startPos >= 0);
        com.google.common.base.Preconditions.checkArgument(nLines >= 0);

        Deque<String> deque = new ArrayDeque<>();
        int buffSize = 8192;
        byte[] byteBuf = new byte[buffSize];

        long pos = endPos;

        // cause by log last char is \n
        hdfsDin.seek(pos - 1);
        int lastChar = hdfsDin.read();
        if ('\n' == lastChar) {
            pos--;
        }

        int bytesRead = (int) ((pos - startPos) % buffSize);
        if (bytesRead == 0) {
            bytesRead = buffSize;
        }

        pos -= bytesRead;
        int lines = nLines;
        while (lines > 0 && pos >= startPos) {
            bytesRead = hdfsDin.read(pos, byteBuf, 0, bytesRead);

            int last = bytesRead;
            for (int i = bytesRead - 1; i >= 0 && lines > 0; i--) {
                if (byteBuf[i] == '\n') {
                    deque.push(new String(byteBuf, i, last - i, StandardCharsets.UTF_8));
                    lines--;
                    last = i;
                }
            }

            if (lines > 0 && last > 0) {
                deque.push(new String(byteBuf, 0, last, StandardCharsets.UTF_8));
            }

            bytesRead = buffSize;
            pos -= bytesRead;
        }

        StringBuilder sb = new StringBuilder();
        while (!deque.isEmpty()) {
            sb.append(deque.pop());
        }

        return sb.length() > 0 && sb.charAt(0) == '\n' ? sb.substring(1) : sb.toString();
    }


    public List<AbstractExecutable> getAllExecutables() {
        try {
            List<AbstractExecutable> ret = Lists.newArrayList();
            for (ExecutablePO po : executableDao.getJobs()) {
                try {
                    AbstractExecutable ae = parseTo(po);
                    ret.add(ae);
                } catch (IllegalArgumentException e) {
                    logger.error("error parsing one executabePO: ", e);
                }
            }
            return ret;
        } catch (PersistentException e) {
            logger.error("error get All Jobs", e);
            throw new RuntimeException(e);
        }
    }

    public List<AbstractExecutable> getAllExecutables(long timeStartInMillis, long timeEndInMillis) {
        try {
            List<AbstractExecutable> ret = Lists.newArrayList();
            for (ExecutablePO po : executableDao.getJobs(timeStartInMillis, timeEndInMillis)) {
                try {
                    AbstractExecutable ae = parseTo(po);
                    ret.add(ae);
                } catch (IllegalArgumentException e) {
                    logger.error("error parsing one executabePO: ", e);
                }
            }
            return ret;
        } catch (PersistentException e) {
            logger.error("error get All Jobs", e);
            throw new RuntimeException(e);
        }
    }

    public List<AbstractExecutable> getAllExecutableDigests(long timeStartInMillis, long timeEndInMillis) {
        List<AbstractExecutable> ret = Lists.newArrayList();
        for (ExecutablePO po : executableDao.getJobDigests(timeStartInMillis, timeEndInMillis)) {
            try {
                AbstractExecutable ae = parseTo(po);
                ret.add(ae);
            } catch (IllegalArgumentException e) {
                logger.error("error parsing one executabePO: ", e);
            }
        }
        return ret;
    }

    public List<String> getAllJobIds() {
        try {
            return executableDao.getJobIds();
        } catch (PersistentException e) {
            logger.error("error get All Job Ids", e);
            throw new RuntimeException(e);
        }
    }

    public void updateAllRunningJobsToError() {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs();
            for (ExecutableOutputPO executableOutputPO : jobOutputs) {
                if (executableOutputPO.getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
                    executableOutputPO.setStatus(ExecutableState.ERROR.toString());
                    executableDao.updateJobOutput(executableOutputPO);
                }
            }
        } catch (PersistentException e) {
            logger.error("error reset job status from RUNNING to ERROR", e);
            throw new RuntimeException(e);
        }
    }

    public List<String> getAllJobIdsInCache() {
        return executableDao.getJobIdsInCache();
    }

    public void resumeAllRunningJobs() {
        try {
            final List<ExecutableOutputPO> jobOutputs = executableDao.getJobOutputs();
            for (ExecutableOutputPO executableOutputPO : jobOutputs) {
                if (executableOutputPO.getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
                    executableOutputPO.setStatus(ExecutableState.READY.toString());
                    executableDao.updateJobOutput(executableOutputPO);
                }
            }
        } catch (PersistentException e) {
            logger.error("error reset job status from RUNNING to READY", e);
            throw new RuntimeException(e);
        }
    }

    public void resumeRunningJobForce(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (task.getStatus() == ExecutableState.RUNNING) {
                    updateJobOutput(task.getParam(MetadataConstants.P_PROJECT_NAME), task.getId(), ExecutableState.READY, null, null, task.getLogPath());
                    break;
                }
            }
        }
        updateJobOutput(job.getParam(MetadataConstants.P_PROJECT_NAME), jobId, ExecutableState.READY, null, null, job.getLogPath());
    }

    public void resumeJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        Map<String, String> info = null;
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (task.getStatus() == ExecutableState.ERROR || task.getStatus() == ExecutableState.STOPPED) {
                    updateJobOutput(task.getParam(MetadataConstants.P_PROJECT_NAME), task.getId(), ExecutableState.READY, null, "no output", task.getLogPath());
                    break;
                }
            }
            final long endTime = job.getEndTime();
            if (endTime != 0) {
                long interruptTime = System.currentTimeMillis() - endTime + job.getInterruptTime();
                info = Maps.newHashMap(getJobOutput(jobId).getInfo());
                info.put(AbstractExecutable.INTERRUPT_TIME, Long.toString(interruptTime));
                info.remove(AbstractExecutable.END_TIME);
            }
        }
        updateJobOutput(job.getParam(MetadataConstants.P_PROJECT_NAME), jobId, ExecutableState.READY, info, null, job.getLogPath());
    }

    public void discardJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }
        if (job.getStatus().isFinalState()) {
            if (job.getStatus() != ExecutableState.DISCARDED) {
                logger.warn("The status of job " + jobId + " is " + job.getStatus().toString()
                        + ". It's final state and cannot be transfer to be discarded!!!");
            } else {
                logger.warn("The job " + jobId + " has been discarded.");
            }
            throw new IllegalStateException(
                    "The job " + job.getId() + " has already been finished and cannot be discarded.");
        }
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (!task.getStatus().isFinalState()) {
                    updateJobOutput(task.getParam(MetadataConstants.P_PROJECT_NAME), task.getId(), ExecutableState.DISCARDED, null, null, task.getLogPath());
                }
            }
        }
        updateJobOutput(job.getParam(MetadataConstants.P_PROJECT_NAME), jobId, ExecutableState.DISCARDED, null, null, job.getLogPath());
    }

    public void rollbackJob(String jobId, String stepId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (task.getId().compareTo(stepId) >= 0) {
                    logger.debug("rollback task : " + task);
                    updateJobOutput(task.getParam(MetadataConstants.P_PROJECT_NAME), task.getId(), ExecutableState.READY, Maps.<String, String>newHashMap(), "", task.getLogPath());
                }
            }
        }

        if (job.getStatus() == ExecutableState.SUCCEED) {
            updateJobOutput(job.getParam(MetadataConstants.P_PROJECT_NAME), job.getId(), ExecutableState.READY, null, null, job.getLogPath());
        }
    }

    public void pauseJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job == null) {
            return;
        }

        if (!(job.getStatus() == ExecutableState.READY
                || job.getStatus() == ExecutableState.RUNNING)) {
            logger.warn("The status of job " + jobId + " is " + job.getStatus().toString()
                    + ". It's final state and cannot be transfer to be stopped!!!");
            throw new IllegalStateException(
                    "The job " + job.getId() + " has already been finished and cannot be stopped.");
        }
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (!task.getStatus().isFinalState()) {
                    updateJobOutput(task.getParam(MetadataConstants.P_PROJECT_NAME), task.getId(), ExecutableState.STOPPED, null, null, task.getLogPath());
                    break;
                }
            }
        }
        updateJobOutput(job.getParam(MetadataConstants.P_PROJECT_NAME), jobId, ExecutableState.STOPPED, null, null, job.getLogPath());
    }

    public ExecutableOutputPO getJobOutput(String jobId) {
        try {
            return executableDao.getJobOutput(jobId);
        } catch (PersistentException e) {
            logger.error("Can't get output of Job " + jobId);
            throw new RuntimeException(e);
        }
    }

    public void updateJobOutput(String project, String jobId, ExecutableState newStatus, Map<String, String> info, String output, String logPath) {
        if (Thread.currentThread().isInterrupted()) {
            throw new RuntimeException("Current thread is interruptted, aborting");
        }

        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(jobId);
            Preconditions.checkArgument(jobOutput != null, "there is no related output for job id:" + jobId);
            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
            if (newStatus != null && oldStatus != newStatus) {
                if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                    throw new IllegalStateTranferException("there is no valid state transfer from:" + oldStatus + " to:"
                            + newStatus + ", job id: " + jobId);
                }
                jobOutput.setStatus(newStatus.toString());
                logger.info("job id:" + jobId + " from " + oldStatus + " to " + newStatus);
            }
            if (info != null) {
                if (null != jobOutput.getInfo()) {
                    jobOutput.getInfo().putAll(info);
                } else {
                    jobOutput.setInfo(info);
                }
            }
            if ((ExecutableState.ERROR.equals(oldStatus) || ExecutableState.STOPPED.equals(oldStatus))
                    && ExecutableState.READY.equals(newStatus)) {
                jobOutput.getInfo().remove(AbstractExecutable.END_TIME);
            }
            if (output != null) {
                if (output.length() > config.getJobOutputMaxSize()) {
                    output = output.substring(0, config.getJobOutputMaxSize());
                }
                jobOutput.setContent(output);
            }
            executableDao.updateJobOutput(jobOutput);

            if (needDestroyProcess(oldStatus, newStatus)) {
                logger.debug("need kill {}, from {} to {}", jobId, oldStatus, newStatus);
                // kill spark-submit process
                destroyProcess(jobId);
            }
        } catch (PersistentException e) {
            logger.error("error change job:" + jobId + " to " + newStatus);
            throw new RuntimeException(e);
        }

        if (project != null && logPath != null) {
            updateJobOutputToHDFS(project, jobId, output, logPath);
        }
    }

    public void updateJobOutputToHDFS(String project, String jobId, String output, String logPath) {
        ExecutableOutputPO jobOutput = getJobOutput(jobId);
        if (null != output) {
            jobOutput.setContent(output);
        }
        if (null != logPath) {
            jobOutput.setLogPath(logPath);
        }
        String outputHDFSPath = KylinConfig.getInstanceFromEnv().getJobOutputStorePath(project, jobId);
        logger.debug("Update JobOutput To HDFS for {} to {} [{}]", jobId, outputHDFSPath, jobOutput.getContent() != null ? jobOutput.getContent().length() : -1);
        updateJobOutputToHDFS(outputHDFSPath, jobOutput);
    }

    public void updateJobOutputToHDFS(String resPath, ExecutableOutputPO obj) {
        DataOutputStream dout = null;
        try {
            Path path = new Path(resPath);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            dout = fs.create(path, true);
            JsonUtil.writeValue(dout, obj);
        } catch (Exception e) {
            // the operation to update output to hdfs failed, next task should not be interrupted.
            logger.error("update job output [{}] to HDFS failed.", resPath, e);
        } finally {
            IOUtils.closeQuietly(dout);
        }
    }

    private boolean needDestroyProcess(ExecutableState from, ExecutableState to) {
        if (from != ExecutableState.RUNNING || to == null) {
            return false;
        }
        return to == ExecutableState.STOPPED || to == ExecutableState.READY || to == ExecutableState.DISCARDED
                || to == ExecutableState.ERROR;
    }

    public void destroyProcess(String jobId) {
        Process originProc = JobProcessContext.getProcess(jobId);
        if (Objects.nonNull(originProc) && originProc.isAlive()) {
            try {
                final int ppid = JobProcessContext.getPid(originProc);
                logger.info("start to destroy process {} of job {}", ppid, jobId);
                //build cmd template
                StringBuilder cmdBuilder = new StringBuilder("bash ");
                cmdBuilder.append(Paths.get(KylinConfig.getKylinHome(), "bin", KILL_PROCESS_TREE));
                cmdBuilder.append(" ");
                cmdBuilder.append(ppid);
                final String killCmd = cmdBuilder.toString();
                Process killProc = Runtime.getRuntime().exec(killCmd);
                if (killProc.waitFor(CMD_EXEC_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                    logger.info("try to destroy process {} of job {}, exec cmd '{}', exitValue : {}", ppid, jobId,
                            killCmd, killProc.exitValue());
                    if (!originProc.isAlive()) {
                        logger.info("destroy process {} of job {} SUCCEED.", ppid, jobId);
                        return;
                    }
                    logger.info("destroy process {} of job {} FAILED.", ppid, jobId);
                }

                //generally, code executing wouldn't reach here
                logger.warn("destroy process {} of job {} TIMEOUT exceed {}s.", ppid, jobId, CMD_EXEC_TIMEOUT_SEC);
            } catch (Exception e) {
                logger.error("destroy process of job {} FAILED.", jobId, e);
            }
        }
    }

    public void reloadAll() throws IOException {
        executableDao.reloadAll();
    }

    public void forceKillJob(String jobId) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(jobId);
            List<ExecutablePO> tasks = executableDao.getJob(jobId).getTasks();

            for (ExecutablePO task : tasks) {
                if (executableDao.getJobOutput(task.getId()).getStatus().equals("SUCCEED")) {
                    continue;
                } else if (executableDao.getJobOutput(task.getId()).getStatus().equals("RUNNING")) {
                    updateJobOutput(null, task.getId(), ExecutableState.READY, Maps.<String, String>newHashMap(), "", null);
                }
                break;
            }

            if (!jobOutput.getStatus().equals(ExecutableState.ERROR.toString())) {
                jobOutput.setStatus(ExecutableState.ERROR.toString());
                executableDao.updateJobOutput(jobOutput);
            }
        } catch (PersistentException e) {
            throw new RuntimeException(e);
        }
    }

    public void forceKillJobWithRetry(String jobId) {
        boolean done = false;

        while (!done) {
            try {
                forceKillJob(jobId);
                done = true;
            } catch (RuntimeException e) {
                if (!(e.getCause() instanceof PersistentException)) {
                    done = true;
                }
            }
        }
    }

    //for migration only
    //TODO delete when migration finished
    public void resetJobOutput(String jobId, ExecutableState state, String output) {
        try {
            final ExecutableOutputPO jobOutput = executableDao.getJobOutput(jobId);
            jobOutput.setStatus(state.toString());
            if (output != null) {
                jobOutput.setContent(output);
            }
            executableDao.updateJobOutput(jobOutput);
        } catch (PersistentException e) {
            throw new RuntimeException(e);
        }
    }

    public void addJobInfo(String id, Map<String, String> info) {
        if (Thread.currentThread().isInterrupted()) {
            throw new RuntimeException("Current thread is interrupted, aborting");
        }

        if (info == null) {
            return;
        }

        // post process
        if (info.containsKey(MR_JOB_ID) && !info.containsKey(ExecutableConstants.YARN_APP_ID)) {
            String jobId = info.get(MR_JOB_ID);
            if (jobId.startsWith("job_")) {
                info.put(YARN_APP_ID, jobId.replace("job_", "application_"));
            }
        }

        if ((info.containsKey(YARN_APP_ID) || info.containsKey(FLINK_JOB_ID)) && !StringUtils.isEmpty(config.getJobTrackingURLPattern())) {
            String pattern = config.getJobTrackingURLPattern();
            String jobId = info.containsKey(YARN_APP_ID) ? info.get(YARN_APP_ID) : info.get(FLINK_JOB_ID);
            try {
                String newTrackingURL = String.format(Locale.ROOT, pattern, jobId);
                info.put(YARN_APP_URL, newTrackingURL);
            } catch (IllegalFormatException ife) {
                logger.error("Illegal tracking url pattern: " + config.getJobTrackingURLPattern());
            }
        }

        try {
            ExecutableOutputPO output = executableDao.getJobOutput(id);
            Preconditions.checkArgument(output != null, "there is no related output for job id:" + id);
            output.getInfo().putAll(info);
            executableDao.updateJobOutput(output);
        } catch (PersistentException e) {
            logger.error("error update job info, id:" + id + "  info:" + info.toString());
            throw new RuntimeException(e);
        }
    }

    public void addJobInfo(String id, String key, String value) {
        Map<String, String> info = Maps.newHashMap();
        info.put(key, value);
        addJobInfo(id, info);
    }

    private AbstractExecutable parseTo(ExecutablePO executablePO) {
        if (executablePO == null) {
            logger.warn("executablePO is null");
            return null;
        }
        String type = executablePO.getType();
        AbstractExecutable result = newExecutable(type);
        result.initConfig(config);
        result.setId(executablePO.getUuid());
        result.setName(executablePO.getName());
        result.setParams(executablePO.getParams());
        result.setPriority(executablePO.getPriority());

        if (!(result instanceof BrokenExecutable)) {
            List<ExecutablePO> tasks = executablePO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof ChainedExecutable);
                for (ExecutablePO subTask : tasks) {
                    AbstractExecutable subTaskExecutable = parseTo(subTask);
                    if (subTaskExecutable != null) {
                        subTaskExecutable.setParentExecutable(result);
                    }
                    ((ChainedExecutable) result).addTask(parseTo(subTask));
                }
            }
            List<ExecutablePO> tasksForCheck = executablePO.getTasksForCheck();
            if (tasksForCheck != null && !tasksForCheck.isEmpty()) {
                Preconditions.checkArgument(result instanceof CheckpointExecutable);
                for (ExecutablePO subTaskForCheck : tasksForCheck) {
                    ((CheckpointExecutable) result).addTaskForCheck(parseTo(subTaskForCheck));
                }
            }
        }

        return result;
    }

    private AbstractExecutable newExecutable(String type) {
        Class<? extends AbstractExecutable> clazz;
        try {
            clazz = ClassUtil.forName(type, AbstractExecutable.class);
        } catch (ClassNotFoundException ex) {
            clazz = BrokenExecutable.class;
            logger.error("Unknown executable type '" + type + "', using BrokenExecutable");
        }
        try {
            return clazz.getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate " + clazz, e);
        }
    }
}
