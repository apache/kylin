/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.cmd;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.tools.HadoopStatusChecker;

/**
 * @author xduo
 * 
 */
public class JavaHadoopCmdOutput extends BaseCommandOutput implements ICommandOutput {

    protected static final Logger log = LoggerFactory.getLogger(JavaHadoopCmdOutput.class);

    protected StringBuilder output;
    protected int exitCode;
    protected JobStepStatusEnum status;
    private final KylinConfig config;
    private final String jobInstanceID;
    private final int jobStepID;
    private final String yarnUrl;
    private final AbstractHadoopJob job;
    private String mrJobID = null;
    private String trackUrl = null;
    private boolean isAsync;

    public JavaHadoopCmdOutput(String jobInstanceID, int jobStepID, JobEngineConfig engineConfig, AbstractHadoopJob job, boolean isAsync) {
        super();
        this.config = engineConfig.getConfig();
        this.yarnUrl = engineConfig.getYarnStatusServiceUrl();
        this.jobInstanceID = jobInstanceID;
        this.jobStepID = jobStepID;
        this.job = job;
        this.isAsync = isAsync;

        init();
    }

    @Override
    public void setStatus(JobStepStatusEnum status) {
        this.status = status;
    }

    @Override
    public JobStepStatusEnum getStatus() {
        if (this.isAsync) {
            if (this.status == JobStepStatusEnum.ERROR) {
                return status;
            }

            if (null == this.mrJobID || null == this.trackUrl) {
                updateHadoopJobInfo();
            }

            status = new HadoopStatusChecker(this.yarnUrl, this.mrJobID, output).checkStatus();

            if (this.status.isComplete()) {
                updateJobCounter();
            }
        } else {
            status = (this.exitCode == 0) ? JobStepStatusEnum.FINISHED : JobStepStatusEnum.ERROR;
        }

        return status;
    }

    @Override
    public void appendOutput(String message) {
        log.debug(message);
        output.append(message).append("\n");
    }

    @Override
    public String getOutput() {
        return output.toString();
    }

    @Override
    public void setExitCode(int exitCode) {
        this.exitCode = exitCode;
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    @Override
    public void reset() {
        init();
    }

    private void init() {
        output = new StringBuilder();
        exitCode = -1;
        status = JobStepStatusEnum.NEW;
    }

    private void updateHadoopJobInfo() {
        try {
            Map<String, String> jobInfo = job.getInfo();

            JobDAO jobDAO = JobDAO.getInstance(config);
            JobInstance jobInstance = jobDAO.getJob(jobInstanceID);
            JobStep jobStep = jobInstance.getSteps().get(jobStepID);
            boolean hasChange = false;

            if (null == this.mrJobID && jobInfo.containsKey(JobInstance.MR_JOB_ID)) {
                this.mrJobID = jobInfo.get(JobInstance.MR_JOB_ID);
                jobStep.putInfo(JobInstance.MR_JOB_ID, this.mrJobID);
                output.append("Get job id " + this.mrJobID).append("\n");
                hasChange = true;
            }

            if (null == this.trackUrl && jobInfo.containsKey(JobInstance.YARN_APP_URL)) {
                this.trackUrl = jobInfo.get(JobInstance.YARN_APP_URL);
                jobStep.putInfo(JobInstance.YARN_APP_URL, this.trackUrl);
                output.append("Get job track url " + this.trackUrl).append("\n");
                hasChange = true;
            }
            if (hasChange) {
                jobDAO.updateJobInstance(jobInstance);
            }
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
            output.append(e.getLocalizedMessage());
        }
    }

    private void updateJobCounter() {
        try {
            Counters counters = job.getCounters();
            if (counters == null) {
                String errorMsg = "no counters for job " + mrJobID;
                log.warn(errorMsg);
                output.append(errorMsg);
                return;
            }
            this.output.append(counters.toString()).append("\n");
            log.debug(counters.toString());

            JobDAO jobDAO = JobDAO.getInstance(config);
            JobInstance jobInstance = jobDAO.getJob(jobInstanceID);
            JobStep jobStep = jobInstance.getSteps().get(jobStepID);

            long mapInputRecords = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
            jobStep.putInfo(JobInstance.SOURCE_RECORDS_COUNT, String.valueOf(mapInputRecords));
            long hdfsBytesWritten = counters.findCounter("FileSystemCounters", "HDFS_BYTES_WRITTEN").getValue();
            jobStep.putInfo(JobInstance.HDFS_BYTES_WRITTEN, String.valueOf(hdfsBytesWritten));

            jobDAO.updateJobInstance(jobInstance);
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
            output.append(e.getLocalizedMessage());
        }
    }

}
