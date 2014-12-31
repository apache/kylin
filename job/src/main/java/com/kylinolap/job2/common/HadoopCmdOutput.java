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

package com.kylinolap.job2.common;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.cmd.BaseCommandOutput;
import com.kylinolap.job.cmd.ICommandOutput;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.tools.HadoopStatusChecker;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author xduo
 * 
 */
public class HadoopCmdOutput {

    protected static final Logger log = LoggerFactory.getLogger(HadoopCmdOutput.class);

    private StringBuilder output;
    private final String yarnUrl;
    private final AbstractHadoopJob job;
    private String mrJobID = null;
    private String trackUrl = null;

    public HadoopCmdOutput(String yarnUrl, AbstractHadoopJob job) {
        super();
        this.yarnUrl = yarnUrl;
        this.job = job;
        this.output = new StringBuilder();
    }

    public JobStepStatusEnum getStatus() {
        getTrackUrl();
        getMrJobId();
        final JobStepStatusEnum jobStepStatusEnum = new HadoopStatusChecker(this.yarnUrl, this.mrJobID, output).checkStatus();
        if (jobStepStatusEnum.isComplete()) {
            updateJobCounter();
        }
        return jobStepStatusEnum;
    }

    public String getOutput() {
        return output.toString();
    }

    public String getMrJobId() {
        try {
            if (mrJobID == null) {
                mrJobID = job.getInfo().get(JobInstance.MR_JOB_ID);
            }
            return mrJobID;
        } catch (JobException e) {
            throw new RuntimeException(e);
        }
    }

    public String getTrackUrl() {
        try {
            if (trackUrl == null) {
                trackUrl = job.getInfo().get(JobInstance.YARN_APP_URL);
            }
            return trackUrl;
        } catch (JobException e) {
            throw new RuntimeException(e);
        }
    }


    private String mapInputRecords;
    private String hdfsBytesWritten;

    public String getMapInputRecords() {
        return mapInputRecords;
    }

    public String getHdfsBytesWritten() {
        return hdfsBytesWritten;
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

            mapInputRecords = String.valueOf(counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
//            jobStep.putInfo(JobInstance.SOURCE_RECORDS_COUNT, String.valueOf(mapInputRecords));
            hdfsBytesWritten = String.valueOf(counters.findCounter("FileSystemCounters", "HDFS_BYTES_WRITTEN").getValue());
//            jobStep.putInfo(JobInstance.HDFS_BYTES_WRITTEN, String.valueOf(hdfsBytesWritten));
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
            output.append(e.getLocalizedMessage());
        }
    }

}
