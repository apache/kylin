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

package com.kylinolap.job.common;

import com.kylinolap.job.JobInstance;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.tools.HadoopStatusChecker;
import com.kylinolap.job.constants.ExecutableConstants;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xduo
 * 
 */
public class HadoopCmdOutput {

    protected static final Logger log = LoggerFactory.getLogger(HadoopCmdOutput.class);

    private final StringBuilder output;
    private final Job job;

    public HadoopCmdOutput(Job job, StringBuilder output) {
        super();
        this.job = job;
        this.output = output;
    }

    public String getMrJobId() {
        return getInfo().get(ExecutableConstants.MR_JOB_ID);
    }

    public Map<String, String> getInfo() {
        if (job != null) {
            Map<String, String> status = new HashMap<String, String>();
            if (null != job.getJobID()) {
                status.put(ExecutableConstants.MR_JOB_ID, job.getJobID().toString());
            }
            if (null != job.getTrackingURL()) {
                status.put(ExecutableConstants.YARN_APP_URL, job.getTrackingURL().toString());
            }
            return status;
        } else {
            return Collections.emptyMap();
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

    public void updateJobCounter() {
        try {
            Counters counters = job.getCounters();
            if (counters == null) {
                String errorMsg = "no counters for job " + getMrJobId();
                log.warn(errorMsg);
                output.append(errorMsg);
                return;
            }
            this.output.append(counters.toString()).append("\n");
            log.debug(counters.toString());

            mapInputRecords = String.valueOf(counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
            hdfsBytesWritten = String.valueOf(counters.findCounter("FileSystemCounters", "HDFS_BYTES_WRITTEN").getValue());
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
            output.append(e.getLocalizedMessage());
        }
    }

}
