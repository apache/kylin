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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xduo
 * 
 */
public class HadoopCmdOutput {

    protected static final Logger logger = LoggerFactory.getLogger(HadoopCmdOutput.class);

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
    private String hdfsBytesRead;

    public String getMapInputRecords() {
        return mapInputRecords;
    }

    public String getHdfsBytesWritten() {
        return hdfsBytesWritten;
    }

    public String getHdfsBytesRead() {
        return hdfsBytesRead;
    }

    public void updateJobCounter() {
        try {
            Counters counters = job.getCounters();
            if (counters == null) {
                String errorMsg = "no counters for job " + getMrJobId();
                logger.warn(errorMsg);
                output.append(errorMsg);
                return;
            }
            this.output.append(counters.toString()).append("\n");
            logger.debug(counters.toString());

            mapInputRecords = String.valueOf(counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
            hdfsBytesWritten = String.valueOf(counters.findCounter("FileSystemCounters", "HDFS_BYTES_WRITTEN").getValue());
            hdfsBytesRead = String.valueOf(counters.findCounter("FileSystemCounters", "HDFS_BYTES_READ").getValue());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            output.append(e.getLocalizedMessage());
        }
    }

}
