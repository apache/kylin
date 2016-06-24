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

import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopJobStatusChecker {

    protected static final Logger logger = LoggerFactory.getLogger(HadoopJobStatusChecker.class);

    public static JobStepStatusEnum checkStatus(Job job, StringBuilder output) {
        if (job == null || job.getJobID() == null) {
            output.append("Skip status check with empty job id..\n");
            return JobStepStatusEnum.WAITING;
        }

        JobStepStatusEnum status = null;
        try {
            switch (job.getStatus().getState()) {
            case SUCCEEDED:
                status = JobStepStatusEnum.FINISHED;
                break;
            case FAILED:
                status = JobStepStatusEnum.ERROR;
                break;
            case KILLED:
                status = JobStepStatusEnum.KILLED;
                break;
            case RUNNING:
                status = JobStepStatusEnum.RUNNING;
                break;
            case PREP:
                status = JobStepStatusEnum.WAITING;
                break;
            default:
                throw new IllegalStateException();
            }
        } catch (Exception e) {
            logger.error("error check status", e);
            output.append("Exception: " + e.getLocalizedMessage() + "\n");
            status = JobStepStatusEnum.ERROR;
        }

        return status;
    }

}
