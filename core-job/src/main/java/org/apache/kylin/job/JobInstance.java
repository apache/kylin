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

package org.apache.kylin.job;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobStepCmdTypeEnum;
import org.apache.kylin.job.constant.JobStepStatusEnum;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JobInstance extends RootPersistentEntity implements Comparable<JobInstance> {

    public static final String YARN_APP_URL = "yarn_application_tracking_url";
    public static final String MR_JOB_ID = "mr_job_id";

    @JsonProperty("name")
    private String name;
    @JsonProperty("projectName")
    private String projectName;
    @JsonProperty("type")
    private CubeBuildTypeEnum type; // java implementation
    @JsonProperty("duration")
    private long duration;
    @JsonProperty("related_cube")
    private String relatedCube;
    @JsonProperty("display_cube_name")
    private String displayCubeName;
    @JsonProperty("related_segment")
    private String relatedSegment;
    @JsonProperty("related_segment_name")
    private String relatedSegmentName;
    @JsonProperty("exec_start_time")
    private long execStartTime;
    @JsonProperty("exec_end_time")
    private long execEndTime;
    @JsonProperty("exec_interrupt_time")
    private long execInterruptTime;
    @JsonProperty("mr_waiting")
    private long mrWaiting = 0;
    @JsonManagedReference
    @JsonProperty("steps")
    private List<JobStep> steps;
    @JsonProperty("submitter")
    private String submitter;
    @JsonProperty("job_status")
    private JobStatusEnum status;
    @JsonProperty("build_instance")
    private String buildInstance;

    public JobStep getRunningStep() {
        for (JobStep step : this.getSteps()) {
            if (step.getStatus().equals(JobStepStatusEnum.RUNNING)
                    || step.getStatus().equals(JobStepStatusEnum.WAITING)) {
                return step;
            }
        }

        return null;
    }

    @JsonProperty("progress")
    public double getProgress() {
        int completedStepCount = 0;
        for (JobStep step : this.getSteps()) {
            if (step.getStatus().equals(JobStepStatusEnum.FINISHED)) {
                completedStepCount++;
            }
        }

        return 100.0 * completedStepCount / steps.size();
    }

    public JobStatusEnum getStatus() {
        return this.status;
    }

    public void setStatus(JobStatusEnum status) {
        this.status = status;
    }

    //    @JsonProperty("job_status")
    //    public JobStatusEnum getStatus() {
    //
    //        // JobStatusEnum finalJobStatus;
    //        int compositResult = 0;
    //
    //        // if steps status are all NEW, then job status is NEW
    //        // if steps status are all FINISHED, then job status is FINISHED
    //        // if steps status are all PENDING, then job status is PENDING
    //        // if steps status are FINISHED and PENDING, the job status is PENDING
    //        // if one of steps status is RUNNING, then job status is RUNNING
    //        // if one of steps status is ERROR, then job status is ERROR
    //        // if one of steps status is KILLED, then job status is KILLED
    //        // default status is RUNNING
    //
    //        System.out.println(this.getName());
    //
    //        for (JobStep step : this.getSteps()) {
    //            //System.out.println("step: " + step.getSequenceID() + "'s status:" + step.getStatus());
    //            compositResult = compositResult | step.getStatus().getCode();
    //        }
    //
    //        System.out.println();
    //
    //        if (compositResult == JobStatusEnum.FINISHED.getCode()) {
    //            return JobStatusEnum.FINISHED;
    //        } else if (compositResult == JobStatusEnum.NEW.getCode()) {
    //            return JobStatusEnum.NEW;
    //        } else if (compositResult == JobStatusEnum.PENDING.getCode()) {
    //            return JobStatusEnum.PENDING;
    //        } else if (compositResult == (JobStatusEnum.FINISHED.getCode() | JobStatusEnum.PENDING.getCode())) {
    //            return JobStatusEnum.PENDING;
    //        } else if ((compositResult & JobStatusEnum.ERROR.getCode()) == JobStatusEnum.ERROR.getCode()) {
    //            return JobStatusEnum.ERROR;
    //        } else if ((compositResult & JobStatusEnum.DISCARDED.getCode()) == JobStatusEnum.DISCARDED.getCode()) {
    //            return JobStatusEnum.DISCARDED;
    //        } else if ((compositResult & JobStatusEnum.RUNNING.getCode()) == JobStatusEnum.RUNNING.getCode()) {
    //            return JobStatusEnum.RUNNING;
    //        }
    //
    //        return JobStatusEnum.RUNNING;
    //    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public CubeBuildTypeEnum getType() {
        return type;
    }

    public void setType(CubeBuildTypeEnum type) {
        this.type = type;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String getRelatedCube() { // if model check, return model name.
        return relatedCube;
    }

    public void setRelatedCube(String relatedCube) {
        this.relatedCube = relatedCube;
    }

    public String getDisplayCubeName() {
        if (StringUtils.isBlank(displayCubeName)) {
            return relatedCube;
        } else {
            return displayCubeName;
        }
    }

    public void setDisplayCubeName(String displayCubeName) {
        this.displayCubeName = displayCubeName;
    }

    public String getRelatedSegment() {
        return relatedSegment;
    }

    public void setRelatedSegment(String relatedSegment) {
        this.relatedSegment = relatedSegment;
    }

    public String getRelatedSegmentName() {
        return relatedSegmentName;
    }

    public void setRelatedSegmentName(String relatedSegmentName) {
        this.relatedSegmentName = relatedSegmentName;
    }

    /**
     * @return the execStartTime
     */
    public long getExecStartTime() {
        return execStartTime;
    }

    /**
     * @param execStartTime the execStartTime to set
     */
    public void setExecStartTime(long execStartTime) {
        this.execStartTime = execStartTime;
    }

    /**
     * @return the execEndTime
     */
    public long getExecEndTime() {
        return execEndTime;
    }

    /**
     * @return the execInterruptTime
     */
    public long getExecInterruptTime() {
        return execInterruptTime;
    }

    /**
     * @param execInterruptTime the execInterruptTime to set
     */
    public void setExecInterruptTime(long execInterruptTime) {
        this.execInterruptTime = execInterruptTime;
    }

    /**
     * @param execEndTime the execEndTime to set
     */
    public void setExecEndTime(long execEndTime) {
        this.execEndTime = execEndTime;
    }

    public long getMrWaiting() {
        return this.mrWaiting;
    }

    public void setMrWaiting(long mrWaiting) {
        this.mrWaiting = mrWaiting;
    }

    public List<JobStep> getSteps() {
        if (steps == null) {
            steps = Lists.newArrayList();
        }
        return steps;
    }

    public void clearSteps() {
        getSteps().clear();
    }

    public void addSteps(Collection<JobStep> steps) {
        this.getSteps().addAll(steps);
    }

    public void addStep(JobStep step) {
        getSteps().add(step);
    }

    public void addStep(int index, JobStep step) {
        getSteps().add(index, step);
    }

    public JobStep findStep(String stepName) {
        for (JobStep step : getSteps()) {
            if (stepName.equals(step.getName())) {
                return step;
            }
        }
        return null;
    }

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String submitter) {
        this.submitter = submitter;
    }

    public String getBuildInstance() {
        return buildInstance;
    }

    public void setBuildInstance(String buildInstance) {
        this.buildInstance = buildInstance;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JobStep implements Comparable<JobStep> {

        @JsonBackReference
        private JobInstance jobInstance;

        @JsonProperty("id")
        private String id;

        @JsonProperty("name")
        private String name;

        @JsonProperty("sequence_id")
        private int sequenceID;

        @JsonProperty("exec_cmd")
        private String execCmd;

        @JsonProperty("interrupt_cmd")
        private String InterruptCmd;

        @JsonProperty("exec_start_time")
        private long execStartTime;
        @JsonProperty("exec_end_time")
        private long execEndTime;
        @JsonProperty("exec_wait_time")
        private long execWaitTime;

        @JsonProperty("step_status")
        private JobStepStatusEnum status = JobStepStatusEnum.PENDING;

        @JsonProperty("cmd_type")
        private JobStepCmdTypeEnum cmdType = JobStepCmdTypeEnum.SHELL_CMD_HADOOP;

        @JsonProperty("info")
        private ConcurrentHashMap<String, String> info = new ConcurrentHashMap<String, String>();

        @JsonProperty("run_async")
        private boolean runAsync = false;

        private ConcurrentHashMap<String, String> getInfo() {
            return info;
        }

        public void putInfo(String key, String value) {
            getInfo().put(key, value);
        }

        public String getInfo(String key) {
            return getInfo().get(key);
        }

        public void clearInfo() {
            getInfo().clear();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getSequenceID() {
            return sequenceID;
        }

        public void setSequenceID(int sequenceID) {
            this.sequenceID = sequenceID;
        }

        public String getExecCmd() {
            return execCmd;
        }

        public void setExecCmd(String execCmd) {
            this.execCmd = execCmd;
        }

        public JobStepStatusEnum getStatus() {
            return status;
        }

        public void setStatus(JobStepStatusEnum status) {
            this.status = status;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        /**
         * @return the execStartTime
         */
        public long getExecStartTime() {
            return execStartTime;
        }

        /**
         * @param execStartTime the execStartTime to set
         */
        public void setExecStartTime(long execStartTime) {
            this.execStartTime = execStartTime;
        }

        /**
         * @return the execEndTime
         */
        public long getExecEndTime() {
            return execEndTime;
        }

        /**
         * @param execEndTime the execEndTime to set
         */
        public void setExecEndTime(long execEndTime) {
            this.execEndTime = execEndTime;
        }

        public long getExecWaitTime() {
            return execWaitTime;
        }

        public void setExecWaitTime(long execWaitTime) {
            this.execWaitTime = execWaitTime;
        }

        public String getInterruptCmd() {
            return InterruptCmd;
        }

        public void setInterruptCmd(String interruptCmd) {
            InterruptCmd = interruptCmd;
        }

        public JobStepCmdTypeEnum getCmdType() {
            return cmdType;
        }

        public void setCmdType(JobStepCmdTypeEnum cmdType) {
            this.cmdType = cmdType;
        }

        /**
         * @return the runAsync
         */
        public boolean isRunAsync() {
            return runAsync;
        }

        /**
         * @param runAsync the runAsync to set
         */
        public void setRunAsync(boolean runAsync) {
            this.runAsync = runAsync;
        }


        /**
         * @return the jobInstance
         */
        public JobInstance getJobInstance() {
            return jobInstance;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + sequenceID;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            JobStep other = (JobStep) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            if (sequenceID != other.sequenceID)
                return false;
            return true;
        }

        @Override
        public int compareTo(JobStep o) {
            if (this.sequenceID < o.sequenceID) {
                return -1;
            } else if (this.sequenceID > o.sequenceID) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public int compareTo(JobInstance o) {
        return o.lastModified < this.lastModified ? -1 : o.lastModified > this.lastModified ? 1 : 0;
    }

}
