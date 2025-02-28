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

package org.apache.kylin.job.domain;

public class JobInfo implements Comparable<JobInfo> {
    private Long id;

    private String jobId;

    private String jobType;

    private String jobStatus;

    private String project;

    private String subject;

    private String modelId;

    private long createTime;

    private long updateTime;

    private Long jobDurationMillis;

    private byte[] jobContent;

    private Long mvcc = 1L;

    private int priority;

    // placeholder for mybatis ${}
    private String jobInfoTable;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public Long getJobDurationMillis() {
        return jobDurationMillis;
    }

    public void setJobDurationMillis(Long jobDurationMillis) {
        this.jobDurationMillis = jobDurationMillis;
    }

    public byte[] getJobContent() {
        return jobContent;
    }

    public void setJobContent(byte[] jobContent) {
        this.jobContent = jobContent;
    }

    public String getJobInfoTable() {
        return jobInfoTable;
    }

    public void setJobInfoTable(String jobInfoTable) {
        this.jobInfoTable = jobInfoTable;
    }

    public Long getMvcc() {
        return mvcc;
    }

    public void setMvcc(Long mvcc) {
        this.mvcc = mvcc;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    @Override
    public boolean equals(Object jobInfo) {
        if (null == jobInfo || !(jobInfo instanceof JobInfo)) {
            return false;
        }
        return this.getJobId().equals(((JobInfo) jobInfo).getJobId());
    }

    @Override
    public int hashCode() {
        return this.getJobId().hashCode();
    }

    @Override
    public int compareTo(JobInfo jobInfo) {
        int priorityCompare = Integer.compare(this.getPriority(), jobInfo.getPriority());
        if (priorityCompare != 0) {
            return priorityCompare;
        }
        return Long.compare(this.getCreateTime(), jobInfo.getCreateTime());
    }
}
