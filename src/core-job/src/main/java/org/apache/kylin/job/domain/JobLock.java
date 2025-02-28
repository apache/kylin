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

import java.util.Date;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class JobLock {
    private Long id;

    private String lockId;

    private String project;

    private String lockNode;

    private Date lockExpireTime;

    private int priority;

    private long createTime;

    private long updateTime;

    // placeholder for mybatis ${}
    private String jobLockTable;

    private String database;

    private String jobType = JobTypeEnum.OFFLINE.name();

    public JobLock(String lockId, String project, int priority, JobTypeEnum type) {
        this.lockId = lockId;
        this.project = project;
        this.priority = priority;
        this.createTime = System.currentTimeMillis();
        this.updateTime = System.currentTimeMillis();
        this.jobType = type.name();
    }

    public enum JobTypeEnum {
        STREAMING, OFFLINE, MASTER, OTHER
    }
}
