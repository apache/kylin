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

package org.apache.kylin.job.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.domain.PriorityFistRandomOrderJob;

@Mapper
public interface JobLockMapper {
    int deleteByPrimaryKey(@Param("id") Long id);

    int insert(JobLock row);

    int insertSelective(JobLock row);

    JobLock selectByPrimaryKey(@Param("id") Long id);

    JobLock selectByJobId(@Param("jobId") String jobId);

    // --------------------------------------
    String findNodeByLockId(@Param("lockId") String lockId);

    int getActiveJobLockCount();

    int deleteAllJobLock();

    int updateLock(@Param("lockId") String lockId, @Param("lockNode") String lockNode,
            @Param("renewalSec") long renewalSec, @Param("updateTime") long updateTime);

    int removeLock(@Param("lockId") String lockId, @Param("lockNode") String lockNode);

    int batchRemoveLock(@Param("jobIdList") List<String> jobIdList);

    List<PriorityFistRandomOrderJob> findNonLockIdList(@Param("batchSize") int batchSize,
            @Param("projects") List<String> projects);

    List<String> findExpiredORNonLockIdList(@Param("batchSize") int batchSize);

    List<JobLock> fetchAll();
}
