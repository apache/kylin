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
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.rest.JobMapperFilter;

@Mapper
public interface JobInfoMapper {
    int deleteByProject(@Param("project") String project);

    int insert(JobInfo row);

    int deleteByJobId(@Param("jobId") String jobId);

    int deleteByJobIdList(@Param("jobStatusList") List<String> jobStatusList,
            @Param("jobIdList") List<String> jobIdList);

    int deleteAllJob();

    int insertJobInfoSelective(JobInfo jobInfo);

    JobInfo selectByJobId(@Param("jobId") String jobId);

    int updateByJobIdSelective(JobInfo jobInfo);

    List<String> findJobIdListByStatusBatch(@Param("status") String status, @Param("batchSize") int batchSize);

    List<JobInfo> selectByJobFilter(JobMapperFilter jobMapperFilter);

    long countByJobFilter(JobMapperFilter jobMapperFilter);

    long getEarliestCreateTime(@Param("project") String project);

}
