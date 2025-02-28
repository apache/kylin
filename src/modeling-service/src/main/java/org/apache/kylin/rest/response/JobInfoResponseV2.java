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
package org.apache.kylin.rest.response;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class JobInfoResponseV2 {

    @JsonProperty("uuid")
    private String uuid;

    @JsonProperty("job_type")
    private String jobType;

    public static JobInfoResponseV2 convert(JobInfoResponse.JobInfo jobInfo) {
        if (null == jobInfo) {
            return null;
        }
        return new JobInfoResponseV2(jobInfo.getJobId(), jobInfo.getJobName());
    }

    public static List<JobInfoResponseV2> convert(List<JobInfoResponse.JobInfo> jobInfoList) {
        if (CollectionUtils.isEmpty(jobInfoList)) {
            return Lists.newArrayList();
        }

        return jobInfoList.stream().map(JobInfoResponseV2::convert).collect(Collectors.toList());
    }
}
