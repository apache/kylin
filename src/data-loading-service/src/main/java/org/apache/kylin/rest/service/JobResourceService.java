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
package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class JobResourceService {

    @Resource
    private JobInfoService jobInfoService;

    public JobResource adjustJobResource(JobResource resource) throws IOException {
        if (resource.cores >= 0 && resource.memory >= 0) {
            return resource;
        }
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val statuses = Lists.newArrayList(ExecutableState.RUNNING);
        val executablePOList = JobContextUtil.getJobInfoDao(config).getExecutablePoByStatus(null, null, statuses);
        int cores = 0;
        long memory = 0;
        List<String> jobs = Lists.newArrayList();
        for (ExecutablePO po : executablePOList) {
            val subExecutablePO = po.getTasks().stream()
                    .filter(p -> ExecutableState.RUNNING.name().equals(p.getOutput().getStatus())).findFirst();
            if (subExecutablePO.isPresent() && resource.getQueue()
                    .equals(subExecutablePO.get().getOutput().getInfo().get(ExecutableConstants.QUEUE_NAME))) {
                val info = subExecutablePO.get().getOutput().getInfo();
                val jobCore = Integer.parseInt(info.getOrDefault(ExecutableConstants.CORES, "0"));
                val jobMemory = Long.parseLong(info.getOrDefault(ExecutableConstants.MEMORY, "0"));
                if (jobCore > 0) {
                    jobs.add(po.getId());
                    cores += jobCore;
                    memory += jobMemory;
                    if (resource.getCores() + cores >= 0 && resource.getMemory() + memory >= 0) {
                        break;
                    }
                }
            }
        }
        if (!jobs.isEmpty()) {
            log.info("adjustJobResource jobs={}", StringUtils.join(jobs, ","));
            jobInfoService.batchUpdateJobStatus(jobs, null, JobActionEnum.PAUSE.name(), Lists.newArrayList());
            jobInfoService.batchUpdateJobStatus(jobs, null, JobActionEnum.RESUME.name(), Lists.newArrayList());
        }
        return new JobResource(resource.getQueue(), cores * -1, memory * -1);
    }

    public Set<String> getQueueNames() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects();
        val jobInfoDao = JobContextUtil.getJobInfoDao(config);
        Set<String> queues = Sets.newHashSet();
        projects.forEach(projectInstance -> {
            JobMapperFilter jobMapperFilter = new JobMapperFilter();
            jobMapperFilter.setProject(projectInstance.getName());
            jobMapperFilter.setStatuses(ExecutableState.SUCCEED);
            jobMapperFilter.setLimit(10);
            val jobs = jobInfoDao.getJobInfoListByFilter(jobMapperFilter);
            if (CollectionUtils.isNotEmpty(jobs)) {
                for (JobInfo jobInfo : jobs) {
                    val po = JobInfoUtil.deserializeExecutablePO(jobInfo);
                    if (CollectionUtils.isNotEmpty(po.getTasks())) {
                        val taskQueues = po.getTasks().stream()
                                .map(p -> p.getOutput().getInfo().get(ExecutableConstants.QUEUE_NAME))
                                .filter(Objects::nonNull).collect(Collectors.toSet());
                        queues.addAll(taskQueues);
                    }
                }
            }
        });

        return queues;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class JobResource {

        private String queue;
        private int cores;
        private long memory;
    }
}
