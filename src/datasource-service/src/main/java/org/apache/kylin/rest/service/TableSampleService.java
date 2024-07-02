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

import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TableSampleService extends BasicService implements TableSamplingSupporter {

    @Autowired
    private AclEvaluate aclEvaluate;

    public boolean hasSamplingJob(String project, String table) {
        aclEvaluate.checkProjectWritePermission(project);
        return CollectionUtils.isNotEmpty(existingRunningSamplingJobs(project, table));
    }

    private List<JobInfo> existingRunningSamplingJobs(String project, String table) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).fetchNotFinalJobsByTypes(
                project, Lists.newArrayList(JobTypeEnum.TABLE_SAMPLING.name()), Lists.newArrayList(table));
    }

    @Override
    public List<String> sampling(Set<String> tables, String project, int rows, int priority, String yarnQueue,
                                 Object tag) {
        aclEvaluate.checkProjectWritePermission(project);
        List<String> jobIds = Lists.newArrayList();
        for (String table : tables) {
            JobParam jobParam = new JobParam()
                    .withOwner(getUsername())
                    .withTable(table)
                    .withProject(project)
                    .withYarnQueue(yarnQueue)
                    .withPriority(priority)
                    .withJobTypeEnum(JobTypeEnum.TABLE_SAMPLING)
                    .withTag(tag)
                    .addExtParams(NBatchConstants.P_SAMPLING_ROWS, String.valueOf(rows));
            jobIds.add(getManager(JobManager.class, project).addJob(jobParam));
        }
        return jobIds;
    }
}
