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

package org.apache.kylin.job.handler;

import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.factory.JobFactoryConstant;
import org.apache.kylin.job.model.JobParam;

import java.util.stream.Collectors;

public class SecondStorageIndexCleanJobHandler extends AbstractSecondStorageJobHanlder {
    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataflow dataflow = NDataflowManager.getInstance(kylinConfig, jobParam.getProject())
                .getDataflow(jobParam.getModel());
        SecondStorageCleanJobBuildParams params = new SecondStorageCleanJobBuildParams(
                jobParam.getTargetSegments().stream().map(dataflow::getSegment).collect(Collectors.toSet()),
                jobParam,
                JobTypeEnum.SECOND_STORAGE_INDEX_CLEAN);
        params.setProject(dataflow.getProject());
        params.setModelId(dataflow.getModel().getId());
        params.setDataflowId(dataflow.getId());
        params.setSecondStorageDeleteLayoutIds(jobParam.getSecondStorageDeleteLayoutIds());
        return JobFactory.createJob(JobFactoryConstant.STORAGE_INDEX_CLEAN_FACTORY, params);
    }
}
