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

package io.kyligence.kap.secondstorage.test.utils;

import static org.apache.kylin.engine.spark.IndexDataConstructor.firstFailedJobErrorMessage;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageModelCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.junit.Assert;

public interface JobWaiter {
    default void waitJobFinish(String project, String jobId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        DefaultExecutable job = (DefaultExecutable) executableManager.getJob(jobId);
        await().atMost(300, TimeUnit.SECONDS).until(() -> !job.getStatus().isProgressing());
        Assert.assertFalse(job.getStatus().isProgressing());
        if (!Objects.equals(job.getStatus(), ExecutableState.SUCCEED)) {
            Assert.fail(firstFailedJobErrorMessage(executableManager, job));
        }
    }

    default void waitJobEnd(String project, String jobId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        DefaultExecutable job = (DefaultExecutable) executableManager.getJob(jobId);
        await().atMost(300, TimeUnit.SECONDS).until(() -> !job.getStatus().isProgressing());
        Assert.assertFalse(job.getStatus().isProgressing());
    }

    default String triggerClickHouseLoadJob(String project, String modelId, String userName, List<String> segIds) {
        AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
        JobParam jobParam = SecondStorageJobParamUtil.of(project, modelId, userName, segIds.stream());
        ExecutableUtil.computeParams(jobParam);
        localHandler.handle(jobParam);
        return jobParam.getJobId();
    }

    default String triggerModelCleanJob(String project, String modelId, String userName) {
        AbstractJobHandler handler = new SecondStorageModelCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.modelCleanParam(project, modelId, userName);
        ExecutableUtil.computeParams(param);
        handler.handle(param);
        return param.getJobId();
    }

    default void waitAllJobFinish(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NExecutableManager.getInstance(config, project).getAllExecutables()
                .forEach(exec -> waitJobFinish(project, exec.getId()));
    }
}
