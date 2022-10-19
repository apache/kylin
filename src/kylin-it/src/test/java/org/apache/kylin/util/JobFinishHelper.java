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


package org.apache.kylin.util;

import static org.awaitility.Awaitility.with;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.awaitility.core.ConditionFactory;

public class JobFinishHelper {

    public static void waitJobFinish(KylinConfig config, String project, String jobId, long maxWaitMilliseconds) {
        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        getConditionFactory(maxWaitMilliseconds).until(() -> {
            AbstractExecutable job = executableManager.getJob(jobId);
            ExecutableState status = job.getStatus();
            return status == ExecutableState.SUCCEED || status == ExecutableState.ERROR
                    || status == ExecutableState.PAUSED || status == ExecutableState.DISCARDED
                    || status == ExecutableState.SUICIDAL;
        });
    }

    private static ConditionFactory getConditionFactory(long maxWaitMilliseconds) {
        return with().pollInterval(10, TimeUnit.MILLISECONDS) //
                .and().with().pollDelay(10, TimeUnit.MILLISECONDS) //
                .await().atMost(maxWaitMilliseconds, TimeUnit.MILLISECONDS);
    }
}
