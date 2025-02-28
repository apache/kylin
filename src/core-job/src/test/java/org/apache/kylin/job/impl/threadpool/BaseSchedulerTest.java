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

package org.apache.kylin.job.impl.threadpool;

import static org.awaitility.Awaitility.with;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.util.JobContextUtil;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import lombok.val;

public abstract class BaseSchedulerTest extends NLocalFileMetadataTestCase {

    protected static ExecutableManager executableManager;

    protected String project;

    protected AtomicInteger killProcessCount;

    public BaseSchedulerTest(String project) {
        this.project = project;
    }

    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        createTestMetadata();
        killProcessCount = new AtomicInteger();
        val originExecutableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        executableManager = Mockito.spy(originExecutableManager);
        Mockito.doAnswer(invocation -> {
            String jobId = invocation.getArgument(0);
            originExecutableManager.destroyProcess(jobId);
            killProcessCount.incrementAndGet();
            return null;
        }).when(executableManager).destroyProcess(Mockito.anyString());
        startScheduler();
    }

    void startScheduler() {
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    protected void waitForJobFinish(String jobId) {
        waitForJobFinish(jobId, 120000);
    }

    protected void waitForJobFinish(String jobId, int maxWaitTime) {
        waitForJobByStatus(jobId, maxWaitTime, null, executableManager);
    }

    protected void waitForJobByStatus(String jobId, int maxWaitMilliseconds, final ExecutableState state,
            final ExecutableManager executableManager) {
        getConditionFactory(maxWaitMilliseconds).until(() -> {
            AbstractExecutable job = executableManager.getJob(jobId);
            ExecutableState status = job.getStatus();
            if (state != null) {
                return status == state;
            }
            return status == ExecutableState.SUCCEED || status == ExecutableState.ERROR
                    || status == ExecutableState.PAUSED || status == ExecutableState.DISCARDED
                    || status == ExecutableState.SUICIDAL;
        });
    }

    private ConditionFactory getConditionFactory(long maxWaitMilliseconds) {
        return with().pollInterval(10, TimeUnit.MILLISECONDS) //
                .and().with().pollDelay(10, TimeUnit.MILLISECONDS) //
                .await().atMost(maxWaitMilliseconds, TimeUnit.MILLISECONDS);
    }

    protected final ConditionFactory getConditionFactory() {
        return getConditionFactory(60000);
    }

}
