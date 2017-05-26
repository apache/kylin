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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.lock.MockJobLock;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public abstract class BaseSchedulerTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(BaseSchedulerTest.class);
    
    private DefaultScheduler scheduler;

    protected ExecutableManager jobService;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        setFinalStatic(ExecutableConstants.class.getField("DEFAULT_SCHEDULER_INTERVAL_SECONDS"), 10);
        jobService = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        scheduler = DefaultScheduler.createInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

    }

    @After
    public void after() throws Exception {
        DefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }

    protected void waitForJobFinish(String jobId) {
        int error = 0;
        final int errorLimit = 3;
        
        while (error < errorLimit) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                AbstractExecutable job = jobService.getJob(jobId);
                ExecutableState status = job.getStatus();
                if (status == ExecutableState.SUCCEED || status == ExecutableState.ERROR || status == ExecutableState.STOPPED || status == ExecutableState.DISCARDED) {
                    break;
                }
            } catch (Exception ex) {
                logger.error("", ex);
                error++;
            }
        }
        
        if (error >= errorLimit) {
            throw new RuntimeException("waitForJobFinish() encounters exceptions, see logs above");
        }
    }

    protected void waitForJobStatus(String jobId, ExecutableState state, long interval) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == state) {
                break;
            } else {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
