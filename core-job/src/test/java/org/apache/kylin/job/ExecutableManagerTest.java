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

package org.apache.kylin.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class ExecutableManagerTest extends LocalFileMetadataTestCase {

    private ExecutableManager service;

    @Before
    public void setup() throws Exception {
        createTestMetadata("../server/src/test/resources/ut_meta/broken_executable");
        service = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        assertNotNull(service);
        
        // all existing are broken jobs
        List<AbstractExecutable> existing = service.getAllExecutables();
        for (AbstractExecutable exec : existing) {
            assertEquals("BrokenExecutable", exec.getClass().getSimpleName());
            assertEquals(ExecutableState.DISCARDED, exec.getStatus());
        }
        
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        service.addJob(executable);
        List<AbstractExecutable> result = service.getAllExecutables();
        assertEquals(existing.size() + 1, result.size());
        AbstractExecutable another = service.getJob(executable.getId());
        assertJobEqual(executable, another);

        service.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, "test output");
        assertJobEqual(executable, service.getJob(executable.getId()));
    }

    @Test
    public void testDefaultChainedExecutable() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.addTask(new SucceedTestExecutable());
        job.addTask(new SucceedTestExecutable());

        service.addJob(job);
        assertEquals(2, job.getTasks().size());
        AbstractExecutable anotherJob = service.getJob(job.getId());
        assertEquals(DefaultChainedExecutable.class, anotherJob.getClass());
        assertEquals(2, ((DefaultChainedExecutable) anotherJob).getTasks().size());
        assertJobEqual(job, anotherJob);
    }

    @Test
    public void testValidStateTransfer() throws Exception {
        SucceedTestExecutable job = new SucceedTestExecutable();
        String id = job.getId();
        service.addJob(job);
        service.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        service.updateJobOutput(id, ExecutableState.ERROR, null, null);
        service.updateJobOutput(id, ExecutableState.READY, null, null);
        service.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        service.updateJobOutput(id, ExecutableState.READY, null, null);
        service.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        service.updateJobOutput(id, ExecutableState.SUCCEED, null, null);
    }

    @Test(expected = IllegalStateTranferException.class)
    public void testInvalidStateTransfer() {
        SucceedTestExecutable job = new SucceedTestExecutable();
        service.addJob(job);
        service.updateJobOutput(job.getId(), ExecutableState.ERROR, null, null);
        service.updateJobOutput(job.getId(), ExecutableState.STOPPED, null, null);
    }

    private static void assertJobEqual(Executable one, Executable another) {
        assertEquals(one.getClass(), another.getClass());
        assertEquals(one.getId(), another.getId());
        assertEquals(one.getStatus(), another.getStatus());
        assertEquals(one.isRunnable(), another.isRunnable());
        assertEquals(one.getOutput(), another.getOutput());
        assertTrue((one.getParams() == null && another.getParams() == null) || (one.getParams() != null && another.getParams() != null));
        if (one.getParams() != null) {
            assertEquals(one.getParams().size(), another.getParams().size());
            for (String key : one.getParams().keySet()) {
                assertEquals(one.getParams().get(key), another.getParams().get(key));
            }
        }
        if (one instanceof ChainedExecutable) {
            assertTrue(another instanceof ChainedExecutable);
            List<? extends Executable> onesSubs = ((ChainedExecutable) one).getTasks();
            List<? extends Executable> anotherSubs = ((ChainedExecutable) another).getTasks();
            assertTrue((onesSubs == null && anotherSubs == null) || (onesSubs != null && anotherSubs != null));
            if (onesSubs != null) {
                assertEquals(onesSubs.size(), anotherSubs.size());
                for (int i = 0; i < onesSubs.size(); ++i) {
                    assertJobEqual(onesSubs.get(i), anotherSubs.get(i));
                }
            }
        }
    }
}
