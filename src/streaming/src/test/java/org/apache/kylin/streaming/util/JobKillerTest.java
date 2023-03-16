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
package org.apache.kylin.streaming.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Logger;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.util.concurrent.UncheckedTimeoutException;
import lombok.val;

public class JobKillerTest extends StreamingTestCase {
    private static String PROJECT = "streaming_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testKillApplication() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        JobKiller.killApplication(id);
    }

    @Test
    public void testMockCreateClusterManager() {
        ReflectionUtils.setField(JobKiller.class, "mock", new MockClusterManager() {
            @Override
            public boolean applicationExisted(String jobId) {
                return true;
            }
        });
        val clusterManager = JobKiller.createClusterManager();
        Assert.assertTrue(clusterManager != null);
        val mock = ReflectionUtils.getField(JobKiller.class, "mock");
        Assert.assertTrue(mock != null);
        val isYarnEnv = (Boolean) ReflectionUtils.getField(JobKiller.class, "isYarnEnv");
        Assert.assertFalse(isYarnEnv);
        ReflectionUtils.setField(JobKiller.class, "mock", null);
    }

    @Test
    public void testCreateClusterManager() {
        val clusterManager = JobKiller.createClusterManager();
        Assert.assertTrue(!(clusterManager instanceof MockClusterManager));
    }

    @Test
    public void testYarnApplicationExisted() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        ReflectionUtils.setField(JobKiller.class, "mock", new MockClusterManager() {
            @Override
            public boolean applicationExisted(String jobId) {
                return true;
            }
        });
        Assert.assertTrue(JobKiller.applicationExisted(id));
        ReflectionUtils.setField(JobKiller.class, "mock", null);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testYarnApplicationExistedException() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        ReflectionUtils.setField(JobKiller.class, "mock", new MockClusterManager() {
            @Override
            public boolean applicationExisted(String jobId) {
                throw new UncheckedTimeoutException("mock timeout");
            }
        });
        Assert.assertFalse(JobKiller.applicationExisted(id));
        ReflectionUtils.setField(JobKiller.class, "mock", null);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillYarnApplication() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        ReflectionUtils.setField(JobKiller.class, "mock", new MockClusterManager() {
            @Override
            public boolean applicationExisted(String jobId) {
                return true;
            }
        });
        JobKiller.killApplication(id);
        ReflectionUtils.setField(JobKiller.class, "mock", null);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillYarnApplicationException() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        ReflectionUtils.setField(JobKiller.class, "mock", new MockClusterManager() {
            @Override
            public boolean applicationExisted(String jobId) {
                throw new UncheckedTimeoutException("mock timeout");
            }
        });
        JobKiller.killApplication(id);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillBuildProcess() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        Assert.assertEquals(1, JobKiller.killProcess(streamingJobMeta));
    }

    @Test
    public void testKillMergeProcess() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        Assert.assertEquals(1, JobKiller.killProcess(streamingJobMeta));
    }

    @Test
    public void testKillYarnProcess() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        JobKiller.killProcess(streamingJobMeta);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillYarnProcess1() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        streamingJobMeta.setNodeInfo("127.0.0.1:7070");
        JobKiller.killProcess(streamingJobMeta);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillYarnProcess2() {
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", true);
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        streamingJobMeta.setNodeInfo(AddressUtil.getLocalInstance());
        JobKiller.killProcess(streamingJobMeta);
        ReflectionUtils.setField(JobKiller.class, "isYarnEnv", false);
    }

    @Test
    public void testKillYarnEnvProcess() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        val exec = new CliCommandExecutor();
        val strLogger = new JobKiller.StringLogger() {
            public List<String> getContents() {
                val mockList = new ArrayList<String>();
                mockList.add("1");
                return mockList;
            }
        };
        JobKiller.killYarnEnvProcess(exec, streamingJobMeta, strLogger);
    }

    @Test
    public void testKillYarnEnvProcessException() {
        val id = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        val mgr = StreamingJobManager.getInstance(getTestConfig(), PROJECT);
        val streamingJobMeta = mgr.getStreamingJobByUuid(id);
        val exec = new CliCommandExecutor() {
            public CliCmdExecResult execute(String command, Logger logAppender) throws ShellException {
                throw new ShellException("test");
            }
        };
        JobKiller.killYarnEnvProcess(exec, streamingJobMeta, null);
    }

    @Test
    public void testGrepProcess() {
        val config = getTestConfig();

        CliCommandExecutor exec = config.getCliCommandExecutor();
        val strLogger = new JobKiller.StringLogger();
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b73_simple_test";

        try {
            int resultCode = JobKiller.grepProcess(exec, strLogger, jobId);
            Assert.assertEquals(0, resultCode);
            Assert.assertTrue(strLogger.getContents().isEmpty());
        } catch (Exception e) {
        }
    }

    @Test
    public void testDoKillProcess() {
        val config = getTestConfig();

        CliCommandExecutor exec = config.getCliCommandExecutor();
        val jobId = "e78a89dd-847f-4574-8afa-8768b4228b73_simple_test";

        try {
            int resultCode = JobKiller.doKillProcess(exec, jobId, false);
            Assert.assertEquals(0, resultCode);
        } catch (Exception e) {
        }
        try {
            int resultCode = JobKiller.doKillProcess(exec, jobId, true);
            Assert.assertEquals(0, resultCode);
        } catch (Exception e) {
        }
    }

    @Test
    public void testStringLog() {
        val log = new JobKiller.StringLogger();
        Assert.assertTrue(log.getContents().isEmpty());
        log.log("test");
        Assert.assertTrue(!log.getContents().isEmpty());
    }
}
