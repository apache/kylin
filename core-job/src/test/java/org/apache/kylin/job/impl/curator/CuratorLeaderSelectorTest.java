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

package org.apache.kylin.job.impl.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class CuratorLeaderSelectorTest extends LocalFileMetadataTestCase {
    private TestingServer zkTestServer;

    @Before
    public void setup() throws Exception {
        zkTestServer = new TestingServer();
        zkTestServer.start();
        System.setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        System.setProperty("kylin.server.mode", "all");
        createTestMetadata();
    }

    @Test
    public void testGetBasic() throws SchedulerException, IOException, InterruptedException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final String zkString = zkTestServer.getConnectString();
        final String server1 = "server1:1111";
        final String server2 = "server2:2222";
        String jobEnginePath = CuratorScheduler.JOB_ENGINE_LEADER_PATH;
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkString, new ExponentialBackoffRetry(3000, 3));
        client.start();
        CuratorLeaderSelector s1 = new CuratorLeaderSelector(client //
                , jobEnginePath //
                , server1 //
                , new JobEngineConfig(kylinConfig)); //
        Assert.assertFalse(s1.hasDefaultSchedulerStarted());
        CuratorLeaderSelector s2 = new CuratorLeaderSelector(client //
                , jobEnginePath //
                , server2 //
                , new JobEngineConfig(kylinConfig)); //
        s1.start();
        //wait for Selector starting
        Thread.sleep(1000);
        Assert.assertEquals(1, s1.getParticipants().size());
        Assert.assertEquals(s1.getParticipants(), s2.getParticipants());
        s2.start();
        Thread.sleep(1000);
        Assert.assertEquals(2, s1.getParticipants().size());
        Assert.assertEquals(s1.getParticipants(), s2.getParticipants());

        Assert.assertEquals(new Participant(server1, true), s1.getLeader());
        Assert.assertEquals(s1.getLeader(), s2.getLeader());
        assertSchedulerStart(s1);
        s1.close();
        Thread.sleep(1000);
        Assert.assertEquals(1, s1.getParticipants().size());
        Assert.assertEquals(s1.getParticipants(), s2.getParticipants());
        Assert.assertEquals(new Participant(server2, true), s1.getLeader());
        assertSchedulerStart(s2);
        s2.close();
        Thread.sleep(1000);
        Assert.assertEquals(0, s1.getParticipants().size());
        Assert.assertEquals(s1.getParticipants(), s2.getParticipants());
    }

    private void assertSchedulerStart(CuratorLeaderSelector sele) throws InterruptedException {
        for (int i = 0; i < 50 && !sele.hasDefaultSchedulerStarted(); i++) {
            Thread.sleep(300);
        }
        Assert.assertTrue(sele.hasDefaultSchedulerStarted());
    }

    @After
    public void after() throws Exception {
        zkTestServer.close();
        cleanupTestMetadata();
        System.clearProperty("kylin.env.zookeeper-connect-string");
        System.clearProperty("kylin.server.host-address");
        System.clearProperty("kylin.server.mode");
    }
}
