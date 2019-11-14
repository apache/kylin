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

package org.apache.kylin.common.util;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.LocalFileResourceStoreTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestServerRegisterTest {

    private static final Logger LOG = LoggerFactory.getLogger(RestServerRegisterTest.class);

    KylinConfig config;

    @Before
    public void setUp() throws Exception {

        LocalFileResourceStoreTest.staticCreateTestMetadata();
        this.config = KylinConfig.getInstanceFromEnv();
        MiniZookeeperClusterUtil.startMiniZookeeperCluster(config);
        MiniLocalZookeeperCluster zk = MiniZookeeperClusterUtil.getZookeeperCluster();
        if (!zk.isStarted()) {
            throw new RuntimeException("mini zookeeper has not started");
        }
        String zkConnectString = "localhost:" + zk.getZkClientPort();
        LOG.info("zookeeper connect string is " + zkConnectString);
        config.setProperty("kylin.env.zookeeper-connect-string", zkConnectString);
        config.setProperty("kylin.metadata.url", "kylin_meta");
    }

    @After
    public void after() throws Exception {
        MiniZookeeperClusterUtil.shutdownMiniZookeeperCluster();
    }

    @Test
    public void testListServersWithRegister() throws Exception {
        RestServerRegister rs = RestServerRegister.getInstance(this.config);
        List<String> servers = rs.getServers();
        Assert.assertEquals(0, servers.size());
        rs.register();
        //need watch event callback
        Thread.sleep(10);
        Assert.assertEquals(1, rs.getServers().size());

    }
}
