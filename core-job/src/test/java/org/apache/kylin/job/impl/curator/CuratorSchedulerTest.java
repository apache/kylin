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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.ZKUtil;
import org.apache.kylin.job.execution.ExecutableManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 */
public class CuratorSchedulerTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CuratorSchedulerTest.class);

    private TestingServer zkTestServer;

    protected ExecutableManager jobService;

    @Before
    public void setup() throws Exception {
        zkTestServer = new TestingServer();
        zkTestServer.start();
        System.setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        System.setProperty("kylin.server.mode", "query");
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        zkTestServer.close();
        cleanupTestMetadata();
        System.clearProperty("kylin.env.zookeeper-connect-string");
        System.clearProperty("kylin.server.host-address");
        System.clearProperty("kylin.server.cluster-servers");
        System.clearProperty("kylin.server.mode");
    }

    @Test
    public void test() throws Exception {

        final String zkString = zkTestServer.getConnectString();

        ServiceDiscovery<LinkedHashMap> serviceDiscovery = null;
        CuratorFramework curatorClient = null;
        try {

            final CuratorScheduler.JsonInstanceSerializer<LinkedHashMap> serializer = new CuratorScheduler.JsonInstanceSerializer<>(
                    LinkedHashMap.class);
            String servicePath = CuratorScheduler.KYLIN_SERVICE_PATH;
            curatorClient = ZKUtil.newZookeeperClient(zkString, new ExponentialBackoffRetry(3000, 3));
            serviceDiscovery = ServiceDiscoveryBuilder.builder(LinkedHashMap.class).client(curatorClient)
                    .basePath(servicePath).serializer(serializer).build();
            serviceDiscovery.start();

            final ExampleServer server1 = new ExampleServer("localhost:1111");
            final ExampleServer server2 = new ExampleServer("localhost:2222");

            Collection<String> serviceNames = serviceDiscovery.queryForNames();
            Assert.assertTrue(serviceNames.size() == 1);
            Assert.assertTrue(CuratorScheduler.SERVICE_NAME.equals(serviceNames.iterator().next()));
            Collection<ServiceInstance<LinkedHashMap>> instances = serviceDiscovery
                    .queryForInstances(CuratorScheduler.SERVICE_NAME);
            Assert.assertTrue(instances.size() == 2);
            List<ServiceInstance<LinkedHashMap>> instancesList = Lists.newArrayList(instances);

            final List<String> instanceNodes = Lists.transform(instancesList,
                    new Function<ServiceInstance<LinkedHashMap>, String>() {

                        @Nullable
                        @Override
                        public String apply(@Nullable ServiceInstance<LinkedHashMap> stringServiceInstance) {
                            return (String) stringServiceInstance.getPayload()
                                    .get(CuratorScheduler.SERVICE_PAYLOAD_DESCRIPTION);
                        }
                    });

            Assert.assertTrue(instanceNodes.contains(server1.getAddress() + ":query"));
            Assert.assertTrue(instanceNodes.contains(server2.getAddress() + ":query"));

            // stop one server
            server1.close();
            instances = serviceDiscovery.queryForInstances(CuratorScheduler.SERVICE_NAME);
            Assert.assertTrue(instances.size() == 1);
            Assert.assertEquals(server2.getAddress() + ":query",
                    instances.iterator().next().getPayload().get(CuratorScheduler.SERVICE_PAYLOAD_DESCRIPTION));

            // all stop
            server2.close();
            instances = serviceDiscovery.queryForInstances(CuratorScheduler.SERVICE_NAME);
            Assert.assertTrue(instances.size() == 0);

        } finally {
            CloseableUtils.closeQuietly(serviceDiscovery);
            CloseableUtils.closeQuietly(curatorClient);
        }

    }

}
