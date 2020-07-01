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
package org.apache.kylin.common.zookeeper;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.ZKUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 */
public class KylinServerDiscoveryTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(KylinServerDiscoveryTest.class);

    private TestingServer zkTestServer;

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
            String servicePath = KylinServerDiscovery.SERVICE_PATH;
            final KylinServerDiscovery.JsonInstanceSerializer<LinkedHashMap> serializer =
                    new KylinServerDiscovery.JsonInstanceSerializer<>(LinkedHashMap.class);
            curatorClient = ZKUtil.newZookeeperClient(zkString, new ExponentialBackoffRetry(3000, 3));
            serviceDiscovery = ServiceDiscoveryBuilder.builder(LinkedHashMap.class).client(curatorClient)
                    .basePath(servicePath).serializer(serializer).build();
            serviceDiscovery.start();

            final ExampleServer server1 = new ExampleServer("localhost:1111");
            final ExampleServer server2 = new ExampleServer("localhost:2222");

            Collection<String> serviceNames = serviceDiscovery.queryForNames();
            Assert.assertTrue(serviceNames.size() == 1);
            Assert.assertTrue(KylinServerDiscovery.SERVICE_NAME.equals(serviceNames.iterator().next()));
            Collection<ServiceInstance<LinkedHashMap>> instances = serviceDiscovery
                    .queryForInstances(KylinServerDiscovery.SERVICE_NAME);
            Assert.assertTrue(instances.size() == 2);
            List<ServiceInstance<LinkedHashMap>> instancesList = Lists.newArrayList(instances);

            final List<String> instanceNodes = instancesList.stream()
                    .map(input -> input.getAddress() + ":" + input.getPort() + ":"
                            + input.getPayload().get(KylinServerDiscovery.SERVICE_PAYLOAD_DESCRIPTION))
                    .collect(Collectors.toList());

            Assert.assertTrue(instanceNodes.contains(server1.getAddress() + ":query"));
            Assert.assertTrue(instanceNodes.contains(server2.getAddress() + ":query"));

            // stop one server
            server1.close();
            instances = serviceDiscovery.queryForInstances(KylinServerDiscovery.SERVICE_NAME);
            ServiceInstance<LinkedHashMap> existingInstance = instances.iterator().next();
            Assert.assertTrue(instances.size() == 1);
            Assert.assertEquals(server2.getAddress() + ":query",
                    existingInstance.getAddress() + ":" + existingInstance.getPort() + ":"
                            + existingInstance.getPayload().get(KylinServerDiscovery.SERVICE_PAYLOAD_DESCRIPTION));

            // all stop
            server2.close();
            instances = serviceDiscovery.queryForInstances(KylinServerDiscovery.SERVICE_NAME);
            Assert.assertTrue(instances.size() == 0);

        } finally {
            CloseableUtils.closeQuietly(serviceDiscovery);
            CloseableUtils.closeQuietly(curatorClient);
        }

    }

}
