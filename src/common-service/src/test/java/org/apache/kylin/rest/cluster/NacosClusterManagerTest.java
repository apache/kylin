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
package org.apache.kylin.rest.cluster;

import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.ClusterConstant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.test.util.ReflectionTestUtils;

import com.alibaba.cloud.nacos.NacosServiceInstance;
import com.alibaba.cloud.nacos.discovery.NacosDiscoveryClient;
import com.alibaba.cloud.nacos.registry.NacosRegistration;

public class NacosClusterManagerTest {

    @Mock
    private final NacosRegistration registration = Mockito.mock(NacosRegistration.class);

    @Mock
    private final NacosDiscoveryClient discoveryClient = Mockito.mock(NacosDiscoveryClient.class);

    private final NacosClusterManager nacosClusterManager = new NacosClusterManager(registration);

    @Before
    public void setup() {
        Mockito.when(registration.getHost()).thenReturn("127.0.0.1");
        Mockito.when(registration.getPort()).thenReturn(7070);
        Mockito.when(discoveryClient.getInstances(ClusterConstant.DATA_LOADING))
                .thenReturn(generateServiceInstance("127.0.0.2", 7072));
        Mockito.when(discoveryClient.getInstances(ClusterConstant.SMART))
                .thenReturn(generateServiceInstance("127.0.0.3", 7073));
        Mockito.when(discoveryClient.getInstances(ClusterConstant.COMMON))
                .thenReturn(generateServiceInstance("127.0.0.4", 7074));
        Mockito.when(discoveryClient.getInstances(ClusterConstant.QUERY))
                .thenReturn(generateServiceInstance("127.0.0.5", 7075));
        ReflectionTestUtils.setField(nacosClusterManager, "registration", registration);
        ReflectionTestUtils.setField(nacosClusterManager, "discoveryClient", discoveryClient);
    }

    @Test
    public void testClusterManager() {
        Assert.assertEquals("127.0.0.1:7070", nacosClusterManager.getLocalServer());
        Assert.assertEquals(1, nacosClusterManager.getJobServers().size());
        Assert.assertEquals(1, nacosClusterManager.getQueryServers().size());
        Assert.assertEquals(4, nacosClusterManager.getServers().size());
        Assert.assertEquals(4, nacosClusterManager.getServersFromCache().size());
        Assert.assertThrows(KylinRuntimeException.class,
                () -> nacosClusterManager.getServersByServerId("yinglong-illegal-server"));
    }

    private List<ServiceInstance> generateServiceInstance(String host, int port) {
        NacosServiceInstance instance = new NacosServiceInstance();
        instance.setHost(host);
        instance.setPort(port);
        return Collections.singletonList(instance);
    }

}
