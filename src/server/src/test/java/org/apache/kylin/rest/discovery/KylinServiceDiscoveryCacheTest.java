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

package org.apache.kylin.rest.discovery;

import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum.ALL;
import static org.apache.kylin.common.util.ClusterConstant.ServerModeEnum.JOB;

import java.util.concurrent.ThreadFactory;

import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCacheBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheImpl;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.curator.x.discovery.details.ServiceDiscoveryImpl;
import org.apache.kylin.common.util.LogOutputTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "javax.management.*", "org.apache.hadoop.*" })
@PrepareForTest(KylinServiceDiscoveryCache.class)
@Slf4j
public class KylinServiceDiscoveryCacheTest extends LogOutputTestCase {

    private KylinServiceDiscoveryCache mockCache;
    private final ServiceDiscoveryImpl serviceDiscovery = PowerMockito.mock(ServiceDiscoveryImpl.class);
    private final ServiceCacheImpl serviceCacheAll = PowerMockito.mock(ServiceCacheImpl.class);
    private final ServiceCacheImpl serviceCacheJob = PowerMockito.mock(ServiceCacheImpl.class);

    private final ServiceInstance serviceInstanceAll = PowerMockito.mock(ServiceInstance.class);

    private final KylinServiceDiscoveryClient kylinServiceDiscoveryClient = PowerMockito
            .mock(KylinServiceDiscoveryClient.class);
    private final Registration registration = PowerMockito.mock(Registration.class);

    @Before
    public void setUp() {
        createTestMetadata();
        mockCache = PowerMockito.spy(new KylinServiceDiscoveryCache());
        ReflectionTestUtils.setField(mockCache, "serviceDiscovery", serviceDiscovery);
        PowerMockito.doReturn("addr").when(registration).getHost();
        PowerMockito.doReturn(7070).when(registration).getPort();
        ReflectionTestUtils.setField(kylinServiceDiscoveryClient, "registration", registration);
        PowerMockito.when(kylinServiceDiscoveryClient.getLocalServiceServer()).thenCallRealMethod();
        ReflectionTestUtils.setField(mockCache, "kylinServiceDiscoveryClient", kylinServiceDiscoveryClient);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testServiceListener1() throws Exception {
        val listenerAll = (ServiceCacheListener) ReflectionTestUtils.invokeMethod(mockCache, "getListener", ALL,
                (KylinServiceDiscovery.Callback) () -> log.info("action all."));
        val mockListenerAll = PowerMockito.spy(listenerAll);
        val listenerJob = (ServiceCacheListener) ReflectionTestUtils.invokeMethod(mockCache, "getListener", JOB,
                (KylinServiceDiscovery.Callback) () -> log.info("action job."));
        val mockListenerJob = PowerMockito.spy(listenerJob);
        PowerMockito.doReturn("addr").when(serviceInstanceAll).getAddress();
        PowerMockito.doReturn(7070).when(serviceInstanceAll).getPort();
        PowerMockito.doReturn(Lists.newArrayList(serviceInstanceAll)).when(serviceCacheAll).getInstances();

        val mockBuilder = PowerMockito.mock(ServiceCacheBuilder.class);
        PowerMockito.when(serviceDiscovery.serviceCacheBuilder()).thenReturn(mockBuilder);
        PowerMockito.when(mockBuilder.name(Mockito.anyString())).thenReturn(mockBuilder);
        PowerMockito.when(mockBuilder.threadFactory(Mockito.any(ThreadFactory.class))).thenReturn(mockBuilder);
        PowerMockito.when(mockBuilder.build()).thenReturn(serviceCacheAll);

        PowerMockito.doReturn("").when(mockCache, "getZkPathByModeEnum", ALL);
        PowerMockito.doNothing().when(mockCache, "createZkNodeIfNeeded", "");
        PowerMockito.doNothing().when(serviceCacheAll).addListener(mockListenerAll);

        PowerMockito.doReturn(serviceCacheAll).when(mockCache, "createServiceCache", serviceDiscovery, ALL,
                (KylinServiceDiscovery.Callback) () -> log.info("test"));
        PowerMockito.when(mockCache, "registerServiceCacheByMode", ALL).thenCallRealMethod();

        PowerMockito.doNothing().when(serviceCacheJob).addListener(mockListenerJob);
        PowerMockito.doReturn("").when(mockCache, "getZkPathByModeEnum", JOB);
        PowerMockito.doNothing().when(mockCache, "createZkNodeIfNeeded", "");
        PowerMockito.when(mockCache, "registerServiceCacheByMode", JOB).thenCallRealMethod();

        mockListenerAll.cacheChanged();
        mockListenerAll.stateChanged(null, ConnectionState.CONNECTED);
        Assertions.assertTrue(containsLog("action all."));
        mockListenerJob.cacheChanged();
        mockListenerJob.stateChanged(null, ConnectionState.CONNECTED);
        Assertions.assertTrue(containsLog("action job."));
        clearLogs();
    }

    @Test
    public void testServiceListener2() throws Exception {
        val listenerAll = (ServiceCacheListener) ReflectionTestUtils.invokeMethod(mockCache, "getListener", ALL,
                (KylinServiceDiscovery.Callback) () -> log.info("action all."));
        val mockListenerAll = PowerMockito.spy(listenerAll);
        val listenerJob = (ServiceCacheListener) ReflectionTestUtils.invokeMethod(mockCache, "getListener", JOB,
                (KylinServiceDiscovery.Callback) () -> log.info("action job."));
        val mockListenerJob = PowerMockito.spy(listenerJob);
        PowerMockito.doReturn("addr").when(serviceInstanceAll).getAddress();
        PowerMockito.doReturn(7071).when(serviceInstanceAll).getPort();
        PowerMockito.doReturn(Lists.newArrayList(serviceInstanceAll)).when(serviceCacheAll).getInstances();

        val mockBuilder = PowerMockito.mock(ServiceCacheBuilder.class);
        PowerMockito.when(serviceDiscovery.serviceCacheBuilder()).thenReturn(mockBuilder);
        PowerMockito.when(mockBuilder.name(Mockito.anyString())).thenReturn(mockBuilder);
        PowerMockito.when(mockBuilder.threadFactory(Mockito.any(ThreadFactory.class))).thenReturn(mockBuilder);
        PowerMockito.when(mockBuilder.build()).thenReturn(serviceCacheAll);

        PowerMockito.doReturn("").when(mockCache, "getZkPathByModeEnum", ALL);
        PowerMockito.doNothing().when(mockCache, "createZkNodeIfNeeded", "");
        PowerMockito.doNothing().when(serviceCacheAll).addListener(mockListenerAll);

        PowerMockito.doReturn(serviceCacheAll).when(mockCache, "createServiceCache", serviceDiscovery, ALL,
                (KylinServiceDiscovery.Callback) () -> log.info("test"));
        PowerMockito.when(mockCache, "registerServiceCacheByMode", ALL).thenCallRealMethod();

        PowerMockito.doNothing().when(serviceCacheJob).addListener(mockListenerJob);
        PowerMockito.doReturn("").when(mockCache, "getZkPathByModeEnum", JOB);
        PowerMockito.doNothing().when(mockCache, "createZkNodeIfNeeded", "");
        PowerMockito.when(mockCache, "registerServiceCacheByMode", JOB).thenCallRealMethod();

        mockListenerAll.cacheChanged();
        mockListenerJob.cacheChanged();
        Assertions.assertFalse(containsLog("action all."));
        Assertions.assertFalse(containsLog("action job."));
        clearLogs();
    }
}
