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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.helper.MetadataToolHelper;
import org.apache.kylin.metadata.resourcegroup.ResourceGroup;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.metadata.streaming.ReflectionUtils;
import org.apache.kylin.rest.request.StorageCleanupRequest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;

import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HttpMethod.class, ResourceGroupManager.class, KylinConfig.class })
public class MetaStoreTenantServiceTest {
    @InjectMocks
    private MetaStoreService metaStoreService = Mockito.spy(MetaStoreService.class);
    @InjectMocks
    private RouteService routeService = Mockito.spy(RouteService.class);
    @Mock
    private RestTemplate restTemplate = Mockito.mock(RestTemplate.class);
    @Mock
    private ResourceGroupManager rgManager = Mockito.mock(ResourceGroupManager.class);
    @Mock
    private KylinConfig kylinConfig = Mockito.mock(KylinConfig.class);
    @Mock
    private MetadataToolHelper metadataToolHelper = Mockito.spy(MetadataToolHelper.class);

    @Test
    public void cleanupStorageMultiTenantMode() throws IOException {
        PowerMockito.mockStatic(HttpMethod.class, ResourceGroupManager.class, KylinConfig.class);
        PowerMockito.when(HttpMethod.valueOf(ArgumentMatchers.anyString())).thenAnswer(invocation -> HttpMethod.GET);
        PowerMockito.when(ResourceGroupManager.getInstance(ArgumentMatchers.any())).thenAnswer(invocation -> rgManager);
        PowerMockito.when(KylinConfig.getInstanceFromEnv()).thenAnswer(invocation -> kylinConfig);
        Mockito.when(kylinConfig.getKylinMultiTenantRouteTaskTimeOut()).thenReturn(30 * 60 * 1000L);
        Mockito.when(kylinConfig.isKylinMultiTenantEnabled()).thenReturn(false);

        ReflectionUtils.setField(metaStoreService, "routeService", routeService);
        ReflectionUtils.setField(metaStoreService, "metadataToolHelper", metadataToolHelper);
        ReflectionUtils.setField(routeService, "restTemplate", restTemplate);

        val restResult = JsonUtil.writeValueAsBytes(RestResponse.ok(true));
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<byte[]>> any())).thenReturn(resp);

        val resourceGroupJson = "{\"create_time\":1669704879469,\"instances\":[{\"instance\":\"10.1.2.185:7878\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\"},{\"instance\":\"10.1.2.184:7878\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\"}],"
                + "\"mapping_info\":[{\"project\":\"184\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\",\"request_type\":\"BUILD\"},{\"project\":\"184\",\"resource_group_id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\",\"request_type\":\"QUERY\"},{\"project\":\"185\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\",\"request_type\":\"BUILD\"},{\"project\":\"185\",\"resource_group_id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\",\"request_type\":\"QUERY\"}],\"resource_groups\":[{\"id\":\"c444879a-b3b0-4946-aed1-018cbc946c4a\"},{\"id\":\"27dc039e-2778-49c0-80d0-8e4e025d25ba\"},{\"id\":\"cebdfbca-25bc-49b6-8ee4-71946219b4bb\"}],\"uuid\":\"d5c316ed-b977-6efb-aea3-1735feb75d02\",\"last_modified\":1669952899667,\"version\":\"4.0.0.0\",\"resource_group_enabled\":true}\n";
        Mockito.when(rgManager.getResourceGroup())
                .thenReturn(JsonUtil.readValue(resourceGroupJson, new TypeReference<ResourceGroup>() {
                }));
        Mockito.doNothing().when(metadataToolHelper).cleanStorage(ArgumentMatchers.anyBoolean(), ArgumentMatchers.any(),
                ArgumentMatchers.anyDouble(), ArgumentMatchers.anyInt());

        val storageCleanupRequest = new StorageCleanupRequest();
        storageCleanupRequest.setCleanupStorage(false);
        storageCleanupRequest.setProjectsToClean(new String[]{});
        val request = new MockHttpServletRequest();
        metaStoreService.cleanupStorage(storageCleanupRequest, request);

        Mockito.when(kylinConfig.isKylinMultiTenantEnabled()).thenReturn(true);
        Mockito.when(rgManager.isResourceGroupEnabled()).thenReturn(true);

        metaStoreService.cleanupStorage(storageCleanupRequest, request);

        val asyncExecutors = ((ExecutorService) ReflectionUtils.getField(routeService, "asyncExecutors"));
        val threadPoolExecutor = (ThreadPoolExecutor) asyncExecutors;
        Assert.assertEquals(0, threadPoolExecutor.getActiveCount());
    }
}
