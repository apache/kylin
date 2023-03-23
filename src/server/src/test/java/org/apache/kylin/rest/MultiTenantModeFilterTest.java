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

package org.apache.kylin.rest;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.rest.BaseFilter.ROUTED;
import static org.apache.kylin.rest.BaseFilter.TRUE;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.metadata.streaming.ReflectionUtils;
import org.apache.kylin.rest.service.RouteService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockFilterConfig;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.client.RestTemplate;

import lombok.val;

@MetadataInfo
class MultiTenantModeFilterTest {
    @Test
    @OverwriteProp(key = "kylin.multi-tenant.enabled", value = "true")
    void testRouteApi() throws Exception {
        testApi("GET", "/kylin/api/async_query/query_id/status", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/status", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/file_status", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/file_status", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/metadata", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/metadata", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/result_download", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/result_download", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/result_path", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testApi("GET", "/kylin/api/async_query/query_id/result_path", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testApi("GET", "/kylin/api/access/type/project", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testApi("GET", "/kylin/api/cube_desc/projectName/cubeName", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testApi("GET", "/kylin/api/cubes", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testApi("GET", "/kylin/api/jobs/jobId", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("PUT", "/kylin/api/jobs/jobId/resume", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("GET", "/kylin/api/jobs/jobId/steps/step_id/output", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testApi("GET", "/kylin/api/cubes/{cubeName}", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("PUT", "/kylin/api/cubes/{cubeName}/rebuild", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("PUT", "/kylin/api/cubes/{cubeName}/segments", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("GET", "/kylin/api/cubes/{cubeName}/holes", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testApi("GET", "/kylin/api/cubes/{cubeName}/sql", HTTP_VND_APACHE_KYLIN_V2_JSON);
    }

    private void testApi(String method, String uri, String accept) throws Exception {
        val res = getMockHttpServletResponse(method, uri, accept);
        val content = res.getContentAsByteArray();
        val restResponse = JsonUtil.readValue(content, RestResponse.class);
        Assertions.assertEquals("000", restResponse.getCode());
        Assertions.assertTrue(((Boolean) restResponse.getData()));
    }

    private static MockHttpServletResponse getMockHttpServletResponse(String method, String uri, String accept)
            throws ServletException, IOException {
        val filter = new MultiTenantModeFilter();
        val restTemplate = Mockito.mock(RestTemplate.class);
        val routeService = Mockito.mock(RouteService.class);
        ReflectionUtils.setField(filter, "restTemplate", restTemplate);
        ReflectionUtils.setField(filter, "routeService", routeService);
        val restResult = JsonUtil.writeValueAsBytes(RestResponse.ok(true));
        val resp = new ResponseEntity<>(restResult, HttpStatus.OK);
        Mockito.when(restTemplate.exchange(ArgumentMatchers.anyString(), ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(), ArgumentMatchers.<Class<byte[]>> any())).thenReturn(resp);
        Mockito.when(routeService.needRoute()).thenReturn(true);
        Mockito.when(routeService.getProjectByJobIdUseInFilter(ArgumentMatchers.anyString())).thenReturn("default");

        val config = new MockFilterConfig();
        val req = new MockHttpServletRequest(method, uri);
        req.addHeader("Accept", accept);
        val res = new MockHttpServletResponse();
        val filterChain = new MockFilterChain();
        filter.init(config);
        filter.doFilter(req, res, filterChain);
        return res;
    }

    private void testNotRouteApi(String method, String uri, String accept) throws Exception {
        val res = getMockHttpServletResponse(method, uri, accept);
        val content = res.getContentAsByteArray();
        Assertions.assertTrue(ArrayUtils.isEmpty(content));
    }

    @Test
    @OverwriteProp(key = "kylin.multi-tenant.enabled", value = "true")
    void testNotRouteApi() throws Exception {
        testNotRouteApi("GET", "/kylin/api/access/type/project", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testNotRouteApi("GET", "/kylin/api/access/all/users", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testNotRouteApi("GET", "/kylin/api/access/all/users", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testNotRouteApi("GET", "/kylin/api/access/all/users", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testNotRouteApi("GET", "/kylin/api/access/all/groups", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);

        testNotRouteApi("GET", "/kylin/api/cube_desc/projectName/cubeName", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);

        testNotRouteApi("GET", "/kylin/api/cubes", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);

        testNotRouteApi("GET", "/kylin/api/jobs/jobId", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testNotRouteApi("PUT", "/kylin/api/jobs/jobId/resume", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testNotRouteApi("GET", "/kylin/api/jobs/jobId/steps/step_id/output", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);

        testNotRouteApi("GET", "/kylin/api/jobs/jobId/output", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
    }

    @Test
    @OverwriteProp(key = "kylin.multi-tenant.enabled", value = "true")
    void testIsRouteApi() throws Exception {
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/status", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/status", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/file_status", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/file_status", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/metadata", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/metadata", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/result_download", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/result_download", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/result_path", HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        testIsRouteApi("GET", "/kylin/api/async_query/query_id/result_path", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testIsRouteApi("GET", "/kylin/api/access/type/project", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testIsRouteApi("GET", "/kylin/api/cube_desc/projectName/cubeName", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testIsRouteApi("GET", "/kylin/api/cubes", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testIsRouteApi("GET", "/kylin/api/jobs/jobId", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("PUT", "/kylin/api/jobs/jobId/resume", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("GET", "/kylin/api/jobs/jobId/steps/step_id/output", HTTP_VND_APACHE_KYLIN_V2_JSON);

        testIsRouteApi("GET", "/kylin/api/cubes/{cubeName}", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("PUT", "/kylin/api/cubes/{cubeName}/rebuild", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("PUT", "/kylin/api/cubes/{cubeName}/segments", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("GET", "/kylin/api/cubes/{cubeName}/holes", HTTP_VND_APACHE_KYLIN_V2_JSON);
        testIsRouteApi("GET", "/kylin/api/cubes/{cubeName}/sql", HTTP_VND_APACHE_KYLIN_V2_JSON);
    }

    private void testIsRouteApi(String method, String uri, String accept) throws ServletException, IOException {
        val filter = new MultiTenantModeFilter();
        val routeService = Mockito.mock(RouteService.class);
        ReflectionUtils.setField(filter, "routeService", routeService);
        Mockito.when(routeService.needRoute()).thenReturn(true);

        val config = new MockFilterConfig();
        val req = new MockHttpServletRequest(method, uri);
        req.addHeader("Accept", accept);
        req.addHeader(ROUTED, TRUE);
        val res = new MockHttpServletResponse();
        val filterChain = new MockFilterChain();
        filter.init(config);
        filter.doFilter(req, res, filterChain);
        val content = res.getContentAsByteArray();
        Assertions.assertTrue(ArrayUtils.isEmpty(content));
    }
}
