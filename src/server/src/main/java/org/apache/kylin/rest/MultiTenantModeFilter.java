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

import static java.lang.String.format;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.rest.service.RouteService;
import org.glassfish.jersey.uri.UriTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 4)
public class MultiTenantModeFilter extends BaseFilter {
    private static final Set<String> NOT_ROUTE_APIS = com.google.common.collect.Sets.newHashSet();
    private static final Set<String> ASYNC_QUERY_APIS = Sets.newHashSet();
    private static final Set<String> ROUTES_APIS = Sets.newHashSet();
    private static final Set<String> JOB_APIS = Sets.newHashSet();
    private static final Set<String> V2_CUBES_APIS = Sets.newHashSet();
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private RouteService routeService;

    static {
        NOT_ROUTE_APIS.add("/kylin/api/access/all/users");
        NOT_ROUTE_APIS.add("/kylin/api/access/all/groups");

        val asyncQuerySubUris = Arrays.asList("status", "file_status", "metadata", "result_download", "result_path");
        asyncQuerySubUris.forEach(asyncQuerySubUri -> ASYNC_QUERY_APIS
                .add(format(Locale.ROOT, "/kylin/api/async_query/{query_id}/%s", asyncQuerySubUri)));

        ROUTES_APIS.add("/kylin/api/access/{type}/{project}");

        ROUTES_APIS.add("/kylin/api/cube_desc/{projectName}/{cubeName}");

        ROUTES_APIS.add("/kylin/api/cubes");

        JOB_APIS.add("/kylin/api/jobs/{jobId}");
        JOB_APIS.add("/kylin/api/jobs/{jobId}/resume");
        JOB_APIS.add("/kylin/api/jobs/{jobId}/steps/{step_id}/output");

        V2_CUBES_APIS.add("/kylin/api/cubes/{cubeName}");
        V2_CUBES_APIS.add("/kylin/api/cubes/{cubeName}/rebuild");
        V2_CUBES_APIS.add("/kylin/api/cubes/{cubeName}/segments");
        V2_CUBES_APIS.add("/kylin/api/cubes/{cubeName}/holes");
        V2_CUBES_APIS.add("/kylin/api/cubes/{cubeName}/sql");
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        log.info("init multi tenant mode filter");
        // just override it
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
            throws IOException, ServletException {
        if (servletRequest instanceof HttpServletRequest) {
            HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
            if (routeService.needRoute()) {
                String uri = StringUtils.stripEnd(httpServletRequest.getRequestURI(), "/");
                if (checkIsRoutedApi(httpServletRequest, uri)) {
                    chain.doFilter(servletRequest, servletResponse);
                    return;
                }
                val result = checkNeedToRouteAndGetProject(httpServletRequest, uri);
                if (result.getFirst()) {
                    val project = result.getSecond();
                    if (StringUtils.isNotBlank(project)) {
                        httpServletRequest.setAttribute("project", project);
                    }
                    log.debug("proxy {} {} to all", httpServletRequest.getMethod(), httpServletRequest.getRequestURI());
                    routeAPI(restTemplate, servletRequest, servletResponse, project);
                    return;
                }
            }
            chain.doFilter(servletRequest, servletResponse);
            return;
        }
        throw new KylinRuntimeException("unknown status");
    }

    @Override
    public void destroy() {
        // just override it
    }

    private boolean checkIsRoutedApi(HttpServletRequest servletRequest, String uri) {
        return TRUE.equalsIgnoreCase(servletRequest.getHeader(ROUTED)) || NOT_ROUTE_APIS.contains(uri);
    }

    private Pair<Boolean, String> checkNeedToRouteAndGetProject(HttpServletRequest servletRequest, String uri) {
        val matchAsyncQueryApis = matchAsyncQueryApis(uri);
        if (Objects.nonNull(matchAsyncQueryApis))
            return matchAsyncQueryApis;

        val matchRoutesApis = matchRoutesApis(servletRequest, uri);
        if (Objects.nonNull(matchRoutesApis))
            return matchRoutesApis;

        val matchJobApis = matchJobApis(servletRequest, uri);
        if (Objects.nonNull(matchJobApis))
            return matchJobApis;

        val matchV2CubesApis = matchV2CubesApis(servletRequest, uri);
        if (Objects.nonNull(matchV2CubesApis))
            return matchV2CubesApis;

        return Pair.newPair(false, "");
    }

    private Pair<Boolean, String> matchAsyncQueryApis(String uri) {
        for (String needParserUrl : ASYNC_QUERY_APIS) {
            val uriTemplate = new UriTemplate(needParserUrl);
            val kvMap = new HashMap<String, String>();
            if (uriTemplate.match(uri, kvMap)) {
                return Pair.newPair(true, "");
            }
        }
        return null;
    }

    private Pair<Boolean, String> matchRoutesApis(HttpServletRequest servletRequest, String uri) {
        for (String needParserUrl : ROUTES_APIS) {
            val uriTemplate = new UriTemplate(needParserUrl);
            val kvMap = new HashMap<String, String>();
            if (uriTemplate.match(uri, kvMap)) {
                val accept = servletRequest.getHeader("Accept");
                return Pair.newPair(StringUtils.equals(accept, HTTP_VND_APACHE_KYLIN_V2_JSON), "");
            }
        }
        return null;
    }

    private Pair<Boolean, String> matchJobApis(HttpServletRequest servletRequest, String uri) {
        for (String needParserUrl : JOB_APIS) {
            val uriTemplate = new UriTemplate(needParserUrl);
            val kvMap = new HashMap<String, String>();
            if (uriTemplate.match(uri, kvMap)) {
                return getJobApisProject(servletRequest, kvMap);
            }
        }
        return null;
    }

    private Pair<Boolean, String> matchV2CubesApis(HttpServletRequest servletRequest, String uri) {
        for (String needParserUrl : V2_CUBES_APIS) {
            val uriTemplate = new UriTemplate(needParserUrl);
            val kvMap = new HashMap<String, String>();
            if (uriTemplate.match(uri, kvMap)) {
                return getV2CubesApisProject(servletRequest, kvMap);
            }
        }
        return null;
    }

    private Pair<Boolean, String> getJobApisProject(HttpServletRequest request, HashMap<String, String> kvMap) {
        val accept = request.getHeader("Accept");
        if (StringUtils.equals(accept, HTTP_VND_APACHE_KYLIN_V2_JSON)) {
            val jobId = kvMap.get("jobId");
            val project = routeService.getProjectByJobIdUseInFilter(jobId);
            return Pair.newPair(true, project);
        }
        return Pair.newPair(false, "");
    }

    private Pair<Boolean, String> getV2CubesApisProject(HttpServletRequest request, HashMap<String, String> kvMap) {
        val accept = request.getHeader("Accept");
        if (StringUtils.equals(accept, HTTP_VND_APACHE_KYLIN_V2_JSON)) {
            val project = request.getParameter("project");
            if (StringUtils.isNotEmpty(project)) {
                return Pair.newPair(true, project);
            }

            val cubeName = kvMap.get("cubeName");
            String projectName = routeService.getProjectByModelNameUseInFilter(cubeName);
            return Pair.newPair(true, projectName);
        }
        return Pair.newPair(false, "");
    }
}
