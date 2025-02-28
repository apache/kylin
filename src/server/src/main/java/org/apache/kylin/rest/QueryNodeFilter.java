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
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CONNECT_CATALOG;
import static org.apache.kylin.common.exception.ServerErrorCode.NO_ACTIVE_ALL_NODE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeSystem.QUERY_NODE_API_INVALID;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.interceptor.ProjectInfoParser;
import org.apache.kylin.rest.response.ErrorResponse;
import org.apache.kylin.rest.service.RouteService;
import org.glassfish.jersey.uri.UriTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.client.RestTemplate;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 3)
public class QueryNodeFilter extends BaseFilter {

    private static Set<String> routeGetApiSet = Sets.newHashSet();
    private static Set<String> notRoutePostApiSet = Sets.newHashSet();
    private static Set<String> notRouteDeleteApiSet = Sets.newHashSet();
    private static Set<String> notRoutePutApiSet = Sets.newHashSet();
    private static Set<String> routeMultiTenantModeFilterApiSet = Sets.newHashSet();

    static {
        // data source
        routeGetApiSet.add("/kylin/api/tables/reload_hive_table_name");
        routeGetApiSet.add("/kylin/api/tables/project_table_names");
        routeGetApiSet.add("/kylin/api/query/favorite_queries");

        // jdbc, odbc, query, maintain
        notRoutePostApiSet.add("/kylin/api/query");
        notRoutePostApiSet.add("/kylin/api/async_query");
        notRoutePostApiSet.add("/kylin/api/query/if_big_query");
        notRoutePostApiSet.add("/kylin/api/query/prestate");
        notRoutePostApiSet.add("/kylin/api/user/authentication");
        notRoutePostApiSet.add("/kylin/api/system/maintenance_mode");
        notRouteDeleteApiSet.add("/kylin/api/query");
        notRouteDeleteApiSet.add("/kylin/api/query/if_big_query");

        notRoutePostApiSet.add("/kylin/api/kg/health/instance_info");
        notRoutePostApiSet.add("/kylin/api/kg/health/instance_service/query_up_grade");
        notRoutePostApiSet.add("/kylin/api/kg/health/instance_service/query_down_grade");

        // license
        notRoutePostApiSet.add("/kylin/api/system/license/content");
        notRoutePostApiSet.add("/kylin/api/system/license/file");

        //diag
        notRoutePostApiSet.add("/kylin/api/system/diag");
        notRouteDeleteApiSet.add("/kylin/api/system/diag");
        notRouteDeleteApiSet.add("/kylin/api/system/maintenance_mode");
        notRoutePutApiSet.add("/kylin/api/system/diag/progress");

        //download
        notRoutePostApiSet.add("/kylin/api/metastore/backup/models");
        notRoutePostApiSet.add("/kylin/api/query/format/csv");

        //refresh catalog
        notRoutePutApiSet.add("/kylin/api/tables/catalog_cache");
        notRoutePutApiSet.add("/kylin/api/tables/single_catalog_cache");
        notRoutePutApiSet.add("/kylin/api/index_plans/agg_index_count");
        notRoutePutApiSet.add("/kylin/api/system/roll_event_log");

        //epoch
        notRoutePostApiSet.add("/kylin/api/epoch");
        notRoutePostApiSet.add("/kylin/api/epoch/all");

        //reload metadata
        notRoutePostApiSet.add("/kylin/api/system/metadata/reload");

        //second storage
        routeGetApiSet.add("/kylin/api/storage/table/sync");
        notRoutePostApiSet.add("/kylin/api/storage/config/refresh");
        notRoutePostApiSet.add("/kylin/api/storage/node/status");

        // snapshot source table
        notRoutePostApiSet.add("/kylin/api/snapshots/source_table_stats");
        notRoutePostApiSet.add("/kylin/api/snapshots/view_mapping");

        //user refresh
        notRoutePutApiSet.add("/kylin/api/user/refresh");

        // custom parse
        routeGetApiSet.add("/kylin/api/kafka/parsers");
        // tenant node metadata backup
        notRoutePostApiSet.add("/kylin/api/system/metadata_backup");
        notRoutePostApiSet.add("/kylin/api/system/broadcast_metadata_backup");

        // metastore cleanup
        notRoutePostApiSet.add("/kylin/api/metastore/cleanup_storage/tenant_node");
        notRoutePostApiSet.add("/kylin/api/metastore/cleanup_storage");

        notRouteDeleteApiSet.add("/kylin/api/system/clean_sparder_event_log");

        notRouteDeleteApiSet.add("/kylin/api/async_query/tenant_node");

        routeMultiTenantModeFilterApiSet.add("/kylin/api/jobs/{jobId}/resume");
        routeMultiTenantModeFilterApiSet.add("/kylin/api/cubes/{cubeName}/rebuild");
        routeMultiTenantModeFilterApiSet.add("/kylin/api/cubes/{cubeName}/segments");

        // spark report job stage status
        notRoutePutApiSet.add("/kylin/api/jobs/stage/status");
        notRoutePutApiSet.add("/kylin/api/jobs/spark");
        notRoutePutApiSet.add("/kylin/api/jobs/wait_and_run_time");

        // gluten cache
        notRoutePostApiSet.add("/kylin/api/jobs/gluten_cache");
        notRoutePostApiSet.add("/kylin/api/cache/gluten_cache");
        notRoutePostApiSet.add("/kylin/api/cache/gluten_cache_async");
    }

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    ClusterManager clusterManager;

    @Autowired
    RouteService routeService;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        log.info("init query request filter");
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest servletRequest = (HttpServletRequest) request;
            HttpServletResponse servletResponse = (HttpServletResponse) response;
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            String project;
            try {
                // not start with /kylin/api
                if (checkNeedToRoute(servletRequest)) {
                    chain.doFilter(request, response);
                    return;
                }

                // no leaders
                if (CollectionUtils.isEmpty(clusterManager.getJobServers())) {
                    Message msg = MsgPicker.getMsg();
                    servletRequest.setAttribute(ERROR,
                            new KylinException(NO_ACTIVE_ALL_NODE, msg.getNoActiveLeaders()));
                    servletRequest.getRequestDispatcher(API_ERROR).forward(servletRequest, response);
                    return;
                }

                String contentType = request.getContentType();
                Pair<String, HttpServletRequest> projectInfo = ProjectInfoParser.parseProjectInfo(servletRequest);
                project = projectInfo.getFirst();
                if (!checkProjectExist(project)) {
                    servletRequest.setAttribute(ERROR, new KylinException(PROJECT_NOT_EXIST, project));
                    servletRequest.getRequestDispatcher(API_ERROR).forward(servletRequest, response);
                    return;
                }

                request = projectInfo.getSecond();

                if (checkServer(request, response, chain, kylinConfig, contentType))
                    return;

                if (checkNeedToMultiTenantFilter(servletRequest)) {
                    chain.doFilter(request, response);
                    return;
                }
            } catch (CannotCreateTransactionException e) {
                writeConnectionErrorResponse(servletRequest, servletResponse);
                return;
            }

            log.debug("proxy {} {} to all", servletRequest.getMethod(), servletRequest.getRequestURI());
            routeAPI(restTemplate, request, servletResponse, project);
            return;
        }
        throw new KylinRuntimeException("unknown status");
    }

    private boolean checkServer(ServletRequest request, ServletResponse response, FilterChain chain,
            KylinConfig kylinConfig, String contentType) throws IOException, ServletException {
        if (checkProcessLocal(kylinConfig, contentType)) {
            log.info("process local caused by project owner");
            chain.doFilter(request, response);
            return true;
        }

        if (Boolean.FALSE.equals(kylinConfig.isQueryNodeRequestForwardEnabled()) && kylinConfig.isQueryNodeOnly()) {
            request.setAttribute(ERROR, new KylinException(QUERY_NODE_API_INVALID));
            request.getRequestDispatcher(API_ERROR).forward(request, response);
            return true;
        }
        return false;
    }

    @Override
    public void destroy() {
        // just override it
    }

    private boolean checkNeedToRoute(HttpServletRequest servletRequest) {
        final String uri = StringUtils.stripEnd(servletRequest.getRequestURI(), "/");
        final String method = servletRequest.getMethod();
        return (!uri.startsWith(API_PREFIX)) || (uri.startsWith(ERROR_REQUEST_URL))
                || (method.equals("GET") && !routeGetApiSet.contains(uri))
                || (method.equals("POST") && notRoutePostApiSet.contains(uri))
                || (method.equals("PUT") && notRoutePutApiSet.contains(uri))
                || (method.equals("DELETE") && notRouteDeleteApiSet.contains(uri))
                || TRUE.equalsIgnoreCase(servletRequest.getHeader(ROUTED))
                || TRUE.equals(servletRequest.getAttribute(FILTER_PASS)) || KylinConfig.getInstanceFromEnv().isUTEnv();
    }

    /**
     * when api is not get method, and need use MultiTenantFilter route API
     */
    private boolean checkNeedToMultiTenantFilter(HttpServletRequest servletRequest) {
        String uri = StringUtils.stripEnd(servletRequest.getRequestURI(), "/");
        val accept = servletRequest.getHeader("Accept");
        if (StringUtils.equals(accept, HTTP_VND_APACHE_KYLIN_V2_JSON) && routeService.needRoute()) {
            for (String needParserUrl : routeMultiTenantModeFilterApiSet) {
                val uriTemplate = new UriTemplate(needParserUrl);
                val kvMap = new HashMap<String, String>();
                if (uriTemplate.match(uri, kvMap)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean checkProcessLocal(KylinConfig kylinConfig, String contentType) {
        if (kylinConfig.isDevOrUT()) {
            return true;
        }

        if (kylinConfig.isQueryNodeOnly()) {
            return false;
        }

        return StringUtils.isEmpty(contentType) || !contentType.contains("multipart/form-data");
    }

    private boolean checkProjectExist(String project) {
        if (!UnitOfWork.GLOBAL_UNIT.equals(project)) {
            val prj = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
            if (prj == null) {
                return false;
            }
        }
        return true;
    }

    public void writeConnectionErrorResponse(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
            throws IOException {
        ErrorResponse errorResponse = new ErrorResponse(servletRequest.getRequestURL().toString(),
                new KylinException(FAILED_CONNECT_CATALOG, MsgPicker.getMsg().getConnectDatabaseError(), false));
        byte[] responseBody = JsonUtil.writeValueAsBytes(errorResponse);
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentType(MediaType.APPLICATION_JSON);
        servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        setResponseHeaders(responseHeaders, servletResponse);
        servletResponse.getOutputStream().write(responseBody);
    }
}
