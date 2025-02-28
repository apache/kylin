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

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.JOB_NODE_API_INVALID;

import java.io.IOException;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import lombok.extern.slf4j.Slf4j;

/**
 **/
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 2)
public class JobNodeFilter implements Filter {
    private static final String ERROR = "error";
    private static final String API_ERROR = "/api/error";
    private static final String FILTER_PASS = "filter_pass";
    private static Set<String> jobNodeAbandonApiSet = Sets.newHashSet();

    static {
        //jobNode abandon url
        jobNodeAbandonApiSet.add("/data_range/latest_data");
        jobNodeAbandonApiSet.add("/kylin/api/tables/partition_column_format");
    }

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    ClusterManager clusterManager;

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (!(servletRequest instanceof HttpServletRequest)) {
            return;
        }
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (checkJobNodeAbandon(kylinConfig, request, response, chain)) {
            return;
        }
        chain.doFilter(request, response);
    }

    private boolean checkJobNodeAbandon(KylinConfig kylinConfig, HttpServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {

        boolean isJobNodePass = jobNodeAbandonApiSet.stream().filter(request.getRequestURI()::contains).count() == 0;
        if (!isJobNodePass) {
            if (kylinConfig.isQueryNode()) {
                request.setAttribute(FILTER_PASS, "true");
                chain.doFilter(request, response);
            } else {
                request.setAttribute(ERROR, new KylinException(JOB_NODE_API_INVALID));
                request.getRequestDispatcher(API_ERROR).forward(request, response);
            }
            return true;
        }
        return false;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        log.info("init query request filter");
        // just override it
    }

    @Override
    public void destroy() {
        // just override it
    }
}
