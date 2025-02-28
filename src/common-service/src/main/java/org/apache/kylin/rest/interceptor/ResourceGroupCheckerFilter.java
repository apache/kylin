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

package org.apache.kylin.rest.interceptor;

import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_WITHOUT_RESOURCE_GROUP;

import java.io.IOException;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import lombok.val;

@Component
@Order(200)
public class ResourceGroupCheckerFilter implements Filter {
    private static final String ERROR = "error";
    private static final String API_ERROR = "/api/error";

    private static Set<String> notCheckSpecialApiSet = Sets.newHashSet();

    static {
        notCheckSpecialApiSet.add("/kylin/api/error");
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        val manager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (!manager.isResourceGroupEnabled()) {
            chain.doFilter(request, response);
            return;
        }

        if (!(request instanceof HttpServletRequest)) {
            return;
        }

        if (checkRequestPass((HttpServletRequest) request)) {
            chain.doFilter(request, response);
            return;
        }

        // project is not bound to resource group
        Pair<String, HttpServletRequest> projectInfo = ProjectInfoParser.parseProjectInfo((HttpServletRequest) request);
        String project = projectInfo.getFirst();

        if (!manager.isProjectBindToResourceGroup(project)) {
            Message msg = MsgPicker.getMsg();
            request.setAttribute(ERROR,
                    new KylinException(PROJECT_WITHOUT_RESOURCE_GROUP, msg.getProjectWithoutResourceGroup()));
            request.getRequestDispatcher(API_ERROR).forward(request, response);
            return;
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // just override it
    }

    private boolean checkRequestPass(HttpServletRequest request) {
        final String uri = StringUtils.stripEnd(request.getRequestURI(), "/");
        final String method = request.getMethod();

        return "GET".equals(method) || notCheckSpecialApiSet.contains(uri);
    }

}
