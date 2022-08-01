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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.rest.exception.NotFoundException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(-400)
public class V2ApiFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest servletRequest = (HttpServletRequest) request;
            if (HTTP_VND_APACHE_KYLIN_V2_JSON.equalsIgnoreCase(servletRequest.getHeader("Accept"))) {
                String uri = servletRequest.getRequestURI();
                try {
                    HandlerMapping handlerMapping = this.getRequestMappingHandlerMapping(servletRequest);
                    if (handlerMapping != null) {
                        HandlerExecutionChain handler = handlerMapping.getHandler((HttpServletRequest) request);
                        if (handler == null || handler.getHandler() == null) {
                            throw new NotFoundException(
                                    String.format(Locale.ROOT, "%s API of version v2 is no longer supported", uri));
                        }
                    }
                } catch (Exception e) {
                    log.warn("get hander from request uri {} failed", uri, e);
                    throw new NotFoundException(
                            String.format(Locale.ROOT, "%s API of version v2 is no longer supported", uri));
                }
            }
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // just override it
    }

    private HandlerMapping getRequestMappingHandlerMapping(HttpServletRequest request) {
        ServletContext servletContext = request.getSession().getServletContext();
        if (servletContext == null) {
            return null;
        }
        WebApplicationContext appContext = WebApplicationContextUtils.getWebApplicationContext(servletContext);

        Map<String, HandlerMapping> allRequestMappings = BeanFactoryUtils.beansOfTypeIncludingAncestors(appContext,
                HandlerMapping.class, true, false);

        return allRequestMappings.values().stream()
                .filter(handlerMapping -> handlerMapping.getClass().equals(RequestMappingHandlerMapping.class))
                .findAny().orElse(null);
    }
}
