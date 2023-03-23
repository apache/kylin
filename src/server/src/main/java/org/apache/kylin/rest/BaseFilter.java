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

import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_WITHOUT_RESOURCE_GROUP;
import static org.apache.kylin.common.exception.ServerErrorCode.SYSTEM_IS_RECOVER;
import static org.apache.kylin.common.exception.ServerErrorCode.TRANSFER_FAILED;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import javax.servlet.Filter;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.rest.response.ErrorResponse;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseFilter implements Filter {
    protected static final String API_PREFIX = "/kylin/api";
    protected static final String ROUTED = "routed";
    protected static final String TRUE = "true";
    protected static final String ERROR = "error";
    protected static final String API_ERROR = "/api/error";
    protected static final String FILTER_PASS = "filter_pass";

    protected static final String ERROR_REQUEST_URL = "/kylin/api/error";

    protected KylinException getKylinException(String project, Message msg) {
        KylinException exception;
        val manager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (manager.isResourceGroupEnabled() && !manager.isProjectBindToResourceGroup(project)) {
            exception = new KylinException(PROJECT_WITHOUT_RESOURCE_GROUP, msg.getProjectWithoutResourceGroup());
        } else {
            exception = new KylinException(SYSTEM_IS_RECOVER, msg.getLeadersHandleOver());
        }
        return exception;
    }

    protected void setResponseHeaders(HttpHeaders responseHeaders, HttpServletResponse servletResponse) {
        responseHeaders.forEach((k, v) -> {
            if (k.equals(HttpHeaders.TRANSFER_ENCODING)) {
                return;
            }
            for (String headerValue : v) {
                servletResponse.setHeader(k, headerValue);
            }
        });
    }

    protected void routeAPI(RestTemplate restTemplate, ServletRequest servletRequest, ServletResponse servletResponse,
            String project) throws ServletException, IOException {
        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
        ServletRequestAttributes attributes = new ServletRequestAttributes(httpServletRequest);
        RequestContextHolder.setRequestAttributes(attributes);

        val body = IOUtils.toByteArray(servletRequest.getInputStream());
        HttpHeaders headers = new HttpHeaders();
        Collections.list(httpServletRequest.getHeaderNames())
                .forEach(k -> headers.put(k, Collections.list(httpServletRequest.getHeaders(k))));
        headers.add(ROUTED, TRUE);
        byte[] responseBody;
        int responseStatus;
        HttpHeaders responseHeaders;
        MsgPicker.setMsg(httpServletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
        ErrorCode.setMsg(httpServletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
        try {
            val queryString = StringUtils.isBlank(httpServletRequest.getQueryString()) ? ""
                    : "?" + httpServletRequest.getQueryString();
            val exchange = restTemplate.exchange("http://all" + httpServletRequest.getRequestURI() + queryString,
                    HttpMethod.valueOf(httpServletRequest.getMethod()), new HttpEntity<>(body, headers), byte[].class);
            tryCatchUp();
            responseHeaders = exchange.getHeaders();
            responseBody = Optional.ofNullable(exchange.getBody()).orElse(new byte[0]);
            responseStatus = exchange.getStatusCodeValue();
        } catch (IllegalStateException | ResourceAccessException e) {
            responseStatus = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
            Message msg = MsgPicker.getMsg();
            KylinException exception = getKylinException(project, msg);
            ErrorResponse errorResponse = new ErrorResponse(httpServletRequest.getRequestURL().toString(), exception);
            responseBody = JsonUtil.writeValueAsBytes(errorResponse);
            responseHeaders = new HttpHeaders();
            responseHeaders.setContentType(MediaType.APPLICATION_JSON);
            log.error("no job node", e);
        } catch (HttpStatusCodeException e) {
            responseStatus = e.getRawStatusCode();
            responseBody = e.getResponseBodyAsByteArray();
            responseHeaders = Optional.ofNullable(e.getResponseHeaders()).orElse(new HttpHeaders());
            log.warn("code {}, error {}", e.getStatusCode(), e.getMessage());
        } catch (Exception e) {
            log.error("transfer failed", e);
            servletRequest.setAttribute(ERROR,
                    new KylinException(TRANSFER_FAILED, MsgPicker.getMsg().getTransferFailed()));
            servletRequest.getRequestDispatcher(API_ERROR).forward(servletRequest, servletResponse);
            return;
        }
        httpServletResponse.setStatus(responseStatus);
        setResponseHeaders(responseHeaders, httpServletResponse);
        httpServletResponse.getOutputStream().write(responseBody);
    }

    protected void tryCatchUp() {
        try {
            ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.getAuditLogStore().catchupWithTimeout();
        } catch (Exception e) {
            log.error("Failed to catchup manually.", e);
        }
    }
}
