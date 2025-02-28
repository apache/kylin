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

package org.apache.kylin.rest.filter;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.ARGS_TYPE_CHECK;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.rest.response.ErrorResponse;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import lombok.extern.slf4j.Slf4j;

@Component
@Order
@Slf4j
public class SegmentsRequestFilter implements Filter {
    public static final Pattern REQUEST_URI_PATTERN = Pattern.compile("(/kylin/api/models/)\\S+(/segments)");
    public static final String BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME = "build_all_sub_partitions";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // just override it
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        if (request != null && httpServletRequest.getMethod().equals(HttpMethod.POST)
                && REQUEST_URI_PATTERN.matcher(httpServletRequest.getRequestURI()).matches()) {
            try {
                JsonNode bodyNode = JsonUtil
                        .readValueAsTree(IOUtils.toString(httpServletRequest.getInputStream(), StandardCharsets.UTF_8));
                Set<String> bodyKeys = Sets.newHashSet(bodyNode.fieldNames());
                if (bodyKeys.contains(BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME)) {
                    checkBooleanArg(BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME,
                            bodyNode.get(BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME).asText());
                }
            } catch (KylinException e) {
                MsgPicker.setMsg(httpServletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
                ErrorCode.setMsg(httpServletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));

                ErrorResponse errorResponse = new ErrorResponse(httpServletRequest.getRequestURL().toString(), e);
                byte[] responseBody = JsonUtil.writeValueAsBytes(errorResponse);

                HttpServletResponse httpServletResponse = (HttpServletResponse) response;
                httpServletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                httpServletResponse.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON.toString());
                httpServletResponse.getOutputStream().write(responseBody);
                return;
            }
        }

        chain.doFilter(request, response);
    }

    private void checkRequiredArg(String fieldName, Object fieldValue) {
        if (fieldValue == null || StringUtils.isEmpty(String.valueOf(fieldValue))) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, fieldName);
        }
    }

    public void checkBooleanArg(String fieldName, Object fieldValue) {
        checkRequiredArg(fieldName, fieldValue);
        String booleanString = String.valueOf(fieldValue);
        if (!String.valueOf(true).equalsIgnoreCase(booleanString)
                && !String.valueOf(false).equalsIgnoreCase(booleanString)) {
            throw new KylinException(ARGS_TYPE_CHECK, booleanString, "Boolean");
        }
    }

    @Override
    public void destroy() {
        // just override it
    }
}
