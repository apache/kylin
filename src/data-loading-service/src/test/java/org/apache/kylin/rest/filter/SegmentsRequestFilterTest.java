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

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

class SegmentsRequestFilterTest {
    private final SegmentsRequestFilter filter = Mockito.spy(new SegmentsRequestFilter());

    @Test
    void doFilter() throws ServletException, IOException {
        FilterChain chain = Mockito.spy(FilterChain.class);
        MockHttpServletResponse response = Mockito.spy(MockHttpServletResponse.class);
        Mockito.doNothing().when(chain).doFilter(null, response);
        filter.doFilter(null, response, chain);

        chain = new MockFilterChain();
        MockHttpServletRequest request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        request.setMethod(HttpMethod.GET);
        filter.doFilter(request, response, chain);

        chain = new MockFilterChain();
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        request.setMethod(HttpMethod.POST);
        request.setRequestURI("/kylin/api/models//segments");
        filter.doFilter(request, response, chain);

        ObjectNode node = new ObjectMapper().createObjectNode();
        chain = new MockFilterChain();
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        request.setMethod(HttpMethod.POST);
        request.setRequestURI("/kylin/api/models/model1/segments");
        filter.doFilter(request, response, chain);

        chain = new MockFilterChain();
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        request.setMethod(HttpMethod.POST);
        request.setRequestURI("/kylin/api/models/model1/segments");
        node.set(SegmentsRequestFilter.BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME, null);
        request.setContent(JsonUtil.writeValueAsBytes(node));
        filter.doFilter(request, response, chain);
        Assertions.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response.getStatus());
        JsonNode jsonNode = JsonUtil.readValueAsTree(response.getContentAsString());
        Assertions.assertEquals("999", jsonNode.get("code").asText());
        Assertions.assertEquals(ARGS_TYPE_CHECK.getErrorCode().getCode(), jsonNode.get("error_code").asText());
        Assertions.assertEquals(ARGS_TYPE_CHECK.getCodeMsg("null", "Boolean"),
                jsonNode.get("msg").asText());

        chain = new MockFilterChain();
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        request.setMethod(HttpMethod.POST);
        request.setRequestURI("/kylin/api/models/model1/segments");
        node.put(SegmentsRequestFilter.BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME, "");
        request.setContent(JsonUtil.writeValueAsBytes(node));
        filter.doFilter(request, response, chain);
        Assertions.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response.getStatus());
        jsonNode = JsonUtil.readValueAsTree(response.getContentAsString());
        Assertions.assertEquals("999", jsonNode.get("code").asText());
        Assertions.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorCode().getCode(),
                jsonNode.get("error_code").asText());
        Assertions.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getCodeMsg("build_all_sub_partitions"),
                jsonNode.get("msg").asText());

        chain = new MockFilterChain();
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        request.setMethod(HttpMethod.POST);
        request.setRequestURI("/kylin/api/models/model1/segments");
        node.put(SegmentsRequestFilter.BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME, 123);
        request.setContent(JsonUtil.writeValueAsBytes(node));
        filter.doFilter(request, response, chain);
        Assertions.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response.getStatus());
        jsonNode = JsonUtil.readValueAsTree(response.getContentAsString());
        Assertions.assertEquals("999", jsonNode.get("code").asText());
        Assertions.assertEquals(ARGS_TYPE_CHECK.getErrorCode().getCode(), jsonNode.get("error_code").asText());
        Assertions.assertEquals(ARGS_TYPE_CHECK.getCodeMsg("123", "Boolean"),
                jsonNode.get("msg").asText());

        chain = new MockFilterChain();
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        request.setMethod(HttpMethod.POST);
        request.setRequestURI("/kylin/api/models/model1/segments");
        node.put(SegmentsRequestFilter.BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME, true);
        request.setContent(JsonUtil.writeValueAsBytes(node));
        filter.doFilter(request, response, chain);
        Assertions.assertEquals(HttpServletResponse.SC_OK, response.getStatus());
        Assertions.assertEquals("", response.getContentAsString());

        chain = new MockFilterChain();
        request = new MockHttpServletRequest();
        response = new MockHttpServletResponse();
        request.setMethod(HttpMethod.POST);
        request.setRequestURI("/kylin/api/models/model1/segments");
        node.put(SegmentsRequestFilter.BUILD_ALL_SUB_PARTITIONS_PARAMETER_NAME, false);
        request.setContent(JsonUtil.writeValueAsBytes(node));
        filter.doFilter(request, response, chain);
        Assertions.assertEquals(HttpServletResponse.SC_OK, response.getStatus());
        Assertions.assertEquals("", response.getContentAsString());
    }

    @Test
    void checkBooleanArg() {
        try {
            filter.checkBooleanArg("test", null);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof KylinException);
            Assertions.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorCode().getCode(),
                    ((KylinException) e).getErrorCode().getCodeString());
            Assertions.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("test"), e.getMessage());
        }
        try {
            filter.checkBooleanArg("test", "");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof KylinException);
            Assertions.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getErrorCode().getCode(),
                    ((KylinException) e).getErrorCode().getCodeString());
            Assertions.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("test"), e.getMessage());
        }
        try {
            filter.checkBooleanArg("test", "123");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof KylinException);
            Assertions.assertEquals(ARGS_TYPE_CHECK.getErrorCode().getCode(),
                    ((KylinException) e).getErrorCode().getCodeString());
            Assertions.assertEquals(ARGS_TYPE_CHECK.getMsg("123", "Boolean"), e.getMessage());
        }
        filter.checkBooleanArg("test", "true");
        filter.checkBooleanArg("test", "false");
        filter.checkBooleanArg("test", true);
        filter.checkBooleanArg("test", false);
        filter.checkBooleanArg("test", "True");
        filter.checkBooleanArg("test", "False");
        filter.checkBooleanArg("test", "TRUE");
        filter.checkBooleanArg("test", "FALSE");
    }

    @Test
    void testPattern() {
        Assertions.assertFalse(
                SegmentsRequestFilter.REQUEST_URI_PATTERN.matcher("/kylin/api/models//segments").matches());
        Assertions.assertFalse(
                SegmentsRequestFilter.REQUEST_URI_PATTERN.matcher("/kylin/api/models/ /segments").matches());
        Assertions.assertFalse(
                SegmentsRequestFilter.REQUEST_URI_PATTERN.matcher("/kylin/api/models/12 /segments").matches());
        Assertions.assertTrue(SegmentsRequestFilter.REQUEST_URI_PATTERN
                .matcher("/kylin/api/models/d998562b-cbbf-b768-83a1-540085392e6e/segments").matches());
        Assertions.assertTrue(SegmentsRequestFilter.REQUEST_URI_PATTERN
                .matcher("/kylin/api/models/d998562b-cbbf-b768-83a1-540085392e6e_00/segments").matches());
        Assertions.assertTrue(SegmentsRequestFilter.REQUEST_URI_PATTERN
                .matcher("/kylin/api/models/d998562b-cbbf-b768-83a1-540085392e6e_00_01/segments").matches());
    }
}