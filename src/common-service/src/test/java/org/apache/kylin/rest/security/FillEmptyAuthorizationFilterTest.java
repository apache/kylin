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
package org.apache.kylin.rest.security;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.common.util.Unsafe;
import org.junit.Assert;
import org.junit.Test;
import org.sparkproject.jetty.servlet.DefaultServlet;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import lombok.val;

public class FillEmptyAuthorizationFilterTest extends ServiceTestBase {

    @Test
    public void testEmptyAuthorization() throws IOException, ServletException {
        val fillEmptyAuthorizationFilter = new FillEmptyAuthorizationFilter();
        MockHttpServletRequest mockRequest = new MockHttpServletRequest();
        mockRequest.setRequestURI("/api/test");
        mockRequest.setContentType("application/json");
        MutableHttpServletRequest request = new MutableHttpServletRequest(mockRequest);
        MockHttpServletResponse response = new MockHttpServletResponse();
        Unsafe.setProperty("kap.authorization.skip-basic-authorization", "TRUE");
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) {
            }
        });

        fillEmptyAuthorizationFilter.doFilter(request, response, chain);
        Assert.assertTrue(chain.getRequest() instanceof MutableHttpServletRequest);
        MutableHttpServletRequest temp = ((MutableHttpServletRequest) chain.getRequest());
        temp.getHeaderNames();
        Assert.assertEquals("basic MDow", temp.getHeader("Authorization"));
    }

    @Test
    public void testBasicAuthorization() throws IOException, ServletException {
        val fillEmptyAuthorizationFilter = new FillEmptyAuthorizationFilter();
        MockHttpServletRequest mockRequest = new MockHttpServletRequest();
        mockRequest.setRequestURI("/api/test");
        mockRequest.setContentType("application/json");
        Unsafe.setProperty("kap.authorization.skip-basic-authorization", "FALSE");
        MutableHttpServletRequest request = new MutableHttpServletRequest(mockRequest);
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) {
            }
        });

        fillEmptyAuthorizationFilter.doFilter(request, response, chain);
        Assert.assertNull(((MutableHttpServletRequest) chain.getRequest()).getHeader("Authorization"));
    }

}
