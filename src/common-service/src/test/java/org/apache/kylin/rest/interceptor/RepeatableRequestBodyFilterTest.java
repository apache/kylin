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

import java.io.IOException;
import java.nio.charset.Charset;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.project.NProjectLoader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.jetty.servlet.DefaultServlet;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class RepeatableRequestBodyFilterTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testHitTheCache() throws IOException, ServletException {
        val filter = new RepeatableRequestBodyFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/api/test");
        request.setContentType("application/json");
        request.setContent(("" + "{\n" + "    \"project\": \"default\",\n" + "    \"hello\": \"world\"\n" + "}")
                .getBytes(Charset.defaultCharset()));

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
                Assert.assertNotNull(getCache());
            }
        });

        filter.doFilter(request, response, chain);
        Assert.assertNull(getCache());
    }

    @Test
    public void testCleanupCacheAfterException() {
        val filter = new RepeatableRequestBodyFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/api/test");
        request.setContentType("application/json");
        request.setContent(("" + "{\n" + "    \"project\": \"default\",\n" + "    \"hello\": \"world\"\n" + "}")
                .getBytes(Charset.defaultCharset()));

        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain(new DefaultServlet() {
            @Override
            public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
                throw new RuntimeException("error");
            }
        });

        try {
            filter.doFilter(request, response, chain);
        } catch (Exception ignore) {
        }
        Assert.assertNull(getCache());
    }

    private Object getCache() {
        return ((ThreadLocal) ReflectionTestUtils.getField(NProjectLoader.class, "cache")).get();
    }
}
