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

package org.apache.kylin.rest.util;

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.rest.util.HttpUtil.formatRequest;
import static org.apache.kylin.rest.util.HttpUtil.formatSession;
import static org.apache.kylin.rest.util.HttpUtil.getFullRequestUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.response.ErrorResponse;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpSession;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HttpUtilTest extends NLocalFileMetadataTestCase {
    private static final MockHttpServletRequest REQUEST_SAMPLE = new MockHttpServletRequest();

    private static final MockHttpSession DEFAULT_SESSION = new MockHttpSession();

    @Before
    public void init() {
        REQUEST_SAMPLE.setSession(DEFAULT_SESSION);
        REQUEST_SAMPLE.setMethod("Get");
        REQUEST_SAMPLE.setRequestURI("/api/projects");
        REQUEST_SAMPLE.setServerPort(8081);
        REQUEST_SAMPLE.setRemoteAddr("127.0.0.1");
        REQUEST_SAMPLE.setParameter("project", "test");
        REQUEST_SAMPLE.setParameter("valid", "true");
        REQUEST_SAMPLE.setContentType(MediaType.APPLICATION_JSON_VALUE);
        REQUEST_SAMPLE.setContent("{\"sample\": true}".getBytes(StandardCharsets.UTF_8));
        REQUEST_SAMPLE.setAttribute("traceId", "1");
        createTestMetadata();
    }

    @Test
    public void testFormatUrl() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRequestURI("/api/projects");
        request.setQueryString("project=test");
        assertEquals("http://localhost/api/projects?project=test", getFullRequestUrl(request));
        request.setRequestURI("/api/projects?valid=true");
        assertEquals("http://localhost/api/projects?valid=true&project=test", getFullRequestUrl(request));
    }

    @Test
    public void testErrorResponse() throws IOException {
        MockHttpServletResponse response = new MockHttpServletResponse();
        HttpUtil.setErrorResponse(REQUEST_SAMPLE, response, 200, new RuntimeException("Empty"));

        assertEquals(200, response.getStatus());
        assertEquals(StandardCharsets.UTF_8.name(), response.getCharacterEncoding());
        assertEquals(MediaType.APPLICATION_JSON_VALUE + ";charset=UTF-8", response.getContentType());
        assertTrue(response.getContentLength() > 0);

        ObjectMapper mapper = new ObjectMapper();
        ErrorResponse errorResponse = mapper.reader().readValue(mapper.createParser(response.getContentAsString()),
                ErrorResponse.class);

        assertEquals(errorResponse.getUrl(), getFullRequestUrl(REQUEST_SAMPLE));
        assertEquals(KylinException.CODE_UNDEFINED, errorResponse.getCode());
        assertTrue(errorResponse.getStacktrace().length() > 0);
        assertTrue(errorResponse.getMsg().length() > 0);
        assertNull(errorResponse.getSuggestion());
        assertNull(errorResponse.getErrorCode());
    }

    @Test
    public void testFormatEmptyRequest() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setSession(DEFAULT_SESSION);
        assertNotNull(request.getSession());

        String expected = "Url: " + getFullRequestUrl(request) + "\n" + "Headers: []\n" + "RemoteAddr: 127.0.0.1\n"
                + "RemoteUser: null\n" + "Session: " + formatSession(request.getSession()) + "\n";
        assertEquals(expected, formatRequest(request));
    }

    @Test
    public void testFormatRequest() {
        assertNotNull(REQUEST_SAMPLE.getSession());
        String expected = "Url: " + getFullRequestUrl(REQUEST_SAMPLE) + "\n"
                + "Headers: [Content-Type:\"application/json\", Content-Length:\"16\"]\n" + "RemoteAddr: 127.0.0.1\n"
                + "RemoteUser: null\n" + "Session: " + formatSession(REQUEST_SAMPLE.getSession()) + "\n"
                + "TraceId: 1\n";
        assertEquals(expected, formatRequest(REQUEST_SAMPLE));
    }

    @Test
    public void testErrorResponseWithoutStackTrace() throws IOException {
        overwriteSystemProp("kylin.server.stack-interception-enabled", "true");
        // RuntimeException
        MockHttpServletResponse response = new MockHttpServletResponse();
        HttpUtil.setErrorResponse(REQUEST_SAMPLE, response, 200, new RuntimeException("Empty"));

        ObjectMapper mapper = new ObjectMapper();
        ErrorResponse errorResponse = mapper.reader().readValue(mapper.createParser(response.getContentAsString()),
                ErrorResponse.class);

        assertEquals(errorResponse.getUrl(), getFullRequestUrl(REQUEST_SAMPLE));
        assertEquals(KylinException.CODE_UNDEFINED, errorResponse.getCode());
        assertEquals(ErrorResponse.STACK_MSG, errorResponse.getStacktrace());
        assertFalse(errorResponse.getMsg().isEmpty());

        // KylinException
        response = new MockHttpServletResponse();
        HttpUtil.setErrorResponse(REQUEST_SAMPLE, response, 200,
                new KylinException(PERMISSION_DENIED, "Operation failed, unknown permission."));

        mapper = new ObjectMapper();
        errorResponse = mapper.reader().readValue(mapper.createParser(response.getContentAsString()),
                ErrorResponse.class);

        assertEquals(errorResponse.getUrl(), getFullRequestUrl(REQUEST_SAMPLE));
        assertEquals(KylinException.CODE_UNDEFINED, errorResponse.getCode());
        assertEquals(ErrorResponse.STACK_MSG, errorResponse.getStacktrace());
        assertFalse(errorResponse.getMsg().isEmpty());
    }
}
