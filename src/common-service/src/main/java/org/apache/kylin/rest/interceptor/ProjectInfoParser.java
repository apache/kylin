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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.rest.constant.ProjectInfoParserConstant;
import org.glassfish.jersey.uri.UriTemplate;

import lombok.Data;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectInfoParser {

    private static final String PROJECT_PARAM = "project";

    private ProjectInfoParser() {
        throw new IllegalStateException("Utility class");
    }

    public static Pair<String, HttpServletRequest> parseProjectInfo(HttpServletRequest request) {
        HttpServletRequest requestWrapper = request;
        String project = null;
        try {
            String contentType = request.getContentType();
            if (contentType != null && contentType.contains("application/x-www-form-urlencoded")) {
                // parse parameter and load form from body
                project = requestWrapper.getParameter(PROJECT_PARAM);
            }

            requestWrapper = new RepeatableBodyRequestWrapper(request);
            project = requestWrapper.getParameter(PROJECT_PARAM);
            if (StringUtils.isEmpty(project) && contentType != null && contentType.contains("json")) {
                val projectRequest = JsonUtil.readValue(((RepeatableBodyRequestWrapper) requestWrapper).getBody(),
                        ProjectRequest.class);
                if (projectRequest != null) {
                    project = projectRequest.getProject();
                }

            }
        } catch (IOException e) {
            // ignore JSON exception
        }

        if (StringUtils.isEmpty(project)) {
            project = extractProject((request).getRequestURI());
        }

        if (StringUtils.isEmpty(project)) {
            project = UnitOfWork.GLOBAL_UNIT;
        }

        log.debug("Parsed project {} from request {}", project, (request).getRequestURI());
        return new Pair<>(project, requestWrapper);
    }

    public static class RepeatableBodyRequestWrapper extends HttpServletRequestWrapper {

        @Getter
        private final byte[] body;

        public RepeatableBodyRequestWrapper(HttpServletRequest request) throws IOException {
            super(request);
            body = IOUtils.toByteArray(request.getInputStream());
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(body);
            return new ServletInputStream() {

                @Override
                public boolean isFinished() {
                    return isFinished;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener readListener) {
                    // Do not support it
                }

                private boolean isFinished;

                public int read() throws IOException {
                    int b = byteArrayInputStream.read();
                    isFinished = b == -1;
                    return b;
                }

            };
        }

        @Override
        public BufferedReader getReader() throws IOException {
            return new BufferedReader(new InputStreamReader(this.getInputStream(), Charset.defaultCharset()));
        }

    }

    @Data
    public static class ProjectRequest {
        private String project;
    }

    static String extractProject(String url) {
        for (String needParserURI : ProjectInfoParserConstant.INSTANCE.PROJECT_PARSER_URI_LIST) {
            val uriTemplate = new UriTemplate(needParserURI);
            val kvMap = new HashMap<String, String>();

            if (uriTemplate.match(url, kvMap)) {
                return kvMap.get(PROJECT_PARAM);
            }
        }

        return null;
    }
}
