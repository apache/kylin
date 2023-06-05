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

import static org.apache.kylin.common.constant.Constants.TRACE_ID;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.response.ErrorResponse;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServletServerHttpRequest;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HttpUtil {
    private static final String DEFAULT_CONTENT_TYPE = MediaType.APPLICATION_JSON_VALUE;
    private static final Charset DEFAULT_CONTENT_CHARSET = StandardCharsets.UTF_8;

    private HttpUtil() {}

    public static String getFullRequestUrl(HttpServletRequest request) {
        String url = request.getRequestURL().toString();
        if (StringUtils.isNotBlank(request.getQueryString())) {
            if (url.lastIndexOf("?") > -1) {
                return url + "&" + request.getQueryString();
            } else {
                return url + "?" + request.getQueryString();
            }
        }
        return request.getRequestURL().toString();
    }

    public static void setErrorResponse(HttpServletRequest request, HttpServletResponse response, int statusCode, Exception ex)
            throws IOException {
        response.setStatus(statusCode);
        response.setContentType(DEFAULT_CONTENT_TYPE);
        ErrorResponse errorResponse = new ErrorResponse(getFullRequestUrl(request), ex);

        String errorStr = JsonUtil.writeValueAsIndentString(errorResponse);
        response.setCharacterEncoding(DEFAULT_CONTENT_CHARSET.name());
        byte[] responseData = errorStr.getBytes(DEFAULT_CONTENT_CHARSET);
        ServletOutputStream writer = response.getOutputStream();
        response.setContentLength(responseData.length);
        writer.write(responseData, 0, responseData.length);
        writer.flush();
        writer.close();
    }

    public static String formatRequest(HttpServletRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("Url: ").append(getFullRequestUrl(request)).append("\n");
        sb.append("Headers: ").append(new ServletServerHttpRequest(request).getHeaders()).append("\n");
        sb.append("RemoteAddr: ").append(request.getRemoteAddr()).append("\n");
        sb.append("RemoteUser: ").append(request.getRemoteUser()).append("\n");
        sb.append("Session: ").append(formatSession(request.getSession(false))).append("\n");
        Object attr = request.getAttribute(TRACE_ID);
        if (attr != null) {
            sb.append("TraceId: ").append(attr).append("\n");
        }
        return sb.toString();
    }

    public static String formatSession(HttpSession session) {
        return String.format("Id=%s; createTime=%s; lastAccessedTime=%s",
                session.getId(), session.getCreationTime(), session.getLastAccessedTime());
    }
}
