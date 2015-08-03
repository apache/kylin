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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.web.filter.OncePerRequestFilter;

import com.github.isrsal.logging.RequestWrapper;
import com.github.isrsal.logging.ResponseWrapper;
import com.sun.jersey.core.util.Base64;

/**
 * Created by jiazhong on 2015/4/20.
 */

public class KylinApiFilter extends OncePerRequestFilter {
    protected static final Logger logger = LoggerFactory.getLogger(KylinApiFilter.class);
    private static final String REQUEST_PREFIX = "Request: ";
    private static final String RESPONSE_PREFIX = "Response: ";
    private AtomicLong id = new AtomicLong(1L);

    public KylinApiFilter() {
    }

    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        if (logger.isDebugEnabled()) {
            long requestId = this.id.incrementAndGet();
            request = new RequestWrapper(Long.valueOf(requestId), (HttpServletRequest) request);
            response = new ResponseWrapper(Long.valueOf(requestId), (HttpServletResponse) response);
        }

        try {
            filterChain.doFilter((ServletRequest) request, (ServletResponse) response);
        } finally {
            if (logger.isDebugEnabled()) {
                this.logRequest((HttpServletRequest) request, (ResponseWrapper) response);
                //                this.logResponse((ResponseWrapper)response);
            }

        }

    }

    private void logRequest(HttpServletRequest request, ResponseWrapper response) {
        StringBuilder msg = new StringBuilder();
        msg.append("REQUEST: ");
        HttpSession session = request.getSession(true);
        SecurityContext context = (SecurityContext) session.getAttribute("SPRING_SECURITY_CONTEXT");

        String requester = "";
        if (context != null) {
            Authentication authentication = context.getAuthentication();
            if (authentication != null) {
                requester = authentication.getName();
            }

        } else {
            final String authorization = request.getHeader("Authorization");
            if (authorization != null && authorization.startsWith("Basic")) {
                // Authorization: Basic base64credentials
                String base64Credentials = authorization.substring("Basic".length()).trim();
                String credentials = new String(Base64.decode(base64Credentials), Charset.forName("UTF-8"));
                // credentials = username:password
                String[] values = credentials.split(":", 2);
                requester = values[0];
            }
        }
        msg.append("REQUESTER=" + requester);

        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        msg.append(";REQ_TIME=" + format.format(new Date()));
        msg.append(";URI=").append(request.getRequestURI());
        msg.append(";METHOD=").append(request.getMethod());
        msg.append(";QUERY_STRING=").append(request.getQueryString());
        if (request instanceof RequestWrapper && !this.isMultipart(request)) {
            RequestWrapper requestWrapper = (RequestWrapper) request;

            try {
                String e = requestWrapper.getCharacterEncoding() != null ? requestWrapper.getCharacterEncoding() : "UTF-8";
                msg.append(";PAYLOAD=").append(new String(requestWrapper.toByteArray(), e));
            } catch (UnsupportedEncodingException var6) {
                logger.warn("Failed to parse request payload", var6);
            }
        }
        msg.append(";RESP_STATUS=" + response.getStatus()).append(";");

        logger.debug(msg.toString());
    }

    private boolean isMultipart(HttpServletRequest request) {
        return request.getContentType() != null && request.getContentType().startsWith("multipart/form-data");
    }

    private void logResponse(ResponseWrapper response) {
        StringBuilder msg = new StringBuilder();
        msg.append("RESPONSE: ");
        msg.append("REQUEST_ID=").append(response.getId());

        try {
            msg.append("; payload=").append(new String(response.toByteArray(), response.getCharacterEncoding()));
        } catch (UnsupportedEncodingException var4) {
            logger.warn("Failed to parse response payload", var4);
        }

        logger.debug(msg.toString());
    }
}