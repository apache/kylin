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

package org.apache.kylin.rest.service;

import static org.apache.commons.net.util.Base64.decodeBase64;
import static org.apache.commons.net.util.Base64.encodeBase64;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.util.SparderUIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("sparderUIService")
public class SparderUIService extends BasicService {
    private static final String ROUTED = "routed";
    private static final String SERVER = "server";
    private static final String TRUE = "true";
    @Autowired
    private RouteService routeService;
    @Autowired
    @Qualifier("sparderUIUtil")
    private SparderUIUtil sparderUIUtil;
    @Autowired
    @Qualifier("normalRestTemplate")
    private RestTemplate restTemplate;

    public void proxy(HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws Exception {
        val server = getServer(servletRequest);
        if (StringUtils.isNotBlank(server) && !TRUE.equalsIgnoreCase(servletRequest.getHeader(ROUTED))
                && routeService.needRoute()) {
            log.info("proxy sparder UI to server : [{}]", server);
            val queryString = servletRequest.getQueryString();
            proxyToServer(server, queryString, restTemplate, servletRequest, servletResponse);
            return;
        }
        sparderUIUtil.proxy(servletRequest, servletResponse);
    }

    private static String getServer(HttpServletRequest servletRequest) {
        AtomicReference<String> serverAtomic = new AtomicReference<>("");
        if (Objects.isNull(servletRequest.getCookies())) {
            return null;
        }
        Arrays.stream(servletRequest.getCookies()).filter(cookie -> cookie.getName().equals(SERVER)).findFirst()
                .ifPresent(cookie -> serverAtomic.set(cookie.getValue()));
        val serverBytes = decodeBase64(serverAtomic.get().getBytes(Charset.defaultCharset()));
        return new String(serverBytes, Charset.defaultCharset());
    }

    public void proxy(String id, String queryId, String server, HttpServletRequest servletRequest,
            HttpServletResponse servletResponse) throws Exception {
        var realServer = server;
        if (StringUtils.isBlank(server)) {
            realServer = getServer(servletRequest);
        }
        if (StringUtils.isNotBlank(realServer) && !TRUE.equalsIgnoreCase(servletRequest.getHeader(ROUTED))
                && routeService.needRoute()) {
            log.info("proxy sparder UI to server : [{}] queryId : [{}] Id : [{}]", realServer, queryId, id);
            val queryString = "id=" + id;
            proxyToServer(realServer, queryString, restTemplate, servletRequest, servletResponse);
            return;
        }
        sparderUIUtil.proxy(servletRequest, servletResponse);
    }

    private void proxyToServer(String server, String queryString, RestTemplate restTemplate,
            HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
        ServletRequestAttributes attributes = new ServletRequestAttributes(httpServletRequest);
        RequestContextHolder.setRequestAttributes(attributes);

        HttpHeaders headers = new HttpHeaders();
        Collections.list(httpServletRequest.getHeaderNames())
                .forEach(k -> headers.put(k, Collections.list(httpServletRequest.getHeaders(k))));
        headers.add(ROUTED, TRUE);
        MsgPicker.setMsg(httpServletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
        ErrorCode.setMsg(httpServletRequest.getHeader(HttpHeaders.ACCEPT_LANGUAGE));
        val realQueryString = StringUtils.isBlank(queryString) ? "" : "?" + queryString;
        val exchange = restTemplate.exchange(
                httpServletRequest.getScheme() + "://" + server + httpServletRequest.getRequestURI() + realQueryString,
                HttpMethod.valueOf(httpServletRequest.getMethod()), new HttpEntity<>(new byte[] {}, headers),
                byte[].class);
        HttpHeaders responseHeaders = exchange.getHeaders();
        byte[] responseBody = Optional.ofNullable(exchange.getBody()).orElse(new byte[0]);
        int responseStatus = exchange.getStatusCodeValue();
        httpServletResponse.setStatus(responseStatus);
        setResponseHeaders(responseHeaders, httpServletResponse);

        val serverBytes = server.getBytes(Charset.defaultCharset());
        val serverCookie = new String(encodeBase64(serverBytes), Charset.defaultCharset());
        Cookie cookie = new Cookie(SERVER, serverCookie);
        cookie.setPath(SparderUIUtil.KYLIN_UI_BASE);
        httpServletResponse.addCookie(cookie);

        httpServletResponse.getOutputStream().write(responseBody);
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
}
