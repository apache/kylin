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

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.util.UriComponentsBuilder;

public class SparkUIUtil {

    private static final HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(
            HttpClientBuilder.create().setMaxConnPerRoute(128).setMaxConnTotal(1024).disableRedirectHandling().build());
    private static final Logger logger = LoggerFactory.getLogger(SparkUIUtil.class);

    private static final int REDIRECT_THRESHOLD = 5;

    private static final String SPARK_UI_PROXY_HEADER = "X-Kylin-Proxy-Path";

    private SparkUIUtil() {
    }

    public static void resendSparkUIRequest(HttpServletRequest servletRequest, HttpServletResponse servletResponse,
            String sparkUiUrl, String uriPath, String proxyLocationBase) throws IOException {
        URI target = UriComponentsBuilder.fromHttpUrl(sparkUiUrl).path(uriPath).query(servletRequest.getQueryString())
                .build(true).toUri();

        final HttpMethod method = HttpMethod.resolve(servletRequest.getMethod());

        try (ClientHttpResponse response = execute(target, method, proxyLocationBase)) {
            rewrite(response, servletResponse, method, servletRequest.getRequestURL().toString(), REDIRECT_THRESHOLD,
                    proxyLocationBase);
        }
    }

    public static ClientHttpResponse execute(URI uri, HttpMethod method, String proxyLocationBase) throws IOException {
        ClientHttpRequest clientHttpRequest = factory.createRequest(uri, method);
        clientHttpRequest.getHeaders().put(SPARK_UI_PROXY_HEADER, Collections.singletonList(proxyLocationBase));
        return clientHttpRequest.execute();
    }

    private static void rewrite(final ClientHttpResponse response, final HttpServletResponse servletResponse,
            final HttpMethod originMethod, final String originUrlStr, final int depth, String proxyLocationBase)
            throws IOException {
        if (depth <= 0) {
            final String msg = String.format(Locale.ROOT, "redirect exceed threshold: %d, origin request: [%s %s]",
                    REDIRECT_THRESHOLD, originMethod, originUrlStr);
            logger.warn("UNEXPECTED_THINGS_HAPPENED {}", msg);
            servletResponse.getWriter().write(msg);
            return;
        }
        HttpHeaders headers = response.getHeaders();
        if (response.getStatusCode().is3xxRedirection()) {
            try (ClientHttpResponse r = execute(headers.getLocation(), originMethod, proxyLocationBase)) {
                rewrite(r, servletResponse, originMethod, originUrlStr, depth - 1, proxyLocationBase);
            }
            return;
        }

        servletResponse.setStatus(response.getRawStatusCode());

        if (response.getHeaders().getContentType() != null) {
            servletResponse.setHeader(HttpHeaders.CONTENT_TYPE,
                    Objects.requireNonNull(headers.getContentType()).toString());
        }
        IOUtils.copy(response.getBody(), servletResponse.getOutputStream());

    }

}
