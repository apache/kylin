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
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.AddressUtil;
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
        URI originTarget = validateProxyUrl(sparkUiUrl);
        URI target = UriComponentsBuilder.fromUri(originTarget).path(uriPath).query(servletRequest.getQueryString())
                .build(true).toUri();

        final HttpMethod method = HttpMethod.resolve(servletRequest.getMethod());

        try (ClientHttpResponse response = execute(target, method, proxyLocationBase)) {
            rewrite(response, servletResponse, method, servletRequest.getRequestURL().toString(), REDIRECT_THRESHOLD,
                    proxyLocationBase, originTarget);
        }
    }

    public static ClientHttpResponse execute(URI uri, HttpMethod method, String proxyLocationBase) throws IOException {
        URI validatedUri = validateProxyUrl(uri.toString());
        ClientHttpRequest clientHttpRequest = factory.createRequest(validatedUri, method);
        clientHttpRequest.getHeaders().put(SPARK_UI_PROXY_HEADER, Collections.singletonList(proxyLocationBase));
        return clientHttpRequest.execute();
    }

    private static void rewrite(final ClientHttpResponse response, final HttpServletResponse servletResponse,
            final HttpMethod originMethod, final String originUrlStr, final int depth, String proxyLocationBase)
            throws IOException {
        rewrite(response, servletResponse, originMethod, originUrlStr, depth, proxyLocationBase, null);
    }

    private static void rewrite(final ClientHttpResponse response, final HttpServletResponse servletResponse,
            final HttpMethod originMethod, final String originUrlStr, final int depth, String proxyLocationBase,
            final URI originTarget) throws IOException {
        if (depth <= 0) {
            final String msg = String.format(Locale.ROOT, "redirect exceed threshold: %d, origin request: [%s %s]",
                    REDIRECT_THRESHOLD, originMethod, originUrlStr);
            logger.warn("UNEXPECTED_THINGS_HAPPENED {}", msg);
            servletResponse.getWriter().write(msg);
            return;
        }
        HttpHeaders headers = response.getHeaders();
        if (response.getStatusCode().is3xxRedirection()) {
            URI redirectTarget = validateProxyUrl(Objects.toString(headers.getLocation(), ""));
            if (originTarget != null && !isSameAuthority(originTarget, redirectTarget)) {
                throw new IOException("redirect target is not allowed: " + redirectTarget);
            }
            try (ClientHttpResponse r = execute(redirectTarget, originMethod, proxyLocationBase)) {
                rewrite(r, servletResponse, originMethod, originUrlStr, depth - 1, proxyLocationBase, originTarget);
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

    public static URI validateProxyUrl(String url) {
        if (StringUtils.isBlank(url)) {
            throw new KylinRuntimeException("Proxy target URL cannot be empty");
        }

        final URI target;
        try {
            target = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new KylinRuntimeException("Invalid proxy target URL: " + url, e);
        }

        String scheme = target.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new KylinRuntimeException("Proxy target URL must use http or https");
        }
        if (StringUtils.isBlank(target.getHost()) || target.getUserInfo() != null || target.getFragment() != null) {
            throw new KylinRuntimeException("Invalid proxy target URL: " + url);
        }

        String host = target.getPort() < 0 ? target.getHost() : target.getHost() + ":" + target.getPort();
        AddressUtil.validateHost(host);
        return target;
    }

    public static void validateJobTrackingUrl(String url, String requestHost) {
        URI target = validateProxyUrl(url);
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        String trackingUrlPattern = config.getJobTrackingURLPattern();
        if (StringUtils.isNotBlank(trackingUrlPattern) && url.matches(trackingUrlPattern)) {
            return;
        }

        String yarnStatusUrl = config.getYarnStatusCheckUrl();
        if (StringUtils.isNotBlank(yarnStatusUrl)) {
            URI allowedYarnTarget = validateProxyUrl(yarnStatusUrl);
            if (isSameAuthority(target, allowedYarnTarget)) {
                return;
            }
        }

        if (StringUtils.isNotBlank(requestHost) && isSameHost(target.getHost(), requestHost)) {
            return;
        }

        throw new KylinRuntimeException("Tracking URL host is not allowed: " + target.getHost());
    }

    private static boolean isSameAuthority(URI first, URI second) {
        return StringUtils.equalsIgnoreCase(first.getHost(), second.getHost())
                && getEffectivePort(first) == getEffectivePort(second);
    }

    private static int getEffectivePort(URI uri) {
        if (uri.getPort() >= 0) {
            return uri.getPort();
        }
        return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    private static boolean isSameHost(String firstHost, String secondHost) {
        try {
            InetAddress first = InetAddress.getByName(firstHost);
            InetAddress second = InetAddress.getByName(secondHost);
            return first.equals(second);
        } catch (UnknownHostException e) {
            return StringUtils.equalsIgnoreCase(firstHost, secondHost);
        }
    }

}
