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
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.KylinConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.ui.SparkUI;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

@Component("sparderUIUtil")
public class SparderUIUtil {

    public static final String UI_BASE = "/sparder";
    public static final String PROXY_LOCATION_BASE = KylinConfig.getInstanceFromEnv().getUIProxyLocation() + UI_BASE;
    private static final String KYLIN_UI_BASE = "/kylin" + UI_BASE;
    private static final String SQL_EXECUTION_PAGE = "/SQL/execution/";
    private volatile String webUrl;
    private volatile String amSQLBase;
    private volatile String appId;
    private volatile String proxyBase;

    public void proxy(HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws IOException {
        final String currentWebUrl = getWebUrl();
        String uriPath = servletRequest.getRequestURI().substring(KYLIN_UI_BASE.length());
        if (!isProxyBaseEnabled()) {
            uriPath = uriPath.replace(proxyBase, "");
        }

        SparkUIUtil.resendSparkUIRequest(servletRequest, servletResponse, currentWebUrl, uriPath, PROXY_LOCATION_BASE);
    }

    public String getSQLTrackingPath(String id) throws IOException {
        checkVersion();
        return PROXY_LOCATION_BASE + (isProxyBaseEnabled() ? amSQLBase : SQL_EXECUTION_PAGE) + "?id=" + id;
    }

    private boolean isProxyBaseEnabled() {
        return Objects.nonNull(amSQLBase);
    }

    private String getWebUrl() throws IOException {
        checkVersion();
        return webUrl;
    }

    private void checkVersion() throws IOException {

        final SparkContext sc = SparderEnv.getSparkSession().sparkContext();
        final SparkUI ui = sc.ui().get();
        if (ui.appId().equals(appId)) {
            return;
        }

        // KE-12678
        proxyBase = "/proxy/" + ui.appId();

        // reset
        amSQLBase = null;
        // try to fetch amWebUrl if exists
        try (ClientHttpResponse response = SparkUIUtil.execute(
                UriComponentsBuilder.fromHttpUrl(ui.webUrl()).path(SQL_EXECUTION_PAGE).query("id=1").build().toUri(),
                HttpMethod.GET, PROXY_LOCATION_BASE)) {
            if (response.getStatusCode().is3xxRedirection()) {
                URI uri = response.getHeaders().getLocation();
                amSQLBase = Objects.requireNonNull(uri).getPath();
                webUrl = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort();
                appId = ui.appId();
                return;
            }

            webUrl = ui.webUrl();
            appId = ui.appId();
        }
    }

}
