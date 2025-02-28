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
import java.util.Locale;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.KylinConfig;
import org.apache.spark.deploy.history.HistoryServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import lombok.SneakyThrows;

@Component("SparkHistoryUIUtil")
@ConditionalOnProperty(name = "kylin.history-server.enable", havingValue = "true")
public class SparkHistoryUIUtil {

    @Autowired
    @Qualifier("historyServer")
    private HistoryServer historyServer;

    public static final String HISTORY_UI_BASE = "/history_server";

    public static final String KYLIN_HISTORY_UI_BASE = "/kylin" + HISTORY_UI_BASE;

    public static final String PROXY_LOCATION_BASE = KylinConfig.getInstanceFromEnv().getUIProxyLocation()
            + HISTORY_UI_BASE;

    public void proxy(HttpServletRequest servletRequest, HttpServletResponse servletResponse) throws IOException {
        final String historyServerUrl = getHistoryServerUrl();
        String uriPath = servletRequest.getRequestURI().substring(KYLIN_HISTORY_UI_BASE.length());

        SparkUIUtil.resendSparkUIRequest(servletRequest, servletResponse, historyServerUrl, uriPath,
                PROXY_LOCATION_BASE);
    }

    @SneakyThrows
    private String getHistoryServerUrl() {
        return String.format(Locale.ROOT, "http://%s:%s", InetAddress.getLocalHost().getHostAddress(),
                historyServer.boundPort());
    }

    public static String getHistoryTrackerUrl(String appId) {
        return PROXY_LOCATION_BASE + "/history/" + appId;
    }

}
