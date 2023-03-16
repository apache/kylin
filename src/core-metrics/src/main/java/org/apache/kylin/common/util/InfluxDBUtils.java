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
package org.apache.kylin.common.util;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.net.util.TrustManagerUtils;

import org.apache.kylin.shaded.influxdb.okhttp3.OkHttpClient;
import org.apache.kylin.shaded.influxdb.org.influxdb.InfluxDB;
import org.apache.kylin.shaded.influxdb.org.influxdb.InfluxDBFactory;

public class InfluxDBUtils {

    private InfluxDBUtils() {
    }

    public static final String HTTP = "http://";

    public static final String HTTPS = "https://";

    public static InfluxDB getInfluxDBInstance(String addr, String username, String password, boolean enableSsl,
            boolean enableUnsafeSsl) throws Exception {
        String protocol = enableSsl ? HTTPS : HTTP;
        String url = protocol + addr;
        if (enableSsl && enableUnsafeSsl) {
            return InfluxDBFactory.connect(url, username, password, getBuilderWithAllTrustManager());
        }
        return InfluxDBFactory.connect(url, username, password);
    }

    private static OkHttpClient.Builder getBuilderWithAllTrustManager() throws Exception {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        X509TrustManager x509TrustManager = TrustManagerUtils.getAcceptAllTrustManager();
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(null, new TrustManager[] { x509TrustManager }, null);
        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        builder.sslSocketFactory(sslSocketFactory, x509TrustManager);
        builder.hostnameVerifier((hostname, session) -> true);
        return builder;
    }

}
