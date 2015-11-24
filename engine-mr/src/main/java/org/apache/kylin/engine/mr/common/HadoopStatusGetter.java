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

package org.apache.kylin.engine.mr.common;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;

/**
 */
public class HadoopStatusGetter {

    private final String mrJobId;
    private final String yarnUrl;

    protected static final Logger logger = LoggerFactory.getLogger(HadoopStatusChecker.class);

    public HadoopStatusGetter(String yarnUrl, String mrJobId) {
        this.yarnUrl = yarnUrl;
        this.mrJobId = mrJobId;
    }

    public Pair<RMAppState, FinalApplicationStatus> get() throws IOException {
        String applicationId = mrJobId.replace("job", "application");
        String url = yarnUrl.replace("${job_id}", applicationId);
        JsonNode root = new ObjectMapper().readTree(getHttpResponse(url));
        RMAppState state = RMAppState.valueOf(root.findValue("state").getTextValue());
        FinalApplicationStatus finalStatus = FinalApplicationStatus.valueOf(root.findValue("finalStatus").getTextValue());
        return Pair.of(state, finalStatus);
    }

    private String getHttpResponse(String url) throws IOException {
        HttpClient client = new HttpClient();

        String response = null;
        while (response == null) { // follow redirects via 'refresh'
            if (url.startsWith("https://")) {
                registerEasyHttps();
            }
            if (url.contains("anonymous=true") == false) {
                url += url.contains("?") ? "&" : "?";
                url += "anonymous=true";
            }

            HttpMethod get = new GetMethod(url);
            get.addRequestHeader("accept", "application/json");

            try {
                client.executeMethod(get);

                String redirect = null;
                Header h = get.getResponseHeader("Location");
                if (h != null) {
                    redirect = h.getValue();
                    if (isValidURL(redirect) == false) {
                        logger.info("Get invalid redirect url, skip it: " + redirect);
                        Thread.sleep(1000l);
                        continue;
                    }
                } else {
                    h = get.getResponseHeader("Refresh");
                    if (h != null) {
                        String s = h.getValue();
                        int cut = s.indexOf("url=");
                        if (cut >= 0) {
                            redirect = s.substring(cut + 4);

                            if (isValidURL(redirect) == false) {
                                logger.info("Get invalid redirect url, skip it: " + redirect);
                                Thread.sleep(1000l);
                                continue;
                            }
                        }
                    }
                }

                if (redirect == null) {
                    response = get.getResponseBodyAsString();
                    logger.debug("Job " + mrJobId + " get status check result.\n");
                } else {
                    url = redirect;
                    logger.debug("Job " + mrJobId + " check redirect url " + url + ".\n");
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            } finally {
                get.releaseConnection();
            }
        }

        return response;
    }

    private static Protocol EASY_HTTPS = null;

    private static void registerEasyHttps() {
        // by pass all https issue
        if (EASY_HTTPS == null) {
            EASY_HTTPS = new Protocol("https", (ProtocolSocketFactory) new DefaultSslProtocolSocketFactory(), 443);
            Protocol.registerProtocol("https", EASY_HTTPS);
        }
    }

    private static boolean isValidURL(String value) {
        if (StringUtils.isNotEmpty(value)) {
            java.net.URL url;
            try {
                url = new java.net.URL(value);
            } catch (MalformedURLException var5) {
                return false;
            }

            return StringUtils.isNotEmpty(url.getProtocol()) && StringUtils.isNotEmpty(url.getHost());
        }

        return false;
    }
    
}
