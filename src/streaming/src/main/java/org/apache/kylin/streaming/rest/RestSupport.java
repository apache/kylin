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
package org.apache.kylin.streaming.rest;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.io.Closeable;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.val;
import lombok.var;

public class RestSupport implements Closeable {
    protected static final Logger logger = LoggerFactory.getLogger(RestSupport.class);
    private static int MAX_RETRY = 3;
    private static long RETRY_INTERVAL = 10000;
    protected String baseUrl;
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
    CloseableHttpClient httpClient;

    public RestSupport(String baseUrl) {
        this.baseUrl = baseUrl;
        httpClient = httpClientBuilder.build();
    }

    public RestSupport(KylinConfig config) {
        String restServer = System.getProperty(StreamingConstants.REST_SERVER_IP);
        if (StringUtils.isEmpty(restServer)) {
            restServer = "127.0.0.1";
        }
        String restPort = config.getServerPort();
        this.baseUrl = "http://" + restServer + ":" + restPort + "/kylin/api";

        httpClientBuilder.setMaxConnPerRoute(config.getRestClientDefaultMaxPerRoute());
        httpClientBuilder.setMaxConnTotal(config.getRestClientMaxTotal());
        httpClient = httpClientBuilder.build();
    }

    public RestResponse<String> execute(HttpRequestBase httpReqBase, Object param) {
        int retry = 0;
        Exception err = null;

        while (retry++ < MAX_RETRY) {
            try {
                if (param != null && httpReqBase instanceof HttpEntityEnclosingRequestBase) {
                    ObjectMapper mapper = new ObjectMapper();
                    ((HttpEntityEnclosingRequestBase) httpReqBase)
                            .setEntity(new StringEntity(mapper.writeValueAsString(param), "UTF-8"));
                }

                HttpResponse response = httpClient.execute(httpReqBase);
                int code = response.getStatusLine().getStatusCode();

                logger.info("code={},url={}", code, httpReqBase.getURI());
                if (code == HttpStatus.SC_OK) {
                    return JsonUtil.readValue(response.getEntity().getContent(),
                            new TypeReference<RestResponse<String>>() {
                            });
                } else {
                    InputStream inputStream = response.getEntity().getContent();
                    String responseContent = IOUtils.toString(inputStream);
                    logger.error(responseContent);
                    checkMaintenceMode();
                }
                Thread.sleep(RETRY_INTERVAL * retry);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                err = e;
                logger.error(e.getMessage(), e);
                checkMaintenceMode();
            }
        }
        throw new RuntimeException(err);
    }

    public Boolean checkMaintenceMode() {
        var maintenanceMode = isMaintenanceMode();
        while (maintenanceMode) {
            try {
                if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
                    Thread.sleep((long) 60 * 1000);
                }
                maintenanceMode = isMaintenanceMode();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            } catch (Exception ex) {
                logger.error(ex.getMessage(), ex);
            }
        }
        return maintenanceMode;
    }

    public Boolean isMaintenanceMode() {
        int retry = 0;
        Exception err = null;
        String url = "/epoch/maintenance_mode";
        val httpReqBase = createHttpGet(url);
        while (retry++ < MAX_RETRY) {
            try {
                HttpResponse response = httpClient.execute(httpReqBase);
                int code = response.getStatusLine().getStatusCode();

                logger.info("code=" + code + ",url=" + httpReqBase.getURI());
                if (code == HttpStatus.SC_OK) {
                    RestResponse<String> resp = JsonUtil.readValue(response.getEntity().getContent(),
                            new TypeReference<RestResponse<String>>() {
                            });
                    return Boolean.parseBoolean(resp.getData());
                } else {
                    InputStream inputStream = response.getEntity().getContent();
                    String responseContent = IOUtils.toString(inputStream);
                    logger.error(responseContent);
                }
                Thread.sleep(RETRY_INTERVAL * retry);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                err = e;
                logger.error(e.getMessage(), e);
            }
        }
        throw new RuntimeException(err);
    }

    public void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public HttpPut createHttpPut(String url) {
        HttpPut httpPut = new HttpPut(baseUrl + url);
        addHeader(httpPut);
        return httpPut;
    }

    public HttpPost createHttpPost(String url) {
        HttpPost httpPost = new HttpPost(baseUrl + url);
        addHeader(httpPost);
        return httpPost;
    }

    public HttpGet createHttpGet(String url) {
        HttpGet httpGet = new HttpGet(baseUrl + url);
        return httpGet;
    }

    private void addHeader(HttpEntityEnclosingRequestBase httpReqBase) {
        httpReqBase.addHeader("Accept", HTTP_VND_APACHE_KYLIN_JSON);
        httpReqBase.addHeader(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_JSON);
    }
}
