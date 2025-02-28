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

package org.apache.kylin.engine.spark.job;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.exception.KylinException.CODE_SUCCESS;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestfulJobProgressReport implements IJobProgressReport {

    private static final Logger logger = LoggerFactory.getLogger(RestfulJobProgressReport.class);
    public static final String JOB_HAS_STOPPED = "Job has stopped";

    /**
     * http request the spark job controller
     *
     * @param json The payload json string.
     */
    @Override
    public synchronized boolean updateSparkJobInfo(Map<String, String> params, String url, String json) {
        String serverAddress = System.getProperty("spark.driver.rest.server.address", "127.0.0.1:7070");
        String requestApi = String.format(Locale.ROOT, "http://%s%s", serverAddress, url);
        int timeOut = Integer.parseInt(params.get(ParamsConstants.TIME_OUT));
        try {
            RequestConfig defaultRequestConfig = RequestConfig.custom().setSocketTimeout(timeOut)
                    .setConnectTimeout(timeOut).setConnectionRequestTimeout(timeOut)
                    .setStaleConnectionCheckEnabled(true).build();
            CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();
            HttpPut httpPut = new HttpPut(requestApi);
            httpPut.addHeader(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_JSON);
            httpPut.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

            HttpResponse response = httpClient.execute(httpPut);
            int code = response.getStatusLine().getStatusCode();
            InputStream inputStream = response.getEntity().getContent();
            @SuppressWarnings("rawtypes")
            HashMap kylinResponse = JsonUtil.readValue(inputStream, HashMap.class);
            if (code == HttpStatus.SC_OK && kylinResponse.get("code").equals(CODE_SUCCESS)) {
                return true;
            } else {
                logger.warn("update spark job failed, info: {}", kylinResponse);
                if (kylinResponse.get("msg").toString().startsWith(JOB_HAS_STOPPED)) {
                    throw new IllegalStateException(JOB_HAS_STOPPED);
                }
            }
        } catch (Exception e) {
            if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
                logger.error("http request {} failed!", requestApi, e);
            }
            if (e instanceof IllegalStateException && e.getMessage().equals(JOB_HAS_STOPPED)) {
                throw (IllegalStateException) e;
            }
        }
        return false;
    }

    /**
     * when
     * update spark job extra info, link yarn_application_tracking_url & yarn_application_id
     */
    @Override
    public boolean updateSparkJobExtraInfo(Map<String, String> params, String url, String project, String jobId,
            Map<String, String> extraInfo) {
        Map<String, String> payload = new HashMap<>(5);
        payload.put("project", project);
        payload.put("job_id", jobId);
        payload.put("task_id", System.getProperty("spark.driver.param.taskId", jobId));
        payload.putAll(extraInfo);

        try {
            String payloadJson = JsonUtil.writeValueAsString(payload);
            int retry = 3;
            for (int i = 0; i < retry; i++) {
                if (updateSparkJobInfo(params, url, payloadJson)) {
                    return Boolean.TRUE;
                }
                Thread.sleep(3000);
                logger.warn("retry request rest api update spark extra job info");
            }
        } catch (InterruptedException exception) {
            logger.error("update spark job extra info failed!", exception);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("update spark job extra info failed!", e);
            if (e instanceof IllegalStateException && e.getMessage().equals(JOB_HAS_STOPPED)) {
                throw (IllegalStateException) e;
            }
        }

        return Boolean.FALSE;
    }

}
