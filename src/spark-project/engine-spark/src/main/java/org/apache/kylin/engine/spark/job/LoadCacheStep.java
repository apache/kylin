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
import static org.apache.kylin.common.msg.Message.LOAD_GLUTEN_CACHE_ROUTE_ERROR;
import static org.apache.kylin.common.msg.Message.LOAD_GLUTEN_CACHE_ROUTE_EXECUTE_ERROR;
import static org.apache.kylin.common.msg.Message.LOAD_GLUTEN_CACHE_ROUTE_RESPONSE_EMPTY;

import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.rest.response.EnvelopeResponse;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class LoadCacheStep extends AbstractExecutable {
    private static final String CACHE_API = "/kylin/api/jobs/gluten_cache";

    protected LoadCacheStep() {
        this.setName(ExecutableConstants.LOAD_GLUTEN_CACHE);
    }

    protected LoadCacheStep(Object notSetId) {
        super(notSetId);
    }

    public void routeCacheToAllQueryNode(String project, Set<String> cacheCommand) throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (config.isUTEnv()) {
            return;
        }
        val localInstance = AddressUtil.getLocalInstance();
        val requestApi = String.format(Locale.ROOT, "http://%s%s", localInstance, CACHE_API);

        val request = Maps.<String, Object> newHashMap();
        request.put("project", project);
        request.put("cache_commands", cacheCommand);
        byte[] requestEntity = JsonUtil.writeValueAsBytes(request);

        val timeout = config.getGlutenCacheRequestTimeout();
        val defaultRequestConfig = RequestConfig.custom().setSocketTimeout(timeout).setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout).setStaleConnectionCheckEnabled(true).build();
        val httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();

        val httpPost = new HttpPost(requestApi);
        httpPost.addHeader(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_JSON);
        httpPost.setEntity(new ByteArrayEntity(requestEntity, ContentType.APPLICATION_JSON));

        val httpResponse = httpClient.execute(httpPost);
        byte[] content = EntityUtils.toByteArray(httpResponse.getEntity());
        if (content != null) {
            val response = JsonUtil.readValue(content, EnvelopeResponse.class);
            if (!StringUtils.equals(response.getCode(), KylinException.CODE_SUCCESS)) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, LOAD_GLUTEN_CACHE_ROUTE_ERROR, response.getMsg()));
            }
            val result = (boolean) response.getData();
            if (result) {
                return;
            }
            throw new KylinRuntimeException(LOAD_GLUTEN_CACHE_ROUTE_EXECUTE_ERROR);
        }
        throw new KylinRuntimeException(LOAD_GLUTEN_CACHE_ROUTE_RESPONSE_EMPTY);
    }
}
