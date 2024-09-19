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

package org.apache.kylin.tool.restclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 */
public class RestClient {

    private static final Logger logger = LoggerFactory.getLogger(RestClient.class);

    private static final Pattern FULL_REST_PATTERN = Pattern.compile("(?:([^:]+)[:]([^@]+)[@])?([^:]+)(?:[:](\\d+))?");

    private static final int HTTP_CONNECTION_TIMEOUT_MS = 30000;
    private static final int HTTP_SOCKET_TIMEOUT_MS = 120000;
    public static final String ROUTED = "routed";
    private static final String COOKIE = "Cookie";
    private static final String AUTHORIZATION = "Authorization";

    private static final String SCHEME_HTTP = "http://";
    private static final String KYLIN_API_PATH = "/kylin/api";

    public static boolean matchFullRestPattern(String uri) {
        return FULL_REST_PATTERN.matcher(uri).matches();
    }

    // ============================================================================

    protected String host;
    protected int port;
    protected String baseUrl;
    protected String userName;
    protected String password;
    protected DefaultHttpClient client;

    /**
     * @param uri "user:pwd@host:port"
     */
    public RestClient(String uri) {
        Matcher m = FULL_REST_PATTERN.matcher(uri);
        if (!m.matches())
            throw new IllegalArgumentException("URI: " + uri + " -- does not match pattern " + FULL_REST_PATTERN);

        String mUser = m.group(1);
        String mPwd = m.group(2);
        String mHost = m.group(3);
        String mPortStr = m.group(4);
        int mPort = Integer.parseInt(mPortStr == null ? "7070" : mPortStr);

        init(mHost, mPort, mUser, mPwd);
    }

    public RestClient(String host, int port, String userName, String password) {
        init(host, port, userName, password);
    }

    private void init(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.baseUrl = SCHEME_HTTP + host + ":" + port + KYLIN_API_PATH;

        final HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setSoTimeout(httpParams, HTTP_SOCKET_TIMEOUT_MS);
        HttpConnectionParams.setConnectionTimeout(httpParams, HTTP_CONNECTION_TIMEOUT_MS);

        final PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        cm.setDefaultMaxPerRoute(config.getRestClientDefaultMaxPerRoute());
        cm.setMaxTotal(config.getRestClientMaxTotal());

        client = new DefaultHttpClient(cm, httpParams);

        if (userName != null && password != null) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            client.setCredentialsProvider(provider);
        }
    }

    public RestClient resetBaseUrlWithoutKylin() {
        this.baseUrl = SCHEME_HTTP + host + ":" + port;
        return this;
    }

    public HttpResponse query(String sql, String project) throws IOException {
        String url = baseUrl + "/query";
        HttpPost post = newPost(url);
        HashMap<String, String> paraMap = new HashMap<>();
        paraMap.put("sql", sql);
        paraMap.put("project", project);
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        post.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        return client.execute(post);
    }

    public HttpResponse updateUser(Object object) throws IOException {
        String url = baseUrl + "/user/update_user";
        HttpPost post = newPost(url);
        post.addHeader(ROUTED, "true");
        String jsonMsg = JsonUtil.writeValueAsIndentString(object);
        post.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = null;
        try {
            response = client.execute(post);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                logger.error("Invalid response {} with update user {}\n{}", response.getStatusLine().getStatusCode(),
                        url, msg);
            }
        } finally {
            cleanup(post, response);
            tryCatchUp();
        }
        return response;
    }

    public HttpResponse updateSourceUsage() throws IOException {
        String url = baseUrl + "/broadcast/capacity/refresh_all";
        HttpPut put = newPut(url);
        put.addHeader(ROUTED, "true");
        HttpResponse response = null;
        try {
            response = client.execute(put);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                logger.error("Invalid response: {} for refresh capacity: {} \n{}",
                        response.getStatusLine().getStatusCode(), url, msg);
            }
        } finally {
            cleanup(put, response);
        }
        return response;
    }

    public HttpResponse notify(BroadcastEventReadyNotifier notifier) throws IOException {
        String url = baseUrl + "/broadcast";
        HttpPost post = newPost(url);
        post.addHeader(ROUTED, "true");
        HttpResponse response = null;
        try {
            post.setEntity(new ByteArrayEntity(JsonUtil.writeValueAsBytes(notifier), ContentType.APPLICATION_JSON));
            response = client.execute(post);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new KylinException(CommonErrorCode.FAILED_NOTIFY_CATCHUP, "Invalid response "
                        + response.getStatusLine().getStatusCode() + " with notify catch up url " + url + "\n" + msg);
            }
        } finally {
            cleanup(post, response);
        }
        return response;
    }

    public HttpResponse forwardGet(HttpHeaders headers, String targetUrl, boolean autoClean) throws IOException {
        String url = baseUrl + targetUrl;
        HttpGet get = newGet(url);
        get.addHeader(ROUTED, "true");
        get.addHeader(AUTHORIZATION, headers.getFirst(AUTHORIZATION));
        get.addHeader(COOKIE, headers.getFirst(COOKIE));
        HttpResponse response = null;
        try {
            response = client.execute(get);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new KylinException(CommonErrorCode.FAILED_FORWARD_METADATA_ACTION,
                        response.getStatusLine().getStatusCode() + "\n" + url + "\n" + msg);
            }
        } finally {
            if (autoClean) {
                cleanup(get, response);
            }
        }
        return response;
    }

    public HttpResponse forwardPut(byte[] requestEntity, HttpHeaders headers, String targetUrl, boolean autoClean)
            throws IOException {
        String url = baseUrl + targetUrl;
        HttpPut put = newPut(url);
        put.addHeader(ROUTED, "true");
        put.addHeader(AUTHORIZATION, headers.getFirst(AUTHORIZATION));
        put.addHeader(COOKIE, headers.getFirst(COOKIE));
        HttpResponse response = null;
        try {
            put.setEntity(new ByteArrayEntity(requestEntity, ContentType.APPLICATION_JSON));
            response = client.execute(put);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new KylinException(CommonErrorCode.FAILED_FORWARD_METADATA_ACTION, "Invalid response "
                        + response.getStatusLine().getStatusCode() + " with url " + url + "\n" + msg);
            }
        } finally {
            if (autoClean) {
                cleanup(put, response);
            }
        }
        return response;
    }

    public HttpResponse forwardPostWithUrlEncodedForm(String targetUrl, HttpHeaders headers, Map<String, String> form)
            throws IOException {
        String url = baseUrl + targetUrl;
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("Content-type", "application/x-www-form-urlencoded; charset=utf-8");
        httpPost.addHeader(ROUTED, "true");
        if (null != headers) {
            httpPost.addHeader(AUTHORIZATION, headers.getFirst(AUTHORIZATION));
            httpPost.addHeader(COOKIE, headers.getFirst(COOKIE));
        }
        List<NameValuePair> nameValuePairs = new ArrayList<>();
        if (null != form) {
            form.entrySet()
                    .forEach(entry -> nameValuePairs.add(new BasicNameValuePair(entry.getKey(), entry.getValue())));
        }
        HttpResponse response = null;
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs, "UTF-8"));
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new KylinException(CommonErrorCode.FAILED_FORWARD_METADATA_ACTION, "Invalid response "
                        + response.getStatusLine().getStatusCode() + " with url " + url + "\n" + msg);
            }
        } finally {
            cleanup(httpPost, response);
        }
        return response;
    }

    private void addHttpHeaders(HttpRequestBase method) {
        method.addHeader("Accept", "application/json, text/plain, */*");
        method.addHeader("Content-Type", "application/json");
    }

    private HttpGet newGet(String url) {
        HttpGet get = new HttpGet(url);
        addHttpHeaders(get);
        return get;
    }

    private HttpPost newPost(String url) {
        HttpPost post = new HttpPost(url);
        addHttpHeaders(post);
        return post;
    }

    private HttpPut newPut(String url) {
        HttpPut put = new HttpPut(url);
        addHttpHeaders(put);
        return put;
    }

    private String getContent(HttpResponse response) throws IOException {
        StringBuilder result = new StringBuilder();
        try (InputStreamReader reader = new InputStreamReader(response.getEntity().getContent(),
                Charset.defaultCharset()); BufferedReader rd = new BufferedReader(reader)) {
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        }
        return result.toString();
    }

    private void cleanup(HttpRequestBase request, HttpResponse response) {
        try {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        } catch (Exception ex) {
            logger.error("Error during HTTP connection cleanup", ex);
        }
        request.releaseConnection();
    }

    public <T> T getKapHealthStatus(TypeReference<T> clz, byte[] encryptedToken)
            throws IOException, URISyntaxException {
        String url = baseUrl + "/kg/health/instance_info";

        HttpPost httpPost = new HttpPost(url);
        httpPost.setEntity(new ByteArrayEntity(encryptedToken));
        HttpResponse response = null;
        try {
            httpPost.setURI(new URI(url));
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with health status url " + url + "\n" + msg);
            }

            return JsonUtil.readValue(getContent(response), clz);
        } finally {
            cleanup(httpPost, response);
        }
    }

    public void downOrUpGradeKE(String status, byte[] encryptedToken) throws IOException, URISyntaxException {
        String url = baseUrl + "/kg/health/instance_service/" + status;

        HttpPost httpPost = new HttpPost(url);
        httpPost.setEntity(new ByteArrayEntity(encryptedToken));
        HttpResponse response = null;
        try {
            httpPost.setURI(new URI(url));
            response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with downOrUpGradeKE url " + url + "\n" + msg);
            }
        } finally {
            cleanup(httpPost, response);
        }
    }

    public boolean updateDiagProgress(String diagId, String stage, float progress, long updateTime) {
        String url = baseUrl + "/system/diag/progress";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            HashMap<String, Object> paraMap = Maps.newHashMap();
            paraMap.put("diag_id", diagId);
            paraMap.put("stage", stage);
            paraMap.put("progress", progress);
            paraMap.put("updateTime", updateTime);
            put.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(paraMap), "UTF-8"));
            response = client.execute(put);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                logger.warn("Invalid response {} with updateDiagProgress url: {}\n{}",
                        response.getStatusLine().getStatusCode(), url, msg);
                return false;
            }
        } catch (Exception e) {
            logger.warn("Error during update diag progress", e);
        } finally {
            cleanup(put, response);
        }
        return true;
    }

    public boolean rollUpEventLog() {
        String url = baseUrl + "/system/roll_event_log";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            response = client.execute(put);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                String msg = EntityUtils.toString(response.getEntity());
                logger.warn("Invalid response {}  rollup event_log url: {}\n{}",
                        response.getStatusLine().getStatusCode(), url, msg);
                return false;
            }
        } catch (Exception e) {
            logger.warn("Error during get rollup event_log");
        } finally {
            cleanup(put, response);
        }
        return true;
    }

    private void tryCatchUp() {
        try {
            ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            store.getAuditLogStore().catchupWithTimeout();
        } catch (Exception e) {
            logger.error("Failed to catchup manually.", e);
        }
    }

}
