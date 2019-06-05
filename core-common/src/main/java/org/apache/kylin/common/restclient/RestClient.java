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

package org.apache.kylin.common.restclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kylin.shaded.com.google.common.base.Strings;

/**
 */
public class RestClient {

    private static final Logger logger = LoggerFactory.getLogger(RestClient.class);
    protected static final String UTF_8 = "UTF-8";
    private static final String APPLICATION_JSON = "application/json";
    protected static final String INVALID_RESPONSE = "Invalid response ";
    protected static final String CUBES = "/cubes/";
    private static final String WITH_URL = " with url ";

    protected static Pattern fullRestPattern = Pattern.compile("(?:([^:]+)[:]([^@]+)[@])?([^:]+)(?:[:](\\d+))?");

    private int httpConnectionTimeoutMs = 30000;
    private int httpSocketTimeoutMs = 120000;

    public static final String SCHEME_HTTP = "http://";

    public static final String KYLIN_API_PATH = "/kylin/api";

    public static boolean matchFullRestPattern(String uri) {
        Matcher m = fullRestPattern.matcher(uri);
        return m.matches();
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
        this(uri, null, null);
    }

    public RestClient(String uri, Integer httpConnectionTimeoutMs, Integer httpSocketTimeoutMs) {
        Matcher m = fullRestPattern.matcher(uri);
        if (!m.matches())
            throw new IllegalArgumentException("URI: " + uri.replaceAll(":.+@", ":*****@") + " -- does not match pattern " + fullRestPattern);

        String user = m.group(1);
        String pwd = m.group(2);
        String host = m.group(3);
        String portStr = m.group(4);
        int port = Integer.parseInt(portStr == null ? "7070" : portStr);

        if (httpConnectionTimeoutMs != null)
            this.httpConnectionTimeoutMs = httpConnectionTimeoutMs;
        if (httpSocketTimeoutMs != null)
            this.httpSocketTimeoutMs = httpSocketTimeoutMs;

        init(host, port, user, pwd);
    }

    public RestClient(String host, int port, String userName, String password) {
        this(host, port, userName, password, null, null);
    }

    public RestClient(String host, int port, String userName, String password, Integer httpConnectionTimeoutMs,
            Integer httpSocketTimeoutMs) {
        if (httpConnectionTimeoutMs != null)
            this.httpConnectionTimeoutMs = httpConnectionTimeoutMs;
        if (httpSocketTimeoutMs != null)
            this.httpSocketTimeoutMs = httpSocketTimeoutMs;

        init(host, port, userName, password);
    }

    private void init(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.baseUrl = SCHEME_HTTP + host + ":" + port + KYLIN_API_PATH;

        final HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setSoTimeout(httpParams, httpSocketTimeoutMs);
        HttpConnectionParams.setConnectionTimeout(httpParams, httpConnectionTimeoutMs);

        final PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
        KylinConfig config = KylinConfig.getInstanceFromEnv(true);
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

    // return pair {serverAddress, isActiveJobNode}
    public Pair<String, String> getJobServerWithState() throws IOException {
        String url = baseUrl + "/service_discovery/state/is_active_job_node";
        HttpGet get = new HttpGet(url);
        HttpResponse response = null;
        try {
            response = client.execute(get);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode()
                        + " with getting job server state  " + url + "\n" + msg);
            }
            return Pair.newPair(host + ":" + port, msg);
        } finally {
            cleanup(get, response);
        }
    }

    public void announceWipeCache(String entity, String event, String cacheKey) throws IOException {
        String url = baseUrl + "/cache/announce/" + entity + "/" + cacheKey + "/" + event;
        HttpPut request = new HttpPut(url);

        try {
            HttpResponse response = client.execute(request);

            if (response.getStatusLine().getStatusCode() != 200) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with announce cache wipe url " + url + "\n" + msg);
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            request.releaseConnection();
        }
    }
    
    public void wipeCache(String entity, String event, String cacheKey) throws IOException {
        HttpPut request;
        String url;
        if (cacheKey.contains("/")) {
            url = baseUrl + "/cache/" + entity + "/" + event;
            request = new HttpPut(url);
            request.setEntity(new StringEntity(cacheKey, ContentType.create(APPLICATION_JSON, UTF_8)));
        } else {
            url = baseUrl + "/cache/" + entity + "/" + cacheKey + "/" + event;
            request = new HttpPut(url);
        }

        HttpResponse response = null;
        try {
            response = client.execute(request);

            if (response.getStatusLine().getStatusCode() != 200) {
                String msg = EntityUtils.toString(response.getEntity());
                throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode()
                        + " with cache wipe url " + url + "\n" + msg);
            }
        } finally {
            cleanup(request, response);
        }
    }

    public String getKylinProperties() throws IOException {
        return getConfiguration(baseUrl + "/admin/config", true);
    }

    public String getHDFSConfiguration() throws IOException {
        return getConfiguration(baseUrl + "/admin/config/hdfs", true);
    }

    public String getHBaseConfiguration() throws IOException {
        return getConfiguration(baseUrl + "/admin/config/hbase", true);
    }

    private String getConfiguration(String url, boolean ifAuth) throws IOException {
        HttpGet request = ifAuth ? newGet(url) : new HttpGet(url);
        HttpResponse response = null;
        try {
            response = client.execute(request);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode()
                        + " with cache wipe url " + url + "\n" + msg);

            Map<String, String> map = JsonUtil.readValueAsMap(msg);
            msg = map.get("config");
            return msg;
        } finally {
            cleanup(request, response);
        }
    }

    public boolean enableCache() throws IOException {
        return setCache(true);
    }

    public boolean disableCache() throws IOException {
        return setCache(false);
    }

    public boolean buildCube(String cubeName, long startTime, long endTime, String buildType) throws Exception {
        String url = baseUrl + CUBES + cubeName + "/build";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            HashMap<String, String> paraMap = new HashMap<String, String>();
            paraMap.put("startTime", startTime + "");
            paraMap.put("endTime", endTime + "");
            paraMap.put("buildType", buildType);
            String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
            put.setEntity(new StringEntity(jsonMsg, UTF_8));
            response = client.execute(put);
            getContent(response);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode()
                        + " with build cube url " + url + "\n" + jsonMsg);
            } else {
                return true;
            }
        } finally {
            cleanup(put, response);
        }
    }

    public boolean disableCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + CUBES + cubeName + "/disable");
    }

    public boolean enableCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + CUBES + cubeName + "/enable");
    }

    public boolean purgeCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + CUBES + cubeName + "/purge");
    }

    public HashMap getCube(String cubeName) throws Exception {
        String url = baseUrl + CUBES + cubeName;
        HttpGet get = newGet(url);
        HttpResponse response = null;
        try {
            get.setURI(new URI(url));
            response = client.execute(get);
            return dealResponse(response);
        } finally {
            cleanup(get, response);
        }
    }

    private boolean changeCubeStatus(String url) throws Exception {
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            HashMap<String, String> paraMap = new HashMap<String, String>();
            String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
            put.setEntity(new StringEntity(jsonMsg, UTF_8));
            response = client.execute(put);
            getContent(response);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(
                        INVALID_RESPONSE + response.getStatusLine().getStatusCode() + WITH_URL + url + "\n" + jsonMsg);
            } else {
                return true;
            }
        } finally {
            cleanup(put, response);
        }
    }

    public HttpResponse query(String sql, String project) throws IOException {
        String url = baseUrl + "/query";
        HttpPost post = newPost(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("sql", sql);
        paraMap.put("project", project);
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        post.setEntity(new StringEntity(jsonMsg, UTF_8));
        HttpResponse response = client.execute(post);
        return response;
    }

    public void clearCacheForCubeMigration(String cube, String project, String model,
            Map<String, String> tableToProjects) throws IOException {
        String url = baseUrl + "/cache/migration";
        HttpPost post = new HttpPost(url);

        post.addHeader("Accept", "application/json, text/plain, */*");
        post.addHeader("Content-Type", APPLICATION_JSON);

        HashMap<String, Object> paraMap = new HashMap<String, Object>();
        paraMap.put("cube", cube);
        paraMap.put("project", project);
        paraMap.put("model", model);
        paraMap.put("tableToProjects", tableToProjects);
        String jsonMsg = JsonUtil.writeValueAsString(paraMap);
        post.setEntity(new StringEntity(jsonMsg, UTF_8));
        HttpResponse response = client.execute(post);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode());
        }
    }

    public void buildLookupSnapshotCache(String project, String lookupTableName, String snapshotID) throws IOException {
        String url = baseUrl + "/tables/" + project + "/" + lookupTableName + "/" + snapshotID + "/snapshotLocalCache";
        HttpPut put = new HttpPut(url);
        HttpResponse response = client.execute(put);
        getContent(response);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode() + WITH_URL + url + "\n");
        }
    }

    public String getLookupSnapshotCacheState(String lookupTableName, String snapshotID) throws IOException {
        String url = baseUrl + "/tables/" + lookupTableName + "/" + snapshotID + "/snapshotLocalCache/state";
        HttpGet get = new HttpGet(url);
        HttpResponse response = client.execute(get);
        String content = getContent(response);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode() + WITH_URL + url + "\n");
        }
        return content;
    }

    public void checkCompatibility(String jsonRequest) throws IOException {
        checkCompatibility(jsonRequest, false);
    }

    public void checkCompatibility(String jsonRequest, boolean ifHiveCheck) throws IOException {
        if (ifHiveCheck) {
            checkCompatibility(jsonRequest, baseUrl + "/cubes/checkCompatibility/hiveTable");
        }
        checkCompatibility(jsonRequest, baseUrl + "/cubes/checkCompatibility");
    }

    private void checkCompatibility(String jsonRequest, String url) throws IOException {
        HttpPost post = newPost(url);
        try {
            post.setEntity(new StringEntity(jsonRequest, "UTF-8"));
            HttpResponse response = client.execute(post);
            if (response.getStatusLine().getStatusCode() != 200) {
                String msg = getContent(response);
                Map<String, String> kvMap = JsonUtil.readValueAsMap(msg);
                String exception = kvMap.containsKey("exception") ? kvMap.get("exception") : "unknown";
                throw new IOException("Error code: " + response.getStatusLine().getStatusCode() + "\n" + exception);
            }
        } finally {
            post.releaseConnection();
        }
    }
    
    private HashMap dealResponse(HttpResponse response) throws IOException {
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException(INVALID_RESPONSE + response.getStatusLine().getStatusCode());
        }
        String result = getContent(response);
        HashMap resultMap = new ObjectMapper().readValue(result, HashMap.class);
        return resultMap;
    }

    private void addHttpHeaders(HttpRequestBase method) {
        method.addHeader("Accept", "application/json, text/plain, */*");
        method.addHeader("Content-Type", APPLICATION_JSON);
        if (!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
            String basicAuth = DatatypeConverter
                    .printBase64Binary((this.userName + ":" + this.password).getBytes(StandardCharsets.UTF_8));
            method.addHeader("Authorization", "Basic " + basicAuth);
        }
    }

    private HttpPost newPost(String url) {
        HttpPost post = new HttpPost(url);
        addHttpHeaders(post);
        return post;
    }

    protected HttpPut newPut(String url) {
        HttpPut put = new HttpPut(url);
        addHttpHeaders(put);
        return put;
    }

    protected HttpGet newGet(String url) {
        HttpGet get = new HttpGet(url);
        addHttpHeaders(get);
        return get;
    }

    private boolean setCache(boolean flag) throws IOException {
        String url = baseUrl + "/admin/config";
        HttpPut put = newPut(url);
        HttpResponse response = null;
        try {
            HashMap<String, String> paraMap = new HashMap<String, String>();
            paraMap.put("key", "kylin.query.cache-enabled");
            paraMap.put("value", flag + "");
            put.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(paraMap), UTF_8));
            response = client.execute(put);
            EntityUtils.consume(response.getEntity());
            if (response.getStatusLine().getStatusCode() != 200) {
                return false;
            } else {
                return true;
            }
        } finally {
            cleanup(put, response);
        }
    }

    protected String getContent(HttpResponse response) throws IOException {
        StringBuffer result = new StringBuffer();
        try (BufferedReader rd = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        }

        return result.toString();
    }

    protected void cleanup(HttpRequestBase request, HttpResponse response) {
        try {
            if (response != null)
                EntityUtils.consume(response.getEntity());
        } catch (Exception ex) {
            logger.error("Error during HTTP connection cleanup", ex);
        }
        request.releaseConnection();
    }

}
