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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.util.JsonUtil;

import javax.xml.bind.DatatypeConverter;

/**
 * @author yangli9
 */
public class RestClient {

    protected String host;
    protected int port;
    protected String baseUrl;
    protected String userName;
    protected String password;
    protected DefaultHttpClient client;

    protected static Pattern fullRestPattern = Pattern.compile("(?:([^:]+)[:]([^@]+)[@])?([^:]+)(?:[:](\\d+))?");

    public static boolean matchFullRestPattern(String uri) {
        Matcher m = fullRestPattern.matcher(uri);
        return m.matches();
    }

    /**
     * @param uri
     *            "user:pwd@host:port"
     */
    public RestClient(String uri) {
        Matcher m = fullRestPattern.matcher(uri);
        if (!m.matches())
            throw new IllegalArgumentException("URI: " + uri + " -- does not match pattern " + fullRestPattern);

        String user = m.group(1);
        String pwd = m.group(2);
        String host = m.group(3);
        String portStr = m.group(4);
        int port = Integer.parseInt(portStr == null ? "7070" : portStr);

        init(host, port, user, pwd);
    }

    public RestClient(String host, int port, String userName, String password) {
        init(host, port, userName, password);
    }

    private void init(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.baseUrl = "http://" + host + ":" + port + "/kylin/api";

        client = new DefaultHttpClient();

        if (userName != null && password != null) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            client.setCredentialsProvider(provider);
        }
    }

    public void wipeCache(String entity, String event, String cacheKey) throws IOException {
        String url = baseUrl + "/cache/" + entity + "/" + cacheKey + "/" + event;
        HttpPut request = new HttpPut(url);

        try {
            HttpResponse response = client.execute(request);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with cache wipe url " + url + "\n" + msg);
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            request.releaseConnection();
        }
    }

    public String getKylinProperties() throws IOException {
        String url = baseUrl + "/admin/config";
        HttpGet request = new HttpGet(url);
        try {
            HttpResponse response = client.execute(request);
            String msg = EntityUtils.toString(response.getEntity());
            Map<String, String> map = JsonUtil.readValueAsMap(msg);
            msg = map.get("config");

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with cache wipe url " + url + "\n" + msg);
            return msg;
        } finally {
            request.releaseConnection();
        }
    }

    public boolean enableCache() throws IOException {
        return setCache(true);
    }

    public boolean disableCache() throws IOException {
        return setCache(false);
    }

    public boolean buildCube(String cubeName, long startTime, long endTime, String buildType) throws Exception {
        String url = baseUrl + "/cubes/" + cubeName + "/build";
        HttpPut put = newPut(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("startTime", startTime + "");
        paraMap.put("endTime", endTime + "");
        paraMap.put("buildType", buildType);
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        put.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = client.execute(put);
        String result = getContent(response);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with build cube url " + url + "\n" + jsonMsg);
        } else {
            return true;
        }
    }

    public boolean disableCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + "/cubes/" + cubeName + "/disable");
    }

    public boolean enableCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + "/cubes/" + cubeName + "/enable");
    }

    public boolean purgeCube(String cubeName) throws Exception {
        return changeCubeStatus(baseUrl + "/cubes/" + cubeName + "/purge");
    }

    public HashMap getCube(String cubeName) throws Exception {
        String url = baseUrl + "/cubes/" + cubeName;
        HttpGet get = newGet(url);
        get.setURI(new URI(url));
        HttpResponse response = client.execute(get);
        return dealResponse(response);
    }

    private boolean changeCubeStatus(String url) throws Exception {
        HttpPut put = newPut(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        put.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = client.execute(put);
        String result = getContent(response);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with url " + url + "\n" + jsonMsg);
        } else {
            return true;
        }
    }

    public HttpResponse query(String sql, String project) throws IOException {
        String url = baseUrl + "/query";
        HttpPost post = newPost(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("sql", sql);
        paraMap.put("project", project);
        String jsonMsg = new ObjectMapper().writeValueAsString(paraMap);
        post.setEntity(new StringEntity(jsonMsg, "UTF-8"));
        HttpResponse response = client.execute(post);
        return response;
    }

    private HashMap dealResponse(HttpResponse response) throws IOException {
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode());
        }
        String result = getContent(response);
        HashMap resultMap = new ObjectMapper().readValue(result, HashMap.class);
        return resultMap;
    }

    private void addHttpHeaders(HttpRequestBase method) {
        method.addHeader("Accept", "application/json, text/plain, */*");
        method.addHeader("Content-Type", "application/json");
        String basicAuth = DatatypeConverter.printBase64Binary((this.userName + ":" + this.password).getBytes());
        method.addHeader("Authorization", "Basic " + basicAuth);
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

    private HttpGet newGet(String url) {
        HttpGet get = new HttpGet();
        addHttpHeaders(get);
        return get;
    }

    private boolean setCache(boolean flag) throws IOException {
        String url = baseUrl + "/admin/config";
        HttpPut put = newPut(url);
        HashMap<String, String> paraMap = new HashMap<String, String>();
        paraMap.put("key", "kylin.query.cache-enabled");
        paraMap.put("value", flag + "");
        put.setEntity(new StringEntity(new ObjectMapper().writeValueAsString(paraMap), "UTF-8"));
        HttpResponse response = client.execute(put);
        EntityUtils.consume(response.getEntity());
        if (response.getStatusLine().getStatusCode() != 200) {
            return false;
        } else {
            return true;
        }
    }

    private String getContent(HttpResponse response) throws IOException {
        InputStreamReader reader = null;
        BufferedReader rd = null;
        StringBuffer result = new StringBuffer();
        try{
            reader = new InputStreamReader(response.getEntity().getContent());
            rd = new BufferedReader(reader);
            String line = null;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        }finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(rd);
        }
        return result.toString();
    }

}
