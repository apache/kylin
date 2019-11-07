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

package org.apache.kylin.common.livy;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.kylin.common.KylinConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LivyRestClient {

    private int httpConnectionTimeoutMs = 30000;
    private int httpSocketTimeoutMs = 120000;

    protected String baseUrl;
    protected DefaultHttpClient client;

    final private KylinConfig config = KylinConfig.getInstanceFromEnv();

    public LivyRestClient() {
        init();
    }


    private void init() {
        final HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setSoTimeout(httpParams, httpSocketTimeoutMs);
        HttpConnectionParams.setConnectionTimeout(httpParams, httpConnectionTimeoutMs);

        PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
        cm.setDefaultMaxPerRoute(config.getRestClientDefaultMaxPerRoute());
        cm.setMaxTotal(config.getRestClientMaxTotal());

        baseUrl = config.getLivyUrl();
        client = new DefaultHttpClient(cm, httpParams);
    }

    public String livySubmitJobBatches(String jobJson) throws IOException {
        String url = baseUrl + "/batches";
        HttpPost post = newPost(url);

        // Because livy submit job use JDK's ProcessBuilder, here we need to quote backtick
        // otherwise backtick make livy throw org.apache.spark.sql.catalyst.parser.ParseException
        String json = jobJson.replace("`", config.getLivyRestApiBacktick());

        post.setEntity(new StringEntity(json, "UTF-8"));
        HttpResponse response = client.execute(post);
        String content = getContent(response);
        if (response.getStatusLine().getStatusCode() != 201) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with url " + url + "\n");
        }
        return content;
    }


    public String livyGetJobStatusBatches(String jobId) throws IOException {
        String url = baseUrl + "/batches/" + jobId;
        HttpGet get = new HttpGet(url);

        HttpResponse response = client.execute(get);
        String content = getContent(response);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with url " + url + "\n");
        }
        return content;
    }

    public String livyDeleteBatches(String jobId) throws IOException {
        String url = baseUrl + "/batches/" + jobId;
        HttpDelete delete = new HttpDelete(url);

        HttpResponse response = client.execute(delete);
        String content = getContent(response);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with url " + url + "\n");
        }
        return content;
    }

    private HttpPost newPost(String url) {
        HttpPost post = new HttpPost(url);
        addHttpHeaders(post);
        return post;
    }

    private void addHttpHeaders(HttpRequestBase method) {
        method.addHeader("Accept", "application/json, text/plain, */*");
        method.addHeader("Content-Type", "application/json");
    }

    private String getContent(HttpResponse response) throws IOException {
        InputStreamReader reader = null;
        BufferedReader rd = null;
        StringBuffer result = new StringBuffer();
        try {
            reader = new InputStreamReader(response.getEntity().getContent(), "UTF-8");
            rd = new BufferedReader(reader);
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(rd);
        }
        return result.toString();
    }
}
