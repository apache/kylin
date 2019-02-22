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

package org.apache.kylin.stream.core.util;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestService {
    private static final Logger logger = LoggerFactory.getLogger(RestService.class);

    private int connectionTimeout;
    private int readTimeout;

    public RestService(int connectionTimeout, int readTimeout) {
        this.connectionTimeout = connectionTimeout;
        this.readTimeout = readTimeout;

    }

    public String postRequest(String url, String postContent) throws IOException {
        return postRequest(url, postContent, connectionTimeout, readTimeout);
    }

    public String putRequest(String url) throws IOException {
        return putRequest(url, null);
    }

    public String putRequest(String url, String putContent) throws IOException {
        return putRequest(url, putContent, connectionTimeout, readTimeout);
    }

    public String getRequest(String url) throws IOException {
        return getRequest(url, connectionTimeout, readTimeout);
    }

    public String deleteRequest(String url) throws IOException {
        return deleteRequest(url, connectionTimeout, readTimeout);
    }

    public String postRequest(String url, String postContent, int connTimeout, int readTimeout) throws IOException {
        HttpPost request = new HttpPost(url);
        StringEntity requestEntity = new StringEntity(postContent, ContentType.APPLICATION_JSON);
        request.setEntity(requestEntity);
        return execRequest(request, connTimeout, readTimeout);
    }

    public String putRequest(String url, String putContent, int connTimeout, int readTimeout) throws IOException {
        HttpPut request = new HttpPut(url);
        if (putContent != null) {
            StringEntity requestEntity = new StringEntity(putContent, ContentType.APPLICATION_JSON);
            request.setEntity(requestEntity);
        }
        return execRequest(request, connTimeout, readTimeout);
    }

    public String getRequest(String url, int connTimeout, int readTimeout) throws IOException {
        HttpGet request = new HttpGet(url);
        return execRequest(request, connTimeout, readTimeout);
    }

    public String deleteRequest(String url, int connTimeout, int readTimeout) throws IOException {
        HttpDelete request = new HttpDelete(url);
        return execRequest(request, connTimeout, readTimeout);
    }

    private HttpClient getHttpClient(int connectionTimeout, int readTimeout) {
        final HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setSoTimeout(httpParams, readTimeout);
        HttpConnectionParams.setConnectionTimeout(httpParams, connectionTimeout);

        return new DefaultHttpClient(httpParams);
    }

    public String execRequest(HttpRequestBase request, int connectionTimeout, int readTimeout) throws IOException {
        HttpClient httpClient = getHttpClient(connectionTimeout, readTimeout);
        try {
            HttpResponse response = httpClient.execute(request);
            String msg = EntityUtils.toString(response.getEntity());
            int code = response.getStatusLine().getStatusCode();
            if (logger.isTraceEnabled()) {
                String displayMessage;
                if (msg.length() > 500) {
                    displayMessage = msg.substring(0, 500);
                } else {
                    displayMessage = msg;
                }
                logger.trace("Send request: {}. And receive response[{}] which lenght is {}, and content is {}.", code,
                        request.getRequestLine(), msg.length(), displayMessage);
            }
            if (code != 200)
                throw new IOException("Invalid http response " + code + " when send request: "
                        + request.getURI().toString() + "\n" + msg);
            return msg;
        } catch (IOException e) {
            logger.error("error when send http request:" + request.getURI().toString(), e);
            throw e;
        } finally {
            request.releaseConnection();
        }
    }
}
