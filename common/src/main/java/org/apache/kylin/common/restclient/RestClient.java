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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * @author yangli9
 */
public class RestClient {

    String host;
    int port;
    String baseUrl;
    String userName;
    String password;
    HttpClient client;

    private static Pattern fullRestPattern = Pattern.compile("(?:([^:]+)[:]([^@]+)[@])?([^:]+)(?:[:](\\d+))?");

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

    private void init(String host, int port, String userName, String password) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.baseUrl = "http://" + host + ":" + port + "/kylin/api";

        client = new HttpClient();

        if (userName != null && password != null) {
            client.getParams().setAuthenticationPreemptive(true);
            Credentials creds = new UsernamePasswordCredentials(userName, password);
            client.getState().setCredentials(new AuthScope(host, port, AuthScope.ANY_REALM), creds);
        }
    }

    public void wipeCache(String type, String action, String name) throws IOException {
        String url = baseUrl + "/cache/" + type + "/" + name + "/" + action;
        HttpMethod request = new PutMethod(url);

        try {
            int code = client.executeMethod(request);
            String msg = Bytes.toString(request.getResponseBody());

            if (code != 200)
                throw new IOException("Invalid response " + code + " with cache wipe url " + url + "\n" + msg);

        } catch (HttpException ex) {
            throw new IOException(ex);
        } finally {
            request.releaseConnection();
        }
    }

    public String getKylinProperties() throws IOException {
        String url = baseUrl + "/admin/config";
        HttpMethod request = new GetMethod(url);
        try {
            int code = client.executeMethod(request);
            String msg = Bytes.toString(request.getResponseBody());
            JSONObject obj = new JSONObject(msg);
            msg = obj.getString("config");

            if (code != 200)
                throw new IOException("Invalid response " + code + " with cache wipe url " + url + "\n" + msg);

            return msg;

        } catch (JSONException e) {
            throw new IOException("Error when parsing json response from REST");
        } finally {
            request.releaseConnection();
        }
    }

}
