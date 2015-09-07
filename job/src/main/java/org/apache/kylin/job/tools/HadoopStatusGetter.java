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

package org.apache.kylin.job.tools;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.Principal;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by qianzhou on 1/20/15.
 */
public class HadoopStatusGetter {

    private final String mrJobId;
    private final String yarnUrl;

    protected static final Logger log = LoggerFactory.getLogger(HadoopStatusChecker.class);

    public HadoopStatusGetter(String yarnUrl, String mrJobId) {
        this.yarnUrl = yarnUrl;
        this.mrJobId = mrJobId;
    }

    public Pair<RMAppState, FinalApplicationStatus> get(boolean useKerberos) throws IOException {
        String applicationId = mrJobId.replace("job", "application");
        String url = yarnUrl.replace("${job_id}", applicationId);
        String response = useKerberos ? getHttpResponseWithKerberosAuth(url) : getHttpResponse(url);
        JsonNode root = new ObjectMapper().readTree(response);
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
                        log.info("Get invalid redirect url, skip it: " + redirect);
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
                                log.info("Get invalid redirect url, skip it: " + redirect);
                                Thread.sleep(1000l);
                                continue;
                            }
                        }
                    }
                }

                if (redirect == null) {
                    response = get.getResponseBodyAsString();
                    log.debug("Job " + mrJobId + " get status check result.\n");
                } else {
                    url = redirect;
                    log.debug("Job " + mrJobId + " check redirect url " + url + ".\n");
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            } finally {
                get.releaseConnection();
            }
        }

        return response;
    }
    
    private static String DEFAULT_KRB5_CONFIG_LOCATION = "/etc/krb5.conf";
    private String getHttpResponseWithKerberosAuth(String url) throws IOException {
        String krb5ConfigPath = System.getProperty("java.security.krb5.conf");
        if (krb5ConfigPath == null) {
            krb5ConfigPath = DEFAULT_KRB5_CONFIG_LOCATION;
        } 
        log.debug("krb5 config file is " + krb5ConfigPath);
      
        boolean skipPortAtKerberosDatabaseLookup = true;
        System.setProperty("java.security.krb5.conf", krb5ConfigPath);
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly","false");
        Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create()
                .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(skipPortAtKerberosDatabaseLookup))
                .build();
        
        CloseableHttpClient client = HttpClients.custom().setDefaultAuthSchemeRegistry(authSchemeRegistry).build();
        HttpClientContext context = HttpClientContext.create();
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        // This may seem odd, but specifying 'null' as principal tells java to use the logged in user's credentials
        Credentials useJaasCreds = new Credentials() {
            public String getPassword() {
                return null;
            }
            public Principal getUserPrincipal() {
                return null;
            }
        };
        credentialsProvider.setCredentials( new AuthScope(null, -1, null), useJaasCreds );
        context.setCredentialsProvider(credentialsProvider);
        String responseString = null;
        int count = 0;
        int MAX_RETRY_TIME = 3;
        while(responseString == null && count ++ < MAX_RETRY_TIME) {
            if (url.startsWith("https://")) {
                registerEasyHttps();
            }
            if (url.contains("anonymous=true") == false) {
                url += url.contains("?") ? "&" : "?";
                url += "anonymous=true";
            }
            HttpGet httpget = new HttpGet(url);
            try {
                httpget.addHeader("accept", "application/json");
                CloseableHttpResponse response = client.execute(httpget,context);
                String redirect = null;
                org.apache.http.Header h = response.getFirstHeader("Refresh");
                if (h != null) {
                    String s = h.getValue();
                    int cut = s.indexOf("url=");
                    if (cut >= 0) {
                        redirect = s.substring(cut + 4);
                    }
                }
    
                if (redirect == null) {
                    responseString = IOUtils.toString(response.getEntity().getContent());
                    log.debug("Job " + mrJobId + " get status check result.\n");
                } else {
                    url = redirect;
                    log.debug("Job " + mrJobId + " check redirect url " + url + ".\n");
                } 
            } finally {
                httpget.releaseConnection();
            }
        }
        
        return responseString;
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
