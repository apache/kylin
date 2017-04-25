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

package org.apache.kylin.engine.mr.common;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.Principal;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthSchemeRegistry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 */
public class HadoopStatusGetter {

    private final String mrJobId;
    private final String yarnUrl;

    protected static final Logger logger = LoggerFactory.getLogger(HadoopStatusGetter.class);

    public HadoopStatusGetter(String yarnUrl, String mrJobId) {
        this.yarnUrl = yarnUrl;
        this.mrJobId = mrJobId;
    }

    public Pair<RMAppState, FinalApplicationStatus> get(boolean useKerberosAuth) throws IOException {
        String applicationId = mrJobId.replace("job", "application");
        String url = yarnUrl.replace("${job_id}", applicationId);
        String response = useKerberosAuth ? getHttpResponseWithKerberosAuth(url) : getHttpResponse(url);
        logger.debug("Hadoop job " + mrJobId + " status : " + response);
        JsonNode root = new ObjectMapper().readTree(response);
        RMAppState state = RMAppState.valueOf(root.findValue("state").textValue());
        FinalApplicationStatus finalStatus = FinalApplicationStatus.valueOf(root.findValue("finalStatus").textValue());
        return Pair.of(state, finalStatus);
    }

    private static String DEFAULT_KRB5_CONFIG_LOCATION = "/etc/krb5.conf";

    private String getHttpResponseWithKerberosAuth(String url) throws IOException {
        String krb5ConfigPath = System.getProperty("java.security.krb5.conf");
        if (krb5ConfigPath == null) {
            krb5ConfigPath = DEFAULT_KRB5_CONFIG_LOCATION;
        }
        boolean skipPortAtKerberosDatabaseLookup = true;
        System.setProperty("java.security.krb5.conf", krb5ConfigPath);
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        DefaultHttpClient client = new DefaultHttpClient();
        AuthSchemeRegistry authSchemeRegistry = new AuthSchemeRegistry();
        authSchemeRegistry.register(AuthPolicy.SPNEGO, new SPNegoSchemeFactory(skipPortAtKerberosDatabaseLookup));
        client.setAuthSchemes(authSchemeRegistry);

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        Credentials useJaasCreds = new Credentials() {
            public String getPassword() {
                return null;
            }

            public Principal getUserPrincipal() {
                return null;
            }
        };
        credentialsProvider.setCredentials(new AuthScope(null, -1, null), useJaasCreds);
        client.setCredentialsProvider(credentialsProvider);

        String response = null;
        while (response == null) {
            if (url.startsWith("https://")) {
                registerEasyHttps(client);
            }
            if (url.contains("anonymous=true") == false) {
                url += url.contains("?") ? "&" : "?";
                url += "anonymous=true";
            }
            HttpGet httpget = new HttpGet(url);
            httpget.addHeader("accept", "application/json");
            try {
                HttpResponse httpResponse = client.execute(httpget);
                String redirect = null;
                org.apache.http.Header h = httpResponse.getFirstHeader("Location");
                if (h != null) {
                    redirect = h.getValue();
                    if (isValidURL(redirect) == false) {
                        logger.info("Get invalid redirect url, skip it: " + redirect);
                        Thread.sleep(1000L);
                        continue;
                    }
                } else {
                    h = httpResponse.getFirstHeader("Refresh");
                    if (h != null) {
                        String s = h.getValue();
                        int cut = s.indexOf("url=");
                        if (cut >= 0) {
                            redirect = s.substring(cut + 4);

                            if (isValidURL(redirect) == false) {
                                logger.info("Get invalid redirect url, skip it: " + redirect);
                                Thread.sleep(1000L);
                                continue;
                            }
                        }
                    }
                }

                if (redirect == null) {
                    response = IOUtils.toString(httpResponse.getEntity().getContent(), Charset.defaultCharset());
                    logger.debug("Job " + mrJobId + " get status check result.\n");
                } else {
                    url = redirect;
                    logger.debug("Job " + mrJobId + " check redirect url " + url + ".\n");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(e.getMessage());
            } finally {
                httpget.releaseConnection();
            }
        }

        return response;
    }

    private String getHttpResponse(String url) throws IOException {
        HttpClient client = new DefaultHttpClient();

        String response = null;
        while (response == null) { // follow redirects via 'refresh'
            if (url.startsWith("https://")) {
                registerEasyHttps(client);
            }
            if (url.contains("anonymous=true") == false) {
                url += url.contains("?") ? "&" : "?";
                url += "anonymous=true";
            }

            HttpGet get = new HttpGet(url);
            get.addHeader("accept", "application/json");

            try {
                HttpResponse res = client.execute(get);

                String redirect = null;
                Header h = res.getFirstHeader("Location");
                if (h != null) {
                    redirect = h.getValue();
                    if (isValidURL(redirect) == false) {
                        logger.info("Get invalid redirect url, skip it: " + redirect);
                        Thread.sleep(1000L);
                        continue;
                    }
                } else {
                    h = res.getFirstHeader("Refresh");
                    if (h != null) {
                        String s = h.getValue();
                        int cut = s.indexOf("url=");
                        if (cut >= 0) {
                            redirect = s.substring(cut + 4);

                            if (isValidURL(redirect) == false) {
                                logger.info("Get invalid redirect url, skip it: " + redirect);
                                Thread.sleep(1000L);
                                continue;
                            }
                        }
                    }
                }

                if (redirect == null) {
                    response = res.getStatusLine().toString();
                    logger.debug("Job " + mrJobId + " get status check result.\n");
                } else {
                    url = redirect;
                    logger.debug("Job " + mrJobId + " check redirect url " + url + ".\n");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error(e.getMessage());
            } finally {
                get.releaseConnection();
            }
        }

        return response;
    }

    private static void registerEasyHttps(HttpClient client) {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("SSL");

            // set up a TrustManager that trusts everything
            try {
                sslContext.init(null, new TrustManager[] { new DefaultX509TrustManager(null) {
                    public X509Certificate[] getAcceptedIssuers() {
                        logger.debug("getAcceptedIssuers");
                        return null;
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                        logger.debug("checkClientTrusted");
                    }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                        logger.debug("checkServerTrusted");
                    }
                } }, new SecureRandom());
            } catch (KeyManagementException e) {
            }
            SSLSocketFactory ssf = new SSLSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            ClientConnectionManager ccm = client.getConnectionManager();
            SchemeRegistry sr = ccm.getSchemeRegistry();
            sr.register(new Scheme("https", 443, ssf));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
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
