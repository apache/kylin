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

package org.apache.kylin.storage.druid.common;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kylin.storage.druid.http.FullResponseHandler;
import org.apache.kylin.storage.druid.http.FullResponseHolder;
import org.apache.kylin.storage.druid.http.HttpClient;
import org.apache.kylin.storage.druid.http.Request;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.druid.java.util.common.IOE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;

public class DruidLeaderClient {
    private static final Logger logger = LoggerFactory.getLogger(DruidLeaderClient.class);

    private final HttpClient httpClient;
    private final CopyOnWriteArraySet<String> hosts;
    private final int maxRetries;

    private final AtomicReference<String> currentKnownLeader;

    public DruidLeaderClient(HttpClient httpClient, String[] hosts, int maxRetries) {
        Preconditions.checkNotNull(httpClient, "httpClient");
        Preconditions.checkNotNull(hosts, "hosts");
        Preconditions.checkArgument(hosts.length > 0, "");

        this.httpClient = httpClient;
        this.hosts = new CopyOnWriteArraySet<>(Arrays.asList(hosts));
        this.maxRetries = maxRetries;
        this.currentKnownLeader = new AtomicReference<>(hosts[0]);
    }

    /**
     * Make a Request object aimed at the leader. Throws IOException if the leader cannot be located.
     */
    public Request makeRequest(HttpMethod httpMethod, String urlPath) throws IOException {
        return new Request(httpMethod, new URL(StringUtils.format("%s%s", currentKnownLeader.get(), urlPath)));
    }

    /**
     * Executes a Request object aimed at the leader. Throws IOException if the leader cannot be located.
     */
    public FullResponseHolder go(Request request) throws IOException, InterruptedException {
        String leader = currentKnownLeader.get();

        for (int counter = 0; counter < maxRetries; counter++) {
            final FullResponseHolder fullResponseHolder;

            // execute, pick another node and retry if error
            try {
                try {
                    fullResponseHolder = httpClient.go(request, new FullResponseHandler(Charsets.UTF_8)).get();
                } catch (ExecutionException e) {
                    // Unwrap IOExceptions and ChannelExceptions, re-throw others
                    Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
                    Throwables.propagateIfInstanceOf(e.getCause(), ChannelException.class);
                    throw new RE(e, "HTTP request to[%s] failed", request.getUrl());
                }
            } catch (IOException | ChannelException ex) {
                logger.info("Request[{}] failed with msg [{}]. Retry.", request.getUrl(), ex.getMessage());
                leader = getNextHost(leader);
                try {
                    if (request.getUrl().getQuery() == null) {
                        request = withUrl(request,
                                new URL(StringUtils.format("%s%s", leader, request.getUrl().getPath())));
                    } else {
                        request = withUrl(request, new URL(StringUtils.format("%s%s?%s", leader,
                                request.getUrl().getPath(), request.getUrl().getQuery())));
                    }
                    TimeUnit.SECONDS.sleep(1);
                    continue;
                } catch (MalformedURLException e) {
                    // Not an IOException; this is our own fault.
                    throw new ISE(e, "failed to build url with path[%] and query string [%s].",
                            request.getUrl().getPath(), request.getUrl().getQuery());
                }
            }

            if (HttpResponseStatus.TEMPORARY_REDIRECT.equals(fullResponseHolder.getStatus())) {
                // redirect to new leader
                String redirectUrlStr = fullResponseHolder.getResponse().getHeader("Location");
                if (redirectUrlStr == null) {
                    throw new IOE("No redirect location is found in response from url[%s].", request.getUrl());
                }

                logger.info("Request[{}] received redirect response to location [{}].", request.getUrl(),
                        redirectUrlStr);

                final URL redirectUrl;
                try {
                    redirectUrl = new URL(redirectUrlStr);
                } catch (MalformedURLException ex) {
                    throw new IOE(ex,
                            "Malformed redirect location is found in response from url[%s], new location[%s].",
                            request.getUrl(), redirectUrlStr);
                }

                //update known leader location
                leader = StringUtils.format("%s://%s:%s", redirectUrl.getProtocol(), redirectUrl.getHost(),
                        redirectUrl.getPort());
                if (hosts.add(leader)) {
                    logger.info("Found a new leader at {}", leader);
                }
                request = withUrl(request, redirectUrl);

            } else {
                // success, record leader index
                currentKnownLeader.set(leader);
                return fullResponseHolder;
            }
        }
        throw new IOE("Retries exhausted, couldn't fulfill request to [%s].", request.getUrl());
    }

    private String getNextHost(String currentHost) {
        List<String> allHosts = Lists.newArrayList(hosts);
        int index = allHosts.indexOf(currentHost);
        return allHosts.get((index + 1) % allHosts.size());
    }

    private Request withUrl(Request old, URL url) {
        Request req = new Request(old.getMethod(), url);
        req.addHeaderValues(old.getHeaders());
        if (old.hasContent()) {
            req.setContent(old.getContent());
        }
        return req;
    }
}
