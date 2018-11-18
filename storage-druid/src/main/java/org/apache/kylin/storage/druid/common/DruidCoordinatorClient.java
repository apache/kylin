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

import static org.apache.kylin.storage.druid.common.DruidSerdeHelper.JSON_MAPPER;

import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.storage.druid.http.FullResponseHolder;
import org.apache.kylin.storage.druid.http.HttpClient;
import org.apache.kylin.storage.druid.http.HttpClientConfig;
import org.apache.kylin.storage.druid.http.HttpClientFactory;
import org.apache.kylin.storage.druid.http.Request;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;

public class DruidCoordinatorClient {
    private static final String PATH_PREFIX = "/druid/coordinator/v1";
    private static final DruidCoordinatorClient SINGLETON = new DruidCoordinatorClient();

    private DruidLeaderClient leaderClient;

    private DruidCoordinatorClient() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        HttpClientConfig.Builder builder = HttpClientConfig.builder().withNumConnections(10)
                .withReadTimeout(Duration.standardSeconds(30)).withWorkerCount(10)
                .withCompressionCodec(HttpClientConfig.CompressionCodec.GZIP);
        HttpClient httpClient = HttpClientFactory.createClient(builder.build());
        this.leaderClient = new DruidLeaderClient(httpClient, config.getDruidCoordinatorAddresses(), 10);
    }

    public static DruidCoordinatorClient getSingleton() {
        return SINGLETON;
    }

    public List<ImmutableSegmentLoadInfo> fetchServerView(String dataSource, Interval interval) {
        try {
            FullResponseHolder response = leaderClient.go(leaderClient.makeRequest(HttpMethod.GET,
                    StringUtils.format("%s/datasources/%s/intervals/%s/serverview?partial=true", PATH_PREFIX,
                            dataSource, interval.toString().replace("/", "_"))));

            if (!response.getStatus().equals(HttpResponseStatus.OK)) {
                throw new ISE("Error while fetching serverView status[%s] content[%s]", response.getStatus(),
                        response.getContent());
            }
            return JSON_MAPPER.readValue(response.getContent(), new TypeReference<List<ImmutableSegmentLoadInfo>>() {
            });

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public List<Rule> getRules(String dataSource) {
        try {
            FullResponseHolder response = leaderClient.go(leaderClient.makeRequest(HttpMethod.GET,
                    StringUtils.format("%s/rules/%s", PATH_PREFIX, dataSource)));

            if (!response.getStatus().equals(HttpResponseStatus.OK)) {
                throw new ISE("ERROR while fetching rules for %s: status[%s] content[%s]", dataSource,
                        response.getStatus(), response.getContent());
            }
            return JSON_MAPPER.readValue(response.getContent(), new TypeReference<List<Rule>>() {
            });

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public void putRules(String dataSource, List<Rule> rules) {
        try {
            Request request = leaderClient
                    .makeRequest(HttpMethod.POST, StringUtils.format("%s/rules/%s", PATH_PREFIX, dataSource))
                    .setHeader("X-Druid-Author", "Kylin-ADMIN")
                    .setContent(MediaType.APPLICATION_JSON, JSON_MAPPER.writeValueAsBytes(rules));
            FullResponseHolder response = leaderClient.go(request);

            if (!response.getStatus().equals(HttpResponseStatus.OK)) {
                throw new ISE("ERROR while setting rules for %s: status[%s] content[%s]", dataSource,
                        response.getStatus(), response.getContent());
            }

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public void deleteDataSource(String dataSource) {
        try {
            FullResponseHolder response = leaderClient.go(leaderClient.makeRequest(HttpMethod.DELETE,
                    StringUtils.format("%s/datasources/%s", PATH_PREFIX, dataSource)));

            if (!response.getStatus().equals(HttpResponseStatus.OK)) {
                throw new ISE("ERROR while deleting datasource [%s]: status[%s] content[%s]", dataSource,
                        response.getStatus(), response.getContent());
            }

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public  List<String> getDataSource() {
        try {
            FullResponseHolder response = leaderClient.go(leaderClient.makeRequest(HttpMethod.GET,
                    StringUtils.format("%s/datasources", PATH_PREFIX)));

            if (!response.getStatus().equals(HttpResponseStatus.OK)) {
                throw new ISE("ERROR while get all datasources: status[%s] content[%s]",
                        response.getStatus(), response.getContent());
            }
            return JSON_MAPPER.readValue(response.getContent(), new TypeReference<List<String>>() {
            });
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
