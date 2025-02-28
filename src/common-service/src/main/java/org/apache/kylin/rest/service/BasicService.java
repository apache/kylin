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

package org.apache.kylin.rest.service;

import static org.apache.kylin.guava30.shaded.common.net.HttpHeaders.ACCEPT_ENCODING;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.BroadcastEventReadyNotifier;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.CaseFormat;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.streaming.DataParserManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.NullsLastPropertyComparator;
import org.apache.kylin.tool.restclient.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.client.RestTemplate;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BasicService {

    @Autowired
    @Qualifier("normalRestTemplate")
    private RestTemplate restTemplate;

    @Autowired
    @Qualifier("userGroupService")
    protected IUserGroupService userGroupService;

    @Autowired
    ClusterManager clusterManager;

    public KylinConfig getConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }

        return kylinConfig;
    }

    public <T> T getManager(Class<T> clz) {
        return getConfig().getManager(clz);
    }

    public <T> T getManager(Class<T> clz, String project) {
        return getConfig().getManager(project, clz);
    }

    protected static String getUsername() {
        String username = SecurityContextHolder.getContext().getAuthentication().getName();
        if (StringUtils.isEmpty(username)) {
            username = "";
        }
        return username;
    }

    protected static <T> Comparator<T> propertyComparator(String property, boolean ascending) {
        return new PropertyComparator<>(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, property), false,
                ascending);

    }

    protected static <T> Comparator<T> nullsLastPropertyComparator(String property, boolean ascending) {
        return new NullsLastPropertyComparator<>(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, property),
                false, ascending);
    }

    public <T> EnvelopeResponse<T> generateTaskForRemoteHost(final HttpServletRequest request, String url)
            throws Exception {
        val response = getHttpResponse(request, url);
        return JsonUtil.readValue(response.getBody(), EnvelopeResponse.class);
    }

    private ResponseEntity<byte[]> getHttpResponse(final HttpServletRequest request, String url) throws IOException {
        val body = IOUtils.toByteArray(request.getInputStream());
        return getHttpResponse(request, url, body);
    }

    public <T> EnvelopeResponse<T> generateTaskForRemoteHost(final HttpServletRequest request, String url, byte[] body)
            throws Exception {
        val response = getHttpResponse(request, url, body);
        return JsonUtil.readValue(response.getBody(), EnvelopeResponse.class);
    }

    private ResponseEntity<byte[]> getHttpResponse(final HttpServletRequest request, String url, byte[] body) {
        HttpHeaders headers = new HttpHeaders();
        Collections.list(request.getHeaderNames())
                .forEach(k -> headers.put(k, Collections.list(request.getHeaders(k))));
        //remove gzip
        headers.remove(ACCEPT_ENCODING);
        return restTemplate.exchange(url, HttpMethod.valueOf(request.getMethod()), new HttpEntity<>(body, headers),
                byte[].class);
    }

    public Set<String> getCurrentUserGroups() {
        return userGroupService.listUserGroups(AclPermissionUtil.getCurrentUsername());
    }

    public boolean remoteRequest(BroadcastEventReadyNotifier notifier) {
        try {
            List<ServerInfoResponse> jobs = clusterManager.getJobServers();
            String owner = jobs.get(RandomUtil.nextInt(jobs.size())).getHost();
            new RestClient(owner).notify(notifier);
        } catch (Exception e) {
            log.error("Failed to using rest client request.", e);
            return false;
        }
        return true;
    }

    public Pair<String, String> checkDatabaseAndTable(String table) {
        if (table == null)
            table = "";
        String database = null;
        if (table.contains(".")) {
            database = table.split("\\.", 2)[0].trim();
            table = table.split("\\.", 2)[1].trim();
        }
        return Pair.newPair(database, table);
    }

    protected void initDefaultParser(String project) {
        if (getManager(DataParserManager.class, project).isInitialized()) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager(DataParserManager.class, project).initDefault();
            return null;
        }, project);
    }

    public NProjectManager getProjectManager() {
        return NProjectManager.getInstance(getConfig());
    }
}
