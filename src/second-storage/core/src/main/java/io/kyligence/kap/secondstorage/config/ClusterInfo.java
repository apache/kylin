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

package io.kyligence.kap.secondstorage.config;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.JsonUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ClusterInfo {
    private Map<String, List<Node>> cluster;
    private String socketTimeout;
    private String keepAliveTimeout;
    private String installPath;
    private String logPath;

    //username of machine
    private String userName;
    private String password;

    @JsonIgnore
    public List<Node> getNodes() {
        return Collections.unmodifiableList(cluster.values().stream().flatMap(List::stream).collect(Collectors.toList()));
    }

    public ClusterInfo setCluster(Map<String, List<Node>> cluster) {
        this.cluster = cluster;
        return this;
    }

    public Map<String, List<Node>> getCluster() {
        TreeMap<String, List<Node>> orderedMap = new TreeMap<>(cluster);
        return Collections.unmodifiableMap(orderedMap);
    }

    public void transformNode() {
        for (final Map.Entry<String, List<Node>> pair : cluster.entrySet()) {
            List<Node> nodes = pair.getValue();
            List<Node> transformedNodes = new ArrayList<>();
            for (Object node : nodes) {
                Node n;
                if (!(node instanceof Node)) {
                    n = JsonUtil.readValueQuietly(
                            JsonUtil.writeValueAsStringQuietly(node).getBytes(StandardCharsets.UTF_8),
                            Node.class);
                } else {
                    n = (Node) node;
                }
                if (n.getSSHPort() == 0) {
                    n.setSSHPort(22);
                }
                transformedNodes.add(n);
            }
            cluster.put(pair.getKey(), transformedNodes);
        }
    }

    public String getSocketTimeout() {
        return socketTimeout;
    }

    public ClusterInfo setSocketTimeout(String socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public String getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public ClusterInfo setKeepAliveTimeout(String keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
        return this;
    }

    public String getInstallPath() {
        return installPath;
    }

    public ClusterInfo setInstallPath(String installPath) {
        this.installPath = installPath;
        return this;
    }

    public String getLogPath() {
        return logPath;
    }

    public ClusterInfo setLogPath(String logPath) {
        this.logPath = logPath;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return EncryptUtil.isEncrypted(password) ? EncryptUtil.decryptPassInKylin(password) : password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public ClusterInfo(ClusterInfo cluster) {
        this.cluster = Maps.newHashMap(cluster.getCluster());
        this.keepAliveTimeout = cluster.getKeepAliveTimeout();
        this.socketTimeout = cluster.getKeepAliveTimeout();
        this.logPath = cluster.getLogPath();
        this.userName = cluster.getUserName();
        this.password = cluster.getPassword();
        this.installPath = cluster.getInstallPath();
    }

    public boolean emptyCluster() {
        return cluster == null || cluster.isEmpty();
    }

    public ClusterInfo() {
    }

}


