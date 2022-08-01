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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.util.EncryptUtil;

public class Node {
    private String name;
    private String ip;
    private int port;
    private String user;
    private String password;
    @JsonProperty("sshPort")
    private int sshPort;


    public Node(String name, String ip, int port, String user, String password) {
        this.name = name;
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public Node(String name, String ip, int port, String user, String password, int sshPort) {
        this.name = name;
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
        this.sshPort = sshPort;
    }

    public Node(Node node) {
        this(node.name, node.ip, node.port, node.user, node.password);
    }

    public Node() {
    }

    public String getName() {
        return name;
    }

    public Node setName(String name) {
        this.name = name;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public Node setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public int getPort() {
        return port;
    }

    public Node setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUser() {
        return user;
    }

    public Node setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return EncryptUtil.isEncrypted(password) ? EncryptUtil.decryptPassInKylin(password) : password;
    }

    public Node setPassword(String password) {
        this.password = password;
        return this;
    }

    public int getSSHPort() {
        return sshPort;
    }

    public Node setSSHPort(int sshPort) {
        this.sshPort = sshPort;
        return this;
    }
}
