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

package io.kyligence.kap.secondstorage.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.secondstorage.config.Node;

import java.io.Serializable;

public class SecondStorageNode implements Serializable {
    private static final long serialVersionUID = 0L;

    @JsonProperty("name")
    private String name;
    @JsonProperty("ip")
    private String ip;
    @JsonProperty("port")
    private long port;

    public SecondStorageNode(Node node) {
        this.ip = node.getIp();
        this.port = node.getPort();
        this.name = node.getName();
    }

    public String getName() {
        return name;
    }

    public SecondStorageNode setName(String name) {
        this.name = name;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public SecondStorageNode setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public long getPort() {
        return port;
    }

    public SecondStorageNode setPort(long port) {
        this.port = port;
        return this;
    }
}