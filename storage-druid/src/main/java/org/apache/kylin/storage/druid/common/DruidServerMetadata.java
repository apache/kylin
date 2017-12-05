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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DruidServerMetadata {
    private final String name;
    private final String hostAndPort;
    private final String hostAndTlsPort;
    private final long maxSize;
    private final String tier;
    private final String type;
    private final int priority;

    @JsonCreator
    public DruidServerMetadata(@JsonProperty("name") String name, @JsonProperty("host") String hostAndPort,
            @JsonProperty("hostAndTlsPort") String hostAndTlsPort, @JsonProperty("maxSize") long maxSize,
            @JsonProperty("type") String type, @JsonProperty("tier") String tier,
            @JsonProperty("priority") int priority) {
        this.name = name;
        this.hostAndPort = hostAndPort;
        this.hostAndTlsPort = hostAndTlsPort;
        this.maxSize = maxSize;
        this.tier = tier;
        this.type = type;
        this.priority = priority;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    public String getHost() {
        return getHostAndTlsPort() != null ? getHostAndTlsPort() : getHostAndPort();
    }

    @JsonProperty("host")
    public String getHostAndPort() {
        return hostAndPort;
    }

    @JsonProperty
    public String getHostAndTlsPort() {
        return hostAndTlsPort;
    }

    @JsonProperty
    public long getMaxSize() {
        return maxSize;
    }

    @JsonProperty
    public String getTier() {
        return tier;
    }

    @JsonProperty
    public String getType() {
        return type;
    }

    @JsonProperty
    public int getPriority() {
        return priority;
    }
}
