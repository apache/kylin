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

package org.apache.kylin.stream.core.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Node {
    @JsonProperty("host")
    private String host;
    @JsonProperty("port")
    private int port;
    @JsonProperty("properties")
    private Map<String, String> properties;

    @JsonCreator
    public Node(@JsonProperty("host") String host, @JsonProperty("port") int port) {
        this.host = host;
        this.port = port;
    }

    public Node() {

    }

    public static Node from(String nodeString) {
        return from(nodeString, ":");
    }

    public static Node fromNormalizeString(String nodeString) {
        return from(nodeString, "_");
    }

    public static Node from(String nodeString, String separateStr) {
        int lastIdx = nodeString.lastIndexOf(separateStr);
        if (lastIdx == -1) {
            throw new IllegalArgumentException("illegal host port string:" + nodeString);
        }
        String host = nodeString.substring(0, lastIdx);
        int port = Integer.parseInt(nodeString.substring(lastIdx + separateStr.length()));
        return new Node(host, port);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String toString() {
        return host + ":" + port;
    }

    public String toNormalizeString() {
        return host + "_" + port;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Node node = (Node) o;

        if (port != node.port)
            return false;
        return host != null ? host.equals(node.host) : node.host == null;

    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }

}
