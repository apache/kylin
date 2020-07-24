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

package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class CuboidTreeResponse implements Serializable {

    private static final long serialVersionUID = 2835980715891990832L;

    private NodeInfo root;

    public NodeInfo getRoot() {
        return root;
    }

    public void setRoot(NodeInfo root) {
        this.root = root;
    }

    public static class NodeInfo {
        @JsonProperty("cuboid_id")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        private Long id;
        @JsonProperty("name")
        private String name;
        @JsonProperty("query_count")
        private Long queryCount;
        @JsonProperty("query_rate")
        private Float queryRate;
        @JsonProperty("exactly_match_count")
        private Long exactlyMatchCount;
        @JsonProperty("row_count")
        private Long rowCount;
        @JsonProperty("existed")
        private Boolean existed;
        @JsonProperty("children")
        List<NodeInfo> children = Lists.newArrayList();

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getQueryCount() {
            return queryCount;
        }

        public void setQueryCount(Long queryCount) {
            this.queryCount = queryCount;
        }

        public Float getQueryRate() {
            return queryRate;
        }

        public void setQueryRate(Float queryRate) {
            this.queryRate = queryRate;
        }

        public Long getExactlyMatchCount() {
            return exactlyMatchCount;
        }

        public void setExactlyMatchCount(Long exactlyMatchCount) {
            this.exactlyMatchCount = exactlyMatchCount;
        }

        public Long getRowCount() {
            return rowCount;
        }

        public void setRowCount(Long rowCount) {
            this.rowCount = rowCount;
        }

        public Boolean getExisted() {
            return existed;
        }

        public void setExisted(Boolean existed) {
            this.existed = existed;
        }

        public void addChild(NodeInfo child) {
            this.children.add(child);
        }

        public List<NodeInfo> getChildren() {
            return children;
        }
    }
}
