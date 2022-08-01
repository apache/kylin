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

package org.apache.kylin.rest.request;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SQLBlacklistRequest implements Serializable {
    @JsonProperty("project")
    private String project;

    @JsonProperty("blacklist_items")
    private List<SQLBlacklistItemRequest> blacklistItems;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public List<SQLBlacklistItemRequest> getBlacklistItems() {
        return blacklistItems;
    }

    public void setBlacklistItems(List<SQLBlacklistItemRequest> blacklistItems) {
        this.blacklistItems = blacklistItems;
    }
}
