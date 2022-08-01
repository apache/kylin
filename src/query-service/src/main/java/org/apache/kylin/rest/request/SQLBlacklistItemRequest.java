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

import com.fasterxml.jackson.annotation.JsonProperty;

public class SQLBlacklistItemRequest implements Serializable {

    @JsonProperty("id")
    private String id;

    @JsonProperty("regex")
    private String regex;

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("concurrent_limit")
    private int concurrentLimit;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public int getConcurrentLimit() {
        return concurrentLimit;
    }

    public void setConcurrentLimit(int concurrentLimit) {
        this.concurrentLimit = concurrentLimit;
    }
}
