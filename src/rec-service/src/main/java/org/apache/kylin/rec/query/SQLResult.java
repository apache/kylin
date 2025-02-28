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

package org.apache.kylin.rec.query;

import java.io.Serializable;
import java.sql.SQLException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
public class SQLResult implements Serializable {

    public static final String NON_SELECT_CLAUSE = "Statement is not a select clause";

    @JsonProperty("status")
    private Status status;

    @JsonProperty("message")
    private String message;

    @JsonIgnore
    private Throwable exception;

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("duration")
    private long duration;

    @JsonProperty("project")
    private String project;

    public void writeNonQueryException(String project, String sql, long duration) {
        write(project, sql, duration, NON_SELECT_CLAUSE);
        writeExceptionInfo(NON_SELECT_CLAUSE, new SQLException(NON_SELECT_CLAUSE + ":" + sql));
    }

    public void writeExceptionInfo(String message, Throwable e) {
        this.status = Status.FAILED;
        this.message = message;
        this.exception = e;
    }

    public void writeNormalInfo(String project, String sql, long elapsed, String queryId) {
        write(project, sql, elapsed, queryId);
        if (this.exception == null) {
            this.status = Status.SUCCESS;
        }
    }

    private void write(String project, String sql, long elapsed, String queryId) {
        this.project = project;
        this.sql = sql;
        this.duration = elapsed;
        this.queryId = queryId;
    }

    public enum Status {
        SUCCESS, FAILED
    }
}
