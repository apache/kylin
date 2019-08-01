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

package org.apache.kylin.metadata.badquery;

import java.util.Objects;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class BadQueryEntry extends RootPersistentEntity implements Comparable<BadQueryEntry> {
    
    public static final String ADJ_SLOW = "Slow";
    public static final String ADJ_PUSHDOWN = "Pushdown";

    @JsonProperty("adj")
    private String adj;
    @JsonProperty("sql")
    private String sql;
    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("running_seconds")
    private float runningSec;
    @JsonProperty("server")
    private String server;
    @JsonProperty("thread")
    private String thread;
    @JsonProperty("user")
    private String user;
    @JsonProperty("query_id")
    private String queryId;
    @JsonProperty("cube")
    private String cube;

    public BadQueryEntry(String sql, String adj, long startTime, float runningSec, String server, String thread,
            String user, String queryId, String cube) {
        this.updateRandomUuid();
        this.adj = adj;
        this.sql = sql;
        this.startTime = startTime;
        this.runningSec = runningSec;
        this.server = server;
        this.thread = thread;
        this.user = user;
        this.queryId = queryId;
        this.cube = cube;
    }

    public BadQueryEntry() {

    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public float getRunningSec() {
        return runningSec;
    }

    public void setRunningSec(float runningSec) {
        this.runningSec = runningSec;
    }

    public String getAdj() {
        return adj;
    }

    public void setAdj(String adj) {
        this.adj = adj;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getThread() {
        return thread;
    }

    public void setThread(String thread) {
        this.thread = thread;
    }

    public String getCube() {
        return cube;
    }

    public void setCube(String cube) {
        this.cube = cube;
    }

    @Override
    public int compareTo(BadQueryEntry obj) {
        int comp = Long.compare(this.startTime, obj.startTime);
        if (comp != 0)
            return comp;
        else
            return this.sql.compareTo(obj.sql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, startTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BadQueryEntry entry = (BadQueryEntry) o;

        if (startTime != entry.startTime)
            return false;

        if (!sql.equals(entry.sql))
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "BadQueryEntry [ adj=" + adj + ", server=" + server + ", startTime=" + DateFormat.formatToTimeStr(startTime) + " ]";
    }
}
