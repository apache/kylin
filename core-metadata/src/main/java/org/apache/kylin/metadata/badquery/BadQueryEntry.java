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

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.DateFormat;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class BadQueryEntry extends RootPersistentEntity implements Comparable<BadQueryEntry> {

    @JsonProperty("adj")
    private String adj;
    @JsonProperty("sql")
    private String sql;
    @JsonProperty("start_time")
    private long startTime;
    @JsonProperty("running_seconds")
    private int runningSec;
    @JsonProperty("server")
    private String server;
    @JsonProperty("thread")
    private String thread;

    public BadQueryEntry(String sql, String adj, long startTime, int runningSec, String server, String thread) {
        this.updateRandomUuid();
        this.adj = adj;
        this.sql = sql;
        this.startTime = startTime;
        this.runningSec = runningSec;
        this.server = server;
        this.thread = thread;
    }

    public BadQueryEntry() {
    }

    public int getRunningSec() {
        return runningSec;
    }

    public void setRunningSec(int runningSec) {
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

    @Override
    public int compareTo(BadQueryEntry obj) {
        return this.startTime >= obj.startTime ? 1 : -1;
    }

    @Override
    public String toString() {
        return "BadQueryEntry [ adj=" + adj + ", server=" + server + ", startTime=" + DateFormat.formatToTimeStr(startTime) + "]";
    }
}
