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
import java.util.Map;

import com.google.common.collect.Lists;

/**
 * if you're adding/removing fields from SQLRequest, take a look at getCacheKey
 */
public class SQLRequest implements Serializable {
    protected static final long serialVersionUID = 1L;

    private String sql;

    private String project;
    private String username = "";
    private Integer offset = 0;
    private Integer limit = 0;
    private boolean acceptPartial = false;

    private Map<String, String> backdoorToggles;

    protected volatile Object cacheKey = null;

    public SQLRequest() {
    }

    public Map<String, String> getBackdoorToggles() {
        return backdoorToggles;
    }

    public void setBackdoorToggles(Map<String, String> backdoorToggles) {
        this.backdoorToggles = backdoorToggles;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Integer getOffset() {
        return offset == null ? 0 : offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Integer getLimit() {
        return limit == null ? 0 : limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public boolean isAcceptPartial() {
        return acceptPartial;
    }

    public void setAcceptPartial(boolean acceptPartial) {
        this.acceptPartial = acceptPartial;
    }

    public Object getCacheKey() {
        if (cacheKey != null)
            return cacheKey;

        cacheKey = Lists.newArrayList(sql.replaceAll("[ ]", " ") //
                , project //
                , offset //
                , limit //
                , acceptPartial //
                , backdoorToggles //
                , username);
        return cacheKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SQLRequest that = (SQLRequest) o;

        if (acceptPartial != that.acceptPartial)
            return false;
        if (sql != null ? !sql.equals(that.sql) : that.sql != null)
            return false;
        if (project != null ? !project.equals(that.project) : that.project != null)
            return false;
        if (offset != null ? !offset.equals(that.offset) : that.offset != null)
            return false;
        if (limit != null ? !limit.equals(that.limit) : that.limit != null)
            return false;
        return backdoorToggles != null ? backdoorToggles.equals(that.backdoorToggles) : that.backdoorToggles == null;

    }

    @Override
    public int hashCode() {
        int result = sql != null ? sql.hashCode() : 0;
        result = 31 * result + (project != null ? project.hashCode() : 0);
        result = 31 * result + (offset != null ? offset.hashCode() : 0);
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        result = 31 * result + (acceptPartial ? 1 : 0);
        result = 31 * result + (backdoorToggles != null ? backdoorToggles.hashCode() : 0);
        return result;
    }
}
