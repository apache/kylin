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

package org.apache.kylin.query.blacklist;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.common.util.RandomUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class SQLBlacklistItem implements java.io.Serializable {

    @JsonProperty("id")
    private String id;

    @JsonProperty("regex")
    private String regex;

    @JsonProperty("sql")
    private String sql;

    @JsonProperty("concurrent_limit")
    private int concurrentLimit;

    private Pattern pattern;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void updateRandomUuid() {
        setId(RandomUtil.randomUUIDStr());
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
        if (null != this.regex) {
            pattern = Pattern.compile(regex);
        }
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean match(String sql) {
        if (null != this.sql && this.sql.equals(sql)) {
            return true;
        }
        if (null == this.regex) {
            return false;
        }
        if (null == this.pattern) {
            this.pattern = Pattern.compile(regex);
        }
        Matcher matcher = this.pattern.matcher(sql);
        return matcher.matches();
    }

    public int getConcurrentLimit() {
        return concurrentLimit;
    }

    public void setConcurrentLimit(int concurrentLimit) {
        this.concurrentLimit = concurrentLimit;
    }
}
