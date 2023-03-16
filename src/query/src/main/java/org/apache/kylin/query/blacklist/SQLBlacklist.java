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

import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_PROJECT;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.Data;

@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SQLBlacklist extends RootPersistentEntity implements Serializable {

    public static final String SQL_BLACKLIST_RESOURCE_ROOT = GLOBAL_PROJECT + "/sql_blacklist";

    public SQLBlacklist() {
        updateRandomUuid();
    }

    @JsonProperty("project")
    private String project;

    @JsonProperty("blacklist_items")
    private List<SQLBlacklistItem> blacklistItems;

    public List<SQLBlacklistItem> getBlacklistItems() {
        return blacklistItems;
    }

    public void setBlacklistItems(List<SQLBlacklistItem> blacklistItems) {
        this.blacklistItems = blacklistItems;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public void addBlacklistItem(SQLBlacklistItem sqlBlacklistItem) {
        if (null == this.blacklistItems) {
            this.blacklistItems = Lists.newArrayList();
        }
        this.blacklistItems.add(sqlBlacklistItem);
    }

    public SQLBlacklistItem getSqlBlacklistItem(String id) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return null;
        }
        for (SQLBlacklistItem sqlBlacklistItem : blacklistItems) {
            if (id.equals(sqlBlacklistItem.getId())) {
                return sqlBlacklistItem;
            }
        }
        return null;
    }

    public SQLBlacklistItem getSqlBlacklistItemByRegex(String regex) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return null;
        }
        for (SQLBlacklistItem sqlBlacklistItem : blacklistItems) {
            if (regex.equals(sqlBlacklistItem.getRegex())) {
                return sqlBlacklistItem;
            }
        }
        return null;
    }

    public SQLBlacklistItem getSqlBlacklistItemBySql(String sql) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return null;
        }
        for (SQLBlacklistItem sqlBlacklistItem : blacklistItems) {
            if (sql.equals(sqlBlacklistItem.getSql())) {
                return sqlBlacklistItem;
            }
        }
        return null;
    }

    public SQLBlacklistItem match(String sql) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return null;
        }
        for (SQLBlacklistItem sqlBlacklistItem : blacklistItems) {
            if (sqlBlacklistItem.match(sql)) {
                return sqlBlacklistItem;
            }
        }
        return null;
    }

    public void deleteSqlBlacklistItem(String id) {
        if (null == blacklistItems || blacklistItems.isEmpty()) {
            return;
        }
        List<SQLBlacklistItem> newBlacklistItems = Lists.newArrayList();
        for (SQLBlacklistItem item : blacklistItems) {
            if (!item.getId().equals(id)) {
                newBlacklistItems.add(item);
            }
        }
        setBlacklistItems(newBlacklistItems);
    }

    @Override
    public String resourceName() {
        return project;
    }
}
