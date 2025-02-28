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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.blacklist.SQLBlacklist;
import org.apache.kylin.query.blacklist.SQLBlacklistItem;
import org.apache.kylin.query.blacklist.SQLBlacklistManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.SQLBlacklistItemRequest;
import org.apache.kylin.rest.request.SQLBlacklistRequest;
import org.springframework.stereotype.Component;

@Component("querySQLBlacklistService")
public class QuerySQLBlacklistService extends BasicService {

    private SQLBlacklistManager getSQLBlacklistManager() {
        return SQLBlacklistManager.getInstance(getConfig());
    }

    public SQLBlacklist getSqlBlacklist(String project) {
        return getSQLBlacklistManager().getSqlBlacklist(project);
    }

    @Transaction(project = 0)
    public SQLBlacklist saveSqlBlacklist(SQLBlacklistRequest sqlBlacklistRequest) throws IOException {
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        sqlBlacklist.setProject(sqlBlacklistRequest.getProject());
        List<SQLBlacklistItemRequest> itemRequestList = sqlBlacklistRequest.getBlacklistItems();
        List<SQLBlacklistItem> itemList = Lists.newArrayList();
        if (null != itemRequestList) {
            for (SQLBlacklistItemRequest itemRequest : itemRequestList) {
                SQLBlacklistItem item = new SQLBlacklistItem();
                item.updateRandomUuid();
                item.setSql(itemRequest.getSql());
                item.setRegex(itemRequest.getRegex());
                item.setConcurrentLimit(itemRequest.getConcurrentLimit());
                itemList.add(item);
            }
        }
        sqlBlacklist.setBlacklistItems(itemList);
        return getSQLBlacklistManager().saveSqlBlacklist(sqlBlacklist);
    }

    public SQLBlacklistItem getItemById(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        return getSQLBlacklistManager().getSqlBlacklistItemById(project, sqlBlacklistItemRequest.getId());
    }

    public SQLBlacklistItem getItemByRegex(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        String regex = sqlBlacklistItemRequest.getRegex();
        if (null == regex) {
            return null;
        }
        return getSQLBlacklistManager().getSqlBlacklistItemByRegex(project, regex);
    }

    public SQLBlacklistItem getItemBySql(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        String sql = sqlBlacklistItemRequest.getSql();
        if (null == sql) {
            return null;
        }
        return getSQLBlacklistManager().getSqlBlacklistItemBySql(project, sql);
    }

    @Transaction(project = 0)
    public SQLBlacklist addSqlBlacklistItem(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest)
            throws IOException {
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.updateRandomUuid();
        sqlBlacklistItem.setRegex(sqlBlacklistItemRequest.getRegex());
        sqlBlacklistItem.setSql(sqlBlacklistItemRequest.getSql());
        sqlBlacklistItem.setConcurrentLimit(sqlBlacklistItemRequest.getConcurrentLimit());
        return getSQLBlacklistManager().addSqlBlacklistItem(project, sqlBlacklistItem);
    }

    public SQLBlacklistItem checkConflictRegex(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        String itemId = sqlBlacklistItemRequest.getId();
        String regex = sqlBlacklistItemRequest.getRegex();
        SQLBlacklist sqlBlacklist = getSQLBlacklistManager().getSqlBlacklist(project);
        if (null == regex || null == sqlBlacklist) {
            return null;
        }
        SQLBlacklistItem originItem = sqlBlacklist.getSqlBlacklistItem(itemId);
        SQLBlacklistItem regexItem = sqlBlacklist.getSqlBlacklistItemByRegex(regex);
        if (null != regexItem && !regexItem.getId().equals(originItem.getId())) {
            return regexItem;
        }
        return null;
    }

    public SQLBlacklistItem checkConflictSql(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest) {
        String itemId = sqlBlacklistItemRequest.getId();
        String sql = sqlBlacklistItemRequest.getSql();
        SQLBlacklist sqlBlacklist = getSQLBlacklistManager().getSqlBlacklist(project);
        if (null == sql || null == sqlBlacklist) {
            return null;
        }
        SQLBlacklistItem originItem = sqlBlacklist.getSqlBlacklistItem(itemId);
        SQLBlacklistItem sqlItem = sqlBlacklist.getSqlBlacklistItemBySql(sql);
        if (null != sqlItem && !sqlItem.getId().equals(originItem.getId())) {
            return sqlItem;
        }
        return null;
    }

    @Transaction(project = 0)
    public SQLBlacklist updateSqlBlacklistItem(String project, SQLBlacklistItemRequest sqlBlacklistItemRequest)
            throws IOException {
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setId(sqlBlacklistItemRequest.getId());
        sqlBlacklistItem.setRegex(sqlBlacklistItemRequest.getRegex());
        sqlBlacklistItem.setSql(sqlBlacklistItemRequest.getSql());
        sqlBlacklistItem.setConcurrentLimit(sqlBlacklistItemRequest.getConcurrentLimit());
        return getSQLBlacklistManager().updateSqlBlacklistItem(project, sqlBlacklistItem);
    }

    @Transaction(project = 0)
    public SQLBlacklist deleteSqlBlacklistItem(String project, String id) throws IOException {
        return getSQLBlacklistManager().deleteSqlBlacklistItem(project, id);
    }

    @Transaction(project = 0)
    public SQLBlacklist clearSqlBlacklist(String project) throws IOException {
        return getSQLBlacklistManager().clearBlacklist(project);
    }
}
