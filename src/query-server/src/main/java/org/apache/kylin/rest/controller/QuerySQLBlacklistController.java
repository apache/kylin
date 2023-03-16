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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_ITEM_ID_EMPTY;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_PROJECT_EMPTY;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_REGEX_AND_SQL_EMPTY;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_REGEX_EMPTY;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_REGEX_EXISTS;
import static org.apache.kylin.common.exception.ServerErrorCode.BLACKLIST_SQL_EXISTS;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.query.blacklist.SQLBlacklist;
import org.apache.kylin.query.blacklist.SQLBlacklistItem;
import org.apache.kylin.rest.request.SQLBlacklistItemRequest;
import org.apache.kylin.rest.request.SQLBlacklistRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.QuerySQLBlacklistService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/query_sql_blacklist", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON,
        HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class QuerySQLBlacklistController extends NBasicController {

    @Autowired
    @Qualifier("querySQLBlacklistService")
    private QuerySQLBlacklistService querySQLBlacklistService;

    @ApiOperation(value = "getSqlBlacklist", tags = { "QE" })
    @GetMapping(value = "/{project}")
    @ResponseBody
    public EnvelopeResponse getSqlBlacklist(@PathVariable(value = "project") String project) {
        Message msg = Message.getInstance();
        if (null == project) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSqlBlacklistItemProjectEmpty());
        }
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.getSqlBlacklist(project);
        return new EnvelopeResponse(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    @ApiOperation(value = "overwrite", tags = { "QE" })
    @PostMapping(value = "/overwrite")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> overwrite(@RequestBody SQLBlacklistRequest sqlBlacklistRequest)
            throws IOException {
        Message msg = Message.getInstance();
        if (null == sqlBlacklistRequest.getProject()) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSqlBlacklistItemProjectEmpty());
        }
        validateSqlBlacklist(sqlBlacklistRequest.getBlacklistItems());
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.saveSqlBlacklist(sqlBlacklistRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    private void validateSqlBlacklist(List<SQLBlacklistItemRequest> sqlBlacklistItemRequests) {
        Message msg = Message.getInstance();

        Set<String> regexSet = Sets.newHashSet();
        Set<String> sqlSet = Sets.newHashSet();
        for (SQLBlacklistItemRequest item : sqlBlacklistItemRequests) {
            if (null == item.getRegex() && null == item.getSql()) {
                throw new KylinException(BLACKLIST_REGEX_AND_SQL_EMPTY, msg.getSqlBlacklistItemRegexAndSqlEmpty());
            }
            String regex = item.getRegex();
            if (null != regex && regexSet.contains(regex)) {
                throw new KylinException(BLACKLIST_REGEX_EXISTS, msg.getSqlBlacklistItemRegexExists());
            }
            String sql = item.getSql();
            if (null != sql && sqlSet.contains(sql)) {
                throw new KylinException(BLACKLIST_SQL_EXISTS, msg.getSqlBlacklistItemSqlExists());
            }
            if (null != regex) {
                regexSet.add(item.getRegex());
            }
            if (null != sql) {
                sqlSet.add(item.getSql());
            }
        }
    }

    @ApiOperation(value = "add_item", tags = { "QE" })
    @PostMapping(value = "/add_item/{project}")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> addItem(@PathVariable(value = "project") String project,
            @RequestBody SQLBlacklistItemRequest sqlBlacklistItemRequest) throws IOException {
        Message msg = Message.getInstance();
        if (null == sqlBlacklistItemRequest.getRegex() && null == sqlBlacklistItemRequest.getSql()) {
            throw new KylinException(BLACKLIST_REGEX_AND_SQL_EMPTY, msg.getSqlBlacklistItemRegexAndSqlEmpty());
        }
        if (null == project) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSqlBlacklistItemProjectEmpty());
        }
        SQLBlacklistItem sqlBlacklistItemOfRegex = querySQLBlacklistService.getItemByRegex(project,
                sqlBlacklistItemRequest);
        if (null != sqlBlacklistItemOfRegex) {
            throw new KylinException(BLACKLIST_REGEX_EMPTY, String.format(Locale.ROOT, msg.getSqlBlacklistItemRegexExists(), sqlBlacklistItemOfRegex.getId()));
        }
        SQLBlacklistItem sqlBlacklistItemOfSql = querySQLBlacklistService.getItemBySql(project,
                sqlBlacklistItemRequest);
        if (null != sqlBlacklistItemOfSql) {
            throw new KylinException(BLACKLIST_SQL_EXISTS, String.format(Locale.ROOT, msg.getSqlBlacklistItemSqlExists(), sqlBlacklistItemOfSql.getId()));
        }

        SQLBlacklist sqlBlacklist = querySQLBlacklistService.addSqlBlacklistItem(project, sqlBlacklistItemRequest);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    @ApiOperation(value = "update_item", tags = { "QE" })
    @PostMapping(value = "/update_item/{project}")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> updateItem(@PathVariable(value = "project") String project,
            @RequestBody SQLBlacklistItemRequest sqlBlacklistItemRequest) throws IOException {
        Message msg = Message.getInstance();
        if (null == sqlBlacklistItemRequest.getId()) {
            throw new KylinException(BLACKLIST_ITEM_ID_EMPTY, msg.getSqlBlacklistItemIdEmpty());
        }
        if (null == sqlBlacklistItemRequest.getRegex() && null == sqlBlacklistItemRequest.getSql()) {
            throw new KylinException(BLACKLIST_REGEX_AND_SQL_EMPTY, msg.getSqlBlacklistItemRegexAndSqlEmpty());
        }
        if (null == project) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSqlBlacklistItemProjectEmpty());
        }
        if (null == querySQLBlacklistService.getItemById(project, sqlBlacklistItemRequest)) {
            throw new KylinException(BLACKLIST_ITEM_ID_EMPTY, msg.getSqlBlacklistItemIdNotExists());
        }
        SQLBlacklistItem conflictRegexItem = querySQLBlacklistService.checkConflictRegex(project,
                sqlBlacklistItemRequest);
        if (null != conflictRegexItem) {
            throw new KylinException(BLACKLIST_REGEX_EMPTY, String.format(Locale.ROOT, msg.getSqlBlacklistItemRegexExists(), conflictRegexItem.getId()));
        }
        SQLBlacklistItem conflictSqlItem = querySQLBlacklistService.checkConflictSql(project, sqlBlacklistItemRequest);
        if (null != conflictSqlItem) {
            throw new KylinException(BLACKLIST_SQL_EXISTS, String.format(Locale.ROOT, msg.getSqlBlacklistItemSqlExists(), conflictSqlItem.getId()));
        }
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.updateSqlBlacklistItem(project, sqlBlacklistItemRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    @ApiOperation(value = "delete_item", tags = { "QE" })
    @DeleteMapping(value = "/delete_item/{project}/{id}")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> deleteItem(@PathVariable(value = "project") String project,
            @PathVariable(value = "id") String id) throws IOException {
        Message msg = Message.getInstance();
        if (null == project) {
            throw new KylinException(BLACKLIST_PROJECT_EMPTY, msg.getSqlBlacklistItemProjectEmpty());
        }
        if (null == id) {
            throw new KylinException(BLACKLIST_ITEM_ID_EMPTY, msg.getSqlBlacklistItemIdToDeleteEmpty());
        }
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.deleteSqlBlacklistItem(project, id);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }

    @ApiOperation(value = "clearBlacklist", tags = { "QE" })
    @DeleteMapping(value = "/clear/{project}")
    @ResponseBody
    public EnvelopeResponse<SQLBlacklist> clearBlacklist(@PathVariable String project) throws IOException {
        SQLBlacklist sqlBlacklist = querySQLBlacklistService.clearSqlBlacklist(project);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, sqlBlacklist, "");
    }
}
