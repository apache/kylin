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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.INVALID_SQL_FORMAT;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rec.query.validator.AbstractSQLValidator;
import org.apache.kylin.rec.query.validator.SQLValidateResult;
import org.apache.kylin.rec.query.validator.SqlSyntaxValidator;
import org.apache.kylin.rest.response.ImportSqlResponse;
import org.apache.kylin.rest.response.SQLParserResponse;
import org.apache.kylin.rest.response.SQLValidateResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

@Component("favoriteRuleService")
public class FavoriteRuleService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(FavoriteRuleService.class);

    private static final String DEFAULT_SCHEMA = "DEFAULT";

    @Autowired
    private AclEvaluate aclEvaluate;

    public Map<String, SQLValidateResult> batchSqlValidate(List<String> sqls, String project) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        AbstractSQLValidator sqlValidator = new SqlSyntaxValidator(project, projectConfig);
        return sqlValidator.batchValidate(sqls.toArray(new String[0]));
    }

    public SQLParserResponse importSqls(MultipartFile[] files, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        List<String> sqls = Lists.newArrayList();
        List<String> wrongFormatFiles = Lists.newArrayList();

        // add user info before invoking row-filter and hack-select-star
        QueryContext.AclInfo aclInfo = AclPermissionUtil.createAclInfo(project, getCurrentUserGroups());
        QueryContext context = QueryContext.current();
        context.setAclInfo(aclInfo);
        // parse file to sqls
        for (MultipartFile file : files) {
            String fileName = file.getOriginalFilename();
            if (!StringUtils.endsWithIgnoreCase(fileName, ".sql")
                    && !StringUtils.endsWithIgnoreCase(fileName, ".txt")) {
                wrongFormatFiles.add(file.getOriginalFilename());
                continue;
            }
            try {
                sqls.addAll(transformFileToSqls(file, project));
            } catch (Exception | Error ex) {
                logger.error("[UNEXPECTED_THINGS_HAPPENED]Error caught when parsing file {} because {} ",
                        file.getOriginalFilename(), ex);
                wrongFormatFiles.add(file.getOriginalFilename());
            }
        }
        SQLParserResponse result = new SQLParserResponse();
        KylinConfig kylinConfig = NProjectManager.getProjectConfig(project);
        if (sqls.size() > kylinConfig.getFavoriteImportSqlMaxSize()) {
            result.setSize(sqls.size());
            result.setWrongFormatFile(wrongFormatFiles);
            return result;
        }

        List<ImportSqlResponse> sqlData = Lists.newArrayList();
        int capableSqlNum = 0;

        //sql validation
        Map<String, SQLValidateResult> map = batchSqlValidate(sqls, project);
        int id = 0;

        for (Map.Entry<String, SQLValidateResult> entry : map.entrySet()) {
            String sql = entry.getKey();
            SQLValidateResult validateResult = entry.getValue();

            if (validateResult.isCapable()) {
                capableSqlNum++;
            }

            ImportSqlResponse sqlResponse = new ImportSqlResponse(sql, validateResult.isCapable());
            sqlResponse.setId(id);
            sqlResponse.setSqlAdvices(validateResult.getSqlAdvices());
            sqlData.add(sqlResponse);

            id++;
        }

        // make sql grammar failed sqls ordered first
        sqlData.sort((object1, object2) -> {
            boolean capable1 = object1.isCapable();
            boolean capable2 = object2.isCapable();

            if (capable1 && !capable2) {
                return 1;
            }

            if (capable2 && !capable1) {
                return -1;
            }

            return 0;
        });

        result.setData(sqlData);
        result.setSize(sqlData.size());
        result.setCapableSqlNum(capableSqlNum);
        result.setWrongFormatFile(wrongFormatFiles);

        return result;
    }

    List<String> transformFileToSqls(MultipartFile file, String project) throws IOException {
        List<String> sqls = new ArrayList<>();

        String content = new String(file.getBytes(), StandardCharsets.UTF_8);

        if (content.isEmpty()) {
            return sqls;
        }
        content = QueryUtil.removeCommentInSql(content);
        List<String> sqlList = splitBySemicolon(content);
        if (sqlList.isEmpty()) {
            return sqls;
        }
        KylinConfig kylinConfig = NProjectManager.getProjectConfig(project);
        for (String sql : sqlList) {
            if (StringUtils.isBlank(sql) || sql.replace('\n', ' ').trim().length() == 0) {
                continue;
            }
            QueryParams queryParams = new QueryParams(kylinConfig, sql, project, 0, 0, DEFAULT_SCHEMA, false);
            queryParams.setAclInfo(QueryContext.current().getAclInfo());
            // massage sql and expand CC columns
            String correctedSql = QueryUtil.massageSqlAndExpandCC(queryParams);
            sqls.add(correctedSql);
        }

        return sqls;
    }

    private List<String> splitBySemicolon(String s) {
        List<String> r = Lists.newArrayList();
        StringBuilder sb = new StringBuilder();
        boolean withinQuote = false;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\'') {
                withinQuote = !withinQuote;
            }
            if (s.charAt(i) == ';' && !withinQuote) {
                if (sb.length() != 0) {
                    r.add(sb.toString());
                    sb = new StringBuilder();
                }
                continue;
            }
            sb.append(s.charAt(i));
        }
        if (sb.length() != 0) {
            r.add(sb.toString());
        }
        return r;
    }

    public SQLValidateResponse sqlValidate(String project, String sql) {
        aclEvaluate.checkProjectWritePermission(project);
        KylinConfig kylinConfig = NProjectManager.getProjectConfig(project);
        QueryParams queryParams = new QueryParams(kylinConfig, sql, project, 0, 0, DEFAULT_SCHEMA, false);
        queryParams.setAclInfo(AclPermissionUtil.createAclInfo(project, getCurrentUserGroups()));
        String correctedSql = QueryUtil.massageSql(queryParams);
        // sql validation
        Map<String, SQLValidateResult> map = batchSqlValidate(Lists.newArrayList(correctedSql), project);
        SQLValidateResult result = map.get(correctedSql);
        if (result == null) {
            throw new KylinException(INVALID_SQL_FORMAT);
        }

        return new SQLValidateResponse(result.isCapable(), result.getSqlAdvices());
    }
}
