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
package org.apache.kylin.query.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.query.exception.NoAuthorizedColsError;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class HackSelectStarWithColumnACL implements IQueryTransformer, IPushDownConverter {

    private static final String SELECT_STAR = "*";

    static String getNewSelectClause(SqlNode sqlNode, String project, String defaultSchema,
            QueryContext.AclInfo aclInfo) {
        StringBuilder newSelectClause = new StringBuilder();
        List<String> allCols = getColsCanAccess(sqlNode, project, defaultSchema, aclInfo);
        if (CollectionUtils.isEmpty(allCols)) {
            throw new NoAuthorizedColsError();
        }
        for (String col : allCols) {
            if (!col.equals(allCols.get(allCols.size() - 1))) {
                newSelectClause.append(col).append(", ");
            } else {
                newSelectClause.append(col);
            }
        }
        return newSelectClause.toString();
    }

    static List<String> getColsCanAccess(SqlNode sqlNode, String project, String defaultSchema,
            QueryContext.AclInfo aclInfo) {
        List<String> cols = new ArrayList<>();
        String user = Objects.nonNull(aclInfo) ? aclInfo.getUsername() : null;
        Set<String> groups = Objects.nonNull(aclInfo) ? aclInfo.getGroups() : null;
        final List<AclTCR> aclTCRs = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getAclTCRs(user, groups);
        List<RowFilter.Table> tblWithAlias = RowFilter.getTblWithAlias(defaultSchema, getSingleSelect(sqlNode));
        for (RowFilter.Table table : tblWithAlias) {
            TableDesc tableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getTableDesc(table.getName());
            if (Objects.isNull(tableDesc)) {
                throw new IllegalStateException(
                        "Table " + table.getAlias() + " not found. Please add table " + table.getAlias()
                                + " to data source. If this table does exist, mention it as DATABASE.TABLE.");
            }

            List<ColumnDesc> columns = Lists.newArrayList(tableDesc.getColumns());
            Collections.sort(columns, Comparator.comparing(ColumnDesc::getZeroBasedIndex));
            String quotingChar = Quoting.valueOf(KylinConfig.getInstanceFromEnv().getCalciteQuoting()).string;
            for (ColumnDesc column : columns) {
                if (aclTCRs.stream()
                        .anyMatch(aclTCR -> aclTCR.isAuthorized(tableDesc.getIdentity(), column.getName()))) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(quotingChar).append(table.getAlias()).append(quotingChar) //
                            .append('.') //
                            .append(quotingChar).append(column.getName()).append(quotingChar);
                    cols.add(sb.toString());
                }
            }
        }
        return cols;
    }

    private static boolean isSingleSelectStar(SqlNode sqlNode) {
        if (SelectNumVisitor.getSelectNum(sqlNode) != 1 || sqlNode instanceof SqlExplain) {
            return false;
        }
        SqlSelect singleSelect = getSingleSelect(sqlNode);
        return singleSelect.getSelectList().toString().equals(SELECT_STAR);
    }

    private static int getSelectStarPos(String sql, SqlNode sqlNode) {
        SqlSelect singleSelect = getSingleSelect(sqlNode);
        Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(singleSelect.getSelectList(), sql);
        Preconditions.checkState(replacePos.getSecond() - replacePos.getFirst() == 1);
        return replacePos.getFirst();
    }

    private static SqlSelect getSingleSelect(SqlNode sqlNode) {
        if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
            return (SqlSelect) orderBy.query;
        } else {
            return (SqlSelect) sqlNode;
        }
    }

    private static boolean hasAdminPermission(QueryContext.AclInfo aclInfo) {
        if (Objects.isNull(aclInfo) || Objects.isNull(aclInfo.getGroups())) {
            return false;
        }
        return aclInfo.getGroups().stream().anyMatch(Constant.ROLE_ADMIN::equals) || aclInfo.isHasAdminPermission();
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return transform(originSql, project, defaultSchema);
    }

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        QueryContext.AclInfo aclLocal = QueryContext.current().getAclInfo();
        if (!KylinConfig.getInstanceFromEnv().isAclTCREnabled() || hasAdminPermission(aclLocal)) {
            return sql;
        }

        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql, project);
        } catch (SqlParseException e) {
            throw new KylinRuntimeException("Failed to parse SQL \'" + sql + "\', please make sure the SQL is valid");
        }

        if (!isSingleSelectStar(sqlNode)) {
            return sql;
        }

        String newSelectClause = getNewSelectClause(sqlNode, project, defaultSchema, aclLocal);
        int selectStarPos = getSelectStarPos(sql, sqlNode);
        StringBuilder result = new StringBuilder(sql);
        result.replace(selectStarPos, selectStarPos + 1, newSelectClause);
        return result.toString();
    }

    static class SelectNumVisitor extends SqlBasicVisitor<SqlNode> {
        int selectNum = 0;

        static int getSelectNum(SqlNode sqlNode) {
            SelectNumVisitor snv = new SelectNumVisitor();
            sqlNode.accept(snv);
            return snv.getNum();
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                selectNum++;
            }
            if (call instanceof SqlOrderBy) {
                SqlOrderBy sqlOrderBy = (SqlOrderBy) call;
                sqlOrderBy.query.accept(this);
            } else {
                for (SqlNode operand : call.getOperandList()) {
                    if (operand != null) {
                        operand.accept(this);
                    }
                }
            }
            return null;
        }

        private int getNum() {
            return selectNum;
        }
    }
}
