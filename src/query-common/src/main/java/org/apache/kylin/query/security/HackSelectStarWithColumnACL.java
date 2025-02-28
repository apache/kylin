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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import lombok.Getter;

public class HackSelectStarWithColumnACL implements IQueryTransformer, IPushDownConverter {

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
            throw new KylinRuntimeException("Failed to parse invalid SQL: " + sql);
        }

        // Make sure the position of the resolved's sqlNode in the sql is from left to right
        SelectStarAuthVisitor replacer = new SelectStarAuthVisitor(project, defaultSchema, aclLocal);
        sqlNode.accept(replacer);
        Map<SqlNode, String> resolved = replacer.getResolved();
        if (resolved.isEmpty()) {
            return sql;
        }
        StringBuilder sb = new StringBuilder();
        AtomicInteger offset = new AtomicInteger(0);
        resolved.forEach((node, replaced) -> {
            Pair<Integer, Integer> replacePos = CalciteParser.getReplacePos(node, sql);
            sb.append(sql, offset.get(), replacePos.getKey());
            sb.append(replaced);
            offset.set(replacePos.getValue());
        });
        sb.append(sql.substring(offset.get()));
        return sb.toString();
    }

    static class SelectStarAuthVisitor extends SqlBasicVisitor<SqlNode> {
        @Getter
        Map<SqlNode, String> resolved = new LinkedHashMap<>();
        /** A cache to avoid the same table generate the replaced sql twice. */
        private final Map<String, String> tableToReplacedSubQuery = new HashMap<>();
        private final Set<String> namesOfWithItems = new HashSet<>();
        private final String defaultSchema;
        private final List<AclTCR> aclTCRList;
        private final NTableMetadataManager tableMgr;
        private final boolean sourceNameCaseSensitiveEnabled;
        private final boolean isPushdownSelectStarCaseSensitiveEnable;
        private final boolean isPushdownSelectStarLowerCaseEnable;

        SelectStarAuthVisitor(String project, String defaultSchema, QueryContext.AclInfo aclInfo) {
            this.defaultSchema = defaultSchema;
            KylinConfig config = NProjectManager.getProjectConfig(project);
            this.sourceNameCaseSensitiveEnabled = config.getSourceNameCaseSensitiveEnabled();
            this.isPushdownSelectStarCaseSensitiveEnable = config.getPushdownSelectStarCaseSensitiveEnable();
            this.isPushdownSelectStarLowerCaseEnable = config.getPushdownSelectStarLowercaseEnable();
            this.tableMgr = NTableMetadataManager.getInstance(config, project);
            // init aclTCR
            String user = Objects.nonNull(aclInfo) ? aclInfo.getUsername() : null;
            Set<String> groups = Objects.nonNull(aclInfo) ? aclInfo.getGroups() : null;
            aclTCRList = AclTCRManager.getInstance(config, project).getAclTCRs(user, groups);
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (SqlNode node : nodeList) {
                if (node instanceof SqlWithItem) {
                    SqlWithItem item = (SqlWithItem) node;
                    item.query.accept(this);
                    namesOfWithItems.add(item.name.toString());
                } else if (node instanceof SqlCall) {
                    node.accept(this);
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                select.getSelectList().accept(this);
                markCall(select.getFrom());
            }
            if (call instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) call;
                basicCall.getOperandList().stream().filter(Objects::nonNull).forEach(node -> node.accept(this));
            }

            if (call instanceof SqlJoin) {
                markCall(((SqlJoin) call).getLeft());
                markCall(((SqlJoin) call).getRight());
            }
            if (call instanceof SqlWith) {
                SqlWith sqlWith = (SqlWith) call;
                sqlWith.withList.accept(this);
                sqlWith.body.accept(this);
            }
            if (call instanceof SqlOrderBy) {
                call.getOperandList().stream().filter(Objects::nonNull).forEach(node -> node.accept(this));
            }
            return null;
        }

        private void markCall(SqlNode operand) {
            if (Objects.isNull(operand)) {
                return;
            } else if (operand instanceof SqlIdentifier) {
                String replaced = markTableIdentifier((SqlIdentifier) operand, null);
                resolved.put(operand, replaced);
                return;
            } else if (isCallWithAlias(operand)) {
                List<SqlNode> operandList = ((SqlBasicCall) operand).getOperandList();
                SqlNode tableNode = operandList.get(0);
                if (tableNode instanceof SqlIdentifier) {
                    String replaced = markTableIdentifier((SqlIdentifier) tableNode, operandList.get(1));
                    resolved.put(operand, replaced);
                } else {
                    tableNode.accept(this);
                }
                return;
            }

            // a sub-query, continue to mark
            operand.accept(this);
        }

        private boolean isCallWithAlias(SqlNode from) {
            return from instanceof SqlBasicCall && from.getKind() == SqlKind.AS;
        }

        private String markTableIdentifier(SqlIdentifier operand, SqlNode alias) {
            if (namesOfWithItems.contains(operand.toString())) {
                String withItemName = StringHelper.doubleQuote(operand.toString());
                return alias == null ? withItemName
                        : withItemName + " as " + StringHelper.doubleQuote(alias.toString());
            }
            List<String> names = operand.names;
            String schema = names.size() == 1 ? defaultSchema : names.get(0);
            String table = names.size() == 1 ? names.get(0) : names.get(1);
            String tableMetadataKey = schema + '.' + table;
            if (!sourceNameCaseSensitiveEnabled) {
                tableMetadataKey = tableMetadataKey.toUpperCase(Locale.ROOT);
            }
            TableDesc tableDesc = tableMgr.getTableDesc(tableMetadataKey);
            if (tableDesc == null) {
                throw new KylinRuntimeException("Failed to parse table: " + operand);
            }
            String subQueryAlias = alias == null ? StringHelper.doubleQuote(table)
                    : StringHelper.doubleQuote(alias.toString());
            String tableIdentity = tableDesc.getDoubleQuoteIdentity();
            String tableToReplacedSubQueryKey = tableIdentity + subQueryAlias;
            if (tableToReplacedSubQuery.containsKey(tableToReplacedSubQueryKey)) {
                return tableToReplacedSubQuery.get(tableToReplacedSubQueryKey);
            } else {
                List<String> authorizedCols = getAuthorizedCols(tableDesc);
                String replacedSubQuery = "( select " + String.join(", ", authorizedCols) //
                        + " from " + tableIdentity + ") as " + subQueryAlias;
                tableToReplacedSubQuery.put(tableToReplacedSubQueryKey, replacedSubQuery);
                return replacedSubQuery;
            }
        }

        List<String> getAuthorizedCols(TableDesc tableDesc) {
            List<String> colList = new ArrayList<>();
            List<ColumnDesc> columns = Arrays.stream(tableDesc.getColumns()) //
                    .sorted(Comparator.comparing(ColumnDesc::getZeroBasedIndex)) //
                    .collect(Collectors.toList());
            for (ColumnDesc column : columns) {
                for (AclTCR aclTCR : aclTCRList) {
                    if (aclTCR.isAuthorized(tableDesc.getIdentity(), column.getName())) {
                        colList.add(getQuotedColName(column));
                        break;
                    }
                }
            }
            return colList;
        }

        String getQuotedColName(ColumnDesc column) {
            if (isPushdownSelectStarLowerCaseEnable) {
                return StringHelper.doubleQuote(column.getTable().getName()) + "."
                        + StringHelper.doubleQuote(StringUtils.lowerCase(column.getName()));
            }
            if (isPushdownSelectStarCaseSensitiveEnable) {
                // caseSensitiveName is required to have a value.
                // If caseSensitiveName is null, then name will be used,
                // but using name will not guarantee case sensitivity.
                return StringHelper.doubleQuote(column.getTable().getName()) + "."
                        + StringHelper.doubleQuote(column.getCaseSensitiveName());
            }
            return StringHelper.doubleQuote(column.getTable().getName()) + "."
                    + StringHelper.doubleQuote(column.getName());
        }
    }
}
