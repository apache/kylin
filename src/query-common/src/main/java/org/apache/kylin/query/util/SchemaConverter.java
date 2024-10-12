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
package org.apache.kylin.query.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaConverter implements IPushDownConverter {

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        KylinConfig config = NProjectManager.getProjectConfig(project);
        if (!config.isInternalTableEnabled()) {
            log.debug("PushdownToInternal is not enabled, skip it.");
            return originSql;
        }
        if (!QueryContext.current().getQueryTagInfo().isPushdown()) {
            log.debug("Pushdown tag is not found, skip it.");
            return originSql;
        }
        if (QueryContext.current().getQueryTagInfo().isAsyncQuery() && config.isUniqueAsyncQueryYarnQueue()) {
            log.debug("Async query, skip it");
            return originSql;
        }
        try {
            String transformedSql = transform(originSql, project, config);
            QueryContext.current().setPushdownEngine("GLUTEN");
            return transformedSql;
        } catch (Exception e) {
            log.error("Convert failed! return origin SQL: {}", originSql, e);
            Thread.currentThread().interrupt();
            QueryContext.current().setPushdownEngine(QueryContext.PUSHDOWN_GLUTEN);
            QueryContext.current().getQueryTagInfo().setErrInterrupted(true);
            QueryContext.current().getQueryTagInfo().setInterruptReason(e.getMessage());
            return originSql;
        }

    }

    public String transform(String originSql, String project, KylinConfig config) throws SqlParseException {
        SqlNode node = CalciteParser.parse(originSql, project);
        TableNameVisitor visitor = new TableNameVisitor(originSql);
        node.accept(visitor);
        List<Pair<SqlIdentifier, Pair<Integer, Integer>>> tableNamesWithPos = visitor.getTableNamesWithPos();
        return replaceDbNameAndAddCatalog(tableNamesWithPos, originSql, config, project);
    }

    private String replaceDbNameAndAddCatalog(List<Pair<SqlIdentifier, Pair<Integer, Integer>>> positions,
            String originSql, KylinConfig config, String project) {
        positions.sort(((o1, o2) -> o2.getSecond().getFirst() - o1.getSecond().getFirst()));
        String sql = originSql + " ";
        InternalTableManager manager = InternalTableManager.getInstance(config, project);
        for (Pair<SqlIdentifier, Pair<Integer, Integer>> pos : positions) {
            String tableIdentity = pos.getFirst().toString();
            InternalTableDesc table = manager.getInternalTableDesc(tableIdentity);
            if (table == null) {
                throw new IllegalStateException("Table " + tableIdentity + " is not an internal table.");
            }
            sql = sql.substring(0, pos.getSecond().getFirst()) + table.getDoubleQuoteInternalIdentity()
                    + sql.substring(pos.getSecond().getSecond());
        }
        return sql.trim();
    }

    static class TableNameVisitor extends SqlBasicVisitor<SqlNode> {
        @Getter
        private final List<Pair<SqlIdentifier, Pair<Integer, Integer>>> tableNamesWithPos = new ArrayList<>();
        private final String originSql;

        public TableNameVisitor(String originSql) {
            this.originSql = originSql;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if (call.getKind() == SqlKind.SELECT) {
                SqlNode from = ((SqlSelect) call).getFrom();
                checkIdentifier(from);
            } else if (call.getKind() == SqlKind.JOIN) {
                SqlJoin join = (SqlJoin) call;
                checkIdentifier(join.getLeft());
                checkIdentifier(join.getRight());
            }
            return super.visit(call);
        }

        private void checkIdentifier(SqlNode node) {
            if (node instanceof SqlBasicCall && node.getKind() == SqlKind.AS) {
                node = ((SqlBasicCall) node).operand(0);
            }
            if (node instanceof SqlIdentifier && ((SqlIdentifier) node).names.size() == 2) {
                tableIdentifierFound((SqlIdentifier) node);
            }
        }

        public void tableIdentifierFound(SqlIdentifier node) {
            Pair<Integer, Integer> pos = CalciteParser.getReplacePos(node, originSql);
            tableNamesWithPos.add(new Pair<>(node, pos));
        }

    }
}
