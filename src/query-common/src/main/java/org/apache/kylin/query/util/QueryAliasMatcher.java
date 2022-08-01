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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinsGraph;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.apache.kylin.query.schema.OLAPTable;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.alias.ExpressionComparator;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.schema.KapOLAPSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

// match alias in query to alias in model
// Not designed to reuse, re-new per query
public class QueryAliasMatcher {
    static final ColumnRowType MODEL_VIEW_COLUMN_ROW_TYPE = new ColumnRowType(new ArrayList<>());
    private static final Logger logger = LoggerFactory.getLogger(QueryAliasMatcher.class);
    private static final ColumnRowType SUBQUERY_TAG = new ColumnRowType(null);
    private static final String[] COLUMN_ARRAY_MARKER = new String[0];
    private final String project;
    private final String defaultSchema;
    private final Map<String, KapOLAPSchema> schemaMap = Maps.newHashMap();
    private final Map<String, Map<String, OLAPTable>> schemaTables = Maps.newHashMap();
    public QueryAliasMatcher(String project, String defaultSchema) {
        this.project = project;
        this.defaultSchema = defaultSchema;
    }

    /**
     * only return null if it's a column on subquery
     */
    static TblColRef resolveTblColRef(SqlIdentifier sqlIdentifier, LinkedHashMap<String, ColumnRowType> alias2CRT) {
        TblColRef ret = null;
        ImmutableList<String> namesOfIdentifier = sqlIdentifier.names;
        if (namesOfIdentifier.size() == 3) {
            // db.tableAlias.colName
            String tableAlias = namesOfIdentifier.get(1);
            String colName = namesOfIdentifier.get(2);
            ColumnRowType columnRowType = alias2CRT.get(tableAlias);
            Preconditions.checkState(columnRowType != null, "Alias " + tableAlias + " is not defined");
            return columnRowType == QueryAliasMatcher.SUBQUERY_TAG ? null : columnRowType.getColumnByName(colName);
        } else if (namesOfIdentifier.size() == 2) {
            // tableAlias.colName
            String tableAlias = namesOfIdentifier.get(0);
            String colName = namesOfIdentifier.get(1);
            ColumnRowType columnRowType = alias2CRT.get(tableAlias);
            Preconditions.checkState(columnRowType != null, "Alias " + tableAlias + " is not defined");
            return columnRowType == QueryAliasMatcher.SUBQUERY_TAG ? null : columnRowType.getColumnByName(colName);
        } else if (namesOfIdentifier.size() == 1) {
            // only colName
            String col = namesOfIdentifier.get(0);
            ret = resolveTblColRef(alias2CRT, col);
        }

        return ret;
    }

    static TblColRef resolveTblColRef(LinkedHashMap<String, ColumnRowType> alias2CRT, String col) {

        List<String> potentialAlias = Lists.newArrayList();
        for (Map.Entry<String, ColumnRowType> entry : alias2CRT.entrySet()) {
            if (entry.getValue() != QueryAliasMatcher.SUBQUERY_TAG && entry.getValue().getColumnByName(col) != null) {
                potentialAlias.add(entry.getKey());
            }
        }
        if (potentialAlias.size() == 1) {
            ColumnRowType columnRowType = alias2CRT.get(potentialAlias.get(0));
            return columnRowType.getColumnByName(col);
        } else if (potentialAlias.size() > 1) {
            throw new IllegalStateException(
                    "The column " + col + " is found on multiple alias: " + StringUtils.join(potentialAlias, ","));
        } else {
            throw new IllegalStateException("The column " + col + " can't be found");
        }
    }

    /**
     * match `sqlSelect` with the model in terms of join relations
     * @param model
     * @param sqlSelect
     * @return QueryAliasMatchInfo with
     *    1. the map(table alias in sql -> column row type)
     *    2. match result map(table alias in sql -> table alias in model)
     */
    public QueryAliasMatchInfo match(NDataModel model, SqlSelect sqlSelect) {
        if (sqlSelect.getFrom() == null || SqlKind.VALUES == sqlSelect.getFrom().getKind()) {
            return null;
        }

        SqlSelect subQuery = getSubquery(sqlSelect.getFrom());
        boolean reUseSubqeury = false;

        // find subquery with permutation only projection
        // for which the project can be eliminated
        if (subQuery != null) {

            if (subQuery.getSelectList().size() == 1 && subQuery.getSelectList().get(0).toString().equals("*")
                    && subQuery.getFrom() instanceof SqlIdentifier) {
                reUseSubqeury = true;
            } else {
                return null;
            }
        }

        SqlJoinCapturer sqlJoinCapturer = new SqlJoinCapturer(model.getAlias());

        if (reUseSubqeury) {
            // if it's just a permutation only project, use the inner subQuery
            subQuery.getFrom().accept(sqlJoinCapturer);
        } else {
            sqlSelect.getFrom().accept(sqlJoinCapturer);
        }

        // collect columnRowType and join relations
        LinkedHashMap<String, ColumnRowType> queryAlias = sqlJoinCapturer.getAlias2CRT();

        // return if matches one single model view
        if (queryAlias.size() == 1) {
            Map.Entry<String, ColumnRowType> entry = queryAlias.entrySet().iterator().next();
            if (entry.getValue() == MODEL_VIEW_COLUMN_ROW_TYPE) {
                return QueryAliasMatchInfo.fromModelView(entry.getKey(), model);
            }
        }

        List<JoinDesc> joinDescs = sqlJoinCapturer.getJoinDescs();
        TableRef firstTable = sqlJoinCapturer.getFirstTable();
        if (firstTable == null) {
            return null;
        }
        JoinsGraph joinsGraph = new JoinsGraph(firstTable, joinDescs);
        KylinConfigExt projectConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project)
                .getConfig();

        if (sqlJoinCapturer.foundJoinOnCC) {
            // 1st round: dry run without cc expr comparison to collect model alias matching
            joinsGraph.setJoinEdgeMatcher(new CCJoinEdgeMatcher(null, false));

            Map<String, String> matches = joinsGraph.matchAlias(model.getJoinsGraph(), projectConfig);
            if (matches == null || matches.isEmpty()) {
                return null;
            }

            BiMap<String, String> aliasMapping = HashBiMap.create();
            aliasMapping.putAll(matches);

            QueryAliasMatchInfo ccAliasMatch = new QueryAliasMatchInfo(aliasMapping, queryAlias);

            // 2nd round: real run with cc expr comparison
            joinsGraph.setJoinEdgeMatcher(new CCJoinEdgeMatcher(ccAliasMatch, true));
        }

        // try match the subquery with model
        Map<String, String> matches = joinsGraph.matchAlias(model.getJoinsGraph(), projectConfig);
        if (matches == null || matches.isEmpty()) {
            return null;
        }
        BiMap<String, String> aliasMapping = HashBiMap.create();
        aliasMapping.putAll(matches);

        return new QueryAliasMatchInfo(aliasMapping, queryAlias);
    }

    private SqlSelect getSubquery(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            return (SqlSelect) sqlNode;
        } else if (SqlKind.UNION == sqlNode.getKind()) {
            return (SqlSelect) ((SqlBasicCall) sqlNode).getOperandList().get(0);
        } else if (SqlKind.AS == sqlNode.getKind()) {
            return getSubquery(((SqlBasicCall) sqlNode).getOperandList().get(0));
        }

        return null;
    }

    private static class CCJoinEdgeMatcher extends JoinsGraph.DefaultJoinEdgeMatcher {
        transient QueryAliasMatchInfo matchInfo;
        boolean compareCCExpr;

        public CCJoinEdgeMatcher(QueryAliasMatchInfo matchInfo, boolean compareCCExpr) {
            this.matchInfo = matchInfo;
            this.compareCCExpr = compareCCExpr;
        }

        @Override
        protected boolean columnDescEquals(ColumnDesc a, ColumnDesc b) {
            if (a == null) {
                return b == null;
            } else if (b == null) {
                return false;
            }

            if (!a.isComputedColumn() && !b.isComputedColumn()) {
                return super.columnDescEquals(a, b);
            } else if ((a.isComputedColumn() && !b.isComputedColumn())
                    || (!a.isComputedColumn() && b.isComputedColumn())) {
                return false;
            } else {
                if (!compareCCExpr)
                    return true;

                SqlNode node1 = CalciteParser.getExpNode(a.getComputedColumnExpr());
                SqlNode node2 = CalciteParser.getExpNode(b.getComputedColumnExpr());
                return ExpressionComparator.isNodeEqual(node1, node2, matchInfo, new AliasDeduceImpl(matchInfo));
            }
        }
    }

    //capture all the join within a SqlSelect's from clause, won't go into any subquery
    private class SqlJoinCapturer extends SqlBasicVisitor<SqlNode> {

        private List<JoinDesc> joinDescs;
        private LinkedHashMap<String, ColumnRowType> alias2CRT = Maps.newLinkedHashMap(); // aliasInQuery => ColumnRowType representing the alias table
        private String modelName;

        private boolean foundJoinOnCC = false;

        SqlJoinCapturer(String modelName) {
            this.joinDescs = new ArrayList<>();
            this.modelName = modelName;
        }

        List<JoinDesc> getJoinDescs() {
            return joinDescs;
        }

        LinkedHashMap<String, ColumnRowType> getAlias2CRT() {
            return alias2CRT;
        }

        TableRef getFirstTable() {
            if (alias2CRT.size() == 0) {
                throw new IllegalStateException("alias2CRT is empty");
            }
            ColumnRowType first = Iterables.getFirst(alias2CRT.values(), null);
            Preconditions.checkNotNull(first);
            if (first.getAllColumns() == null || first.getAllColumns().isEmpty()) {
                return null;
            }
            return first.getAllColumns().get(0).getTableRef();
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {

            if (call instanceof SqlSelect) {
                //don't go into sub-query
                return null;
            }

            // record alias, since the passed in root is a FROM clause, any AS is related to table alias
            if (call instanceof SqlBasicCall && call.getOperator() instanceof SqlAsOperator) {
                //join Table as xxx
                SqlNode[] operands = ((SqlBasicCall) call).getOperands();
                if (operands != null && operands.length == 2) {

                    //both side of the join is SqlIdentifier (not subquery), table as alias
                    if (operands[0] instanceof SqlIdentifier && operands[1] instanceof SqlIdentifier) {
                        String alias = operands[1].toString();
                        SqlIdentifier tableIdentifier = (SqlIdentifier) operands[0];
                        Pair<String, String> schemaAndTable = getSchemaAndTable(tableIdentifier);

                        ColumnRowType columnRowType = buildColumnRowType(alias, schemaAndTable.getFirst(),
                                schemaAndTable.getSecond());
                        alias2CRT.put(alias, columnRowType);
                    }

                    //subquery as alias
                    if ((operands[0] instanceof SqlSelect)
                            || ((operands[0] instanceof SqlOrderBy) && operands[1] instanceof SqlIdentifier)) {
                        String alias = operands[1].toString();
                        alias2CRT.put(alias, SUBQUERY_TAG);
                    }

                    // union-all as alias
                    if ((operands[0] instanceof SqlBasicCall && operands[0].getKind() == SqlKind.UNION
                            && operands[1] instanceof SqlIdentifier)) {
                        String alias = operands[1].toString();
                        alias2CRT.put(alias, SUBQUERY_TAG);
                    }
                }

                return null; //don't visit child any more
            }

            List<SqlNode> operandList = call.getOperandList();
            // if it's a join, the last element is "condition".
            // skip its part as it may also contain SqlIdentifier(representing condition column),
            // which is hard to tell from SqlIdentifier representing join tables (without AS)
            List<SqlNode> operands = call instanceof SqlJoin ? operandList.subList(0, operandList.size() - 1)
                    : operandList;
            for (SqlNode operand : operands) {
                if (operand != null) {
                    operand.accept(this);
                }
            }

            //Note this part is after visiting all children, it's on purpose so that by the time the SqlJoin
            //is handled, all of its related join tables must have been recorded in alias2CRT
            if (call instanceof SqlJoin) {
                SqlJoin join = (SqlJoin) call;
                if (join.getConditionType() != JoinConditionType.ON) {
                    throw new IllegalArgumentException(
                            "JoinConditionType is not ON: " + join.toSqlString(CalciteSqlDialect.DEFAULT));
                }
                if (join.getJoinType() != JoinType.INNER && join.getJoinType() != JoinType.LEFT) {
                    throw new IllegalArgumentException("JoinType must be INNER or LEFT");
                }

                if (join.getCondition() instanceof SqlBasicCall) {
                    JoinConditionCapturer joinConditionCapturer = new JoinConditionCapturer(alias2CRT,
                            join.getJoinType().toString());
                    join.getCondition().accept(joinConditionCapturer);
                    JoinDesc joinDesc = joinConditionCapturer.getJoinDescs();
                    foundJoinOnCC = foundJoinOnCC || joinConditionCapturer.foundCC;
                    if (joinDesc.getForeignKey().length != 0 && !joinConditionCapturer.foundNonEqualJoin)
                        joinDescs.add(joinDesc);
                } else {
                    throw new IllegalArgumentException("join condition should be SqlBasicCall");
                }
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            Pair<String, String> schemaAndTable = getSchemaAndTable(id);
            ColumnRowType columnRowType = buildColumnRowType(schemaAndTable.getSecond(), schemaAndTable.getFirst(),
                    schemaAndTable.getSecond());
            alias2CRT.put(schemaAndTable.getSecond(), columnRowType);

            return null;
        }

        private ColumnRowType buildColumnRowType(String alias, String schemaName, String tableName) {
            OLAPTable olapTable = getTable(schemaName.toUpperCase(Locale.ROOT), tableName);

            // check if it is the model view
            if (olapTable == null && (schemaName.equalsIgnoreCase(project) && tableName.equalsIgnoreCase(modelName))) {
                return MODEL_VIEW_COLUMN_ROW_TYPE;
            }

            List<TblColRef> columns = new ArrayList<>();
            if (olapTable != null) {
                TableRef tableRef = TblColRef.tableForUnknownModel(alias, olapTable.getSourceTable());
                for (ColumnDesc sourceColumn : olapTable.getSourceColumns()) {
                    TblColRef colRef = TblColRef.columnForUnknownModel(tableRef, sourceColumn);
                    columns.add(colRef);
                }

            }

            return new ColumnRowType(columns);
        }

        private OLAPTable getTable(String schemaName, String tableName) {
            Map<String, OLAPTable> localTables = schemaTables.get(schemaName);
            if (localTables == null) {
                KapOLAPSchema olapSchema = getSchema(schemaName);
                if (!olapSchema.hasTables()) {
                    return null;
                }
                localTables = Maps.newHashMap();
                for (Map.Entry<String, Table> entry : olapSchema.getTableMap().entrySet()) {
                    localTables.put(entry.getKey(), (OLAPTable) entry.getValue());
                }
                schemaTables.put(schemaName, localTables);
            }
            return localTables.get(tableName);
        }

        private KapOLAPSchema getSchema(String name) {
            return schemaMap.computeIfAbsent(name, schemaName -> new KapOLAPSchema(project, schemaName,
                    NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                            .listTablesGroupBySchema().get(schemaName),
                    NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getModelsGroupbyTable()));
        }

        private Pair<String, String> getSchemaAndTable(SqlIdentifier tableIdentifier) {
            String schemaName;
            String tableName;
            if (tableIdentifier.names.size() == 2) {
                schemaName = tableIdentifier.names.get(0);
                tableName = tableIdentifier.names.get(1);
            } else if (tableIdentifier.names.size() == 1) {
                schemaName = defaultSchema;
                tableName = tableIdentifier.names.get(0);
            } else {
                throw new IllegalStateException("table.names size being " + tableIdentifier.names.size());
            }
            return Pair.newPair(schemaName, tableName);
        }
    }

    private class JoinConditionCapturer extends SqlBasicVisitor<SqlNode> {
        private final LinkedHashMap<String, ColumnRowType> alias2CRT;
        private final String joinType;

        private List<TblColRef> pks = Lists.newArrayList();
        private List<TblColRef> fks = Lists.newArrayList();

        private boolean foundCC = false;
        private boolean foundNonEqualJoin = false;

        JoinConditionCapturer(LinkedHashMap<String, ColumnRowType> alias2CRT, String joinType) {
            this.alias2CRT = alias2CRT;
            this.joinType = joinType;
        }

        public JoinDesc getJoinDescs() {
            List<String> pkNames = Lists.transform(pks, new Function<TblColRef, String>() {
                @Nullable
                @Override
                public String apply(@Nullable TblColRef input) {
                    return input == null ? null : input.getName();
                }
            });
            List<String> fkNames = Lists.transform(fks, new Function<TblColRef, String>() {
                @Nullable
                @Override
                public String apply(@Nullable TblColRef input) {
                    return input == null ? null : input.getName();
                }
            });

            JoinDesc join = new JoinDesc();
            join.setType(joinType);
            join.setForeignKey(fkNames.toArray(COLUMN_ARRAY_MARKER));
            join.setForeignKeyColumns(fks.toArray(new TblColRef[fks.size()]));
            join.setPrimaryKey(pkNames.toArray(COLUMN_ARRAY_MARKER));
            join.setPrimaryKeyColumns(pks.toArray(new TblColRef[pks.size()]));
            join.sortByFK();
            return join;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            return null;
        }

        private TblColRef resolveComputedColumnRef(SqlCall call, String... tableCandidates) {
            foundCC = true;
            String table = findComputedColumnTable(call, tableCandidates);
            ColumnDesc columnDesc = new ColumnDesc("-1", RandomUtil.randomUUIDStr(), "string", "", null, null,
                    call.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
            TableRef tableRef = alias2CRT.get(table).getColumnByIndex(0).getTableRef();
            columnDesc.setTable(tableRef.getTableDesc());
            return TblColRef.columnForUnknownModel(tableRef, columnDesc);
        }

        private String findComputedColumnTable(SqlCall call, final String... tableCandidates) {
            final String[] result = new String[1];

            SqlBasicVisitor<SqlNode> visitor = new SqlBasicVisitor<SqlNode>() {
                @Override
                public SqlNode visit(SqlIdentifier sqlIdentifier) {
                    TblColRef colRef = resolveTblColRef(sqlIdentifier, alias2CRT);
                    for (String table : tableCandidates) {
                        if (alias2CRT.get(table).getAllColumns().contains(colRef)) {
                            result[0] = table;
                            return sqlIdentifier;
                        }
                    }
                    return null;
                }
            };
            visitor.visit(call);

            Preconditions.checkNotNull(result[0], "Table not found for SqlCall: " + call.toString());
            return result[0];
        }

        @Override
        public SqlNode visit(SqlCall call) {
            if ((call instanceof SqlBasicCall) && (call.getOperator() instanceof SqlBinaryOperator)) {
                if (call.getOperator().getKind() == SqlKind.AND) {
                    for (SqlNode operand : call.getOperandList()) {
                        if (operand != null) {
                            operand.accept(this);
                        }
                    }
                    return null;
                } else if (call.getOperator().getKind() == SqlKind.EQUALS && call.getOperandList().size() == 2) {
                    SqlNode operand0 = call.getOperandList().get(0);
                    SqlNode operand1 = call.getOperandList().get(1);

                    if ((operand0 instanceof SqlIdentifier || operand0 instanceof SqlCall)
                            && (operand1 instanceof SqlIdentifier || operand1 instanceof SqlCall)) {
                        int numOfAlias = alias2CRT.size();
                        String pkAlias = Iterables.getLast(alias2CRT.keySet());
                        String fkAlias = Iterables.get(alias2CRT.keySet(), numOfAlias - 2);

                        // sqlCall maybe used as join condition, which need to
                        // translate as CC
                        TblColRef tblColRef0 = operand0 instanceof SqlIdentifier
                                ? resolveTblColRef((SqlIdentifier) operand0, alias2CRT)
                                : resolveComputedColumnRef((SqlCall) operand0, pkAlias, fkAlias);
                        TblColRef tblColRef1 = operand1 instanceof SqlIdentifier
                                ? resolveTblColRef((SqlIdentifier) operand1, alias2CRT)
                                : resolveComputedColumnRef((SqlCall) operand1, pkAlias, fkAlias);

                        if (tblColRef0 == null || tblColRef1 == null) {
                            return null;
                        }

                        if (tblColRef1.getTableRef().getAlias().equals(pkAlias)) {
                            // for most cases
                            pks.add(tblColRef1);
                            fks.add(tblColRef0);
                        } else if (tblColRef0.getTableRef().getAlias().equals(pkAlias)) {
                            pks.add(tblColRef0);
                            fks.add(tblColRef1);
                        }
                        return null;
                    }
                }
            }

            foundNonEqualJoin = true;
            return null;
        }

    }

}
