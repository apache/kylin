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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

//very similar to ConvertToComputedColumn in structure, maybe we should extract a common base class?
public class RestoreFromComputedColumn implements IPushDownConverter {

    private static final Logger logger = LoggerFactory.getLogger(RestoreFromComputedColumn.class);

    public static String convertWithGivenModels(String sql, String project, String defaultSchema,
            Map<String, NDataModel> dataModelDescs) throws SqlParseException {

        List<SqlCall> selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql, true);

        /*
            select A, sum(C) from T group by A order by SUM(C)
            topColumns: A, sum(C)
            C is not a topColumn here.
         */
        List<List<SqlNode>> topColumns = Lists.newArrayList();
        for (SqlCall selectOrOrderby : selectOrOrderbys) {
            topColumns.add(collectTopColumns(selectOrOrderby));
        }

        QueryAliasMatcher queryAliasMatcher = new QueryAliasMatcher(project, defaultSchema);

        int recursionTimes = 0;
        int maxRecursionTimes = KapConfig.getInstanceFromEnv().getComputedColumnMaxRecursionTimes();

        while ((recursionTimes++) < maxRecursionTimes) {

            boolean recursionCompleted = true;

            for (int i = 0; i < selectOrOrderbys.size(); i++) { //subquery will precede
                Pair<String, Integer> choiceForCurrentSubquery = restoreComputedColumn(sql, selectOrOrderbys.get(i),
                        topColumns.get(i), dataModelDescs, queryAliasMatcher);

                if (choiceForCurrentSubquery != null) {
                    sql = choiceForCurrentSubquery.getFirst();
                    selectOrOrderbys = SqlSubqueryFinder.getSubqueries(sql, true);
                    recursionCompleted = false;
                }
            }

            if (recursionCompleted) {
                break;
            }
        }
        return sql;
    }

    private static List<SqlNode> collectTopColumns(SqlCall selectOrOrderby) {
        List<SqlNode> columnList = Lists.newArrayList();
        for (SqlNode node : selectOrOrderby.getOperandList()) {

            if (node instanceof SqlNodeList) {
                columnList.addAll(((SqlNodeList) node).getList());
            }
        }
        return columnList;
    }

    /**
     * check whether it needs parenthesis
     * 1. not need if it is a top node
     * 2. not need if it is in parenthesis
     *
     * @param originSql
     * @param topColumns
     * @param replaceRange
     * @return
     */
    private static boolean needParenthesis(String originSql, List<SqlNode> topColumns, ReplaceRange replaceRange) {
        if (replaceRange.addAlias) {
            return false;
        }

        boolean topColumn = false;

        for (SqlNode column : topColumns) {
            if (column != null && column.equalsDeep(replaceRange.column, Litmus.IGNORE)) {
                topColumn = true;
                break;
            }
        }

        if (topColumn) {
            return false;
        }

        boolean hasLeft = false;
        for (int i = replaceRange.beginPos - 1; i >= 0; i--) {

            char c = originSql.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }

            if (c == '(' || c == ',') {
                hasLeft = true;
            }
            break;
        }

        boolean hasRight = false;
        for (int i = replaceRange.endPos; i < originSql.length(); i++) {

            char c = originSql.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }

            if (c == ')' || c == ',') {
                hasRight = true;
            }
            break;
        }

        return !(hasLeft && hasRight);

    }

    static Pair<String, Integer> restoreComputedColumn(String sql, SqlCall selectOrOrderby, List<SqlNode> topColumns,
            Map<String, NDataModel> dataModelDescs, QueryAliasMatcher queryAliasMatcher) throws SqlParseException {
        SqlSelect sqlSelect = KapQueryUtil.extractSqlSelect(selectOrOrderby);
        if (sqlSelect == null)
            return Pair.newPair(sql, 0);

        Pair<String, Integer> choiceForCurrentSubquery = null; //<new sql, number of changes by the model>

        //give each data model a chance to rewrite, choose the model that generates most changes
        for (NDataModel modelDesc : dataModelDescs.values()) {

            QueryAliasMatchInfo info = queryAliasMatcher.match(modelDesc, sqlSelect);
            if (info == null) {
                continue;
            }

            Pair<String, Integer> ret = restoreComputedColumn(sql, selectOrOrderby, topColumns, modelDesc, info);

            if (ret.getSecond() == 0)
                continue;

            if ((choiceForCurrentSubquery == null) || (ret.getSecond() > choiceForCurrentSubquery.getSecond())) {
                choiceForCurrentSubquery = ret;
            }
        }

        return choiceForCurrentSubquery;
    }

    /**
     * return the replaced sql, and the count of changes in the replaced sql
     */
    static Pair<String, Integer> restoreComputedColumn(String inputSql, SqlCall selectOrOrderby,
            List<SqlNode> topColumns, NDataModel dataModelDesc, QueryAliasMatchInfo matchInfo)
            throws SqlParseException {

        String result = inputSql;

        Set<String> ccColNamesWithPrefix = Sets.newHashSet();
        ccColNamesWithPrefix.addAll(dataModelDesc.getComputedColumnNames());
        List<ColumnUsage> columnUsages = ColumnUsagesFinder.getColumnUsages(selectOrOrderby, ccColNamesWithPrefix);
        if (columnUsages.size() == 0) {
            return Pair.newPair(inputSql, 0);
        }

        columnUsages.sort((item1, item2) -> {
            SqlIdentifier column1 = item1.sqlIdentifier;
            SqlIdentifier column2 = item2.sqlIdentifier;
            int linegap = column2.getParserPosition().getLineNum() - column1.getParserPosition().getLineNum();
            if (linegap != 0)
                return linegap;

            return column2.getParserPosition().getColumnNum() - column1.getParserPosition().getColumnNum();
        });

        List<ReplaceRange> toBeReplacedUsages = Lists.newArrayList();

        for (ColumnUsage columnUsage : columnUsages) {
            SqlIdentifier column = columnUsage.sqlIdentifier;
            String columnName = Iterables.getLast(column.names);
            //TODO cannot do this check because cc is not visible on schema without solid realizations
            // after this is done, constrains on #1932 can be relaxed

            //            TblColRef tblColRef = QueryAliasMatchInfo.resolveTblColRef(queryAliasMatchInfo.getQueryAlias(), columnName);
            //            //for now, must be fact table
            //            Preconditions.checkState(tblColRef.getTableRef().getTableIdentity()
            //                    .equals(dataModelDesc.getRootFactTable().getTableIdentity()));
            ComputedColumnDesc computedColumnDesc = dataModelDesc
                    .findCCByCCColumnName(ComputedColumnDesc.getOriginCcName(columnName));
            // The computed column expression is defined based on alias in model, e.g. BUYER_COUNTRY.x + BUYER_ACCOUNT.y
            // however user query might use a different alias, say bc.x + ba.y
            String ccExpression = CalciteParser.replaceAliasInExpr(computedColumnDesc.getExpression(),
                    matchInfo.getAliasMapping().inverse());
            // intend to handle situation like KE-15939
            String replaceExpression = columnUsage.addAlias ? ccExpression + " AS " + computedColumnDesc.getColumnName()
                    : ccExpression;

            Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(column, inputSql);
            int begin = startEndPos.getFirst();
            int end = startEndPos.getSecond();

            ReplaceRange replaceRange = new ReplaceRange();
            replaceRange.beginPos = begin;
            replaceRange.endPos = end;

            replaceRange.column = column;
            replaceRange.replaceExpr = replaceExpression;
            replaceRange.addAlias = columnUsage.addAlias;

            toBeReplacedUsages.add(replaceRange);
        }

        toBeReplacedUsages.sort((o1, o2) -> Integer.compare(o2.beginPos, o1.beginPos));

        //check
        ReplaceRange last = null;
        for (ReplaceRange toBeReplaced : toBeReplacedUsages) {
            if (last != null) {
                if (last.beginPos == toBeReplaced.beginPos && last.endPos == toBeReplaced.endPos) {
                    last = toBeReplaced;
                    continue;
                }
                Preconditions.checkState(last.beginPos > toBeReplaced.endPos,
                        "Positions for two column usage has overlaps");
            }

            logger.debug("Column Usage at [{}, {}] is replaced by {}.", toBeReplaced.beginPos, toBeReplaced.endPos,
                    toBeReplaced.replaceExpr);

            String pattern = (needParenthesis(inputSql, topColumns, toBeReplaced)) ? "{0}({1}){2}" : "{0}{1}{2}";
            result = Unsafe.format(Locale.ROOT, pattern, result.substring(0, toBeReplaced.beginPos),
                    toBeReplaced.replaceExpr, result.substring(toBeReplaced.endPos));

            last = toBeReplaced;// new Pair<>(toBeReplaced.getFirst(), new Pair<>(start, end+6));// toBeReplaced;
        }

        return Pair.newPair(result, toBeReplacedUsages.size());
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        try {

            String sql = originSql;
            if (project == null || sql == null) {
                return sql;
            }

            Map<String, NDataModel> dataModelDescs = new LinkedHashMap<>();

            NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            for (NDataModel modelDesc : dataflowManager.listUnderliningDataModels()) {
                dataModelDescs.put(modelDesc.getUuid(), modelDesc);
            }

            return convertWithGivenModels(sql, project, defaultSchema, dataModelDescs);
        } catch (Exception e) {
            logger.debug(
                    "Something unexpected while RestoreFromComputedColumn transforming the query, return original query",
                    e);
            return originSql;
        }

    }

    private static class ReplaceRange {

        String replaceExpr;
        boolean addAlias;

        int beginPos;
        int endPos;

        SqlIdentifier column;
    }

    //find child inner select first
    static class ColumnUsagesFinder extends SqlBasicVisitor<SqlNode> {
        private final List<ColumnUsage> usages;
        private final Set<String> columnNames;

        ColumnUsagesFinder(Set<String> columnNames) {
            this.columnNames = columnNames;
            this.usages = Lists.newArrayList();
        }

        public static List<ColumnUsage> getColumnUsages(SqlCall selectOrOrderby, Set<String> columnNames)
                throws SqlParseException {
            ColumnUsagesFinder sqlSubqueryFinder = new ColumnUsagesFinder(columnNames);
            selectOrOrderby.accept(sqlSubqueryFinder);
            return sqlSubqueryFinder.getUsages();
        }

        public List<ColumnUsage> getUsages() {
            return usages;
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (matchName(id)) {
                this.usages.add(new ColumnUsage(id, false));
            }
            return null;
        }

        @Override
        public SqlNode visit(SqlCall call) {

            //skip the part after AS
            if (call instanceof SqlBasicCall && call.getOperator() instanceof SqlAsOperator) {
                SqlNode[] operands = ((SqlBasicCall) call).getOperands();
                if (operands != null && operands.length == 2) {
                    operands[0].accept(this);
                }
            } else {
                List<SqlNode> operands = call.getOperandList();
                for (int i = 0; i < operands.size(); i++) {
                    SqlNode operand = operands.get(i);
                    if (operand == null)
                        continue;

                    if (call instanceof SqlSelect && i == 1) {
                        traverseSelectList((SqlNodeList) operand);
                        continue;
                    }

                    operand.accept(this);
                }
            }

            return null;
        }

        private boolean matchName(SqlIdentifier sqlIdentifier) {
            return columnNames.contains(sqlIdentifier.names.get(sqlIdentifier.names.size() - 1));
        }

        private void traverseSelectList(SqlNodeList sqlSelectList) {
            for (SqlNode selectItem : sqlSelectList.getList()) {
                if (selectItem instanceof SqlIdentifier && matchName((SqlIdentifier) selectItem)) {
                    this.usages.add(new ColumnUsage((SqlIdentifier) selectItem, true));
                } else {
                    selectItem.accept(this);
                }
            }
        }
    }

    static private class ColumnUsage {
        SqlIdentifier sqlIdentifier;
        boolean addAlias;

        ColumnUsage(SqlIdentifier sqlIdentifier, boolean addAlias) {
            this.sqlIdentifier = sqlIdentifier;
            this.addAlias = addAlias;
        }
    }

}
