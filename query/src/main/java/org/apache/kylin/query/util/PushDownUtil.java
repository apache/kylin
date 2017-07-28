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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.routing.NoRealizationFoundException;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.apache.kylin.source.adhocquery.IPushDownRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class PushDownUtil {
    private static final Logger logger = LoggerFactory.getLogger(PushDownUtil.class);

    public static boolean doPushDownQuery(String project, String sql, String schema, List<List<String>> results,
            List<SelectedColumnMeta> columnMetas, SQLException sqlException) throws Exception {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isPushDownEnabled()) {
            return false;
        }

        Throwable rootCause = ExceptionUtils.getRootCause(sqlException);
        boolean isExpectedCause = rootCause != null && (rootCause.getClass().equals(NoRealizationFoundException.class));

        if (isExpectedCause) {

            logger.info("Query failed to utilize pre-calculation, routing to other engines", sqlException);
            IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(kylinConfig.getPushDownRunnerClassName());
            IPushDownConverter converter = (IPushDownConverter) ClassUtil
                    .newInstance(kylinConfig.getPushDownConverterClassName());

            runner.init(kylinConfig);

            logger.debug("Query pushdown runner {}", runner);

            String expandCC = restoreComputedColumnToExpr(sql, project);
            if (!StringUtils.equals(expandCC, sql)) {
                logger.info("computed column in sql is expanded to:  " + expandCC);
            }
            if (schema != null && !schema.equals("DEFAULT")) {
                expandCC = schemaCompletion(expandCC, schema);
            }
            String adhocSql = converter.convert(expandCC);
            if (!adhocSql.equals(expandCC)) {
                logger.info("the query is converted to {} according to kylin.query.pushdown.converter-class-name",
                        adhocSql);
            }

            runner.executeQuery(adhocSql, results, columnMetas);
            return true;
        } else {
            return false;
        }
    }

    static String schemaCompletion(String inputSql, String schema) {
        if (inputSql == null || inputSql.equals("")) {
            return "";
        }
        SqlNode fromNode = CalciteParser.getFromNode(inputSql);

        // get all table node that don't have schema by visitor pattern
        FromTablesVisitor ftv = new FromTablesVisitor();
        fromNode.accept(ftv);
        List<SqlNode> tablesWithoutSchema = ftv.getTablesWithoutSchema();

        List<Pair<Integer, Integer>> tablesPos = new ArrayList<>();
        for (SqlNode tables : tablesWithoutSchema) {
            tablesPos.add(CalciteParser.getReplacePos(tables, inputSql));
        }

        // make the behind position in the front of the list, so that the front position will not be affected when replaced
        Collections.sort(tablesPos);
        Collections.reverse(tablesPos);

        StrBuilder afterConvert = new StrBuilder(inputSql);
        for (Pair<Integer, Integer> pos : tablesPos) {
            String tableWithSchema = schema + "." + inputSql.substring(pos.getLeft(), pos.getRight());
            afterConvert.replace(pos.getLeft(), pos.getRight(), tableWithSchema);
        }
        return afterConvert.toString();
    }

    private final static Pattern identifierInSqlPattern = Pattern.compile(
            //find pattern like "table"."column" or "column"
            "((?<![\\p{L}_0-9\\.\\\"])(\\\"[\\p{L}_0-9]+\\\"\\.)?(\\\"[\\p{L}_0-9]+\\\")(?![\\p{L}_0-9\\.\\\"]))" + "|"
            //find pattern like table.column or column
                    + "((?<![\\p{L}_0-9\\.\\\"])([\\p{L}_0-9]+\\.)?([\\p{L}_0-9]+)(?![\\p{L}_0-9\\.\\\"]))");

    private final static Pattern identifierInExprPattern = Pattern.compile(
            // a.b.c
            "((?<![\\p{L}_0-9\\.\\\"])([\\p{L}_0-9]+\\.)([\\p{L}_0-9]+\\.)([\\p{L}_0-9]+)(?![\\p{L}_0-9\\.\\\"]))");

    private final static Pattern endWithAsPattern = Pattern.compile("\\s+as\\s+$", Pattern.CASE_INSENSITIVE);

    public static String restoreComputedColumnToExpr(String beforeSql, String project) {
        final MetadataManager metadataManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<DataModelDesc> dataModelDescs = metadataManager.getModels(project);

        String afterSql = beforeSql;
        for (DataModelDesc dataModelDesc : dataModelDescs) {
            for (ComputedColumnDesc computedColumnDesc : dataModelDesc.getComputedColumnDescs()) {
                afterSql = restoreComputedColumnToExpr(afterSql, computedColumnDesc);
            }
        }
        return afterSql;
    }

    static String restoreComputedColumnToExpr(String sql, ComputedColumnDesc computedColumnDesc) {

        String ccName = computedColumnDesc.getColumnName();
        List<Triple<Integer, Integer, String>> replacements = Lists.newArrayList();
        Matcher matcher = identifierInSqlPattern.matcher(sql);

        while (matcher.find()) {
            if (matcher.group(1) != null) { //with quote case: "TABLE"."COLUMN"

                String quotedColumnName = matcher.group(3);
                Preconditions.checkNotNull(quotedColumnName);
                String columnName = StringUtils.strip(quotedColumnName, "\"");
                if (!columnName.equalsIgnoreCase(ccName)) {
                    continue;
                }

                if (matcher.group(2) != null) { // table name exist 
                    String quotedTableAlias = StringUtils.strip(matcher.group(2), ".");
                    String tableAlias = StringUtils.strip(quotedTableAlias, "\"");
                    replacements.add(Triple.of(matcher.start(1), matcher.end(1),
                            replaceIdentifierInExpr(computedColumnDesc.getExpression(), tableAlias, true)));
                } else { //only column
                    if (endWithAsPattern.matcher(sql.substring(0, matcher.start(1))).find()) {
                        //select DEAL_AMOUNT as "deal_amount" case
                        continue;
                    }
                    replacements.add(Triple.of(matcher.start(1), matcher.end(1),
                            replaceIdentifierInExpr(computedColumnDesc.getExpression(), null, true)));
                }
            } else if (matcher.group(4) != null) { //without quote case: table.column or simply column
                String columnName = matcher.group(6);
                Preconditions.checkNotNull(columnName);
                if (!columnName.equalsIgnoreCase(ccName)) {
                    continue;
                }

                if (matcher.group(5) != null) { //table name exist
                    String tableAlias = StringUtils.strip(matcher.group(5), ".");
                    replacements.add(Triple.of(matcher.start(4), matcher.end(4),
                            replaceIdentifierInExpr(computedColumnDesc.getExpression(), tableAlias, false)));

                } else { //only column 
                    if (endWithAsPattern.matcher(sql.substring(0, matcher.start(4))).find()) {
                        //select DEAL_AMOUNT as deal_amount case
                        continue;
                    }
                    replacements.add(Triple.of(matcher.start(4), matcher.end(4),
                            replaceIdentifierInExpr(computedColumnDesc.getExpression(), null, false)));
                }
            }
        }

        Collections.reverse(replacements);
        for (Triple<Integer, Integer, String> triple : replacements) {
            sql = sql.substring(0, triple.getLeft()) + "(" + triple.getRight() + ")"
                    + sql.substring(triple.getMiddle());
        }
        return sql;
    }

    static String replaceIdentifierInExpr(String expr, String tableAlias, boolean quoted) {
        if (tableAlias == null) {
            return expr;
        }

        return CalciteParser.insertAliasInExpr(expr, tableAlias);
    }
}

/**
 * Created by jiatao.tao
 * Get all the tables from "FROM" clause that without schema
 */
class FromTablesVisitor implements SqlVisitor<SqlNode> {
    private List<SqlNode> tables;

    FromTablesVisitor() {
        this.tables = new ArrayList<>();
    }

    List<SqlNode> getTablesWithoutSchema() {
        return tables;
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
        return null;
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
        return null;
    }

    @Override
    public SqlNode visit(SqlCall call) {
        if (call instanceof SqlBasicCall) {
            SqlBasicCall node = (SqlBasicCall) call;
            node.getOperands()[0].accept(this);
            return null;
        }
        if (call instanceof SqlJoin) {
            SqlJoin node = (SqlJoin) call;
            node.getLeft().accept(this);
            node.getRight().accept(this);
            return null;
        }
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null) {
                operand.accept(this);
            }
        }
        return null;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        if (id.names.size() == 1) {
            tables.add(id);
        }
        return null;
    }

    @Override
    public SqlNode visit(SqlDataTypeSpec type) {
        return null;
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        return null;
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
        return null;
    }
}