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
import java.util.Comparator;
import java.util.List;

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
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.routing.NoRealizationFoundException;
import org.apache.kylin.query.routing.RoutingIndicatorException;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.apache.kylin.source.adhocquery.IPushDownRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class PushDownUtil {
    private static final Logger logger = LoggerFactory.getLogger(PushDownUtil.class);

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownSelectQuery(String project, String sql,
            String defaultSchema, SQLException sqlException) throws Exception {
        return tryPushDownQuery(project, sql, defaultSchema, sqlException, true);
    }

    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownNonSelectQuery(String project,
            String sql, String defaultSchema) throws Exception {
        return tryPushDownQuery(project, sql, defaultSchema, null, false);
    }

    private static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownQuery(String project, String sql,
            String defaultSchema, SQLException sqlException, boolean isSelect) throws Exception {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (!kylinConfig.isPushDownEnabled())
            return null;

        if (isSelect) {
            logger.info("Query failed to utilize pre-calculation, routing to other engines", sqlException);
            if (!isExpectedCause(sqlException)) {
                logger.info("quit doPushDownQuery because prior exception thrown is unexpected");
                return null;
            }
        } else {
            Preconditions.checkState(sqlException == null);
            logger.info("Kylin cannot support non-select queries, routing to other engines");
        }

        IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(kylinConfig.getPushDownRunnerClassName());
        runner.init(kylinConfig);
        logger.debug("Query Pushdown runner {}", runner);

        // default schema in calcite does not apply to other engines.
        // since this is a universql requirement, it's not implemented as a converter
        if (defaultSchema != null && !defaultSchema.equals("DEFAULT")) {
            String completed = sql;
            try {
                completed = schemaCompletion(sql, defaultSchema);
            } catch (SqlParseException e) {
                // fail to parse the pushdown sql, ignore
                logger.debug("fail to do schema completion on the pushdown sql, ignore it.", e.getMessage());
            }
            if (!sql.equals(completed)) {
                logger.info("the query is converted to {} after schema completion", completed);
                sql = completed;
            }
        }

        for (String converterName : kylinConfig.getPushDownConverterClassNames()) {
            IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(converterName);
            String converted = converter.convert(sql, project, defaultSchema);
            if (!sql.equals(converted)) {
                logger.info("the query is converted to {} after applying converter {}", converted, converterName);
                sql = converted;
            }
        }

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        if (isSelect) {
            runner.executeQuery(sql, returnRows, returnColumnMeta);
        } else {
            runner.executeUpdate(sql);
        }
        return Pair.newPair(returnRows, returnColumnMeta);
    }

    private static boolean isExpectedCause(SQLException sqlException) {
        Preconditions.checkArgument(sqlException != null);

        Throwable rootCause = ExceptionUtils.getRootCause(sqlException);
        return rootCause != null && //
                (rootCause instanceof NoRealizationFoundException //
                        || rootCause instanceof SqlValidatorException // 
                        || rootCause instanceof RoutingIndicatorException);
    }

    static String schemaCompletion(String inputSql, String schema) throws SqlParseException {
        if (inputSql == null || inputSql.equals("")) {
            return "";
        }
        SqlNode node = CalciteParser.parse(inputSql);

        // get all table node that don't have schema by visitor pattern
        FromTablesVisitor ftv = new FromTablesVisitor();
        node.accept(ftv);
        List<SqlNode> tablesWithoutSchema = ftv.getTablesWithoutSchema();
        // sql do not need completion
        if (tablesWithoutSchema.isEmpty()) {
            return inputSql;
        }

        List<Pair<Integer, Integer>> tablesPos = new ArrayList<>();
        for (SqlNode tables : tablesWithoutSchema) {
            tablesPos.add(CalciteParser.getReplacePos(tables, inputSql));
        }

        // make the behind position in the front of the list, so that the front position will not be affected when replaced
        Collections.sort(tablesPos, new Comparator<Pair<Integer, Integer>>() {
            @Override
            public int compare(Pair<Integer, Integer> o1, Pair<Integer, Integer> o2) {
                int r = o2.getFirst() - o1.getFirst();
                return r == 0 ? o2.getSecond() - o1.getSecond() : r;
            }
        });

        StrBuilder afterConvert = new StrBuilder(inputSql);
        for (Pair<Integer, Integer> pos : tablesPos) {
            String tableWithSchema = schema + "." + inputSql.substring(pos.getFirst(), pos.getSecond());
            afterConvert.replace(pos.getFirst(), pos.getSecond(), tableWithSchema);
        }
        return afterConvert.toString();
    }

    /**
     * Get all the tables from "FROM clause" that without schema
     * subquery is only considered in "from clause"
     */
    static class FromTablesVisitor implements SqlVisitor<SqlNode> {
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
            if (call instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) call;
                select.getFrom().accept(this);
                return null;
            }
            if (call instanceof SqlOrderBy) {
                SqlOrderBy orderBy = (SqlOrderBy) call;
                ((SqlSelect) orderBy.query).getFrom().accept(this);
                return null;
            }
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
}
