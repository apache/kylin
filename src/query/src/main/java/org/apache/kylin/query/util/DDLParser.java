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

import static org.apache.kylin.measure.percentile.PercentileMeasureType.FUNC_PERCENTILE;
import static org.apache.kylin.measure.percentile.PercentileMeasureType.FUNC_PERCENTILE_100;
import static org.apache.kylin.measure.percentile.PercentileMeasureType.FUNC_PERCENTILE_1000;
import static org.apache.kylin.measure.percentile.PercentileMeasureType.FUNC_PERCENTILE_10000;
import static org.apache.kylin.measure.percentile.PercentileMeasureType.FUNC_PERCENTILE_APPROX;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COUNT;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_MAX;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_MIN;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_SUM;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.ParseException;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.query.engine.KECalciteConfig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

public class DDLParser {
    private final SqlParser.Config config;
    public static final String FUNC_HLL_COUNT = "HLL_COUNT";
    public static final String FUNC_HLL_COUNT_10 = "HLL_COUNT_10";
    public static final String FUNC_HLL_COUNT_12 = "HLL_COUNT_12";
    public static final String FUNC_HLL_COUNT_14 = "HLL_COUNT_14";
    public static final String FUNC_HLL_COUNT_15 = "HLL_COUNT_15";
    public static final String FUNC_HLL_COUNT_16 = "HLL_COUNT_16";
    public static final String FUNC_BITMAP_COUNT = "BITMAP_COUNT";

    private static final List<String> SUPPORT_MEASURE_PREFIX = Lists.newArrayList(FUNC_BITMAP_COUNT, FUNC_HLL_COUNT,
            FUNC_PERCENTILE, FUNC_SUM, FUNC_MAX, FUNC_MIN, FUNC_COUNT, FUNC_HLL_COUNT_10, FUNC_HLL_COUNT_12,
            FUNC_HLL_COUNT_14, FUNC_HLL_COUNT_15, FUNC_HLL_COUNT_16, FUNC_PERCENTILE_APPROX, FUNC_PERCENTILE_100,
            FUNC_PERCENTILE_1000, FUNC_PERCENTILE_10000);

    public DDLParser(SqlParser.Config config) {
        this.config = config;
    }

    public static DDLParser CreateParser(KECalciteConfig connectionConfig) {
        SqlParser.Config parserConfig = SqlParser.configBuilder().setQuotedCasing(connectionConfig.quotedCasing())
                .setUnquotedCasing(connectionConfig.unquotedCasing()).setQuoting(connectionConfig.quoting())
                .setIdentifierMaxLength(1024).setConformance(connectionConfig.conformance())
                .setCaseSensitive(connectionConfig.caseSensitive()).setParserFactory(SqlDdlParserImpl.FACTORY).build();
        return new DDLParser(parserConfig);
    }

    public DDLParserResult parseSQL(String sql) throws Exception {
        SqlCreateMaterializedView sNode = (SqlCreateMaterializedView) SqlParser.create(sql, this.config).parseQuery();
        DDLParserResult result = new DDLParserResult();
        SqlIdentifier identifier = (SqlIdentifier) sNode.getOperandList().get(0);
        SqlSelect sqlSelect = (SqlSelect) sNode.getOperandList().get(2);

        parseFromIdentifier(identifier, result, sql);

        parseDimensionsAndMeasures(sqlSelect, result);
        if (sqlSelect.getFrom() instanceof SqlJoin) {
            SqlJoin from = (SqlJoin) sqlSelect.getFrom();
            parseFromWithJoin(from, result);
        } else {
            SqlIdentifier from = (SqlIdentifier) sqlSelect.getFrom();
            parseFromWithOutJoin(from, result);
        }

        return result;
    }

    private void parseFromWithOutJoin(SqlIdentifier fact, DDLParserResult result) throws ParseException {
        result.setFactTable(getTableFullName(fact.names));
        result.setJoinTables(Lists.newArrayList());
    }

    private final static String emptyJoinConditionErr = "DDL not support without join condition!";
    private final static String joinTypeErr = "DDL only support InnerJoin or LeftJoin!";
    private final static String joinConditionErr = "DDL only support equal join!";

    private void checkJoin(SqlJoin sqlJoin) throws ParseException {
        if (sqlJoin.getCondition() == null) {
            throw new ParseException(emptyJoinConditionErr);
        } else {
            SqlCall cond = (SqlCall) sqlJoin.getCondition();
            if (cond.toString().contains(">") || cond.toString().contains("<")) {
                throw new ParseException(joinConditionErr);
            }
        }
        JoinType joinType = sqlJoin.getJoinType();
        if (joinType != JoinType.INNER && joinType != JoinType.LEFT) {
            throw new ParseException(joinTypeErr);
        }
    }

    private void parseFromWithJoin(SqlJoin sqlJoin, DDLParserResult result) throws ParseException {
        checkJoin(sqlJoin);
        SqlIdentifier fact;
        List<SqlIdentifier> lookUp = Lists.newArrayList();
        List<JoinDesc> joinsDesc = Lists.newArrayList();
        SqlNode left = sqlJoin.getLeft();
        SqlNode right = sqlJoin.getRight();
        if (right != null) {
            lookUp.add((SqlIdentifier) right);
            JoinDesc joinDesc = getJoinDesc(sqlJoin);
            joinsDesc.add(joinDesc);
        }
        while (left instanceof SqlJoin) {
            SqlJoin leftJoin = (SqlJoin) left;
            checkJoin(leftJoin);
            lookUp.add((SqlIdentifier) leftJoin.getRight());
            JoinDesc joinDesc = getJoinDesc((SqlJoin) left);
            joinsDesc.add(joinDesc);
            left = leftJoin.getLeft();
        }
        fact = (SqlIdentifier) left;
        // 1. set factTable
        result.setFactTable(getTableFullName(fact.names));

        if (lookUp.size() != joinsDesc.size()) {
            String msg = "Parse join info size fail" + sqlJoin;
            throw new ParseException(msg);
        }

        // 2. set lookupTable and joinDesc
        List<JoinTableDesc> joinTableDesc = Lists.newArrayList();
        for (int i = 0; i < lookUp.size(); i++) {
            JoinTableDesc jd = new JoinTableDesc();
            SqlIdentifier l = lookUp.get(i);
            if (l.names.size() < 2) {
                throw new ParseException("In joinCondition table name must be db_name.table_name");
            }
            jd.setTable(getTableFullName(l.names));
            // `names` like table_name.col_name
            jd.setAlias(l.names.get(1));
            jd.setJoin(joinsDesc.get(i));
            joinTableDesc.add(jd);
        }
        result.setJoinTables(joinTableDesc);
    }

    private void parseDimensionsAndMeasures(SqlSelect sqlSelect, DDLParserResult result) throws ParseException {
        SqlNodeList selectList = sqlSelect.getSelectList();
        List<SqlIdentifier> dims = Lists.newArrayList();
        List<SqlBasicCall> meas = Lists.newArrayList();

        for (SqlNode node : selectList) {
            if (node instanceof SqlIdentifier) {
                dims.add((SqlIdentifier) node);
            } else if (node instanceof SqlBasicCall) {
                meas.add((SqlBasicCall) node);
            } else {
                throw new ParseException("Unexpected select: ".concat(node.toString()));
            }
        }

        if (dims.isEmpty()) {
            throw new ParseException("In DDL dimensions should not be empty.");
        }
        parseDimsInner(dims, result);
        parseMeasInner(meas, result);

    }

    private void parseMeasInner(List<SqlBasicCall> meas, DDLParserResult result) {
        List<InnerMeasure> measures = meas.stream().map(m -> {
            // 1. set measure type
            InnerMeasure measure = new InnerMeasure();
            String measureName = m.getOperator().getName();
            try {
                checkMeasure(measureName);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            measure.setExpression(getMeasureExprInner(measureName));

            // 2. set related column
            List<Pair<String, String>> parameterValues = Arrays.stream(m.getOperands()).map(operand -> {
                Pair<String, String> pair = new Pair<>();
                pair.setFirst("column");
                try {
                    pair.setSecond(getColNameWithTable(((SqlIdentifier) operand).names));
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
                return pair;
            }).collect(Collectors.toList());
            measure.setParameterValues(parameterValues);

            // 3. set measure return type
            measure.setReturnType(getMeasureTypeInner(measureName));
            return measure;
        }).collect(Collectors.toList());
        result.setSimplifiedMeasures(measures);
    }

    private void checkMeasure(String measureName) throws ParseException {
        String upperCaseName = measureName.toUpperCase();
        boolean res = SUPPORT_MEASURE_PREFIX.stream().anyMatch(str -> str.equals(upperCaseName));
        if (!res) {
            throw new ParseException("Measure type not support: " + measureName);
        }
    }

    private void parseDimsInner(List<SqlIdentifier> dims, DDLParserResult result) {
        List<NDataModel.NamedColumn> cols = dims.stream().map(d -> {
            NDataModel.NamedColumn col = new NDataModel.NamedColumn();
            try {
                col.setAliasDotColumn(getColNameWithTable(d.names));
                col.setName(getColNameWithTable(d.names).replace('.', '_'));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            return col;
        }).collect(Collectors.toList());
        result.setSimplifiedDimensions(cols);
    }

    private void parseFromIdentifier(SqlIdentifier identifier, DDLParserResult result, String sql) throws Exception {
        ImmutableList<String> names = identifier.names;
        if (names.size() == 2 || names.size() == 4) {
            // use `extractSubStringIgnoreSensitive` because project model are case-sensitive.
            result.setProjectName(StringUtil.extractSubStringIgnoreSensitive(sql, names.get(0)));
            result.setModelName(StringUtil.extractSubStringIgnoreSensitive(sql, names.get(1)));
            if (names.size() == 4) {
                result.setPartitionColName(names.get(2) + '.' + names.get(3));
            }
        } else {
            throw new ParseException(
                    "Identifier should contains project_name, model_name, partition_col_name(optional):" + names);
        }
    }

    private JoinDesc getJoinDesc(SqlJoin join) throws ParseException {
        JoinDesc res = new JoinDesc();
        res.setType(join.getJoinType().toString().toUpperCase());
        List<String> pKeys = Lists.newArrayList();
        List<String> fKeys = Lists.newArrayList();
        //Just get the outer condition
        SqlBasicCall condition = (SqlBasicCall) join.getCondition();
        List<SqlNode> operands = Arrays.stream(condition.getOperands()).collect(Collectors.toList());
        for (int i = 0; i < operands.size(); i++) {
            SqlNode operand = operands.get(i);
            if (operand instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) operand;
                operands.addAll(call.getOperandList());
            } else if (operand instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) operand;
                String colNameWithTable = getColNameWithTable(id.names);
                // col should be alternative
                if (pKeys.size() == fKeys.size()) {
                    fKeys.add(colNameWithTable);
                } else {
                    pKeys.add(colNameWithTable);
                }
            }
        }
        res.setPrimaryKey(pKeys.toArray(new String[0]));
        res.setForeignKey(fKeys.toArray(new String[0]));
        return res;
    }

    private String getColNameWithTable(ImmutableList<String> names) throws ParseException {
        if (names.size() == 2) {
            return names.get(0) + '.' + names.get(1);
        } else {
            throw new ParseException("colName must be table_name.col_name, got:" + names);
        }
    }

    private String getTableFullName(ImmutableList<String> names) throws ParseException {
        if (names.size() == 2) {
            return names.get(0) + '.' + names.get(1);
        } else if (names.size() == 1) {
            return "DEFAULT" + '.' + names.get(0);
        } else {
            throw new ParseException("tableName must be db_name.table_name, got:" + names);
        }
    }

    // default set to 14
    public static final String HLL_COUNT_TYPE = "hllc(14)";
    public static final String HLL_COUNT_TYPE_10 = "hllc(10)";
    public static final String HLL_COUNT_TYPE_12 = "hllc(12)";
    public static final String HLL_COUNT_TYPE_14 = "hllc(14)";
    public static final String HLL_COUNT_TYPE_15 = "hllc(15)";
    public static final String HLL_COUNT_TYPE_16 = "hllc(16)";
    public static final String BITMAP_COUNT_TYPE = "bitmap";
    // default set to 100
    public static final String PERCENTILE_TYPE = "percentile(100)";
    public static final String PERCENTILE_TYPE_100 = "percentile(100)";
    public static final String PERCENTILE_TYPE_1000 = "percentile(1000)";
    public static final String PERCENTILE_TYPE_10000 = "percentile(10000)";
    private static final String COUNT_DISTINCT_EXPR = "COUNT_DISTINCT";
    private static final String PERCENTILE_EXPR = "PERCENTILE_APPROX";

    // min, max, sum need set to `UNDEFINED`, then check return type in kylin
    public static final String UNDEFINED_TYPE = "UNDEFINED";

    private String getMeasureTypeInner(String measureName) {
        switch (measureName) {
            case "COUNT":
                return "bigint";

            case FUNC_PERCENTILE:
            case FUNC_PERCENTILE_APPROX:
                return PERCENTILE_TYPE;

            case FUNC_HLL_COUNT:
                return HLL_COUNT_TYPE;

            case FUNC_BITMAP_COUNT:
                return BITMAP_COUNT_TYPE;
            // Support diff precise hll
            case FUNC_HLL_COUNT_10:
                return HLL_COUNT_TYPE_10;
            case FUNC_HLL_COUNT_12:
                return HLL_COUNT_TYPE_12;
            case FUNC_HLL_COUNT_14:
                return HLL_COUNT_TYPE_14;
            case FUNC_HLL_COUNT_15:
                return HLL_COUNT_TYPE_15;
            case FUNC_HLL_COUNT_16:
                return HLL_COUNT_TYPE_16;
            // Support diff precise percentile
            case FUNC_PERCENTILE_100:
                return PERCENTILE_TYPE_100;
            case FUNC_PERCENTILE_1000:
                return PERCENTILE_TYPE_1000;
            case FUNC_PERCENTILE_10000:
                return PERCENTILE_TYPE_10000;
            default:
                return UNDEFINED_TYPE;
        }
    }

    private String getMeasureExprInner(String measureName) {
        switch (measureName) {
            case FUNC_PERCENTILE:
            case FUNC_PERCENTILE_APPROX:
            case FUNC_PERCENTILE_100:
            case FUNC_PERCENTILE_1000:
            case FUNC_PERCENTILE_10000:
                return PERCENTILE_EXPR;

            case FUNC_HLL_COUNT:
            case FUNC_BITMAP_COUNT:
            case FUNC_HLL_COUNT_10:
            case FUNC_HLL_COUNT_12:
            case FUNC_HLL_COUNT_14:
            case FUNC_HLL_COUNT_15:
            case FUNC_HLL_COUNT_16:
                return COUNT_DISTINCT_EXPR;

            default:
                return measureName.toUpperCase();
        }
    }

    @Getter
    @Setter
    @ToString
    public static class DDLParserResult {
        String modelName;
        String ProjectName;
        String partitionColName;
        //just col_name
        List<NDataModel.NamedColumn> simplifiedDimensions;
        // see InnerMeasure
        List<InnerMeasure> simplifiedMeasures;
        String factTable;

        // also means lookup tables
        List<JoinTableDesc> joinTables;
    }

    @Getter
    @Setter
    @ToString
    public static class InnerMeasure {
        // MIN, MAX, HLL(10) etc
        String expression;
        // Measure return type like bigInt, Integer, bitmap, hll(10)
        String returnType;
        // ("column", "db_name.table_name.col_name")
        List<Pair<String, String>> parameterValues;
    }
}
