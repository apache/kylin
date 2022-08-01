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

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSqlConvertletTable;
import org.apache.calcite.rex.RexSqlStandardConvertletTable;
import org.apache.calcite.rex.RexToSqlNodeConverter;
import org.apache.calcite.rex.RexToSqlNodeConverterImpl;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.filter.CompareResultType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.relnode.ColumnRowType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * convert rexNode to TblColRef
 */
@Slf4j
public class RexToTblColRefTranslator {

    private Set<TblColRef> sourceColumnCollector;
    private Map<RexNode, TblColRef> nodeAndTblColMap;
    private Map<String, SqlNode> rexToSqlMap = Maps.newHashMap();

    public RexToTblColRefTranslator() {
        this(new HashSet<>(), new HashMap<>());
    }

    public RexToTblColRefTranslator(Set<TblColRef> sourceColumnCollector, Map<RexNode, TblColRef> nodeAndTblColMap) {
        this.sourceColumnCollector = sourceColumnCollector;
        this.nodeAndTblColMap = nodeAndTblColMap;
    }

    /**
     * @param rexNode
     * @param inputColumnRowType
     * @param fieldName optional arg for error and log printing
     * @param sourceColumnCollector collect all source columns found in rexNode
     * @param nodeAndTblColMap collect all RexNode:TblColRef matchings
     * @return
     */
    public static TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName,
            Set<TblColRef> sourceColumnCollector, Map<RexNode, TblColRef> nodeAndTblColMap) {
        return new RexToTblColRefTranslator(sourceColumnCollector, nodeAndTblColMap).doTranslateRexNode(rexNode,
                inputColumnRowType, fieldName);
    }

    public static TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName,
            Map<RexNode, TblColRef> nodeAndTblColMap) {
        return new RexToTblColRefTranslator(new HashSet<>(), nodeAndTblColMap).doTranslateRexNode(rexNode,
                inputColumnRowType, fieldName);
    }

    public static TblColRef translateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType) {
        return new RexToTblColRefTranslator().doTranslateRexNode(rexNode, inputColumnRowType, rexNode.toString());
    }

    /**
     * In RexNode trees, OR and AND have any number of children,
     * SqlCall requires exactly 2. So, convert to a left-deep binary tree.
     */
    static RexNode createLeftCall(RexNode origin) {
        RexNode newRexNode = origin;
        if (origin instanceof RexCall) {
            RexCall call = (RexCall) origin;
            SqlOperator op = call.getOperator();
            List<RexNode> operands = call.getOperands();
            if ((op.getKind() == SqlKind.AND || op.getKind() == SqlKind.OR) && operands.size() > 2) {
                RexBuilder builder = new RexBuilder(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                RexNode first = builder.makeCall(op, operands.get(0), operands.get(1));
                for (int i = 2; i < operands.size(); i++) {
                    first = builder.makeCall(op, first, operands.get(i));
                }
                newRexNode = first;
            }
        }
        return newRexNode;
    }

    public TblColRef doTranslateRexNode(RexNode rexNode, ColumnRowType inputColumnRowType, String fieldName) {
        if (rexNode instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) rexNode;
            return translateRexInputRef(inputRef, inputColumnRowType, fieldName);
        } else if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;
            return translateRexLiteral(literal);
        } else if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
            return translateRexCall(call, inputColumnRowType, fieldName);
        } else if (rexNode instanceof RexDynamicParam) {
            RexDynamicParam call = (RexDynamicParam) rexNode;
            return translateRexDynamicParam(call);
        } else {
            throw new IllegalStateException("Unsupported RexNode " + rexNode);
        }
    }

    private TblColRef translateRexDynamicParam(RexDynamicParam rexParam) {
        return TblColRef.newDynamicColumn(rexParam.getName());
    }

    private TblColRef translateFirstRexInputRef(RexCall call, ColumnRowType inputColumnRowType, String fieldName) {
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexInputRef) {
                return translateRexInputRef((RexInputRef) operand, inputColumnRowType, fieldName);
            }
            if (operand instanceof RexCall) {
                TblColRef r = translateFirstRexInputRef((RexCall) operand, inputColumnRowType, fieldName);
                if (r != null)
                    return r;
            }
        }
        return null;
    }

    protected TblColRef translateRexInputRef(RexInputRef inputRef, ColumnRowType inputColumnRowType, String fieldName) {
        int index = inputRef.getIndex();
        // check it for rewrite count
        if (index < inputColumnRowType.size()) {
            Set<TblColRef> sourceColumns = inputColumnRowType.getSourceColumnsByIndex(index);
            sourceColumns.stream().filter(col -> !col.isInnerColumn()).forEach(sourceColumnCollector::add);
            return inputColumnRowType.getColumnByIndex(index);
        } else {
            throw new IllegalStateException("Can't find " + inputRef + " from child columnrowtype " + inputColumnRowType
                    + " with fieldname " + fieldName);
        }
    }

    TblColRef translateRexLiteral(RexLiteral literal) {
        if (literal.getTypeName() == SqlTypeName.SYMBOL) {
            final Enum symbol = (Enum) literal.getValue();
            return TblColRef.newInnerColumn(symbol.name(), TblColRef.InnerDataTypeEnum.LITERAL);
        }
        if (RexLiteral.isNullLiteral(literal)) {
            return TblColRef.newInnerColumn("null", TblColRef.InnerDataTypeEnum.LITERAL);
        } else {
            return TblColRef.newInnerColumn(literal.getValue().toString(), TblColRef.InnerDataTypeEnum.LITERAL);
        }

    }

    private TblColRef translateRexCall(RexCall call, ColumnRowType inputColumnRowType, String fieldName) {
        SqlOperator operator = call.getOperator();
        if (operator instanceof SqlUserDefinedFunction && ("QUARTER").equals(operator.getName())) {
            return translateFirstRexInputRef(call, inputColumnRowType, fieldName);
        }

        List<RexNode> children = limitTranslateScope(call.getOperands(), operator);
        List<TblColRef> tblColRefs = Lists.newArrayList();
        for (RexNode operand : children) {
            TblColRef colRef = doTranslateRexNode(operand, inputColumnRowType, fieldName);
            nodeAndTblColMap.put(operand, colRef);
            tblColRefs.add(colRef);
        }

        return TblColRef.newInnerColumn(fieldName, TblColRef.InnerDataTypeEnum.LITERAL, createInnerColumn(call),
                operator, tblColRefs);
    }

    private String createInnerColumn(RexCall call) {
        final RexSqlStandardConvertletTable convertletTable = new OLAPRexSqlStandardConvertletTable(call, rexToSqlMap);
        final RexToSqlNodeConverter rexNodeToSqlConverter = new ExtendedRexToSqlNodeConverter(convertletTable);

        try {
            SqlNode sqlCall = rexNodeToSqlConverter.convertCall(call);
            rexToSqlMap.put(call.toString(), sqlCall);
            return sqlCall.toSqlString(SqlDialect.DatabaseProduct.CALCITE.getDialect()).toString();
        } catch (Exception | Error e) {
            return call.toString();
        }
    }

    List<RexNode> limitTranslateScope(List<RexNode> children, SqlOperator operator) {

        //group by case when 1 = 1 then x 1 = 2 then y else z
        if (operator instanceof SqlCaseOperator) {
            int unknownWhenCalls = 0;
            for (int i = 0; i < children.size() - 1; i += 2) {
                if (children.get(i) instanceof RexCall) {
                    RexCall whenCall = (RexCall) children.get(i);
                    CompareResultType compareResultType = getCompareResultType(whenCall);
                    if (compareResultType == CompareResultType.ALWAYS_TRUE) {
                        return Lists.newArrayList(children.get(i), children.get(i + 1));
                    } else if (compareResultType == CompareResultType.UNKNOWN) {
                        unknownWhenCalls++;
                    }
                }
            }

            if (unknownWhenCalls == 0) {
                return Lists.newArrayList(children.get(children.size() - 1));
            }
        }

        return children;
    }

    CompareResultType getCompareResultType(RexCall whenCall) {
        List<RexNode> operands = whenCall.getOperands();
        if (SqlKind.EQUALS == whenCall.getKind() && operands != null && operands.size() == 2) {
            if (operands.get(0).equals(operands.get(1))) {
                return CompareResultType.ALWAYS_TRUE;
            }

            if (isConstant(operands.get(0)) && isConstant(operands.get(1))) {
                return CompareResultType.ALWAYS_FALSE;
            }
        }
        return CompareResultType.UNKNOWN;
    }

    boolean isConstant(RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            return true;
        }

        return rexNode instanceof RexCall && SqlKind.CAST == rexNode.getKind()
                && ((RexCall) rexNode).getOperands().get(0) instanceof RexLiteral;
    }

    static class OLAPRexSqlStandardConvertletTable extends RexSqlStandardConvertletTable {

        private static final BigDecimal SECONDS_OF_WEEK = new BigDecimal(604800); // a week equals 604800 seconds
        private static final BigDecimal MONTHS_OF_QUARTER = new BigDecimal(3); // a quarter equals 3 months
        final Map<TimeUnit, SqlDatePartFunction> timeUnitFunctions = initTimeUnitFunctionMap();

        private final Map<String, SqlNode> rexToSqlMap;

        public OLAPRexSqlStandardConvertletTable(RexCall call, Map<String, SqlNode> rexToSqlMap) {
            super();
            this.rexToSqlMap = rexToSqlMap;
            registerUdfOperator(call);
            registerCaseOpNew();
            registerReinterpret();
            registerCast();
            registerDivDate();
            registerExtract();
            registerTimestampAdd();
            registerTimestampDiff();
            registerSign();
            registerOperatorIfHasNot(call);
        }

        private void registerUdfOperator(RexCall call) {
            Set<String> udfs = Sets.newHashSet();
            KylinConfig.getInstanceFromEnv().getUDFs().forEach((key, value) -> {
                try {
                    val methods = Class.forName(value).getMethods();
                    for (Method method : methods) {
                        udfs.add(method.getName().toLowerCase(Locale.ROOT));
                    }
                } catch (Exception e) {
                    log.error("registerUdfOperator not found method for :", e);
                }
            });
            if (udfs.contains(call.getOperator().toString().toLowerCase(Locale.ROOT))) {
                SqlOperator operator = call.getOperator();
                this.registerEquivOp(operator);
            }
            List<RexNode> operands = call.getOperands();
            for (RexNode udfRexNode : operands) {
                if (udfRexNode instanceof RexCall) {
                    registerUdfOperator((RexCall) udfRexNode);
                }
            }
        }

        /**
         * ## WARN
         * Simply register the sqlCall, but if the sqlcall is not same with the rexCall,
         * it needs to construct own defined RexSqlConvertlet,just like TimesStampDiff
         * @param call
         */
        private void registerOperatorIfHasNot(RexCall call) {
            if (get(call) == null) {
                registerEquivOp(call.getOperator());
            }
        }

        private Map<TimeUnit, SqlDatePartFunction> initTimeUnitFunctionMap() {
            Map<TimeUnit, SqlDatePartFunction> rst = Maps.newHashMap();
            rst.putIfAbsent(TimeUnit.YEAR, SqlStdOperatorTable.YEAR);
            rst.putIfAbsent(TimeUnit.DAY, SqlStdOperatorTable.DAYOFMONTH);
            rst.putIfAbsent(TimeUnit.MONTH, SqlStdOperatorTable.MONTH);
            rst.putIfAbsent(TimeUnit.QUARTER, SqlStdOperatorTable.QUARTER);
            rst.putIfAbsent(TimeUnit.WEEK, SqlStdOperatorTable.WEEK);
            rst.putIfAbsent(TimeUnit.HOUR, SqlStdOperatorTable.HOUR);
            rst.putIfAbsent(TimeUnit.SECOND, SqlStdOperatorTable.SECOND);
            rst.putIfAbsent(TimeUnit.MINUTE, SqlStdOperatorTable.MINUTE);
            rst.putIfAbsent(TimeUnit.DOW, SqlStdOperatorTable.DAYOFWEEK);
            rst.putIfAbsent(TimeUnit.DOY, SqlStdOperatorTable.DAYOFYEAR);
            return rst;
        }

        /**
         * fix bug in registerCaseOp
         */
        private void registerCaseOpNew() {
            registerOp(SqlStdOperatorTable.CASE, (RexToSqlNodeConverter converter, RexCall call) -> {
                SqlNode[] operands = doConvertExpressionList(converter, call.getOperands());
                if (operands == null) {
                    return null;
                }
                SqlNodeList whenList = new SqlNodeList(SqlParserPos.ZERO);
                SqlNodeList thenList = new SqlNodeList(SqlParserPos.ZERO);
                int i = 0;
                while (i < operands.length - 1) {
                    whenList.add(operands[i]);
                    ++i;
                    thenList.add(operands[i]);
                    ++i;
                }
                SqlNode elseExpr = operands[i];
                SqlNode[] newOperands = new SqlNode[4];
                newOperands[0] = null;
                newOperands[1] = whenList;
                newOperands[2] = thenList;
                newOperands[3] = elseExpr;
                return SqlStdOperatorTable.CASE.createCall(null, SqlParserPos.ZERO, newOperands);
            });
        }

        /**
         *  The RexCall of the expression `timestampdiff(second, time0, time1)` is:
         *  `/(Reinterpret(-($23, $22)), 1000)`.
         *  We need to re-translate this RexCall to a SqlCall of TIMESTAMPDIFF.
         *  <br>
         *  <br>Following steps used for erasing redundant content of REINTERPRET:
         *  <br>1. Detect whether the operator is instance of SqlDatetimeSubtractionOperator;
         *  <br>2. if true, then convert the RexNode call.operands.get(0) to a SqlNode and return.
         *  <br>3. otherwise try default processing method.
         *  <br>
         *  <br>In this example, the translation result of RexNode `Reinterpret(-($23, $22)`
         *  is `timestampdiff(second, time0, time1)`.
         */
        private void registerReinterpret() {
            registerOp(SqlStdOperatorTable.REINTERPRET, (RexToSqlNodeConverter converter, RexCall call) -> {
                RexNode node = call.operands.get(0);
                if (node instanceof RexCall && ((RexCall) node).getOperator() == SqlStdOperatorTable.MINUS_DATE) {
                    return converter.convertNode(node);
                }
                return convertCall(converter, call);
            });
        }

        /**
         * The RexCall of the expression `timestampdiff(second, time0, time1)` is:
         * `/(Reinterpret(-($23, $22)), 1000)`.
         * We need to re-translate this RexCall to a SqlCall of TIMESTAMPDIFF.
         * <br>
         * <br>Following steps used for erasing redundant content of CAST:
         * <br>1. Detect whether the sub-RexNode is divide integer operation;
         * <br>2. if true, then convert the RexCall call.operands.get(0) to a SqlNode and return;
         * <br>3. otherwise, try to translate normally.
         * <br>
         * <br>In this example, the translation result of RexNode `CAST(/INT(Reinterpret(-($23, $22)), 1000)):INTEGER`
         * is `timestampdiff(second, time0, time1)`.
         */
        private void registerCast() {
            registerOp(SqlStdOperatorTable.CAST, (RexToSqlNodeConverter converter, RexCall call) -> {
                RexNode node = call.operands.get(0);
                if (node instanceof RexCall) {
                    RexCall rexCall = (RexCall) node;
                    if (rexCall.getOperator() == SqlStdOperatorTable.REINTERPRET
                            || rexCall.getOperator() == SqlStdOperatorTable.DIVIDE_INTEGER) {
                        return converter.convertNode(node);
                    }
                }

                SqlNode[] operands = doConvertExpressionList(converter, call.operands);
                if (operands == null) {
                    return null;
                }
                List<SqlNode> operandList = Lists.newArrayList(operands);
                SqlDataTypeSpec typeSpec = call.getType().getFamily() == SqlTypeFamily.TIMESTAMP
                        ? new SqlDataTypeSpec(call.getType().getSqlIdentifier(), -1, -1, null, null, SqlParserPos.ZERO)
                        : SqlTypeUtil.convertTypeToSpec(call.getType());
                operandList.add(typeSpec);
                return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, operandList);
            });
        }

        /**
         * The RexCall of the expression `timestampdiff(second, time0, time1)` is:
         * `/(Reinterpret(-($23, $22)), 1000)`.
         * We need to re-translate this RexCall to a SqlCall of TIMESTAMPDIFF.
         * <br>
         * <br>Following steps use for erasing redundant content of DIVIDE_INTEGER:
         * <br>1. Detect whether the first sub-RexNode is reinterpret operation;
         * <br>2. if true, then convert the RexCall call.operands.get(0) to a SqlNode and return;
         * <br>3. otherwise, try default processing method.
         * <br>
         * <br>In this example, the translation result of RexNode
         * `/INT(Reinterpret(-($23, $22)), 1000)` is `timestampdiff(second, time0, time1)`.
         */
        private void registerDivDate() {
            registerOp(SqlStdOperatorTable.DIVIDE_DATE, (RexToSqlNodeConverter converter, RexCall call) -> {
                List<RexNode> rexNodes = call.getOperands();
                if (rexNodes.size() == 2
                        && (rexNodes.get(0).isA(SqlKind.REINTERPRET) || rexNodes.get(0).isA(SqlKind.DIVIDE))) {
                    SqlNode node = converter.convertCall((RexCall) rexNodes.get(0));
                    RexNode secRex = rexNodes.get(1);
                    if (node.getKind() == SqlKind.TIMESTAMP_DIFF && secRex instanceof RexLiteral) {
                        SqlCall diffCall = (SqlBasicCall) node;
                        RexLiteral literal = (RexLiteral) secRex;
                        if (literal.getValue().equals(OLAPRexSqlStandardConvertletTable.SECONDS_OF_WEEK)) {
                            SqlNode week = SqlLiteral.createSymbol(TimeUnit.WEEK, SqlParserPos.ZERO);
                            return SqlStdOperatorTable.TIMESTAMP_DIFF.createCall(SqlParserPos.ZERO,
                                    Lists.newArrayList(week, diffCall.operand(1), diffCall.operand(2)));
                        } else if (literal.getValue().equals(OLAPRexSqlStandardConvertletTable.MONTHS_OF_QUARTER)) {
                            SqlNode quarter = SqlLiteral.createSymbol(TimeUnit.QUARTER, SqlParserPos.ZERO);
                            return SqlStdOperatorTable.TIMESTAMP_DIFF.createCall(SqlParserPos.ZERO,
                                    Lists.newArrayList(quarter, diffCall.operand(1), diffCall.operand(2)));
                        }
                    }
                    return converter.convertCall((RexCall) rexNodes.get(0));
                }

                return convertCall(converter, call);
            });
        }

        /**
         * The RexCall of the expression `timestampdiff(second, time0, time1)` is:
         * `/(Reinterpret(-($23, $22)), 1000)`.
         * We need to re-translate this RexCall to a SqlCall of TIMESTAMPDIFF.
         * <br>
         * <br>Following steps use for translating TIMESTAMPDIFF RexCall to SqlCall:
         * <br>1. get the first operand from call.getType().getIntervalQualifier().getUnit();
         * <br>2. get the second operand from call.operands.get(1);
         * <br>3. get the third operand from call.operands.get(0).
         * <br>
         * <br>In this example, the translation result of RexNode
         * `-($23, $22)` is `timestampdiff(second, time0, time1)`.
         */
        private void registerTimestampDiff() {
            registerOp(SqlStdOperatorTable.MINUS_DATE, (RexToSqlNodeConverter converter, RexCall call) -> {
                SqlNode[] operands = doConvertExpressionList(converter, call.operands);
                TimeUnit unit = call.getType().getIntervalQualifier().getUnit();
                SqlNode first = SqlLiteral.createSymbol(unit, SqlParserPos.ZERO);
                return SqlStdOperatorTable.TIMESTAMP_DIFF.createCall(SqlParserPos.ZERO, first, operands[1],
                        operands[0]);
            });
        }

        /**
         * Translate a TIMESTAMPADD RexCall to a TIMESTAMPADD SqlCall.
         * For example: the RexCall of the expression `timestampadd(minute, 1, time0)` is:
         * `DATETIME_PLUS($22, 60000)`. We need to re-translate the RexCall to a SqlCall.
         * <br>
         * <br>Following steps used for translating TIMESTAMPADD RexCall to SqlCall.
         * <br> 1. get the first operand from the call.operands.get(1).getType().getIntervalQualifier().getUnit();
         * <br> 2. get the second operand from the call.operands.get(1).value / TimeUnit.multiplier;
         * <br> 3. get the third operand from the call.operands.get(0).
         * <br>
         * <br>In this example, the translation result of RexCall `DATETIME_PLUS($22, 60000)`
         * is `timestampadd(minute, 1, time0)`.
         */
        private void registerTimestampAdd() {
            registerOp(SqlStdOperatorTable.DATETIME_PLUS, (RexToSqlNodeConverter converter, RexCall call) -> {
                RexNode firstOperand = call.operands.get(0);
                RexNode secondOperand = call.operands.get(1);
                TimeUnit unit = secondOperand.getType().getIntervalQualifier().getUnit();
                SqlNode first = SqlLiteral.createSymbol(unit, SqlParserPos.ZERO);
                SqlNode third = doConvertExpression(converter, firstOperand);
                BigDecimal multiplier = unit.multiplier;
                if (secondOperand instanceof RexLiteral) {
                    BigDecimal interval = new BigDecimal(((RexLiteral) secondOperand).getValue().toString())
                            .divide(multiplier);
                    SqlNode second = SqlLiteral.createExactNumeric(interval.toString(), SqlParserPos.ZERO);
                    return SqlStdOperatorTable.TIMESTAMP_ADD.createCall(SqlParserPos.ZERO, first, second, third);
                } else if (secondOperand instanceof RexCall) {
                    RexCall call0 = (RexCall) secondOperand;
                    if (call0.getOperands().size() == 2) {
                        RexNode subNode = call0.getOperands().get(1);
                        SqlNode second = doConvertExpression(converter, subNode);
                        return SqlStdOperatorTable.TIMESTAMP_ADD.createCall(SqlParserPos.ZERO, first, second, third);
                    }
                }
                throw new NotImplementedException("Not implement convert for RexCall, " + call.toString());
            });
        }

        private void registerSign() {
            registerOp(SqlStdOperatorTable.SIGN, (RexToSqlNodeConverter converter, RexCall call) -> {
                RexNode operand = call.operands.get(0);
                SqlNode node = doConvertExpression(converter, operand);
                return SqlStdOperatorTable.SIGN.createCall(SqlParserPos.ZERO, node);
            });
        }

        /**
         * Translate EXTRACT RexCall to corresponding SqlCall. At present, we only handle following
         * functions:YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND, DAYOFYEAR, DAYOFWEEK, DAYOFMONTH.
         *
         * For example: the RexCall of expression `year(time0)` is: EXTRACT(FLAG(YEAR), $22).
         * We need to translate this RexCall to a SqlCall to keep consistance with SparkSQL.
         */
        private void registerExtract() {
            registerOp(SqlStdOperatorTable.EXTRACT, (RexToSqlNodeConverter converter, RexCall call) -> {
                RexLiteral firstOperand = (RexLiteral) call.operands.get(0);
                val unit = firstOperand.getValue();
                if (unit instanceof TimeUnitRange && ((TimeUnitRange) unit).endUnit == null) {
                    RexNode secondOperand = call.operands.get(1);
                    SqlNode param = doConvertExpression(converter, secondOperand);
                    TimeUnit startUnit = ((TimeUnitRange) unit).startUnit;
                    if (timeUnitFunctions.containsKey(startUnit)) {
                        return timeUnitFunctions.get(startUnit).createCall(SqlParserPos.ZERO, param);
                    }
                }
                return convertCall(converter, call);
            });
        }

        SqlNode[] doConvertExpressionList(RexToSqlNodeConverter converter, List<RexNode> nodes) {
            final SqlNode[] exprs = new SqlNode[nodes.size()];
            for (int i = 0; i < nodes.size(); i++) {
                RexNode node = nodes.get(i);
                exprs[i] = doConvertExpression(converter, node);
                if (exprs[i] == null) {
                    return null;
                }
            }
            return exprs;
        }

        SqlNode doConvertExpression(RexToSqlNodeConverter converter, RexNode node) {
            SqlNode sqlNode = rexToSqlMap.get(node.toString());
            if (sqlNode == null) {
                sqlNode = converter.convertNode(node);
                rexToSqlMap.put(node.toString(), sqlNode);
            }
            return sqlNode;
        }
    }

    class ExtendedRexToSqlNodeConverter extends RexToSqlNodeConverterImpl {

        ExtendedRexToSqlNodeConverter(RexSqlConvertletTable convertletTable) {
            super(convertletTable);
        }

        @Override
        public SqlNode convertLiteral(RexLiteral literal) {
            SqlNode sqlLiteral = super.convertLiteral(literal);
            if (sqlLiteral == null) {
                if (literal.getTypeName().getName().equals("SYMBOL")) {
                    // INTERVAL QUALIFIER
                    if (literal.getValue() instanceof TimeUnitRange) {
                        TimeUnitRange timeUnitRange = (TimeUnitRange) literal.getValue();
                        return new SqlIntervalQualifier(timeUnitRange.startUnit, timeUnitRange.endUnit,
                                SqlParserPos.ZERO);
                    }
                }
                sqlLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
            }
            return sqlLiteral;
        }

        @Override
        public SqlNode convertInputRef(RexInputRef ref) {
            TblColRef colRef = nodeAndTblColMap.get(ref);
            String colExpr = colRef.isInnerColumn() && colRef.getParserDescription() != null
                    ? colRef.getParserDescription()
                    : "\"" + colRef.getTableAlias() + "\".\"" + colRef.getName() + "\"";
            try {
                return CalciteParser.getExpNode(colExpr);
            } catch (Exception e) {
                return super.convertInputRef(ref); // i.e. return null
            }
        }

        @Override
        public SqlNode convertCall(RexCall call) {
            RexCall newCall = (RexCall) createLeftCall(call);
            return super.convertCall(newCall);
        }
    }
}
