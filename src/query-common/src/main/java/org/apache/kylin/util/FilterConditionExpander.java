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

package org.apache.kylin.util;

import static org.apache.kylin.common.exception.QueryErrorCode.UNSUPPORTED_EXPRESSION;

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimestampString;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPRel;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import lombok.val;
import lombok.var;

public class FilterConditionExpander {
    public static final Logger logger = LoggerFactory.getLogger(FilterConditionExpander.class);

    private static final String DATE = "date";
    private static final String TIMESTAMP = "timestamp";

    private final OLAPContext context;
    private final RelNode currentRel;
    private final RexBuilder rexBuilder;

    public FilterConditionExpander(OLAPContext context, RelNode currentRel) {
        this.context = context;
        this.currentRel = currentRel;
        this.rexBuilder = currentRel.getCluster().getRexBuilder();
    }

    public List<RexNode> convert(RexNode node) {
        if (!(node instanceof RexCall)) {
            return new LinkedList<>();
        }
        try {
            List<RexNode> results = new LinkedList<>();
            RexCall call = (RexCall) node;
            for (RexNode conjunction : RelOptUtil.conjunctions(RexUtil.toCnf(rexBuilder, 100, call))) {
                RexNode converted = convertDisjunctionCall(conjunction);
                if (converted != null) {
                    results.add(converted);
                }
            }
            return results;
        } catch (KylinException e) {
            logger.warn("Filter condition is too complex to be converted");
            return new LinkedList<>();
        }
    }

    // handle calls of form (A or B or C)
    public RexNode convertDisjunctionCall(RexNode node) {
        if (!(node instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) node;
        // OR: discard the whole part if any sub expr fails to push down
        // NOT: discard the whole part if any sub expr fails to push down
        // AND: if AND appears, CNF conversion is failed, throw exception and exit
        if (call.getOperator() == SqlStdOperatorTable.OR) {
            List<RexNode> convertedList = new LinkedList<>();
            for (RexNode operand : call.getOperands()) {
                RexNode converted = convertDisjunctionCall(operand);
                if (converted == null) {
                    return null;
                }
                convertedList.add(converted);
            }
            return convertedList.isEmpty() ? null : rexBuilder.makeCall(SqlStdOperatorTable.OR, convertedList);
        } else if (call.getOperator() == SqlStdOperatorTable.AND) {
            throw new KylinException(UNSUPPORTED_EXPRESSION, "filter expression not in CNF");
        } else if (call.getOperator() == SqlStdOperatorTable.NOT) {
            RexNode converted = convertDisjunctionCall(call.getOperands().get(0));
            return converted == null ? null : rexBuilder.makeCall(SqlStdOperatorTable.NOT, converted);
        } else {
            return convertSimpleCall(call);
        }
    }

    // handles only simple expression of form <RexInputRef> <op> <RexLiteral>
    public RexNode convertSimpleCall(RexCall call) {
        val op0 = call.getOperands().get(0);
        RexInputRef lInputRef = null;
        if (call.getOperands().get(0) instanceof RexInputRef) {
            lInputRef = convertInputRef((RexInputRef) call.getOperands().get(0), currentRel);
        } else if (op0 instanceof RexCall && ((RexCall) op0).getOperator() == SqlStdOperatorTable.CAST
                && ((RexCall) op0).getOperands().get(0) instanceof RexInputRef) {
            lInputRef = convertInputRef((RexInputRef) ((RexCall) op0).getOperands().get(0), currentRel);
        }

        if (lInputRef == null) {
            return null;
        }

        // single operand expr(col)
        if (call.getOperands().size() == 1) {
            return rexBuilder.makeCall(call.getOperator(), lInputRef);
        }

        if (call.getOperands().size() > 1) {
            // col IN (...)
            if (call.getOperands().get(0) instanceof RexInputRef) {
                val operator = call.getOperator();
                if (operator.equals(SqlStdOperatorTable.IN)) {
                    return convertIn(lInputRef, call.getOperands().subList(1, call.getOperands().size()), true);
                } else if (operator.equals(SqlStdOperatorTable.NOT_IN)) {
                    return convertIn(lInputRef, call.getOperands().subList(1, call.getOperands().size()), false);
                }
            }

            // col <op> lit
            val op1 = call.getOperands().get(1);
            if (call.getOperands().size() == 2 && op1 instanceof RexLiteral) {
                var rLit = (RexLiteral) op1;
                rLit = ((RexLiteral) op1).getValue() instanceof NlsString ? transformRexLiteral(lInputRef, rLit) : rLit;
                return rexBuilder.makeCall(call.getOperator(), lInputRef, rLit);
            }
        }

        return null;
    }

    private RexNode convertIn(RexInputRef rexInputRef, List<RexNode> extendedOperands, boolean isIn) {
        val transformedOperands = Lists.<RexNode> newArrayList();
        for (RexNode operand : extendedOperands) {
            RexNode transformedOperand;
            if (!(operand instanceof RexLiteral)) {
                return null;
            }
            if (((RexLiteral) operand).getValue() instanceof NlsString) {
                val transformed = transformRexLiteral(rexInputRef, (RexLiteral) operand);
                transformedOperand = transformed == null ? operand : transformed;
            } else {
                transformedOperand = operand;
            }

            val operator = isIn ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.NOT_EQUALS;
            transformedOperands.add(rexBuilder.makeCall(operator, rexInputRef, transformedOperand));
        }

        if (transformedOperands.size() == 1) {
            return transformedOperands.get(0);
        }

        val operator = isIn ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND;
        return rexBuilder.makeCall(operator, transformedOperands);
    }

    private RexLiteral transformRexLiteral(RexInputRef inputRef, RexLiteral operand2) {
        val literalValue = operand2.getValue();
        val literalValueInString = ((NlsString) literalValue).getValue();
        val typeName = inputRef.getType().getSqlTypeName().getName();
        try {
            if (typeName.equalsIgnoreCase(DATE)) {
                return rexBuilder.makeDateLiteral(new DateString(literalValueInString));
            } else if (typeName.equalsIgnoreCase(TIMESTAMP)) {
                return rexBuilder.makeTimestampLiteral(new TimestampString(literalValueInString),
                        inputRef.getType().getPrecision());
            }
        } catch (Exception ex) {
            logger.warn("transform Date/Timestamp RexLiteral for filterRel failed", ex);
        }

        return operand2;
    }

    private RexInputRef convertInputRef(RexInputRef rexInputRef, RelNode relNode) {
        if (relNode instanceof TableScan) {
            return context.createUniqueInputRefContextTables((OLAPTableScan) relNode, rexInputRef.getIndex());
        }

        if (relNode instanceof Project) {
            val projectRel = (Project) relNode;
            val expression = projectRel.getChildExps().get(rexInputRef.getIndex());
            return expression instanceof RexInputRef ? convertInputRef((RexInputRef) expression, projectRel.getInput(0))
                    : null;
        }

        val index = rexInputRef.getIndex();
        int currentSize = 0;
        for (int i = 0; i < relNode.getInputs().size(); i++) {
            // don't push filters down in some cases of join
            if (relNode instanceof Join) {
                val join = (Join) relNode;
                if (join.getJoinType() == JoinRelType.LEFT && i == 1
                        || join.getJoinType() == JoinRelType.RIGHT && i == 0
                        || join.getJoinType() == JoinRelType.FULL) {
                    continue;
                }
            }

            val child = (OLAPRel) relNode.getInput(i);
            val childRowTypeSize = child.getColumnRowType().size();
            if (index < currentSize + childRowTypeSize) {
                return convertInputRef(RexInputRef.of(index - currentSize, child.getRowType()), child);
            }
            currentSize += childRowTypeSize;
        }

        return null;
    }

}
