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
import java.util.Map;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.NlsString;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.relnode.OlapRel;
import org.apache.kylin.query.relnode.OlapTableScan;
import org.apache.kylin.query.util.RexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;
import lombok.var;

public class FilterConditionExpander {
    public static final Logger logger = LoggerFactory.getLogger(FilterConditionExpander.class);

    private final OlapContext context;
    private final OlapRel currentRel;
    private final RexBuilder rexBuilder;

    private final Map<String, RexNode> cachedConvertedRelMap = Maps.newHashMap();

    public FilterConditionExpander(OlapContext context, OlapRel currentRel) {
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
        RexInputRef lInputRef = convertInputRef(call);

        if (lInputRef == null) {
            return null;
        }

        // single operand expr(col)
        if (call.getOperands().size() == 1) {
            // ifnull(flag, true) simplified by calcite as  `cast(flag): boolean not null`
            return call.isA(SqlKind.CAST) ? lInputRef : rexBuilder.makeCall(call.getOperator(), lInputRef);
        }

        if (call.getOperands().size() > 1) {
            // col IN (xxx) or col not_in (xxx)
            if (call.getOperands().get(0) instanceof RexInputRef) {
                val operator = call.getOperator();
                if (operator.equals(SqlStdOperatorTable.IN) || operator.equals(SqlStdOperatorTable.NOT_IN)) {
                    return convertLiterals(lInputRef, call.getOperands().subList(1, call.getOperands().size()),
                            operator);
                }
            }

            // col <op> lit, optimized with cache
            if (cachedConvertedRelMap.containsKey(call.toString())) {
                return cachedConvertedRelMap.get(call.toString());
            } else {
                RexNode simplified = simplify(call, lInputRef);
                cachedConvertedRelMap.put(call.toString(), simplified); // add cache
                return simplified;
            }
        }

        return null;
    }

    private RexNode simplify(RexCall call, RexInputRef lInputRef) {
        val op1 = call.getOperands().get(1);
        if (call.getOperands().size() == 2) {
            // accept cases: the right rel is literal
            if (op1 instanceof RexLiteral) {
                var rLit = (RexLiteral) op1;
                rLit = transformRexLiteral(lInputRef, rLit);
                return rexBuilder.makeCall(call.getOperator(), lInputRef, rLit);
            }
            // accept cases: the right rel is cast(literal as datatype)
            if (op1.isA(SqlKind.CAST)) {
                RexCall c = (RexCall) op1;
                RexNode rexNode = c.getOperands().get(0);
                if (rexNode instanceof RexLiteral) {
                    RexLiteral rLit = transformRexLiteral(lInputRef, (RexLiteral) rexNode);
                    return rexBuilder.makeCall(call.getOperator(), lInputRef, rLit);
                }
            }
        }
        return null;
    }

    private RexNode convertLiterals(RexInputRef rexInputRef, List<RexNode> rexLiterals, SqlOperator operator) {
        List<RexNode> transformedOperands = Lists.newArrayList();
        transformedOperands.add(rexInputRef);
        for (RexNode operand : rexLiterals) {
            if (!(operand instanceof RexLiteral)) {
                return null;
            }
            transformedOperands.add(operand);
        }
        return rexBuilder.makeCall(operator, transformedOperands);
    }

    private RexLiteral transformRexLiteral(RexInputRef inputRef, RexLiteral operand2) {
        DataType dataType = DataType.getType(inputRef.getType().getSqlTypeName().getName());
        String value;
        if (operand2.getValue() instanceof NlsString) {
            value = RexLiteral.stringValue(operand2);
        } else {
            Comparable c = RexLiteral.value(operand2);
            value = c == null ? null : c.toString();
        }
        try {
            return (RexLiteral) RexUtils.transformValue2RexLiteral(rexBuilder, value, dataType);
        } catch (Exception ex) {
            logger.warn("transform rexLiteral({}) failed: {}", RexLiteral.value(operand2), ex.getMessage());
        }
        return operand2;
    }

    private RexInputRef convertInputRef(RexCall call) {
        RexInputRef resultInputRef = null;
        RexInputRef originInputRef = extractInputRef(call.getOperands().get(0));
        if (originInputRef != null) {
            RexInputRef rexInputRef = extractTableScanInputRef(originInputRef, currentRel);
            if (rexInputRef != null) {
                String tableAliasColName = currentRel.getColumnRowType().getColumnByIndex(originInputRef.getIndex())
                        .getTableAliasColName();
                RelDataType targetType = call.isA(SqlKind.CAST) ? call.getType() : rexInputRef.getType();
                resultInputRef = tableAliasColName == null
                        ? new RexInputRef(rexInputRef.getName(), rexInputRef.getIndex(), targetType)
                        : new RexInputRef(tableAliasColName, rexInputRef.getIndex(), targetType);
            }
        }
        return resultInputRef;
    }

    private RexInputRef extractInputRef(RexNode node) {
        if (node instanceof RexInputRef) {
            return (RexInputRef) node;
        } else if (node instanceof RexCall && ((RexCall) node).getOperator() == SqlStdOperatorTable.CAST) {
            RexNode operand = ((RexCall) node).getOperands().get(0);
            if (operand instanceof RexInputRef) {
                return (RexInputRef) operand;
            }
        }
        return null;
    }

    private RexInputRef extractTableScanInputRef(RexInputRef rexInputRef, RelNode relNode) {
        if (relNode instanceof TableScan) {
            return ContextUtil.createUniqueInputRefAmongTables((OlapTableScan) relNode, rexInputRef.getIndex(),
                    context.getAllTableScans());
        }

        if (relNode instanceof Project) {
            return extractProjectInputRef((Project) relNode, rexInputRef);
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

            val child = (OlapRel) relNode.getInput(i);
            val childRowTypeSize = child.getColumnRowType().size();
            if (index < currentSize + childRowTypeSize) {
                return extractTableScanInputRef(RexInputRef.of(index - currentSize, child.getRowType()), child);
            }
            currentSize += childRowTypeSize;
        }

        return null;
    }

    private RexInputRef extractProjectInputRef(Project projectRel, RexInputRef rexInputRef) {
        val expression = projectRel.getProjects().get(rexInputRef.getIndex());
        return expression instanceof RexInputRef
                ? extractTableScanInputRef((RexInputRef) expression, projectRel.getInput(0))
                : null;
    }

}
