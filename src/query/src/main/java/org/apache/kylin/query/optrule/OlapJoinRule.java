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

package org.apache.kylin.query.optrule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.query.relnode.OlapFilterRel;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.relnode.OlapNonEquiJoinRel;
import org.apache.kylin.query.relnode.OlapRel;

public class OlapJoinRule extends ConverterRule {

    private static final Set<Set<SqlOperator>> ALLOWED_PAIRS = ImmutableSet.of(
            ImmutableSet.of(SqlStdOperatorTable.GREATER_THAN, SqlStdOperatorTable.LESS_THAN_OR_EQUAL),
            ImmutableSet.of(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, SqlStdOperatorTable.LESS_THAN_OR_EQUAL),
            ImmutableSet.of(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, SqlStdOperatorTable.LESS_THAN));

    private SqlOperator inverse(SqlOperator operator) {
        if (operator.equals(SqlStdOperatorTable.GREATER_THAN)) {
            return SqlStdOperatorTable.LESS_THAN;
        } else if (operator.equals(SqlStdOperatorTable.LESS_THAN)) {
            return SqlStdOperatorTable.GREATER_THAN;
        } else if (operator.equals(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)) {
            return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        } else if (operator.equals(SqlStdOperatorTable.LESS_THAN_OR_EQUAL)) {
            return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
        }
        return null;
    }

    public static final ConverterRule INSTANCE = new OlapJoinRule();
    public static final ConverterRule NON_EQUI_INSTANCE = new OlapJoinRule(true, false);
    public static final ConverterRule EQUAL_NULL_SAFE_INSTANT = new OlapJoinRule(false, true);
    private static final ImmutableSet<SqlKind> SCD2_KINDS = ImmutableSet.of(SqlKind.GREATER_THAN, SqlKind.LESS_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN_OR_EQUAL);

    private final boolean isScd2Enabled;
    private final boolean joinCondEqualNullSafe;

    public OlapJoinRule() {
        this(false, false);
    }

    public OlapJoinRule(boolean isScd2Enabled, boolean joinCondEqualNullSafe) {
        super(LogicalJoin.class, Convention.NONE, OlapRel.CONVENTION, "OlapJoinRule");
        this.isScd2Enabled = isScd2Enabled;
        this.joinCondEqualNullSafe = joinCondEqualNullSafe;
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        RelNode left = join.getInput(0);
        RelNode right = join.getInput(1);

        RelTraitSet traitSet = join.getTraitSet().replace(OlapRel.CONVENTION);
        left = left instanceof HepRelVertex ? left : convert(left, left.getTraitSet().replace(OlapRel.CONVENTION));
        right = right instanceof HepRelVertex ? right : convert(right, right.getTraitSet().replace(OlapRel.CONVENTION));

        JoinInfo info = JoinInfo.of(left, right, join.getCondition());

        // handle powerbi inner join
        Join tmpJoin = transformJoinCondition(join, info, traitSet, left, right);
        if (tmpJoin instanceof OlapJoinRel) {
            return tmpJoin;
        }

        // usually non-equiv-join can not be converted to equiv-join + filter
        // keep OlapNonEquiJoinRel and cut to small OlapContexts in the class of OlapNonEquiJoinRel
        try {
            RexBuilder rexBuilder = join.getCluster().getRexBuilder();
            RexNode cnfCondition = RexUtil.toCnf(rexBuilder, join.getCondition());
            info = JoinInfo.of(left, right, cnfCondition);
            boolean isSpecialNonEquivJoin = isSpecialNonEquivJoin(join);
            if (!info.isEqui() || isSpecialNonEquivJoin) {
                List<RexInputRef> scd2Refs = Lists.newArrayList();
                boolean isScd2Rel = isScd2Enabled && isScd2JoinCondition(info, join, scd2Refs);
                if (!isSpecialNonEquivJoin && join.getJoinType() == JoinRelType.INNER && !isScd2Rel
                        && hasEqualJoinPart(left, right, join.getCondition())) {
                    OlapJoinRel joinRel = new OlapJoinRel(join.getCluster(), traitSet, left, right,
                            info.getEquiCondition(left, right, rexBuilder), join.getVariablesSet(), join.getJoinType());
                    joinRel.setJoinCondEqualNullSafe(joinCondEqualNullSafe);
                    RexNode rexNode = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions);
                    return rexNode.isAlwaysTrue() ? joinRel
                            : new OlapFilterRel(join.getCluster(), joinRel.getTraitSet(), joinRel, rexNode);
                }

                // cnf is better, but conflict with transformJoinCondition, need optimize
                RexNode joinCondition = normalizeCondition(rexBuilder, join.getCondition(), scd2Refs);
                return new OlapNonEquiJoinRel(join.getCluster(), traitSet, left, right, joinCondition,
                        join.getVariablesSet(), join.getJoinType(), isScd2Rel);
            } else {
                OlapJoinRel joinRel = new OlapJoinRel(join.getCluster(), traitSet, left, right,
                        info.getEquiCondition(left, right, rexBuilder), join.getVariablesSet(), join.getJoinType());
                joinRel.setJoinCondEqualNullSafe(joinCondEqualNullSafe);
                return joinRel;
            }
        } catch (InvalidRelException e) {
            throw new AssertionError(e);
        }
    }

    private static boolean isSpecialNonEquivJoin(LogicalJoin join) {
        return RexUtil.findOperatorCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, join.getCondition()) != null;
    }

    private RexNode normalizeCondition(RexBuilder rexBuilder, RexNode cnfCondition, List<RexInputRef> scd2Refs) {
        if (scd2Refs.isEmpty() || !(cnfCondition instanceof RexCall)) {
            return cnfCondition;
        }

        List<RexNode> newNodes = Lists.newArrayList();
        List<RexNode> oriNodes = ((RexCall) cnfCondition).getOperands();
        for (RexNode rexNode : oriNodes) {
            RexNode invert = rexNode;
            if (SCD2_KINDS.contains(rexNode.getKind())) {
                RexNode left = ((RexCall) invert).getOperands().get(0);
                RexInputRef rexInputRef = extractInputRef(left);
                if (!scd2Refs.contains(rexInputRef)) {
                    invert = RexUtil.invert(rexBuilder, (RexCall) rexNode);
                }
            } else {
                invert = normalizeCondition(rexBuilder, rexNode, scd2Refs);
            }
            if (invert == null) {
                invert = rexNode;
            }
            newNodes.add(invert);
        }
        if (!Objects.equals(newNodes.toString(), oriNodes.toString())) {
            return rexBuilder.makeCall(cnfCondition.getType(), ((RexCall) cnfCondition).getOperator(), newNodes);
        }
        return cnfCondition;
    }

    private boolean hasEqualJoinPart(RelNode left, RelNode right, RexNode condition) {
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        final List<Boolean> filterNulls = new ArrayList<>();
        RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls);
        return !leftKeys.isEmpty() && !rightKeys.isEmpty();
    }

    private Join transformJoinCondition(LogicalJoin join, JoinInfo info, RelTraitSet traitSet, RelNode left,
            RelNode right) {
        List<RexInputRef> refs = isPowerBiInnerJoin(info, left.getCluster().getRexBuilder());
        if (refs.isEmpty()) {
            return join;
        }

        // The ref index is global index. key index is local.
        RelOptCluster cluster = join.getCluster();
        int index1 = refs.get(0).getIndex();
        int index2 = refs.get(1).getIndex();
        int leftIndex = Math.min(index1, index2);
        int rightIndex = Math.max(index1, index2);
        rightIndex -= left.getRowType().getFieldCount();

        JoinInfo newInfo = JoinInfo.of(ImmutableIntList.of(leftIndex), ImmutableIntList.of(rightIndex));
        try {
            return new OlapJoinRel(cluster, traitSet, left, right,
                    newInfo.getEquiCondition(left, right, cluster.getRexBuilder()), join.getVariablesSet(),
                    join.getJoinType());
        } catch (InvalidRelException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The structure of the join condition should be in the following pattern:
     *
     * OR(
     *  AND(
     *      =($7, $14), IS NOT NULL($7),
     *      IS NOT NULL($14)
     *  ),
     *  AND(
     *      IS NULL($7),
     *      IS NULL($14)
     *  )
     * )
     *
     * The two ANDs may switch position.
     */
    private List<RexInputRef> isPowerBiInnerJoin(JoinInfo info, RexBuilder rexBuilder) {
        if (info.isEqui()) {
            return Collections.emptyList();
        }

        // 1. top call is OR
        RexNode root = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions);
        if (!(root instanceof RexCall && root.getKind() == SqlKind.OR)) {
            return Collections.emptyList();
        }

        // 2. operands are ANDs
        RexCall rootCall = (RexCall) root;
        if (rootCall.operands.size() != 2) {
            return Collections.emptyList();
        }
        if (!(isOperandSqlAnd(rootCall, 0) && isOperandSqlAnd(rootCall, 1))) {
            return Collections.emptyList();
        }

        // 3. which operand contains two IS_NULL checks
        RexCall leftCall = (RexCall) rootCall.operands.get(0);
        RexCall rightCall = (RexCall) rootCall.operands.get(1);
        RexCall twoNullCall, notNullCall;
        if (isOperandSqlIsNull(leftCall, 0) && isOperandSqlIsNull(leftCall, 1)) {
            twoNullCall = leftCall;
            notNullCall = rightCall;
        } else if (isOperandSqlIsNull(rightCall, 0) && isOperandSqlIsNull(rightCall, 1)) {
            twoNullCall = rightCall;
            notNullCall = leftCall;
        } else {
            return Collections.emptyList();
        }

        // 4. two column refs
        RexCall isNull1 = (RexCall) twoNullCall.operands.get(0);
        RexCall isNull2 = (RexCall) twoNullCall.operands.get(1);
        if (!(isOperandInputRef(isNull1, 0) && isOperandInputRef(isNull2, 0))) {
            return Collections.emptyList();
        }
        Set<RexInputRef> refs = Sets.newHashSet((RexInputRef) isNull1.operands.get(0),
                (RexInputRef) isNull2.operands.get(0));

        if (refs.size() != 2) {
            return Collections.emptyList();
        }

        // 5. equal not null
        if (notNullCall.operands.size() != 3) {
            return Collections.emptyList();
        }

        RexCall equalCall, notNull1, notNull2;
        if (isOperandSqlEq(notNullCall, 0) && isOperandSqlIsNotNull(notNullCall, 1)
                && isOperandSqlIsNotNull(notNullCall, 2)) {
            equalCall = (RexCall) notNullCall.operands.get(0);
            notNull1 = (RexCall) notNullCall.operands.get(1);
            notNull2 = (RexCall) notNullCall.operands.get(2);
        } else if (isOperandSqlEq(notNullCall, 1) && isOperandSqlIsNotNull(notNullCall, 0)
                && isOperandSqlIsNotNull(notNullCall, 2)) {
            equalCall = (RexCall) notNullCall.operands.get(1);
            notNull1 = (RexCall) notNullCall.operands.get(0);
            notNull2 = (RexCall) notNullCall.operands.get(2);
        } else if (isOperandSqlEq(notNullCall, 2) && isOperandSqlIsNotNull(notNullCall, 0)
                && isOperandSqlIsNotNull(notNullCall, 1)) {
            equalCall = (RexCall) notNullCall.operands.get(2);
            notNull1 = (RexCall) notNullCall.operands.get(0);
            notNull2 = (RexCall) notNullCall.operands.get(1);
        } else {
            return Collections.emptyList();
        }

        if (equalCall.operands.get(0).equals(equalCall.operands.get(1))) {
            return Collections.emptyList();
        }

        if (!(refs.contains(equalCall.operands.get(0)) && refs.contains(equalCall.operands.get(1)))) {
            return Collections.emptyList();
        }

        if (notNull1.operands.get(0).equals(notNull2.operands.get(0))) {
            return Collections.emptyList();
        }

        if (!(refs.contains(notNull1.operands.get(0)) && refs.contains(notNull2.operands.get(0)))) {
            return Collections.emptyList();
        }

        return Lists.newArrayList(refs);
    }

    private boolean isOperandInputRef(RexCall call, int ordinal) {
        return call.operands.get(ordinal) instanceof RexInputRef;
    }

    private boolean isOperandSqlEq(RexCall call, int ordinal) {
        return isOperandSqlKind(call, ordinal, SqlKind.EQUALS);
    }

    private boolean isOperandSqlAnd(RexCall call, int ordinal) {
        return isOperandSqlKind(call, ordinal, SqlKind.AND);
    }

    private boolean isOperandSqlIsNull(RexCall call, int ordinal) {
        return isOperandSqlKind(call, ordinal, SqlKind.IS_NULL);
    }

    private boolean isOperandSqlIsNotNull(RexCall call, int ordinal) {
        return isOperandSqlKind(call, ordinal, SqlKind.IS_NOT_NULL);
    }

    private boolean isOperandSqlKind(RexCall call, int ordinal, SqlKind kind) {
        return isOperandRexCall(call, ordinal) && call.operands.get(ordinal).getKind() == kind;
    }

    private boolean isOperandRexCall(RexCall call, int ordinal) {
        return call.operands.get(ordinal) instanceof RexCall;
    }

    private boolean isScd2JoinCondition(JoinInfo joinInfo, LogicalJoin join, List<RexInputRef> scd2Refs) {
        if (joinInfo.isEqui() || !isScd2Enabled) {
            return false;
        }
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        RexNode remaining = RexUtil.composeConjunction(rexBuilder, joinInfo.nonEquiConditions);

        if (remaining == null) {
            return false;
        }
        RexNode cnf = RexUtil.toCnf(rexBuilder, remaining);
        if (!(cnf instanceof RexCall) || cnf.getKind() != SqlKind.AND) {
            return false;
        }

        List<RexNode> nodeList = ((RexCall) cnf).getOperands();
        if (nodeList.size() % 2 != 0) {
            return false;
        }
        Map<RexInputRef, List<RexCall>> refOpMap = Maps.newHashMap();
        for (RexNode rexNode : nodeList) {
            if (!rexNode.isA(SCD2_KINDS)) {
                return false;
            }
            RexCall call = (RexCall) rexNode;
            RexInputRef left = extractInputRef(call.getOperands().get(0));
            RexInputRef right = extractInputRef(call.getOperands().get(1));
            if (left == null || right == null) {
                return false;
            }
            refOpMap.putIfAbsent(left, Lists.newArrayList());
            refOpMap.putIfAbsent(right, Lists.newArrayList());
            refOpMap.get(left).add((RexCall) rexBuilder.makeCall(call.getOperator(), left, right));
            refOpMap.get(right).add((RexCall) rexBuilder.makeCall(inverse(call.getOperator()), right, left));
        }
        refOpMap.entrySet().removeIf(entry -> entry.getValue().size() != 2);
        AtomicInteger allSize = new AtomicInteger();
        refOpMap.forEach((k, pairs) -> {
            SqlOperator first = pairs.get(0).getOperator();
            SqlOperator second = pairs.get(1).getOperator();
            if (!first.equals(second) && ALLOWED_PAIRS.contains(ImmutableSet.of(first, second))) {
                allSize.getAndAdd(2);
            }
        });
        boolean isScd2 = allSize.get() == nodeList.size();
        if (isScd2) {
            scd2Refs.addAll(refOpMap.keySet());
        }
        return isScd2;
    }

    private RexInputRef extractInputRef(RexNode node) {
        if (node.isA(SqlKind.INPUT_REF)) {
            return (RexInputRef) node;
        }
        if (node.isA(SqlKind.CAST) && (node instanceof RexCall)) {
            RexCall call = (RexCall) node;
            if (call.operands.size() == 1) {
                RexNode rexNode = call.operands.get(0);
                return rexNode.isA(SqlKind.INPUT_REF) ? (RexInputRef) rexNode : null;
            }

        }
        return null;
    }
}
