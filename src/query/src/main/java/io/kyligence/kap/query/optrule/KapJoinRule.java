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

package io.kyligence.kap.query.optrule;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.kylin.query.relnode.KapFilterRel;
import org.apache.kylin.query.relnode.KapJoinRel;
import org.apache.kylin.query.relnode.KapNonEquiJoinRel;
import org.apache.kylin.query.relnode.KapRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class KapJoinRule extends ConverterRule {
    private static final Logger logger = LoggerFactory.getLogger(KapJoinRule.class);

    public static final ConverterRule INSTANCE = new KapJoinRule();
    public static final ConverterRule NON_EQUI_INSTANCE = new KapJoinRule(true, false);
    public static final ConverterRule EQUAL_NULL_SAFE_INSTANT = new KapJoinRule(false, true);

    private boolean isQueryNonEquiJoinModelEnabled;
    private boolean joinCondEqualNullSafe;

    public KapJoinRule() {
        this(false, false);
    }

    public KapJoinRule(boolean isQueryNonEquiJoinModelEnabled, boolean joinCondEqualNullSafe) {
        super(LogicalJoin.class, Convention.NONE, KapRel.CONVENTION, "KapJoinRule");
        this.isQueryNonEquiJoinModelEnabled = isQueryNonEquiJoinModelEnabled;
        this.joinCondEqualNullSafe = joinCondEqualNullSafe;
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;
        RelNode left = join.getInput(0);
        RelNode right = join.getInput(1);

        RelTraitSet traitSet = join.getTraitSet().replace(KapRel.CONVENTION);
        left = left instanceof HepRelVertex ? left : convert(left, left.getTraitSet().replace(KapRel.CONVENTION));
        right = right instanceof HepRelVertex ? right : convert(right, right.getTraitSet().replace(KapRel.CONVENTION));

        final JoinInfo info = JoinInfo.of(left, right, join.getCondition());

        // handle powerbi inner join, see https://github.com/Kyligence/KAP/issues/1823
        Join tmpJoin = transformJoinCondition(join, info, traitSet, left, right);
        if (tmpJoin instanceof KapJoinRel) {
            return tmpJoin;
        }

        RelNode newRel;
        try {
            if (isNonEquiJoinRelRule(info.isEqui(), join.getJoinType())
                    || isNotDistinctFrom(left, right, join.getCondition())) {
                try {
                    return new KapNonEquiJoinRel(join.getCluster(), traitSet, left, right, join.getCondition(),
                            join.getVariablesSet(), join.getJoinType(), isQueryNonEquiJoinModelEnabled);
                } catch (InvalidRelException e) {
                    throw new IllegalStateException(e);
                }
            } else {
                newRel = new KapJoinRel(join.getCluster(), traitSet, left, right,
                        info.getEquiCondition(left, right, join.getCluster().getRexBuilder()), info.leftKeys,
                        info.rightKeys, join.getVariablesSet(), join.getJoinType());
                ((KapJoinRel) newRel).setJoinCondEqualNullSafe(joinCondEqualNullSafe);
            }
            if (!info.isEqui()) {
                newRel = new KapFilterRel(join.getCluster(), newRel.getTraitSet(), newRel,
                        info.getRemaining(join.getCluster().getRexBuilder()));
            }
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError(e);
            // LOGGER.fine(e.toString());
        }
        return newRel;
    }

    private boolean isNonEquiJoinRelRule(boolean isEqui, JoinRelType joinRelType) {

        if (isEqui || (joinRelType == JoinRelType.INNER && !isQueryNonEquiJoinModelEnabled)) {
            return false;
        }

        return true;
    }

    private boolean isNotDistinctFrom(RelNode left, RelNode right, RexNode condition) {
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        final List<Boolean> filterNulls = new ArrayList<>();
        RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls);

        for (int i = 0; i < leftKeys.size(); i++) {
            if (!filterNulls.get(i))
                return true;
        }
        return false;
    }

    private Join transformJoinCondition(LogicalJoin join, JoinInfo info, RelTraitSet traitSet, RelNode left,
            RelNode right) {
        List<RexInputRef> refs = isPowerBiInnerJoin(info);
        if (refs == null) {
            return join;
        }

        // The ref index is global index. key index is local.
        RelOptCluster cluster = join.getCluster();
        int index1 = refs.get(0).getIndex();
        int index2 = refs.get(1).getIndex();
        int leftIndex = index1 > index2 ? index2 : index1;
        int rightIndex = index1 > index2 ? index1 : index2;
        rightIndex -= left.getRowType().getFieldCount();

        JoinInfo newInfo = JoinInfo.of(ImmutableIntList.of(leftIndex), ImmutableIntList.of(rightIndex));
        try {
            return new KapJoinRel(cluster, traitSet, left, right,
                    newInfo.getEquiCondition(left, right, cluster.getRexBuilder()), newInfo.leftKeys, newInfo.rightKeys,
                    join.getVariablesSet(), join.getJoinType());
        } catch (InvalidRelException e) {
            throw new RuntimeException(e);
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
    private List<RexInputRef> isPowerBiInnerJoin(JoinInfo info) {
        if (info.isEqui()) {
            return null;
        }

        // 1. top call is OR
        RexNode root = info.getRemaining(null);
        if (!(root instanceof RexCall && root.getKind() == SqlKind.OR)) {
            return null;
        }

        // 2. operands are ANDs
        RexCall rootCall = (RexCall) root;
        if (rootCall.operands.size() != 2) {
            return null;
        }
        if (!(isOperandSqlAnd(rootCall, 0) && isOperandSqlAnd(rootCall, 1))) {
            return null;
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
            return null;
        }

        // 4. two column refs
        RexCall isNull1 = (RexCall) twoNullCall.operands.get(0);
        RexCall isNull2 = (RexCall) twoNullCall.operands.get(1);
        if (!(isOperandInputRef(isNull1, 0) && isOperandInputRef(isNull2, 0))) {
            return null;
        }
        Set<RexInputRef> refs = Sets.newHashSet((RexInputRef) isNull1.operands.get(0),
                (RexInputRef) isNull2.operands.get(0));

        if (refs.size() != 2) {
            return null;
        }

        // 5. equal not null
        if (notNullCall.operands.size() != 3) {
            return null;
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
            return null;
        }

        if (equalCall.operands.get(0).equals(equalCall.operands.get(1))) {
            return null;
        }

        if (!(refs.contains(equalCall.operands.get(0)) && refs.contains(equalCall.operands.get(1)))) {
            return null;
        }

        if (notNull1.operands.get(0).equals(notNull2.operands.get(0))) {
            return null;
        }

        if (!(refs.contains(notNull1.operands.get(0)) && refs.contains(notNull2.operands.get(0)))) {
            return null;
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
}
