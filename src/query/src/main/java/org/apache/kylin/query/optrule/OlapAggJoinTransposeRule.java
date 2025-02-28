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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.calcite.KylinSumSplitter;
import org.apache.kylin.query.relnode.OlapAggregateRel;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.util.RuleUtils;
import org.jetbrains.annotations.NotNull;

public class OlapAggJoinTransposeRule extends RelOptRule {

    private static final String STAR_TOKEN = "*";

    public static final OlapAggJoinTransposeRule INSTANCE_JOIN_RIGHT_AGG = new OlapAggJoinTransposeRule(
            operand(OlapAggregateRel.class, operand(OlapJoinRel.class, any())), RelFactories.LOGICAL_BUILDER,
            "OlapAggJoinTransposeRule:agg-join-rightAgg");

    public OlapAggJoinTransposeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    public OlapAggJoinTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public OlapAggJoinTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final OlapAggregateRel aggregate = call.rel(0);
        final OlapJoinRel joinRel = call.rel(1);
        //Only one agg child of join is accepted
        return !aggregate.isContainCountDistinct() && RuleUtils.isJoinOnlyOneAggChild(joinRel);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final OlapAggregateRel aggregate = call.rel(0);
        final OlapJoinRel join = call.rel(1);
        final RelBuilder relBuilder = call.builder();

        // If any aggregate functions do not support splitting, bail out
        // If any aggregate call has a filter, bail out
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            if (aggregateCall.getAggregation().unwrap(SqlSplittableAggFunction.class) == null
                    || aggregateCall.filterArg >= 0) {
                return;
            }
        }

        // If it is not an inner join, we do not push the
        // aggregate operator
        if ((join.getJoinType() != JoinRelType.INNER && join.getJoinType() != JoinRelType.LEFT)) {
            return;
        }

        // Do the columns used by the join appear in the output of the aggregate?
        final ImmutableBitSet aggregateColumns = aggregate.getGroupSet();
        final RelMetadataQuery mq = call.getMetadataQuery();
        final ImmutableBitSet keyColumns = keyColumns(aggregateColumns,
                mq.getPulledUpPredicates(join).pulledUpPredicates);
        final ImmutableBitSet joinColumns = RelOptUtil.InputFinder.bits(join.getCondition());
        final boolean allColumnsInAggregate = keyColumns.contains(joinColumns);
        final ImmutableBitSet belowAggregateColumns = aggregateColumns.union(joinColumns);

        // Split join condition
        final List<Integer> leftKeys = Lists.newArrayList();
        final List<Integer> rightKeys = Lists.newArrayList();
        final List<Boolean> filterNulls = Lists.newArrayList();
        RexNode nonEquiConj = RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(), join.getCondition(),
                leftKeys, rightKeys, filterNulls);
        // If it contains non-equi join conditions, we bail out
        if (!nonEquiConj.isAlwaysTrue()) {
            return;
        }

        // Push each aggregate function down to each side that contains all of its
        // arguments. Note that COUNT(*), because it has no arguments, can go to
        // both sides.
        boolean aggPushDown = aggPushDown(aggregate, join, belowAggregateColumns, mq, relBuilder,
                allColumnsInAggregate);
        if (aggPushDown) {
            call.transformTo(relBuilder.build());
        }
    }

    private boolean aggPushDown(OlapAggregateRel aggregate, OlapJoinRel join, ImmutableBitSet belowAggregateColumns,
            RelMetadataQuery mq, RelBuilder relBuilder, boolean allColumnsInAggregate) {
        final Map<Integer, Integer> map = new HashMap<>();
        final List<Side> sides = new ArrayList<>();
        int uniqueCount = 0;
        int offset = 0;
        int belowOffset = 0;
        for (int s = 0; s < 2; s++) {
            final Side side = new Side();
            final RelNode joinInput = join.getInput(s);
            int fieldCount = joinInput.getRowType().getFieldCount();
            final ImmutableBitSet fieldSet = ImmutableBitSet.range(offset, offset + fieldCount);
            final ImmutableBitSet belowAggregateKeyNotShifted = belowAggregateColumns.intersect(fieldSet);
            for (Ord<Integer> c : Ord.zip(belowAggregateKeyNotShifted)) {
                map.put(c.e, belowOffset + c.i);
            }
            final Mappings.TargetMapping mapping = s == 0 ? Mappings.createIdentity(fieldCount)
                    : Mappings.createShiftMapping(fieldCount + offset, 0, offset, fieldCount);
            final ImmutableBitSet belowAggregateKey = belowAggregateKeyNotShifted.shift(-offset);
            final Boolean unique0 = mq.areColumnsUnique(joinInput, belowAggregateKey);
            final boolean unique = unique0 != null && unique0;
            if (unique) {
                ++uniqueCount;
                processUnique(side, relBuilder, joinInput, aggregate, fieldSet, mapping, belowAggregateKey);
            } else {
                processUnUnique(side, aggregate, relBuilder, joinInput, fieldSet, mapping, belowAggregateKey);
            }
            offset += fieldCount;
            belowOffset += side.newInput.getRowType().getFieldCount();
            sides.add(side);
        }
        if (uniqueCount == 2 || isJoinInputNotChanged(join, sides)) {
            // Both inputs to the join are unique. There is nothing to be gained by
            // this rule. In fact, this aggregate+join may be the result of a previous
            // invocation of this rule; if we continue we might loop forever.
            // Fix OlapAggJoinTransposeRule infinite loop
            return false;
        }

        // Update condition
        updateCondition(sides, map, aggregate, join, belowOffset, relBuilder, allColumnsInAggregate);
        return true;
    }

    private boolean isJoinInputNotChanged(OlapJoinRel join, List<Side> sides) {
        List<RelNode> newInputs = sides.stream().map(side -> side.newInput).collect(Collectors.toList());
        for (int i = 0; i < newInputs.size(); i++) {
            if (!Objects.equals(newInputs.get(i), join.getInput(i))) {
                return false;
            }
        }
        return true;
    }

    private void processUnique(Side side, RelBuilder relBuilder, RelNode joinInput, OlapAggregateRel aggregate,
            ImmutableBitSet fieldSet, Mappings.TargetMapping mapping, ImmutableBitSet belowAggregateKey) {
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        side.aggregate = false;
        relBuilder.push(joinInput);
        final List<RexNode> projects = new ArrayList<>();
        for (Integer i : belowAggregateKey) {
            projects.add(relBuilder.field(i));
        }
        for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
            final SqlAggFunction aggregation = aggCall.e.getAggregation();
            final SqlSplittableAggFunction splitter = Preconditions
                    .checkNotNull(aggregation.unwrap(SqlSplittableAggFunction.class));
            if (!aggCall.e.getArgList().isEmpty() && fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))) {
                final RexNode singleton = splitter.singleton(rexBuilder, joinInput.getRowType(),
                        aggCall.e.transform(mapping));
                if (singleton instanceof RexInputRef) {
                    final int index = ((RexInputRef) singleton).getIndex();
                    if (!belowAggregateKey.get(index)) {
                        projects.add(singleton);
                        side.split.put(aggCall.i, projects.size() - 1);
                    } else {
                        side.split.put(aggCall.i, index);
                    }
                } else {
                    projects.add(singleton);
                    side.split.put(aggCall.i, projects.size() - 1);
                }
            }
        }
        relBuilder.project(projects);
        side.newInput = relBuilder.build();
    }

    private void processUnUnique(Side side, OlapAggregateRel aggregate, RelBuilder relBuilder, RelNode joinInput,
            ImmutableBitSet fieldSet, Mappings.TargetMapping mapping, ImmutableBitSet belowAggregateKey) {
        side.aggregate = true;
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        List<AggregateCall> belowAggCalls = new ArrayList<>();
        final SqlSplittableAggFunction.Registry<AggregateCall> belowAggCallRegistry = registry(belowAggCalls);
        final int oldGroupKeyCount = aggregate.getGroupCount();
        final int newGroupKeyCount = belowAggregateKey.cardinality();
        for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
            final SqlAggFunction aggregation = aggCall.e.getAggregation();
            final SqlSplittableAggFunction splitter = Preconditions
                    .checkNotNull(aggregation.unwrap(SqlSplittableAggFunction.class));
            final AggregateCall call1;
            if (fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))) {
                final AggregateCall splitCall = splitter.split(aggCall.e, mapping);
                call1 = splitCall.adaptTo(joinInput, splitCall.getArgList(), splitCall.filterArg, oldGroupKeyCount,
                        newGroupKeyCount);
            } else {
                call1 = splitter.other(rexBuilder.getTypeFactory(), aggCall.e);
            }
            if (call1 != null) {
                side.split.put(aggCall.i, belowAggregateKey.cardinality() + belowAggCallRegistry.register(call1));
            }
        }
        side.newInput = relBuilder.push(joinInput).aggregate(relBuilder.groupKey(belowAggregateKey), belowAggCalls)
                .build();
    }

    private static void updateCondition(List<Side> sides, Map<Integer, Integer> map, OlapAggregateRel aggregate,
            OlapJoinRel join, int belowOffset, RelBuilder relBuilder, boolean allColumnsInAggregate) {
        final Mapping mapping = (Mapping) Mappings.target(map::get, join.getRowType().getFieldCount(), belowOffset);
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        final RexNode newCondition = RexUtil.apply(mapping, join.getCondition());
        // Create new join
        relBuilder.push(sides.get(0).newInput).push(sides.get(1).newInput).join(join.getJoinType(), newCondition);

        // Aggregate above to sum up the sub-totals
        final List<AggregateCall> newAggCalls = new ArrayList<>();
        final int groupIndicatorCount = aggregate.getGroupCount() + aggregate.getIndicatorCount();
        final int newLeftWidth = sides.get(0).newInput.getRowType().getFieldCount();
        List<RexNode> projects = new ArrayList<>(rexBuilder.identityProjects(relBuilder.peek().getRowType()));
        for (Ord<AggregateCall> aggCall : Ord.zip(aggregate.getAggCallList())) {
            final SqlAggFunction aggregation = aggCall.e.getAggregation();
            SqlSplittableAggFunction splitter = getSqlSplittableAggFunction(aggregation);
            final Integer leftSubTotal = sides.get(0).split.get(aggCall.i);
            final Integer rightSubTotal = sides.get(1).split.get(aggCall.i);
            newAggCalls.add(splitter.topSplit(rexBuilder, registry(projects), groupIndicatorCount,
                    relBuilder.peek().getRowType(), aggCall.e, leftSubTotal == null ? -1 : leftSubTotal,
                    rightSubTotal == null ? -1 : rightSubTotal + newLeftWidth));
        }

        if (join.getJoinType() == JoinRelType.LEFT) {
            projects = createNewProjects(rexBuilder, projects);
        }
        relBuilder.project(projects);

        boolean aggConvertedToProjects = false;
        if (allColumnsInAggregate) {
            // let's see if we can convert aggregate into projects
            List<RexNode> projects2 = new ArrayList<>();
            for (int key : Mappings.apply(mapping, aggregate.getGroupSet())) {
                projects2.add(relBuilder.field(key));
            }
            for (AggregateCall newAggCall : newAggCalls) {
                final SqlSplittableAggFunction splitter = newAggCall.getAggregation()
                        .unwrap(SqlSplittableAggFunction.class);
                if (splitter != null) {
                    final RelDataType rowType = relBuilder.peek().getRowType();
                    projects2.add(splitter.singleton(rexBuilder, rowType, newAggCall));
                }
            }
            if (projects2.size() == aggregate.getGroupSet().cardinality() + newAggCalls.size()) {
                // We successfully converted agg calls into projects.
                relBuilder.project(projects2);
                aggConvertedToProjects = true;
            }
        }

        if (!aggConvertedToProjects) {
            ImmutableBitSet groupSet = Mappings.apply(mapping, aggregate.getGroupSet());
            ImmutableList<ImmutableBitSet> groupSets = Mappings.apply2(mapping, aggregate.getGroupSets());
            if (groupSets == null) {
                relBuilder.aggregate(relBuilder.groupKey(groupSet), newAggCalls);
            } else {
                relBuilder.aggregate(relBuilder.groupKey(groupSet, groupSets), newAggCalls);
            }
        }
    }

    private static @NotNull SqlSplittableAggFunction getSqlSplittableAggFunction(SqlAggFunction aggregation) {
        SqlSplittableAggFunction splitter = Objects.requireNonNull(aggregation.unwrap(SqlSplittableAggFunction.class));
        if (splitter.equals(SqlSplittableAggFunction.SumSplitter.INSTANCE)) {
            splitter = KylinSumSplitter.INSTANCE;
        }
        return splitter;
    }

    private static List<RexNode> createNewProjects(RexBuilder rexBuilder, List<RexNode> projects) {
        List<RexNode> converted = new ArrayList<>();
        Map<Integer, RexInputRef> rexInpufRefMap = new HashMap<>();
        for (RexNode rexNode : projects) {
            if (rexNode instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) rexNode;
                converted.add(inputRef);
                rexInpufRefMap.put(inputRef.getIndex(), inputRef);
            } else {
                OriginInputRefReplacer visitor = new OriginInputRefReplacer(rexInpufRefMap);
                RexNode newRexNode = rewriteRexNode(rexNode, rexBuilder).accept(visitor);
                converted.add(newRexNode);
            }
        }
        return converted;
    }

    private static class OriginInputRefReplacer extends RexVisitorImpl<RexNode> {
        final Map<Integer, RexInputRef> rexInpufRefMap;

        protected OriginInputRefReplacer(Map<Integer, RexInputRef> rexInpufRefMap) {
            super(true);
            this.rexInpufRefMap = rexInpufRefMap;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            RexNode rexNode = rexInpufRefMap.get(inputRef.getIndex());
            return rexNode == null ? inputRef : rexNode;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            List<RexNode> rexNodes = call.getOperands();
            List<RexNode> converted = rexNodes.stream() //
                    .map(rex -> rex.accept(this)) //
                    .collect(Collectors.toList());
            return call.clone(call.getType(), converted);
        }

        @Override
        public RexNode visitLiteral(RexLiteral literal) {
            return literal;
        }
    }

    private static RexNode rewriteRexNode(RexNode rexNode, RexBuilder rexBuilder) {
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            SqlOperator sqlOperator = rexCall.getOperator();
            if (isMultiplicationRexCall(rexCall)) {
                List<RexNode> rewriteRexNodeList = rewriteRexNodeList(rexCall, rexBuilder);
                return rexBuilder.makeCall(rexCall.type, SqlStdOperatorTable.MULTIPLY, rewriteRexNodeList);
            } else if (sqlOperator.getKind() == SqlKind.CAST && rexCall.getOperands().size() == 1
                    && rexCall.getOperands().get(0) instanceof RexCall
                    && isMultiplicationRexCall((RexCall) rexCall.getOperands().get(0))) {

                RexCall innerRexCall = (RexCall) rexCall.getOperands().get(0);
                List<RexNode> rewriteRexNodeList = rewriteRexNodeList(innerRexCall, rexBuilder);
                RexNode rewriteInnerRexCall = rexBuilder.makeCall(innerRexCall.type, SqlStdOperatorTable.MULTIPLY,
                        rewriteRexNodeList);
                return rexBuilder.makeCast(rexCall.type, rewriteInnerRexCall);
            }
        }
        return rexNode;
    }

    private static List<RexNode> rewriteRexNodeList(RexCall rexCall, RexBuilder rexBuilder) {
        List<RexNode> rewriteRexNodeList = new ArrayList<>();
        RelDataType dataType = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT).createSqlType(SqlTypeName.INTEGER);

        for (RexNode rexNode : rexCall.getOperands()) {
            rewriteRexNodeList.add(rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, rexNode,
                    rexBuilder.makeLiteral(1, dataType, false)));
        }
        return rewriteRexNodeList;
    }

    private static boolean isMultiplicationRexCall(RexCall rexCall) {
        return rexCall.getOperator().getName().equals(STAR_TOKEN) && rexCall.getOperands().size() == 2;
    }

    /** Computes the closure of a set of columns according to a given list of
     * constraints. Each 'x = y' constraint causes bit y to be set if bit x is
     * set, and vice versa. */
    private static ImmutableBitSet keyColumns(ImmutableBitSet aggregateColumns, ImmutableList<RexNode> predicates) {
        SortedMap<Integer, BitSet> equivalence = new TreeMap<>();
        for (RexNode predicate : predicates) {
            populateEquivalences(equivalence, predicate);
        }
        ImmutableBitSet keyColumns = aggregateColumns;
        for (Integer aggregateColumn : aggregateColumns) {
            final BitSet bitSet = equivalence.get(aggregateColumn);
            if (bitSet != null) {
                keyColumns = keyColumns.union(bitSet);
            }
        }
        return keyColumns;
    }

    private static void populateEquivalences(Map<Integer, BitSet> equivalence, RexNode predicate) {
        if (predicate.getKind() != SqlKind.EQUALS) {
            return;
        }
        RexCall call = (RexCall) predicate;
        final List<RexNode> operands = call.getOperands();
        if (operands.get(0) instanceof RexInputRef) {
            final RexInputRef ref0 = (RexInputRef) operands.get(0);
            if (operands.get(1) instanceof RexInputRef) {
                final RexInputRef ref1 = (RexInputRef) operands.get(1);
                populateEquivalence(equivalence, ref0.getIndex(), ref1.getIndex());
                populateEquivalence(equivalence, ref1.getIndex(), ref0.getIndex());
            }
        }
    }

    private static void populateEquivalence(Map<Integer, BitSet> equivalence, int i0, int i1) {
        BitSet bitSet = equivalence.computeIfAbsent(i0, bitset -> new BitSet());
        bitSet.set(i1);
    }

    /** Creates a {@link org.apache.calcite.sql.SqlSplittableAggFunction.Registry}
     * that is a view of a list. */
    private static <E> SqlSplittableAggFunction.Registry<E> registry(final List<E> list) {
        return e -> {
            int i = list.indexOf(e);
            if (i < 0) {
                i = list.size();
                list.add(e);
            }
            return i;
        };
    }

    /** Work space for an input to a join. */
    private static class Side {
        final Map<Integer, Integer> split = new HashMap<>();
        RelNode newInput;
        boolean aggregate;
    }
}
