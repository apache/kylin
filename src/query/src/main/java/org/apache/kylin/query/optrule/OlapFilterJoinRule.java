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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.util.RexUtils;

/**
 *  Planner rule that push filters above join node into the join node,
 *  rewrite the cross join to the equal-semantic inner Join.
 *
 */
public class OlapFilterJoinRule extends RelOptRule {

    public static final FilterJoinRule FILTER_ON_JOIN = new FilterJoinRule.FilterIntoJoinRule(true,
            RelFactories.LOGICAL_BUILDER, FilterJoinRule.TRUE_PREDICATE) {

        // https://olapio.atlassian.net/browse/AL-8813
        @Override
        protected boolean canPushIntoFromAbove(Filter filter) {
            if (filter == null) {
                return false;
            }

            if (isFilterContainsCC(filter)) {
                return false;
            }

            RexNode condition = filter.getCondition();
            if (condition.isA(SqlKind.AND)) {
                RexCall call = (RexCall) condition;
                return call.getOperands().stream().allMatch(this::isSimpleInputRefCondition);
            }
            return isSimpleInputRefCondition(condition);
        }

        private boolean isFilterContainsCC(Filter filter) {
            RexNode condition = filter.getCondition();
            if (condition == null) {
                return false;
            }

            Set<RexInputRef> allInputRefs = RexUtils.getAllInputRefs(condition);
            List<RelDataTypeField> fieldList = filter.getRowType().getFieldList();
            return allInputRefs.stream().anyMatch(inputRef -> fieldList.get(inputRef.getIndex()).getName()
                    .startsWith(ComputedColumnUtil.CC_NAME_PREFIX));
        }

        // for example: table1.col1 = table2.col1 can be pushed into the below join relNode,
        // however, table1.col1 + table1.col2 = table2.col1 can't be pushed.
        private boolean isSimpleInputRefCondition(RexNode condition) {
            if (condition.isA(SqlKind.EQUALS)) {
                RexCall call = (RexCall) condition;
                RexNode left = call.getOperands().get(0);
                RexNode right = call.getOperands().get(1);
                return left instanceof RexInputRef && right instanceof RexInputRef;
            }
            return false;
        }
    };

    public static final OlapFilterJoinRule OLAP_FILTER_ON_JOIN_JOIN = new OlapFilterJoinRule(
            operand(Filter.class,
                    operand(Join.class,
                            operand(Join.class, operand(RelNode.class, RelOptRule.any()),
                                    operand(RelNode.class, RelOptRule.any())),
                            operand(RelNode.class, null, input -> !(input instanceof Join), RelOptRule.any()))),
            RelFactories.LOGICAL_BUILDER, true, "OlapFilterJoinRule:filter-join-join");

    public static final OlapFilterJoinRule OLAP_FILTER_ON_JOIN_SCAN = new OlapFilterJoinRule(operand(Filter.class,
            operand(Join.class, operand(RelNode.class, null, input -> !(input instanceof Join), RelOptRule.any()),
                    operand(RelNode.class, null, input -> !(input instanceof Join), RelOptRule.any()))),
            RelFactories.LOGICAL_BUILDER, false, "OlapFilterJoinRule:filter-join-scan");

    private final boolean needTranspose;

    private OlapFilterJoinRule(RelOptRuleOperand relOptRuleOperand, RelBuilderFactory relBuilderFactory,
            boolean needTranspose, String description) {
        super(relOptRuleOperand, relBuilderFactory, description);
        this.needTranspose = needTranspose;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // match case:
        //          filter
        //            |
        //         topJoin
        //         /     \
        //   bottomJoin   A
        //      /   \
        //    C      B
        RuleMatchHandler handler = new RuleMatchHandler(call);
        handler.perform();
    }

    private class RuleMatchHandler {
        private Filter filterRel;
        private Join topJoinRel;
        private AbstractRelNode bottomJoin;
        private RelNode relA;
        private RelNode relB;
        private RelNode relC;
        private RelBuilder relBuilder;
        private RelOptRuleCall call;

        public RuleMatchHandler(RelOptRuleCall call) {
            this.call = call;
            filterRel = call.rel(0);
            topJoinRel = call.rel(1);

            bottomJoin = call.rel(2);
            relC = call.rel(3);
            relB = null;
            relA = null;
            if (call.rels.length > 4) {
                relB = call.rel(4);
                relA = call.rel(5);
            }
            relBuilder = call.builder();
        }

        protected void perform() {
            List<RexNode> joinFilters = RelOptUtil.conjunctions(topJoinRel.getCondition());
            // only match cross join and not-null filter
            if (!joinFilters.isEmpty() || filterRel == null) {
                return;
            }

            List<RexNode> aboveFilters = RelOptUtil.conjunctions(filterRel.getCondition());
            // replace filters with pattern cast(col1 as ...) = col2 with col1 = col2
            // to make such filters to be able to be pushed down to join conditions
            aboveFilters = aboveFilters.stream().map(RexUtils::stripOffCastInColumnEqualPredicate)
                    .collect(Collectors.toList());

            JoinRelType joinType = topJoinRel.getJoinType();
            List<RexNode> leftFilters = new ArrayList<>();
            List<RexNode> rightFilters = new ArrayList<>();
            boolean filterPushed = pushDownFilter(aboveFilters, leftFilters, rightFilters, joinFilters);

            // if nothing actually got pushed and there is nothing leftover, then this rule is a no-op
            if ((!filterPushed && joinType == topJoinRel.getJoinType())
                    || (joinFilters.isEmpty() && leftFilters.isEmpty() && rightFilters.isEmpty())) {
                return;
            }

            // try transpose relNodes of joinRel to make as many relNodes be rewritten to inner join node as possible
            boolean isNeedProject = false;
            if (needTranspose && bottomJoin instanceof Join
                    && RelOptUtil.conjunctions(((Join) bottomJoin).getCondition()).isEmpty() && joinFilters.size() > 1
                    && !(relA instanceof Aggregate)) {
                final int originFilterSize = joinFilters.size();
                final Join originTopJoin = topJoinRel.copy(topJoinRel.getTraitSet(), topJoinRel.getInputs());

                Filter newFilter = (Filter) transposeJoinRel();
                // retry to push down new filters
                List<RexNode> newLeftFilters = Lists.newArrayList();
                List<RexNode> newRightFilters = Lists.newArrayList();
                List<RexNode> newJoinFilters = Lists.newArrayList();
                List<RexNode> newAboveFilter = RelOptUtil.conjunctions(newFilter.getCondition());
                pushDownFilter(newAboveFilter, newLeftFilters, newRightFilters, newJoinFilters);
                if (newJoinFilters.size() < originFilterSize) {
                    filterRel = newFilter;
                    leftFilters = newLeftFilters;
                    rightFilters = newRightFilters;
                    joinFilters = newJoinFilters;
                    aboveFilters = newAboveFilter;
                    isNeedProject = true;
                } else {
                    topJoinRel = originTopJoin;
                }
            }

            // create Filters on top of the children if any filters were pushed to them
            final RexBuilder rexBuilder = topJoinRel.getCluster().getRexBuilder();
            final RelNode leftRel = relBuilder.push(topJoinRel.getLeft()).filter(leftFilters).build();
            final RelNode rightRel = relBuilder.push(topJoinRel.getRight()).filter(rightFilters).build();

            // create the new join node referencing the new children and
            // containing its new join filters (if there are any)
            final ImmutableList<RelDataType> fieldTypes = ImmutableList.<RelDataType> builder()
                    .addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
                    .addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType())).build();
            final RexNode joinFilter = RexUtil.composeConjunction(rexBuilder,
                    RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes), false);

            // If nothing actually got pushed and there is nothing leftover,
            // then this rule is a no-op
            if (joinFilter.isAlwaysTrue() && leftFilters.isEmpty() && rightFilters.isEmpty()
                    && joinType == topJoinRel.getJoinType()) {
                return;
            }

            RelNode newJoinRel = topJoinRel.copy(topJoinRel.getTraitSet(), joinFilter, leftRel, rightRel, joinType,
                    topJoinRel.isSemiJoinDone());
            call.getPlanner().onCopy(topJoinRel, newJoinRel);
            if (!leftFilters.isEmpty()) {
                call.getPlanner().onCopy(filterRel, leftRel);
            }
            if (!rightFilters.isEmpty()) {
                call.getPlanner().onCopy(filterRel, rightRel);
            }

            relBuilder.push(newJoinRel);
            // Create a project on top of the join if some of the columns have become
            // NOT NULL due to the join-type getting stricter.
            relBuilder.convert(topJoinRel.getRowType(), false);
            // create a FilterRel on top of the join if needed
            relBuilder.filter(RexUtil.fixUp(rexBuilder, aboveFilters,
                    RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));
            if (isNeedProject) {
                final int aCount = relA.getRowType().getFieldCount();
                final int bCount = relB.getRowType().getFieldCount();
                final int cCount = relC.getRowType().getFieldCount();
                final Mappings.TargetMapping originJoinFieldsToNew = Mappings.createShiftMapping(
                        aCount + bCount + cCount, 0, 0, cCount, cCount + aCount, cCount, bCount, cCount,
                        cCount + bCount, aCount);
                relBuilder.project(relBuilder.fields(originJoinFieldsToNew));
            }
            call.transformTo(relBuilder.build());
        }

        private boolean pushDownFilter(List<RexNode> aboveFilters, List<RexNode> leftFilters,
                List<RexNode> rightFilters, List<RexNode> joinFilters) {
            // Try to push down above filters. These are typically where clause
            // filters. They can be pushed down if they are not on the NULL
            // generating side.
            final JoinRelType joinType = topJoinRel.getJoinType();
            final List<RexNode> origJoinFilters = ImmutableList.copyOf(joinFilters);
            boolean filterPushed = false;
            // see https://olapio.atlassian.net/browse/KE-42040
            // Calcite 1.30 remove EquiJoinInfo and NonEquiJoinInfo, change to an equivalent judgment condition
            boolean pushInto = !(topJoinRel instanceof OlapJoinRel) || !(topJoinRel.analyzeCondition().isEqui());
            if (RelOptUtil.classifyFilters(topJoinRel, aboveFilters, joinType, pushInto,
                    !joinType.generatesNullsOnLeft(), !joinType.generatesNullsOnRight(), joinFilters, leftFilters,
                    rightFilters)) {
                filterPushed = true;
            }
            pullUpNonEquiFilters(joinFilters, false, topJoinRel.getRowType().getFieldList(), aboveFilters);
            pullUpNonEquiFilters(leftFilters, false, topJoinRel.getInput(0).getRowType().getFieldList(), aboveFilters);
            pullUpNonEquiFilters(rightFilters, true, topJoinRel.getInput(1).getRowType().getFieldList(), aboveFilters);

            // If no filter got pushed after validate, reset filterPushed flag
            if (leftFilters.isEmpty() && rightFilters.isEmpty() && joinFilters.size() == origJoinFilters.size()) {
                if (Sets.newHashSet(joinFilters).equals(Sets.newHashSet(origJoinFilters))) {
                    filterPushed = false;
                }
            }

            // Try to push down filters in ON clause. A ON clause filter can only be
            // pushed down if it does not affect the non-matching set, i.e. it is
            // not on the side which is preserved.
            if (RelOptUtil.classifyFilters(topJoinRel, joinFilters, joinType, false, !joinType.generatesNullsOnRight(),
                    !joinType.generatesNullsOnLeft(), joinFilters, leftFilters, rightFilters)) {
                filterPushed = true;
            }
            return filterPushed;
        }

        private RelNode transposeJoinRel() {
            final RexBuilder rexBuilder = topJoinRel.getCluster().getRexBuilder();
            final int aCount = relA.getRowType().getFieldCount();
            final int bCount = relB.getRowType().getFieldCount();
            final int cCount = relC.getRowType().getFieldCount();
            final Mappings.TargetMapping originJoinFieldsToNew = Mappings.createShiftMapping(aCount + bCount + cCount,
                    0, 0, cCount, cCount + aCount, cCount, bCount, cCount, cCount + bCount, aCount);

            // 1. to transpose relA with relB, create new join rels
            //          filter                       filter
            //            |                           |
            //         topJoin                     topJoin
            //         /     \       ==>           /     \
            //   bottomJoin   A              bottomJoin   B
            //      /   \                     /   \
            //    C      B                  C      A

            final RelNode newRightRel = relBuilder.push(((Join) bottomJoin).getRight()).build();
            Join oldLeft = (Join) bottomJoin;
            final RelNode newLeftRel = oldLeft.copy(oldLeft.getTraitSet(), rexBuilder.makeLiteral(true),
                    relBuilder.push(oldLeft.getLeft()).build(), relBuilder.push(topJoinRel.getRight()).build(),
                    oldLeft.getJoinType(), oldLeft.isSemiJoinDone());
            topJoinRel = topJoinRel.copy(topJoinRel.getTraitSet(), rexBuilder.makeLiteral(true), newLeftRel,
                    newRightRel, topJoinRel.getJoinType(), topJoinRel.isSemiJoinDone());

            // 2. adjust new filter condition, for the fields of top-join changed
            List<RexNode> newFilterList = Lists.newArrayList();
            new RexPermuteInputsShuttle(originJoinFieldsToNew, topJoinRel)
                    .visitList(RelOptUtil.conjunctions(filterRel.getCondition()), newFilterList);
            return relBuilder.push(topJoinRel).filter(newFilterList).build();
        }

        private void pullUpNonEquiFilters(List<RexNode> filters, boolean isFromRight, List<RelDataTypeField> srcFields,
                List<RexNode> aboveFilters) {
            // Move filters up if filters are not eq-cols, e.g (colA > 23) should be move up
            RexBuilder rexBuilder = topJoinRel.getCluster().getRexBuilder();
            int[] offsets = new int[srcFields.size()];
            for (int i = 0; i < srcFields.size(); i++) {
                offsets[i] = isFromRight ? (topJoinRel.getRowType().getFieldCount() - srcFields.size()) : 0;
            }

            Iterator<RexNode> itr = filters.iterator();
            while (itr.hasNext()) {
                RexNode filter = itr.next();
                final RelOptUtil.InputFinder inputFinder = RelOptUtil.InputFinder.analyze(filter);
                if (!(inputFinder.build().asList().size() == 2 && SqlKind.EQUALS == filter.getKind())) {
                    aboveFilters.add(filter.accept(new RelOptUtil.RexInputConverter(rexBuilder, srcFields,
                            topJoinRel.getRowType().getFieldList(), offsets)));
                    itr.remove();
                }
            }
        }
    }
}
