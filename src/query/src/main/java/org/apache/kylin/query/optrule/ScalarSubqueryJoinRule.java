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

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.query.relnode.OlapAggregateRel;
import org.apache.kylin.query.relnode.OlapFilterRel;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.relnode.OlapNonEquiJoinRel;
import org.apache.kylin.query.relnode.OlapProjectRel;
import org.apache.kylin.query.relnode.OlapValuesRel;

public class ScalarSubqueryJoinRule extends RelOptRule {

    // JOIN_PREDICATE from guava's Predicate,

    public static final ScalarSubqueryJoinRule AGG_JOIN = new ScalarSubqueryJoinRule(//
            operand(OlapAggregateRel.class, //
                    operand(Join.class, //
                            null, j -> j instanceof OlapJoinRel || j instanceof OlapNonEquiJoinRel, any())),
            RelFactories.LOGICAL_BUILDER, "ScalarSubqueryJoinRule:AGG_JOIN");

    public static final ScalarSubqueryJoinRule AGG_PRJ_JOIN = new ScalarSubqueryJoinRule(//
            operand(OlapAggregateRel.class, //
                    operand(OlapProjectRel.class, //
                            operand(Join.class, //
                                    null, j -> j instanceof OlapJoinRel || j instanceof OlapNonEquiJoinRel, any()))),
            RelFactories.LOGICAL_BUILDER, "ScalarSubqueryJoinRule:AGG_PRJ_JOIN");

    public static final ScalarSubqueryJoinRule AGG_FLT_JOIN = new ScalarSubqueryJoinRule(//
            operand(OlapAggregateRel.class, //
                    operand(OlapFilterRel.class, //
                            operand(Join.class, //
                                    null, //
                                    j -> j instanceof OlapJoinRel || j instanceof OlapNonEquiJoinRel, any()))),
            RelFactories.LOGICAL_BUILDER, "ScalarSubqueryJoinRule:AGG_FLT_JOIN");

    public static final ScalarSubqueryJoinRule AGG_PRJ_FLT_JOIN = new ScalarSubqueryJoinRule(//
            operand(OlapAggregateRel.class, //
                    operand(OlapProjectRel.class, //
                            operand(OlapFilterRel.class, //
                                    operand(Join.class, //
                                            null, //
                                            j -> j instanceof OlapJoinRel || j instanceof OlapNonEquiJoinRel, any())))),
            RelFactories.LOGICAL_BUILDER, "ScalarSubqueryJoinRule:AGG_PRJ_FLT_JOIN");

    public ScalarSubqueryJoinRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(call.rels.length - 1);
        switch (join.getJoinType()) {
        case INNER:
        case LEFT:
            break;
        default:
            return false;
        }

        OlapAggregateRel aggregate = call.rel(0);
        if (!aggregate.isSimpleGroupType() || aggregate.getAggCallList().isEmpty()) {
            return false;
        }

        // If any aggregate functions do not support splitting, bail outer
        // If any aggregate call has a filter or is distinct, bail out.
        // To-do: Count-distinct is currently not supported,
        //  but this can be achieved in case of scalar-subquery with distinct values.
        if (aggregate.getAggCallList().stream().anyMatch(a -> a.hasFilter() //
                || a.isDistinct() //
                || Objects.isNull(a.getAggregation().unwrap(SqlSplittableAggFunction.class)))) {
            return false;
        }

        return !(call.rel(1) instanceof OlapProjectRel) || canApplyRule(call.rel(1));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Transposer transposer = new Transposer(call);
        if (!transposer.canTranspose()) {
            return;
        }

        RelNode relNode = transposer.getTransposedRel();
        call.transformTo(relNode);
    }

    private boolean canApplyRule(OlapProjectRel project) {
        if (project.getProjects().stream().anyMatch(RexCall.class::isInstance)) {
            // to-do: maybe we should support this.
            return false;
        }
        final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        project.getProjects().forEach(p -> builder.addAll(RelOptUtil.InputFinder.bits(p)));
        // Avoid re-entry of rule.
        return project.getProjects().size() <= builder.build().cardinality();
    }

    private <E> SqlSplittableAggFunction.Registry<E> createRegistry(final List<E> list) {
        return e -> {
            int i = list.indexOf(e);
            if (i < 0) {
                i = list.size();
                list.add(e);
            }
            return i;
        };
    }

    private class Transposer {
        // base variables
        private final RelOptRuleCall call;
        private final AggregateUnit aggUnit;
        private final Join join;

        // join sides
        private final LeftSide left;
        private final RightSide right;

        // maintainer
        public Transposer(RelOptRuleCall ruleCall) {
            // call -> "aggunit, join"
            call = ruleCall;
            aggUnit = createAggUnit(ruleCall);
            join = ruleCall.rel(ruleCall.rels.length - 1);

            // join sides
            RelMetadataQuery mq = call.getMetadataQuery();
            ImmutableBitSet joinCondSet = RelOptUtil.InputFinder.bits(join.getCondition());
            ImmutableBitSet aggUnitJoinSet = aggUnit.getUnitSet().union(joinCondSet);
            left = new LeftSide(join.getLeft(), join.getInput(0), mq, aggUnitJoinSet);
            right = new RightSide(left, join.getRight(), join.getInput(1), mq, aggUnitJoinSet);
        }

        public boolean canTranspose() {
            if (left.hasRelValues() && right.isAggregable()) {
                return true;
            }

            return right.hasRelValues() && left.isAggregable();
        }

        public RelNode getTransposedRel() {
            // builders
            final RelBuilder relBuilder = call.builder();
            final RexBuilder rexBuilder = aggUnit.getRexBuilder();

            // below aggregate
            left.modifyAggregate(aggUnit, relBuilder, rexBuilder);
            right.modifyAggregate(aggUnit, relBuilder, rexBuilder);

            // aggunit-join mapping
            final Mapping aggUnitJoinMapping = getAggUnitJoinMapping();

            // create new join
            final RexNode joinCond = RexUtil.apply(aggUnitJoinMapping, join.getCondition());
            relBuilder.push(left.getNewInput()).push(right.getNewInput()).join(join.getJoinType(), joinCond);

            if (aggUnit instanceof AggregateFilter) {
                // create new filter
                // To-do: maybe we could also push down the relative filter.
                final RexNode filterCond = RexUtil.apply(aggUnitJoinMapping, //
                        ((AggregateFilter) aggUnit).getFilterCond());
                relBuilder.filter(filterCond);
            }

            if (aggUnit instanceof AggregateProjectFilter) {
                // create new filter
                // To-do: maybe we could also push down the relative filter.
                final RexNode filterCond = RexUtil.apply(aggUnitJoinMapping, //
                        ((AggregateProjectFilter) aggUnit).getFilterCond());
                relBuilder.filter(filterCond);
            }

            // agg-project mapping
            final Mapping projectMapping = getProjectMapping(relBuilder, aggUnitJoinMapping);

            // aggregate above to sum up the sub-totals
            final List<RexNode> projectList = //
                    Mappings.apply(projectMapping, //
                            Lists.newArrayList(rexBuilder.identityProjects(relBuilder.peek().getRowType())));

            final List<AggregateCall> aggCallList = Lists.newArrayList();
            aggregateAbove(projectList, aggCallList, relBuilder, rexBuilder);

            // create new project
            relBuilder.project(projectList);

            // above aggregate
            // To-do: maybe we could convert aggregate into projects when inner-join.
            final RelBuilder.GroupKey groupKey = //
                    relBuilder.groupKey(Mappings.apply(projectMapping, //
                            Mappings.apply(aggUnitJoinMapping, aggUnit.getGroupSet())), //
                            Mappings.apply2(projectMapping, //
                                    Mappings.apply2(aggUnitJoinMapping, aggUnit.getGroupSets())));
            relBuilder.aggregate(groupKey, aggCallList);

            return relBuilder.build();
        }

        private Mapping getProjectMapping(final RelBuilder relBuilder, final Mapping aggMapping) {
            final List<Integer> fieldList = //
                    IntStream.range(0, relBuilder.peek().getRowType().getFieldList().size()) //
                            .boxed().collect(Collectors.toList());
            final List<Integer> groupList = //
                    aggUnit.getGroupList().stream().map(aggMapping::getTarget).collect(Collectors.toList());

            // [i0, i1, i2, i3, i4] -> [i1, i3, i0, i2, i4]
            final Mapping projectMapping = //
                    Mappings.create(MappingType.BIJECTION, fieldList.size(), fieldList.size());

            Ord.zip(fieldList).forEach(o -> projectMapping.set(o.i, o.e));
            Ord.zip(groupList).forEach(o -> projectMapping.set(o.e, o.i));

            return projectMapping;
        }

        private AggregateUnit createAggUnit(RelOptRuleCall call) {
            if (call.rels.length > 3) {
                return new AggregateProjectFilter(call.rel(0), call.rel(1), call.rel(2));
            }
            if (call.rels.length > 2) {
                return call.rel(1) instanceof OlapFilterRel ? new AggregateFilter(call.rel(0), call.rel(1))
                        : new AggregateProject(call.rel(0), call.rel(1));
            }
            return new AggregateUnit(call.rel(0));
        }

        private void aggregateAbove(final List<RexNode> projectList, //
                final List<AggregateCall> aggCallList, //
                final RelBuilder relBuilder, //
                final RexBuilder rexBuilder) {
            final int newLeftWidth = left.getNewInputFieldCount();
            final int groupIndicatorCount = aggUnit.getGroupIndicatorCount();
            final SqlSplittableAggFunction.Registry<RexNode> projectRegistry = createRegistry(projectList);
            Ord.zip(aggUnit.getAggCallList()).forEach(aggCallOrd -> {
                // No need to care about args' mapping.
                AggregateCall aggCall = aggCallOrd.e;
                SqlAggFunction aggFunc = aggCall.getAggregation();
                SqlSplittableAggFunction splitAggFunc = Preconditions
                        .checkNotNull(aggFunc.unwrap(SqlSplittableAggFunction.class));
                Integer lst = left.getAggOrdinal(aggCallOrd.i);
                Integer rst = right.getAggOrdinal(aggCallOrd.i);
                final AggregateCall newAggCall = //
                        splitAggFunc.topSplit(rexBuilder, projectRegistry, groupIndicatorCount, //
                                relBuilder.peek().getRowType(), aggCall, //
                                Objects.isNull(lst) ? -1 : lst, //
                                Objects.isNull(rst) ? -1 : rst + newLeftWidth);

                if (aggCall.getAggregation() == SqlStdOperatorTable.COUNT //
                        && newAggCall.getAggregation() == SqlStdOperatorTable.SUM0) {
                    aboveCountSum0(rexBuilder, aggCall, newAggCall, projectList);
                }

                aggCallList.add(newAggCall);
            });
        }

        private void aboveCountSum0(final RexBuilder rexBuilder, //
                AggregateCall aggCall, //
                AggregateCall newAggCall, //
                final List<RexNode> projectList) {
            // COUNT(*), COUNT(1), COUNT(COL), COUNT(COL1, COL2, ...)
            final boolean nullAsOne = aggCall.getArgList().isEmpty();
            newAggCall.getArgList().forEach(i -> {
                // old project node
                RexNode p = projectList.get(i);
                // when-then-else
                List<RexNode> wte = Lists.newLinkedList();
                wte.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, p));
                if (nullAsOne) {
                    wte.add(rexBuilder.makeLiteral(BigDecimal.ONE, p.getType(), true));
                } else {
                    wte.add(rexBuilder.makeZeroLiteral(p.getType()));
                }

                wte.add(p);
                // new project node
                RexNode np = rexBuilder.makeCall(p.getType(), SqlStdOperatorTable.CASE, wte);
                projectList.set(i, np);
            });
        }

        private Mapping getAggUnitJoinMapping() {
            final Map<Integer, Integer> map = Maps.newHashMap();
            map.putAll(left.getAggUnitJoinMap());
            map.putAll(right.getAggUnitJoinMap());
            final int sourceCount = join.getRowType().getFieldCount();
            final int targetCount = left.getNewInputFieldCount() + right.getNewInputFieldCount();
            return (Mapping) Mappings.target(map::get, sourceCount, targetCount);
        }

    } // end of Transposer

    private static class AggregateUnit {
        // base variables
        protected final OlapAggregateRel aggregate;

        // immediate variables
        private RexBuilder rexBuilder;

        public AggregateUnit(OlapAggregateRel aggregate) {
            this.aggregate = aggregate;
        }

        public RexBuilder getRexBuilder() {
            if (Objects.isNull(rexBuilder)) {
                rexBuilder = aggregate.getCluster().getRexBuilder();
            }
            return rexBuilder;
        }

        public ImmutableBitSet getUnitSet() {
            return getGroupSet();
        }

        public ImmutableBitSet getGroupSet() {
            return aggregate.getGroupSet();
        }

        public ImmutableList<ImmutableBitSet> getGroupSets() {
            return ImmutableList.<ImmutableBitSet> builder().addAll(aggregate.groupSets).build();
        }

        public int getGroupCount() {
            return aggregate.getGroupCount();
        }

        public int getGroupIndicatorCount() {
            return getGroupCount() + aggregate.getIndicatorCount();
        }

        public List<AggregateCall> getAggCallList() {
            return aggregate.getAggCallList();
        }

        public List<Integer> getGroupList() {
            return aggregate.getGroupSet().asList();
        }

    } // end of AggregateUnit

    private static class AggregateProject extends AggregateUnit {
        // base variables
        private final OlapProjectRel project;
        private final Mappings.TargetMapping targetMapping;

        // immediate variables
        private ImmutableBitSet groupSet;
        private ImmutableList<ImmutableBitSet> groupSets;
        private List<AggregateCall> aggCallList;

        public AggregateProject(OlapAggregateRel aggregate, OlapProjectRel project) {
            super(aggregate);
            this.project = project;
            this.targetMapping = createTargetMapping();
        }

        @Override
        public ImmutableBitSet getGroupSet() {
            if (Objects.isNull(groupSet)) {
                groupSet = Mappings.apply((Mapping) targetMapping, aggregate.getGroupSet());
            }
            return groupSet;
        }

        @Override
        public ImmutableList<ImmutableBitSet> getGroupSets() {
            if (Objects.isNull(groupSets)) {
                groupSets = ImmutableList.<ImmutableBitSet> builder() //
                        .addAll(Mappings.apply2((Mapping) targetMapping, aggregate.getGroupSets())) //
                        .build();
            }
            return groupSets;
        }

        @Override
        public List<AggregateCall> getAggCallList() {
            if (Objects.isNull(aggCallList)) {
                aggCallList = aggregate.getAggCallList().stream() //
                        .map(a -> a.transform(targetMapping)) //
                        .collect(Collectors.collectingAndThen(Collectors.toList(), //
                                Collections::unmodifiableList));
            }
            return aggCallList;
        }

        @Override
        public List<Integer> getGroupList() {
            return super.getGroupList().stream().map(targetMapping::getTarget).collect(Collectors.toList());
        }

        private Mappings.TargetMapping createTargetMapping() {
            if (Objects.isNull(project.getMapping())) {
                return Mappings.createIdentity(project.getRowType().getFieldCount());
            }
            return project.getMapping().inverse();
        }

    } // end of AggregateProject

    private static class AggregateFilter extends AggregateUnit {

        private final OlapFilterRel filter;

        public AggregateFilter(OlapAggregateRel aggregate, OlapFilterRel filter) {
            super(aggregate);
            this.filter = filter;
        }

        @Override
        public ImmutableBitSet getUnitSet() {
            ImmutableBitSet filterSet = RelOptUtil.InputFinder.bits(filter.getCondition());
            return getGroupSet().union(filterSet);
        }

        public RexNode getFilterCond() {
            return filter.getCondition();
        }
    }

    private static class AggregateProjectFilter extends AggregateProject {

        private final OlapFilterRel filter;

        public AggregateProjectFilter(OlapAggregateRel aggregate, OlapProjectRel project, OlapFilterRel filter) {
            super(aggregate, project);
            this.filter = filter;
        }

        @Override
        public ImmutableBitSet getUnitSet() {
            ImmutableBitSet filterSet = RelOptUtil.InputFinder.bits(filter.getCondition());
            return getGroupSet().union(filterSet);
        }

        public RexNode getFilterCond() {
            return filter.getCondition();
        }

    } // end of AggregateProjectFilter

    private abstract class JoinSide {
        // base variables
        private final boolean isRelValues;
        private final boolean hasRelValues;
        private final RelNode input;
        private final ImmutableBitSet aggUnitJoinSet;

        // util variables
        protected ImmutableBitSet fieldSet;
        protected ImmutableBitSet sideAggUnitJoinSet;
        protected ImmutableBitSet belowAggGroupSet;

        // immediate variables
        private Map<Integer, Integer> aggOrdinalMap;
        private List<AggregateCall> belowAggCallList;
        private SqlSplittableAggFunction.Registry<AggregateCall> belowAggCallRegistry;

        private RelNode newInput;

        private Map<Integer, Integer> aggUnitJoinMap;

        public JoinSide(RelNode relNode, RelNode input, ImmutableBitSet aggUnitJoinSet) {
            this.isRelValues = isRelValues(relNode);
            this.hasRelValues = hasRelValues(relNode);
            this.input = input;
            this.aggUnitJoinSet = aggUnitJoinSet;
        }

        public boolean hasRelValues() {
            return hasRelValues;
        }

        public boolean isRelValues() {
            return isRelValues;
        }

        public abstract boolean isAggregable();

        public RelNode getNewInput() {
            return newInput;
        }

        public Integer getAggOrdinal(int i) {
            return getAggOrdinalMap().get(i);
        }

        public Map<Integer, Integer> getAggUnitJoinMap() {
            if (Objects.isNull(aggUnitJoinMap)) {
                final int belowOffset = getBelowOffset();
                final Map<Integer, Integer> map = Maps.newHashMap();
                Ord.zip(getSideAggUnitJoinSet()).forEach(o -> map.put(o.e, belowOffset + o.i));
                aggUnitJoinMap = map;
            }
            return aggUnitJoinMap;
        }

        public void modifyAggregate(AggregateUnit aggUnit, RelBuilder relBuilder, RexBuilder rexBuilder) {
            if (isRelValues()) {
                newInput = convertSingleton(aggUnit, relBuilder, rexBuilder);
                return;
            }

            if (isAggregable()) {
                newInput = convertSplit(aggUnit, relBuilder, rexBuilder);
                return;
            }

            newInput = convertSingleton(aggUnit, relBuilder, rexBuilder);
        }

        protected final boolean hasRelValues(RelNode node) {
            if (node instanceof HepRelVertex) {
                RelNode current = ((HepRelVertex) node).getCurrentRel();
                if (current instanceof Join) {
                    final Join join = (Join) current;
                    return isRelValues(join.getLeft()) || isRelValues(join.getRight());
                }
                return isRelValues(node);
            }
            return false;
        }

        protected final boolean isRelValues(RelNode node) {
            if (node instanceof HepRelVertex) {
                RelNode current = ((HepRelVertex) node).getCurrentRel();
                if (current instanceof OlapValuesRel) {
                    return true;
                }

                if (current.getInputs().isEmpty()) {
                    return false;
                }

                return current.getInputs().stream().allMatch(this::isRelValues);
            }
            return false;
        }

        protected final boolean isAggregable(RelNode input, RelMetadataQuery mq) {
            // No need to do aggregation. There is nothing to be gained by this rule.
            Boolean unique = mq.areColumnsUnique(input, getBelowAggGroupSet());
            return Objects.isNull(unique) || !unique;
        }

        protected int getInputFieldCount() {
            return input.getRowType().getFieldCount();
        }

        protected int getNewInputFieldCount() {
            return Preconditions.checkNotNull(newInput).getRowType().getFieldCount();
        }

        protected abstract int getOffset();

        protected abstract int getBelowOffset();

        protected abstract Mappings.TargetMapping getTargetMapping();

        private Map<Integer, Integer> getAggOrdinalMap() {
            if (Objects.isNull(aggOrdinalMap)) {
                aggOrdinalMap = Maps.newHashMap();
            }
            return aggOrdinalMap;
        }

        private void registryAggCall(int i, int offset, AggregateCall aggCall) {
            getAggOrdinalMap().put(i, offset + registry(aggCall));
        }

        private void registryOther(int i, int ordinal) {
            getAggOrdinalMap().put(i, ordinal);
        }

        private ImmutableBitSet getFieldSet() {
            if (Objects.isNull(fieldSet)) {
                int offset = getOffset();
                fieldSet = ImmutableBitSet.range(offset, offset + getInputFieldCount());
            }
            return fieldSet;
        }

        private ImmutableBitSet getBelowAggGroupSet() {
            if (Objects.isNull(belowAggGroupSet)) {
                int offset = getOffset();
                belowAggGroupSet = getSideAggUnitJoinSet().shift(-offset);
            }
            return belowAggGroupSet;
        }

        private ImmutableBitSet getSideAggUnitJoinSet() {
            if (Objects.isNull(sideAggUnitJoinSet)) {
                ImmutableBitSet fieldSet0 = getFieldSet();
                sideAggUnitJoinSet = Preconditions.checkNotNull(aggUnitJoinSet).intersect(fieldSet0);
            }
            return sideAggUnitJoinSet;
        }

        private RelNode convertSplit(AggregateUnit aggUnit, RelBuilder relBuilder, RexBuilder rexBuilder) {
            final ImmutableBitSet fields = getFieldSet();
            final int oldGroupSetCount = aggUnit.getGroupCount();
            final int newGroupSetCount = getBelowAggGroupSet().cardinality();
            Ord.zip(aggUnit.getAggCallList()).forEach(aggCallOrd -> {
                AggregateCall aggCall = aggCallOrd.e;
                SqlAggFunction aggFunc = aggCall.getAggregation();
                SqlSplittableAggFunction splitAggFunc = Preconditions
                        .checkNotNull(aggFunc.unwrap(SqlSplittableAggFunction.class));
                ImmutableBitSet aggArgSet = ImmutableBitSet.of(aggCall.getArgList());
                final AggregateCall newAggCall;
                if (fields.contains(aggArgSet)) {
                    // convert split
                    AggregateCall splitAggCall = splitAggFunc.split(aggCall, getTargetMapping());
                    newAggCall = splitAggCall.adaptTo(input, splitAggCall.getArgList(), splitAggCall.filterArg, //
                            oldGroupSetCount, newGroupSetCount);
                } else {
                    newAggCall = splitOther(splitAggFunc, rexBuilder, aggCall, fields, aggArgSet);
                }

                if (Objects.isNull(newAggCall)) {
                    return;
                }
                registryAggCall(aggCallOrd.i, newGroupSetCount, newAggCall);
            });

            return relBuilder.push(input) //
                    .aggregate(relBuilder.groupKey(belowAggGroupSet), //
                            Preconditions.checkNotNull(belowAggCallList)) //
                    .build();
        }

        private AggregateCall splitOther(SqlSplittableAggFunction splitAggFunc, //
                RexBuilder rexBuilder, //
                AggregateCall aggCall, //
                ImmutableBitSet fields, //
                ImmutableBitSet args) {
            // Thinking...aggCall not transformed?
            AggregateCall other = splitAggFunc.other(rexBuilder.getTypeFactory(), aggCall);
            if (Objects.isNull(other)) {
                return null;
            }

            ImmutableBitSet newArgSet = Mappings.apply((Mapping) getTargetMapping(), args.intersect(fields));
            return AggregateCall.create(other.getAggregation(), other.isDistinct(), //
                    other.isApproximate(), newArgSet.asList(), //
                    other.filterArg, other.getType(), other.getName());

        }

        private RelNode convertSingleton(AggregateUnit aggUnit, RelBuilder relBuilder, RexBuilder rexBuilder) {
            relBuilder.push(input);
            final ImmutableBitSet fieldSet0 = getFieldSet();
            final List<RexNode> projectList = Lists.newArrayList();
            getBelowAggGroupSet().forEach(i -> projectList.add(relBuilder.field(i)));
            Ord.zip(aggUnit.getAggCallList()).forEach(aggCallOrd -> {
                AggregateCall aggCall = aggCallOrd.e;
                SqlAggFunction aggFunc = aggCall.getAggregation();
                SqlSplittableAggFunction splitAggFunc = Preconditions
                        .checkNotNull(aggFunc.unwrap(SqlSplittableAggFunction.class));
                if (aggCall.getArgList().isEmpty()) {
                    return;
                }
                ImmutableBitSet aggArgSet = ImmutableBitSet.of(aggCall.getArgList());
                if (!fieldSet0.contains(aggArgSet)) {
                    return;
                }

                // convert singleton
                RexNode singleton = splitAggFunc.singleton(rexBuilder, input.getRowType(), //
                        aggCall.transform(getTargetMapping()));
                if (singleton instanceof RexInputRef) {
                    registryOther(aggCallOrd.i, ((RexInputRef) singleton).getIndex());
                    return;
                }
                int ordinal = projectList.size();
                projectList.add(singleton);
                registryOther(aggCallOrd.i, ordinal);
            });

            relBuilder.project(projectList);

            return relBuilder.build();
        }

        private int registry(AggregateCall aggCall) {
            if (Objects.isNull(belowAggCallRegistry)) {
                if (Objects.isNull(belowAggCallList)) {
                    belowAggCallList = Lists.newArrayList();
                }
                belowAggCallRegistry = createRegistry(belowAggCallList);
            }
            return belowAggCallRegistry.register(aggCall);
        }

    } // end of side

    private class LeftSide extends JoinSide {

        private final boolean isAggregable;

        private final Mappings.TargetMapping targetMapping;

        public LeftSide(RelNode relNode, RelNode input, RelMetadataQuery mq, ImmutableBitSet aggUnitJoinSet) {
            super(relNode, input, aggUnitJoinSet);
            this.isAggregable = isAggregable(input, mq);
            this.targetMapping = createTargetMapping();
        }

        @Override
        public boolean isAggregable() {
            return isAggregable;
        }

        @Override
        protected int getOffset() {
            return 0;
        }

        @Override
        protected int getBelowOffset() {
            return 0;
        }

        @Override
        protected Mappings.TargetMapping getTargetMapping() {
            return targetMapping;
        }

        private Mappings.TargetMapping createTargetMapping() {
            int fieldCount = getInputFieldCount();
            return Mappings.createIdentity(fieldCount);
        }

    } // end of LeftSide

    private class RightSide extends JoinSide {

        private final LeftSide left;

        private final boolean isAggregable;

        private final Mappings.TargetMapping targetMapping;

        public RightSide(LeftSide left, //
                RelNode relNode, RelNode input, //
                RelMetadataQuery mq, ImmutableBitSet aggUnitJoinSet) {
            super(relNode, input, aggUnitJoinSet);
            this.left = left;
            this.isAggregable = isAggregable(input, mq);
            this.targetMapping = createTargetMapping();
        }

        @Override
        public boolean isAggregable() {
            return isAggregable;
        }

        @Override
        protected int getOffset() {
            return left.getInputFieldCount();
        }

        @Override
        protected int getBelowOffset() {
            return left.getNewInputFieldCount();
        }

        @Override
        protected Mappings.TargetMapping getTargetMapping() {
            return targetMapping;
        }

        private Mappings.TargetMapping createTargetMapping() {
            int offset = getOffset();
            int fieldCount = getInputFieldCount();
            return Mappings.createShiftMapping(fieldCount + offset, 0, offset, fieldCount);
        }

    } // end of RightSide

}
