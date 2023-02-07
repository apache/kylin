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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.kylin.query.relnode.KapAggregateRel;
import org.apache.kylin.query.relnode.KapFilterRel;
import org.apache.kylin.query.relnode.KapJoinRel;
import org.apache.kylin.query.relnode.KapProjectRel;
import org.apache.kylin.query.util.RuleUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * agg-project-join  ->  agg-project-agg-join
 * agg-project-filter-join -> agg-project-filter-agg-join
 */
public class KapAggProjectTransposeRule extends RelOptRule {
    public static final KapAggProjectTransposeRule AGG_PROJECT_FILTER_JOIN = new KapAggProjectTransposeRule(
            operand(KapAggregateRel.class,
                    operand(KapProjectRel.class, operand(KapFilterRel.class, operand(KapJoinRel.class, any())))),
            RelFactories.LOGICAL_BUILDER, "KapAggProjectTransposeRule:agg-project-filter-join");

    public static final KapAggProjectTransposeRule AGG_PROJECT_JOIN = new KapAggProjectTransposeRule(
            operand(KapAggregateRel.class, operand(KapProjectRel.class, operand(KapJoinRel.class, any()))),
            RelFactories.LOGICAL_BUILDER, "KapAggProjectTransposeRule:agg-project-join");

    public KapAggProjectTransposeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    public KapAggProjectTransposeRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public KapAggProjectTransposeRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
        final KapProjectRel project = call.rel(1);
        final KapJoinRel joinRel;
        if (call.rel(2) instanceof KapFilterRel) {
            joinRel = call.rel(3);
        } else {
            joinRel = call.rel(2);
        }

        //Only one agg child of join is accepted
        if (!RuleUtils.isJoinOnlyOneAggChild(joinRel)) {
            return false;
        }

        //Not support agg calls contain the same column for now
        Set<Integer> argSet = Sets.newHashSet();
        int argCount = 0;
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            List<Integer> argList = aggregateCall.getArgList();
            argCount += argList.size();
            argSet.addAll(argList);
        }
        if (argSet.size() != argCount) {
            return false;
        }

        for (int i = 0; i < project.getProjects().size(); i++) {
            RexNode rexNode = project.getProjects().get(i);
            // Only handle "GROUP BY expression"
            // If without expression, see KapAggProjectMergeRule
            if (rexNode instanceof RexCall && aggregate.getRewriteGroupKeys().contains(i)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final KapAggregateRel aggregate = call.rel(0);
        final KapProjectRel project = call.rel(1);

        // Do the columns used by the project appear in the output of the aggregate
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int key : aggregate.getGroupSet()) {
            final RexNode rex = project.getProjects().get(key);
            if (rex instanceof RexInputRef) {
                final int newKey = ((RexInputRef) rex).getIndex();
                builder.set(newKey);
            } else if (rex instanceof RexCall) {
                getColumnsFromExpression((RexCall) rex, builder);
            }
        }

        ImmutableBitSet newGroupSet = builder.build();
        Set<Integer> mappingWithOrder = new LinkedHashSet<>();
        mappingWithOrder.addAll(newGroupSet.asList());

        //Add the columns of "project projects" to group set
        for (RexNode rexNode : project.getProjects()) {
            if (rexNode instanceof RexInputRef) {
                int index = ((RexInputRef) rexNode).getIndex();
                if (!mappingWithOrder.contains(index)) {
                    mappingWithOrder.add(index);
                }
            } else if (rexNode instanceof RexCall) {
                getColumnsFromProjects((RexCall) rexNode, mappingWithOrder);
            }
        }

        List<Integer> mappingWithOrderList = Lists.newArrayList(mappingWithOrder);
        final RelNode projectInput = project.getInput();
        final Mappings.TargetMapping mapping = Mappings.target(mappingWithOrderList::indexOf,
                projectInput.getRowType().getFieldCount(), mappingWithOrder.size());

        //Process agg calls
        final ImmutableList.Builder<AggregateCall> aggCalls = ImmutableList.builder();
        final ImmutableList.Builder<AggregateCall> topAggCalls = ImmutableList.builder();

        Map<Integer, RelDataType> countArgMap = new HashMap<>();
        int newAggregateGroupSetSize = newGroupSet.asSet().size();
        List<RexNode> projects = Lists.newArrayList();
        Set<Integer> newTopAggregateSet = new HashSet<>();
        int start = 0;
        for (Integer index : aggregate.getGroupSet().asSet()) {
            projects.add(project.getProjects().get(index));
            newTopAggregateSet.add(start);
            start++;
        }

        //Mapping input: the origin input of project is from filter or join
        // , current input is from new aggregate
        //Origin: agg - project - filter/join
        //Current: agg - project - agg - filter/join
        final List<RexNode> newProjects = Lists.newArrayList();
        for (RexNode rexNode : RexUtil.apply(mapping, projects)) {
            newProjects.add(rexNode);
        }
        processAggCalls(aggregate, project, aggCalls, topAggCalls, countArgMap, newProjects);

        ImmutableList<AggregateCall> aggregateCalls = aggCalls.build();
        final Aggregate newAggregate = aggregate.copy(aggregate.getTraitSet(), project.getInput(), aggregate.indicator,
                newGroupSet, null, aggregateCalls);

        List<String> newProjectFieldNames = new ArrayList<>();
        List<String> oldProjectFieldNameList = project.getRowType().getFieldNames();
        for (int i = 0; i < oldProjectFieldNameList.size(); i++) {
            if (aggregate.getGroupSet().get(i)) {
                newProjectFieldNames.add(oldProjectFieldNameList.get(i));
            }
        }
        for (Map.Entry<Integer, RelDataType> entry : countArgMap.entrySet()) {
            int originalRefIndex = entry.getKey();
            int newRefIndex = newAggregateGroupSetSize + originalRefIndex;
            RelDataType relDataType = entry.getValue();
            String projectRefName = "$" + newRefIndex;
            newProjects.add(new RexInputRef(projectRefName, newRefIndex, relDataType));
            newProjectFieldNames.add(projectRefName);
        }

        final RelDataType newRowType = RexUtil.createStructType(newAggregate.getCluster().getTypeFactory(), newProjects,
                newProjectFieldNames, SqlValidatorUtil.F_SUGGESTER);
        final Project newProject = project.copy(project.getTraitSet(), newAggregate, newProjects, newRowType);
        ImmutableBitSet.Builder topAggregateGroupSetBuilder = ImmutableBitSet.builder();
        topAggregateGroupSetBuilder.addAll(newTopAggregateSet);
        final Aggregate topAggregate = aggregate.copy(aggregate.getTraitSet(), newProject, aggregate.indicator,
                topAggregateGroupSetBuilder.build(), null, topAggCalls.build());
        call.transformTo(topAggregate);
    }

    private void processAggCalls(KapAggregateRel aggregate, KapProjectRel project,
            ImmutableList.Builder<AggregateCall> aggCalls, ImmutableList.Builder<AggregateCall> topAggCalls,
            Map<Integer, RelDataType> countArgMap, List<RexNode> newProjects) {
        int startIndex = 0;
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            final ImmutableList.Builder<Integer> newArgs = ImmutableList.builder();
            for (int arg : aggregateCall.getArgList()) {
                final RexNode rex = project.getProjects().get(arg);
                if (rex instanceof RexInputRef) {
                    newArgs.add(((RexInputRef) rex).getIndex());
                } else {
                    // Cannot handle "AGG(expression)"
                    return;
                }
            }
            int newFilterArg = -1;
            if (aggregateCall.filterArg >= 0
                    && project.getProjects().get(aggregateCall.filterArg) instanceof RexInputRef) {
                newFilterArg = ((RexInputRef) project.getProjects().get(aggregateCall.filterArg)).getIndex();
            }
            aggCalls.add(aggregateCall.copy(newArgs.build(), newFilterArg));
            //Handle COUNT() for top agg
            countArgMap.put(startIndex, aggregateCall.type);
            List<Integer> topAggArgList = new ArrayList<>();
            topAggArgList.add(newProjects.size() + startIndex);
            if (!aggregateCall.getAggregation().getName().equals("COUNT")) {
                topAggCalls.add(AggregateCall.create(aggregateCall.getAggregation(), false, false, topAggArgList, -1,
                        aggregateCall.type, aggregateCall.name));
            } else {
                topAggCalls.add(AggregateCall.create(SqlStdOperatorTable.SUM0, false, false, topAggArgList, -1,
                        aggregateCall.type, aggregateCall.name));
            }
            startIndex++;
        }
    }

    private void getColumnsFromExpression(RexCall rexCall, ImmutableBitSet.Builder builder) {
        List<RexNode> rexNodes = rexCall.operands;
        for (RexNode rexNode : rexNodes) {
            if (rexNode instanceof RexInputRef) {
                builder.set(((RexInputRef) rexNode).getIndex());
            } else if (rexNode instanceof RexCall) {
                getColumnsFromExpression((RexCall) rexNode, builder);
            }
        }
    }

    private void getColumnsFromProjects(RexCall rexCall, Set<Integer> mapping) {
        List<RexNode> rexNodes = rexCall.operands;
        for (RexNode rexNode : rexNodes) {
            if (rexNode instanceof RexInputRef) {
                mapping.add(((RexInputRef) rexNode).getIndex());
            } else if (rexNode instanceof RexCall) {
                getColumnsFromProjects((RexCall) rexNode, mapping);
            }
        }
    }
}
