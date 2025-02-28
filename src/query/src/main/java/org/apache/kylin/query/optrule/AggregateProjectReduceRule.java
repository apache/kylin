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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;

/**
 * Reduce project under aggregate which has unused input ref.
 * The aggregate input ref also need rebuild since project expressions changed.
 * Mainly used for the simple aggregates expanded from grouping aggregate by {@link AggregateMultipleExpandRule}.
 * With this rule, the rolled up dimensions in aggregate will be reduced, we can use higher layer cuboid data.
 */
public class AggregateProjectReduceRule extends RelOptRule {
    public static final AggregateProjectReduceRule INSTANCE = new AggregateProjectReduceRule(//
            operand(LogicalAggregate.class, null, Aggregate.IS_SIMPLE, operand(LogicalProject.class, any())), //
            RelFactories.LOGICAL_BUILDER, "AggregateProjectReduceRule");

    private AggregateProjectReduceRule(RelOptRuleOperand operand, RelBuilderFactory factory, String description) {
        super(operand, factory, description);
    }

    private void mappingKeys(int key, Pair<RexNode, String> project, List<Pair<RexNode, String>> projects,
            Map<Integer, Integer> mapping) {
        if (!projects.contains(project)) {
            projects.add(project);
        }
        mapping.put(key, projects.indexOf(project));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggr = call.rel(0);
        LogicalProject project = call.rel(1);

        // generate input ref in group set mapping between old and new project
        List<Pair<RexNode, String>> projects = project.getNamedProjects();
        List<Pair<RexNode, String>> newProjects = new ArrayList<>();
        Map<Integer, Integer> mapping = new HashMap<>();
        for (int key : aggr.getGroupSet()) {
            mappingKeys(key, projects.get(key), newProjects, mapping);
        }

        // create new group set
        final ImmutableBitSet newGroupSet = aggr.getGroupSet().permute(mapping);

        // mapping input ref in aggr calls and generate new aggr calls
        final ImmutableList.Builder<AggregateCall> newAggrCalls = ImmutableList.builder();
        for (AggregateCall aggrCall : aggr.getAggCallList()) {
            final ImmutableList.Builder<Integer> newArgs = ImmutableList.builder();
            for (int key : aggrCall.getArgList()) {
                mappingKeys(key, projects.get(key), newProjects, mapping);
                newArgs.add(mapping.get(key));
            }
            final int newFilterArg;
            if (aggrCall.filterArg > 0) {
                int key = aggrCall.filterArg;
                mappingKeys(key, projects.get(key), newProjects, mapping);
                newFilterArg = mapping.get(aggrCall.filterArg);
            } else {
                newFilterArg = -1;
            }

            newAggrCalls.add(aggrCall.copy(newArgs.build(), newFilterArg));
        }

        // just return if nothing changed
        if (newProjects.equals(project.getNamedProjects())) {
            return;
        }

        RelBuilder relBuilder = call.builder().transform(c -> c.withPruneInputOfAggregate(false));
        relBuilder.push(project.getInput());
        relBuilder.project(Pair.left(newProjects), Pair.right(newProjects));
        relBuilder.aggregate(relBuilder.groupKey(newGroupSet, false, null), newAggrCalls.build());
        RelNode rel = relBuilder.build();

        call.transformTo(rel);
    }
}
