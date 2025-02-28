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
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Permutation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

/**
 * Introduce OlapProjectMergeRule in replacement of Calcite's ProjectMergeRule to fix OOM issue: KAP-4899
 *
 * Change point, simplify(): simplify "CASE(=($2, 0), null, CASE(=($2, 0), null, $1))" to be "CASE(=($2, 0), null, $1)"
 *
 * Once CALCITE-2223 fixed, this rule is supposed to retire
 *
 * see also:
 *   <a href="https://issues.apache.org/jira/browse/CALCITE-2223">CALCITE-2223</a>
 *   <a href="https://issues.apache.org/jira/browse/DRILL-6212">DRILL-6212</a>
 *
 * @author yifanzhang
 *
 */
public class OlapProjectMergeRule extends RelOptRule {
    public static final OlapProjectMergeRule INSTANCE = new OlapProjectMergeRule(true, RelFactories.LOGICAL_BUILDER);
    private static final Set<String> NON_MERGEABLE_FUNCTION = Sets.newHashSet("EXPLODE");

    //~ Instance fields --------------------------------------------------------

    /** Whether to always merge projects. */
    private final boolean force;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectMergeRule, specifying whether to always merge projects.
     *
     * @param force Whether to always merge projects
     */
    public OlapProjectMergeRule(boolean force, RelBuilderFactory relBuilderFactory) {
        super(operand(Project.class, operand(Project.class, any())), relBuilderFactory,
                "OlapProjectMergeRule" + (force ? ":force_mode" : ""));
        this.force = force;
    }

    @Deprecated // to be removed before 2.0
    public OlapProjectMergeRule(boolean force, RelFactories.ProjectFactory projectFactory) {
        this(force, RelBuilder.proto(projectFactory));
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Project topProject = call.rel(0);
        final Project bottomProject = call.rel(1);
        return topProject.getConvention() == bottomProject.getConvention();
    }

    public void onMatch(RelOptRuleCall call) {
        final Project topProject = call.rel(0);
        final Project bottomProject = call.rel(1);
        final RelBuilder relBuilder = call.builder();

        // skip non mergeable exprs
        if (containsNonMergeableExprs(bottomProject) || containsNonMergeableExprs(topProject)) {
            return;
        }

        // If one or both projects are permutations, short-circuit the complex logic
        // of building a RexProgram.
        final Permutation topPermutation = topProject.getPermutation();
        if (topPermutation != null) {
            if (topPermutation.isIdentity()) {
                // Let ProjectRemoveRule handle this.
                return;
            }
            final Permutation bottomPermutation = bottomProject.getPermutation();
            if (bottomPermutation != null) {
                if (bottomPermutation.isIdentity()) {
                    // Let ProjectRemoveRule handle this.
                    return;
                }
                final Permutation product = topPermutation.product(bottomPermutation);
                relBuilder.push(bottomProject.getInput());
                relBuilder.project(relBuilder.fields(product), topProject.getRowType().getFieldNames());
                call.transformTo(relBuilder.build());
                return;
            }
        }

        // If we're not in force mode and the two projects reference identical
        // inputs, then return and let ProjectRemoveRule replace the projects.
        if (!force && RexUtil.isIdentity(topProject.getProjects(), topProject.getInput().getRowType())) {
            return;
        }

        List<RexNode> newProjects = RelOptUtil.pushPastProjectUnlessBloat(topProject.getProjects(), bottomProject,
                KylinConfig.getInstanceFromEnv().getProjectBloatThreshold());
        if (newProjects == null) {
            // Merged projects are significantly more complex. Do not merge.
            return;
        }
        newProjects = simplifyCast(newProjects, topProject.getCluster().getRexBuilder());
        final RelNode input = bottomProject.getInput();
        if (RexUtil.isIdentity(newProjects, input.getRowType())
                && (force || input.getRowType().getFieldNames().equals(topProject.getRowType().getFieldNames()))) {
            call.transformTo(input);
            return;
        }

        // replace the two projects with a combined projection
        relBuilder.push(bottomProject.getInput());
        relBuilder.project(newProjects, topProject.getRowType().getFieldNames());
        call.transformTo(relBuilder.build());
    }

    public static List<RexNode> simplifyCast(List<RexNode> projects, RexBuilder rexBuilder) {
        final List<RexNode> list = new ArrayList<>();
        for (RexNode rex : projects) {
            if (rex.getKind() == SqlKind.CAST) {
                RexNode inner = ((RexCall) rex).getOperands().get(0);
                RexNode simplified = simplify(inner);
                if (simplified.getType() == rex.getType()) {
                    list.add(simplified);
                } else {
                    List<RexNode> newNodes = Collections.singletonList(simplified);
                    RexNode rexNode = rexBuilder.makeCall(rex.getType(), ((RexCall) rex).getOperator(), newNodes);
                    list.add(rexNode);
                }
            } else {
                list.add(rex);
            }
        }
        return list;
    }

    private static RexNode simplify(RexNode node) {
        RexNode current = node;
        if (current.isA(SqlKind.CAST)) {
            RexNode operand = ((RexCall) current).getOperands().get(0);
            if (operand.getType().equals(current.getType())) {
                current = simplify(operand);
            }
        }
        return current;
    }

    private boolean containsNonMergeableExprs(Project project) {
        for (RexNode expr : project.getProjects()) {
            if (containsNonMergeableExprs(expr)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsNonMergeableExprs(RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            if (NON_MERGEABLE_FUNCTION.contains(((RexCall) rexNode).getOperator().getName())) {
                return true;
            }
            for (RexNode operand : ((RexCall) rexNode).getOperands()) {
                if (containsNonMergeableExprs(operand)) {
                    return true;
                }
            }
        }
        return false;
    }
}
