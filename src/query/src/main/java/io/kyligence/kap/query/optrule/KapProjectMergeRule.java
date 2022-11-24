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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Permutation;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Sets;

/**
 * Introduce KapProjectMergeRule in replacement of Calcite's ProjectMergeRule to fix OOM issue: KAP-4899
 *
 * Change point, simplify(): simplify "CASE(=($2, 0), null, CASE(=($2, 0), null, $1))" to be "CASE(=($2, 0), null, $1)"
 *
 * Once CALCITE-2223 fixed, this rule is supposed to retire
 *
 * see also:
 *   https://issues.apache.org/jira/browse/CALCITE-2223
 *   https://issues.apache.org/jira/browse/DRILL-6212 and https://github.com/apache/drill/pull/1319/files
 *
 * @author yifanzhang
 *
 */
public class KapProjectMergeRule extends RelOptRule {
    public static final KapProjectMergeRule INSTANCE = new KapProjectMergeRule(true, RelFactories.LOGICAL_BUILDER);

    //~ Instance fields --------------------------------------------------------

    /** Whether to always merge projects. */
    private final boolean force;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ProjectMergeRule, specifying whether to always merge projects.
     *
     * @param force Whether to always merge projects
     */
    public KapProjectMergeRule(boolean force, RelBuilderFactory relBuilderFactory) {
        super(operand(Project.class, operand(Project.class, any())), relBuilderFactory,
                "KapProjectMergeRule" + (force ? ":force_mode" : ""));
        this.force = force;
    }

    @Deprecated // to be removed before 2.0
    public KapProjectMergeRule(boolean force, RelFactories.ProjectFactory projectFactory) {
        this(force, RelBuilder.proto(projectFactory));
    }

    //~ Methods ----------------------------------------------------------------

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

        final List<RexNode> pushedProjects;
        if (KylinConfig.getInstanceFromEnv().isProjectMergeWithBloatEnabled()) {
            pushedProjects = RelOptUtil.pushPastProjectUnlessBloat(topProject.getProjects(),
                    bottomProject, KylinConfig.getInstanceFromEnv().getProjectMergeRuleBloatThreshold());
            if (pushedProjects == null) {
                // Merged projects are significantly more complex. Do not merge.
                return;
            }
        } else {
            pushedProjects = RelOptUtil.pushPastProject(topProject.getProjects(), bottomProject);
        }
        final List<RexNode> newProjects = simplify(pushedProjects);
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

    private static List<RexNode> simplify(List<RexNode> projectExprs) {

        final List<RexNode> list = new ArrayList<>();
        simplifyRecursiveCase().visitList(projectExprs, list);
        return list;
    }

    private static RexShuttle simplifyRecursiveCase() {
        return new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (call.getKind() == SqlKind.CASE && call.getOperands().size() == 3) {
                    RexNode op0 = call.getOperands().get(0);
                    RexNode op1 = call.getOperands().get(1);
                    RexNode op2 = call.getOperands().get(2);
                    if (op1 instanceof RexCall && ((RexCall) op1).getKind() == SqlKind.CASE
                            && ((RexCall) op1).getOperands().size() == 3
                            && RexUtil.eq(op0, ((RexCall) op1).getOperands().get(0))
                            && RexUtil.eq(op2, ((RexCall) op1).getOperands().get(2))) {
                        return visitCall((RexCall) op1);
                    }
                    if (op2 instanceof RexCall && ((RexCall) op2).getKind() == SqlKind.CASE
                            && ((RexCall) op2).getOperands().size() == 3
                            && RexUtil.eq(op0, ((RexCall) op2).getOperands().get(0))
                            && RexUtil.eq(op1, ((RexCall) op2).getOperands().get(1))) {
                        return visitCall((RexCall) op2);
                    }
                }
                return super.visitCall(call);
            }
        };
    }

    private static Set<String> NON_MERGEABLE_FUNCTION = Sets.newHashSet("EXPLODE");

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
