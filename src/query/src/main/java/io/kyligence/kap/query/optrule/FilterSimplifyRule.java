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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kylin.query.relnode.KapFilterRel;


public class FilterSimplifyRule extends RelOptRule {

    public static final FilterSimplifyRule INSTANCE = new FilterSimplifyRule(
            operand(KapFilterRel.class, any()),
            RelFactories.LOGICAL_BUILDER, "FilterSimpifyRule");

    public FilterSimplifyRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
                              String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RelBuilder relBuilder = call.builder();

        boolean changed = false;
        List<RexNode> conjunctions = new LinkedList<>();
        for (RexNode conjunction : RelOptUtil.conjunctions(filter.getCondition())) {
            RexNode simpified = simpiflyOrs(conjunction, relBuilder.getRexBuilder());
            if (simpified != conjunction) {
                changed = true;
            }
            conjunctions.add(simpified);
        }

        if (changed) {
            relBuilder.push(filter.getInput());
            relBuilder.filter(
                    RexUtil.composeConjunction(relBuilder.getRexBuilder(), conjunctions, true));
            call.transformTo(relBuilder.build());
        }
    }

    private RexNode simpiflyOrs(RexNode conjunction, RexBuilder rexBuilder) {
        List<RexNode> terms = RelOptUtil.disjunctions(conjunction);

        // combine simple expr=lit1 or expr=lit2 ... --> expr in (lit1, lit2, ...)
        HashMap<String, List<RexNode>> equals = new HashMap<>();
        HashMap<String, List<Integer>> equalsIdxes = new HashMap<>();
        findPattern(terms, equals, equalsIdxes);

        List<RexNode> mergedTerms = new LinkedList<>();
        Set<Integer> toRemoveIdxes = new HashSet<>();
        equalsIdxes.forEach((String digest, List<Integer> idxes) -> {
            if (idxes.size() >= 5) {
                mergedTerms.add(rexBuilder.makeCall(SqlStdOperatorTable.IN, equals.get(digest)));
                toRemoveIdxes.addAll(idxes);
            }
        });

        if (toRemoveIdxes.isEmpty()) {
            return conjunction;
        }

        for (int i = 0; i < terms.size(); i++) {
            if (!toRemoveIdxes.contains(i)) {
                mergedTerms.add(terms.get(i));
            }
        }
        return RexUtil.composeDisjunction(rexBuilder, mergedTerms);
    }

    private void findPattern(List<RexNode> terms, HashMap<String, List<RexNode>> equals, HashMap<String, List<Integer>> equalsIdxes) {
        for (int i = 0; i < terms.size(); i++) {
            if (!(terms.get(i) instanceof RexCall)) {
                continue;
            }

            final RexCall call = (RexCall) terms.get(i);
            findEquals(equals, equalsIdxes, i, call);
        }
    }

    private void findEquals(HashMap<String, List<RexNode>> equals, HashMap<String, List<Integer>> equalsIdxes, int i, RexCall call) {
        if (call.getOperator() != SqlStdOperatorTable.EQUALS) {
            return;
        }

        final RexNode op0 = call.getOperands().get(0);
        final RexNode op1 = call.getOperands().get(1);
        if (op0 instanceof RexLiteral && !(op1 instanceof RexLiteral)) {
            if (!equals.containsKey(op1.toString())) {
                equals.put(op1.toString(), new ArrayList<>());
                equals.get(op1.toString()).add(op1);
                equalsIdxes.put(op1.toString(), new ArrayList<>());
            }
            equals.get(op1.toString()).add(op0);
            equalsIdxes.get(op1.toString()).add(i);
        } else if (!(op0 instanceof RexLiteral) && op1 instanceof RexLiteral) {
            if (!equals.containsKey(op0.toString())) {
                equals.put(op0.toString(), new ArrayList<>());
                equals.get(op0.toString()).add(op0);
                equalsIdxes.put(op0.toString(), new ArrayList<>());
            }
            equals.get(op0.toString()).add(op1);
            equalsIdxes.get(op0.toString()).add(i);
        }
    }
}
