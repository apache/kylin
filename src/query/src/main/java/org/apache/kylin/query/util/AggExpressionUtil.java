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

package org.apache.kylin.query.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.query.exception.SumExprUnSupportException;

import lombok.Getter;
import lombok.Setter;

public class AggExpressionUtil {

    private AggExpressionUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static boolean hasAggInput(RelNode current) {
        if (current == null)
            return false;
        if (current.getInputs().isEmpty())
            return false;
        RelNode input = current.getInput(0);
        if (input == null)
            return false;

        if (input instanceof HepRelVertex) {
            input = ((HepRelVertex) input).getCurrentRel();
        }

        if (input instanceof Aggregate)
            return true;

        if (!(input instanceof RelSubset)) {
            return hasAggInput(input);
        }
        input = ((RelSubset) input).getOriginal();
        if (input instanceof Aggregate)
            return true;
        return hasAggInput(input);
    }

    @Getter
    @Setter
    public static class AggExpression {
        // original info
        private AggregateCall agg;
        private RexNode expression;
        private RelDataType type;

        private boolean isSumCase = false;
        private boolean isCountDistinctCase = false;
        private List<RexNode> conditions;
        private int[] bottomProjConditionsInput;
        private int[] bottomAggConditionsInput;
        private int[] topProjConditionsInput;
        // skip topAggConditionsInput
        private List<RexNode> valuesList;
        // skip bottomProjValuesInput
        private int[] bottomAggValuesInput;
        private int[] topProjValuesInput;
        // skip topAggValuesInput

        private boolean isCount = false;
        private int[] bottomProjInput;
        private int[] bottomAggInput;
        private int[] topProjInput;
        private int[] topAggInput;

        private boolean isSumConst = false;

        public AggExpression(AggregateCall agg) {
            this.agg = agg;
        }

        public AggregateCall getAggCall() {
            return agg;
        }

        public void setConditionsList(List<RexNode> conditionsList) {
            this.conditions = conditionsList;
            this.bottomProjConditionsInput = RelOptUtil.InputFinder.bits(conditions, null).toArray();
            this.bottomAggConditionsInput = newArray(bottomProjConditionsInput.length);
            this.topProjConditionsInput = newArray(bottomProjConditionsInput.length);
        }

        public void setValuesList(List<RexNode> valuesList) {
            this.valuesList = valuesList;
            this.bottomAggValuesInput = newArray(valuesList.size());
            this.topProjValuesInput = newArray(valuesList.size());
        }
    }

    @Getter
    @Setter
    public static class GroupExpression {
        private RexNode expression;

        private boolean isLiteral = false;

        private int[] bottomProjInput;
        private int[] bottomAggInput;
        private int[] topProjInput;
        private int[] topAggInput;
    }

    public static boolean hasSumCaseWhen(AggregateCall call, RexNode exp) {
        return isSum(call.getAggregation().getKind()) && (isCaseWhenCondition(exp) || isIfCondition(exp));
    }

    public static boolean hasCountDistinctCaseWhen(AggregateCall call, RexNode exp) {
        boolean isCountDistinct = call.getAggregation().getKind() == SqlKind.COUNT && call.isDistinct();
        return isCountDistinct && (isCaseWhenCondition(exp) || isIfCondition(exp));
    }

    private static boolean isIfCondition(RexNode exp) {
        if (exp instanceof RexCall) {
            return ((RexCall) exp).getOperator().isName("IF", false);
        }
        return false;
    }

    private static boolean isCaseWhenCondition(RexNode expression) {
        return expression.isA(SqlKind.CASE);
    }

    public static boolean isSum(SqlKind kind) {
        return SqlKind.SUM == kind || SqlKind.SUM0 == kind;
    }

    public static List<AggExpression> collectSumExpressions(Aggregate oldAgg, Project oldProject) {
        List<AggExpression> aggExpressions = Lists.newArrayList();
        for (AggregateCall call : oldAgg.getAggCallList()) {
            assertCondition(call.getArgList().size() <= 1, "Only support aggregate with 0 or 1 argument");

            AggExpression aggExpression = new AggExpression(call);
            aggExpressions.add(aggExpression);
            if (SqlKind.COUNT == call.getAggregation().getKind()) {
                aggExpression.setCount(true);
            }
            if (call.getArgList().isEmpty()) {
                // COUNT(*)
                aggExpression.bottomProjInput = newArray(0);
                aggExpression.bottomAggInput = newArray(0);
                aggExpression.topProjInput = newArray(1);
                aggExpression.topAggInput = newArray(1);
                continue;
            }
            int input = call.getArgList().get(0);
            RexNode expression = oldProject.getProjects().get(input);
            int[] sourceInput = RelOptUtil.InputFinder.bits(expression).toArray();
            aggExpression.setExpression(expression);
            if (hasSumCaseWhen(call, expression)) {
                aggExpression.setSumCase(true);
                List<RexNode> conditions = extractCaseWhenConditions(expression);
                aggExpression.setConditionsList(conditions);
                List<RexNode> valuesList = extractCaseThenElseValues(expression);
                aggExpression.setValuesList(valuesList);
                // Default values, not applicable for SumCaseWhenFunctionRule
                aggExpression.bottomProjInput = sourceInput;
                aggExpression.bottomAggInput = newArray(1);
                aggExpression.topProjInput = newArray(1);
                aggExpression.topAggInput = newArray(1);
            } else if (hasCountDistinctCaseWhen(call, expression)) {
                aggExpression.setCountDistinctCase(true);
                List<RexNode> conditions = extractCaseWhenConditions(expression);
                aggExpression.setConditionsList(conditions);
                List<RexNode> valuesList = extractCaseThenElseValues(expression);
                aggExpression.setValuesList(valuesList);
                // Default values, not applicable for SumCaseWhenFunctionRule
                aggExpression.bottomProjInput = sourceInput;
                aggExpression.bottomAggInput = newArray(1);
                aggExpression.topProjInput = newArray(1);
                aggExpression.topAggInput = newArray(1);
            } else if (isSum(call.getAggregation().getKind()) && sourceInput.length == 0) {
                aggExpression.setSumConst(true);
                aggExpression.bottomProjInput = newArray(0);
                aggExpression.bottomAggInput = newArray(1);
                aggExpression.topProjInput = newArray(1);
                aggExpression.topAggInput = newArray(1);
            } else {
                aggExpression.bottomProjInput = sourceInput;
                aggExpression.bottomAggInput = newArray(1);
                aggExpression.topProjInput = newArray(1);
                aggExpression.topAggInput = newArray(1);
            }
        }
        return aggExpressions;
    }

    private static List<RexNode> extractCaseWhenConditions(RexNode caseWhenExpr) {
        checkConditionExpr(caseWhenExpr);
        RexCall caseWhenCall = (RexCall) caseWhenExpr;

        List<RexNode> conditions = Lists.newArrayList();
        int operandsCnt = caseWhenCall.getOperands().size();
        assertCondition(operandsCnt > 2 && operandsCnt % 2 == 1, "case-when operands mismatch");
        for (int i = 0; i < operandsCnt - 1; i += 2) {
            conditions.add(caseWhenCall.getOperands().get(i));
        }

        return conditions;
    }

    private static List<RexNode> extractCaseThenElseValues(RexNode conditionExpr) {
        checkConditionExpr(conditionExpr);
        RexCall caseWhenCall = (RexCall) conditionExpr;

        List<RexNode> values = Lists.newArrayList();
        int operandsCnt = caseWhenCall.getOperands().size();
        assertCondition(operandsCnt > 2 && operandsCnt % 2 == 1, "case-when operands mismatch");
        for (int i = 1; i < operandsCnt - 1; i += 2) {
            RexNode thenRexNode = caseWhenCall.getOperands().get(i);
            values.add(thenRexNode);
        }

        RexNode elseRexNode = ((RexCall) conditionExpr).getOperands().get(operandsCnt - 1);
        values.add(elseRexNode);
        return values;
    }

    public static class CollectRexVisitor extends RexVisitorImpl<RexCall> {
        private final Set<RexInputRef> rexInputRefs = Sets.newHashSet();
        private final Set<RexLiteral> rexLiterals = Sets.newHashSet();

        CollectRexVisitor(boolean deep) {
            super(deep);
        }

        @Override
        public RexCall visitInputRef(RexInputRef inputRef) {
            rexInputRefs.add(inputRef);
            return null;
        }

        @Override
        public RexCall visitLiteral(RexLiteral literal) {
            rexLiterals.add(literal);
            return null;
        }

        boolean isRexLiteral() {
            return CollectionUtils.isEmpty(rexInputRefs) && CollectionUtils.isNotEmpty(rexLiterals);
        }
    }

    public static Pair<List<GroupExpression>, ImmutableList<ImmutableBitSet>> collectGroupExprAndGroup(Aggregate oldAgg,
            Project oldProject) {
        List<GroupExpression> groupExpressions = Lists.newArrayListWithCapacity(oldAgg.getGroupCount());
        Map<Integer, Integer> old2new = Maps.newHashMap();
        for (int groupBy : oldAgg.getGroupSet()) {
            RexNode projectExpr = oldProject.getProjects().get(groupBy);
            CollectRexVisitor visitor = new CollectRexVisitor(true);
            projectExpr.accept(visitor);
            GroupExpression groupExpr = new GroupExpression();
            if (visitor.isRexLiteral()) {
                groupExpr.setLiteral(true);
            }
            int[] sourceInput = RelOptUtil.InputFinder.bits(projectExpr).toArray();
            groupExpr.expression = projectExpr;
            groupExpr.bottomProjInput = sourceInput;
            groupExpr.bottomAggInput = newArray(sourceInput.length);
            groupExpr.topProjInput = newArray(sourceInput.length);
            groupExpr.topAggInput = newArray(1);

            old2new.put(groupBy, groupExpressions.size());
            groupExpressions.add(groupExpr);
        }

        return Pair.newPair(groupExpressions, collectGroupSets(oldAgg.getGroupSets(), old2new).orElse(null));
    }

    private static Optional<ImmutableList<ImmutableBitSet>> collectGroupSets(
            ImmutableList<ImmutableBitSet> oldGroupSets, Map<Integer, Integer> old2new) {

        if (oldGroupSets.size() <= 1) {
            return Optional.empty();
        }

        List<ImmutableBitSet> groupSets = Lists.newArrayListWithCapacity(oldGroupSets.size());
        for (ImmutableBitSet set : oldGroupSets) {
            ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
            for (int oldIndex : set) {
                builder.set(old2new.get(oldIndex));
            }
            groupSets.add(builder.build());
        }
        return Optional.of(ImmutableList.copyOf(groupSets));
    }

    public static void assertCondition(boolean condition, String errorMsg) {
        if (!condition)
            throw new SumExprUnSupportException(errorMsg);
    }

    private static void checkConditionExpr(RexNode conditionExpr) {
        assertCondition(conditionExpr instanceof RexCall, conditionExpr + " is not a case-when or if expression");
    }

    public static int[] generateAdjustments(int[] src, int[] dst) {
        AggExpressionUtil.assertCondition(src.length == dst.length, "Failed to generate adjustments");
        int maxRange = Arrays.stream(src).max().orElse(0);
        int[] adjustments = new int[maxRange + 1];
        for (int i = 0; i < src.length; i++) {
            int srcIndex = src[i];
            int dstIndex = dst[i];
            adjustments[srcIndex] = dstIndex - srcIndex;
        }
        return adjustments;
    }

    // create array filled with default value -1 (value 0 is legal index and hard to uncover bugs)
    private static int[] newArray(int size) {
        int[] arr = new int[size];
        Arrays.fill(arr, -1);
        return arr;
    }
}
