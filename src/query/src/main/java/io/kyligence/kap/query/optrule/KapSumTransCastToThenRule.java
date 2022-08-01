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

import java.util.List;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kylin.query.relnode.KapAggregateRel;
import org.apache.kylin.query.relnode.KapProjectRel;
import org.apache.kylin.query.util.AggExpressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class KapSumTransCastToThenRule extends RelOptRule {

    private static final Logger logger = LoggerFactory.getLogger(KapSumTransCastToThenRule.class);

    public static final KapSumTransCastToThenRule INSTANCE = new KapSumTransCastToThenRule(
            operand(KapAggregateRel.class,
                    operand(KapProjectRel.class, null, KapSumTransCastToThenRule::existCastCase, any())),
            RelFactories.LOGICAL_BUILDER, "KapSumTransCastToThenRule");

    public static boolean existCastCase(Project logicalProject) {
        List<RexNode> childExps = logicalProject.getChildExps();
        for (RexNode rexNode : childExps) {
            if (isCastCase(rexNode)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isCastCase(RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        if (SqlKind.CAST != rexNode.getKind()) {
            return false;
        }
        return SqlKind.CASE == ((RexCall) rexNode).operands.get(0).getKind();
    }

    public KapSumTransCastToThenRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
            String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return checkSumHasCaseCastInput(call);
    }

    private boolean checkSumHasCaseCastInput(RelOptRuleCall call) {
        Aggregate logicalAggregate = call.rel(0);
        Project logicalProject = call.rel(1);

        List<RexNode> projectExps = logicalProject.getChildExps();
        RexNode curProExp;
        List<Integer> castIndexs = Lists.newArrayList();
        for (int i = 0; i < projectExps.size(); i++) {
            curProExp = projectExps.get(i);
            if (isCastCase(curProExp)) {
                castIndexs.add(i);
            }
        }
        List<AggregateCall> aggCalls = logicalAggregate.getAggCallList();
        for (int i = 0; i < aggCalls.size(); i++) {
            if (checkAggNeedToRewrite(aggCalls.get(i), castIndexs)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        try {

            RelBuilder relBuilder = call.builder();
            Aggregate logicalAggregate = call.rel(0);
            Project logicalProject = call.rel(1);

            switch (getCastType(logicalProject)) {
            case HAS_COLUMN_NOT_NUMBER:
                return;
            case HAS_COLUMN_NUMBER:
            case OTHER:
                if (canCaseType(logicalProject)) {
                    innerMatchNumericColumn(call, relBuilder, logicalAggregate, logicalProject);
                }
                break;
            default:
                return;
            }
        } catch (Exception e) {
            logger.error("KapSumTransCastToThenRule apply failed", e);
        }
    }

    private List<RexNode> getOperandsFromCaseWhen(RexNode curProExp) {
        RexNode caseWhenRexNode = ((RexCall) curProExp).getOperands().get(0);
        return ((RexCall) caseWhenRexNode).getOperands();
    }

    private void innerMatchNumericColumn(RelOptRuleCall call, RelBuilder relBuilder, Aggregate logicalAggregate,
            Project logicalProject) {
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        relBuilder.push(logicalProject.getInput());

        List<RexNode> projectExps = logicalProject.getChildExps();
        List<RexNode> projectRexNodes = Lists.newArrayList();

        // step 1. build bottom project
        List<CastInfo> castInfos = Lists.newArrayList();
        for (int i = 0; i < projectExps.size(); i++) {
            RexNode curProExp = projectExps.get(i);
            RexNode newRexNode = curProExp;
            if (isCastCase(curProExp)) {
                List<RexNode> operands = getOperandsFromCaseWhen(curProExp);
                Set<RelDataType> allColumnType = getAllColumnType(operands);
                RelDataType columnType = allColumnType.size() == 1 ? allColumnType.iterator().next() : null;
                castInfos.add(new CastInfo(i, columnType, curProExp.getType(), columnType == null));
                List<RexNode> castedOperands = getCastedOperands(operands, getCurCastType(operands),
                        curProExp.getType(), rexBuilder);
                newRexNode = rexBuilder.makeCall(columnType == null ? curProExp.getType() : columnType,
                        SqlCaseOperator.INSTANCE, castedOperands);
            }
            projectRexNodes.add(newRexNode);
        }
        relBuilder.project(projectRexNodes);

        // step 2. build agg
        List<AggregateCall> aggCalls = logicalAggregate.getAggCallList();
        List<AggregateCall> newAggs = Lists.newArrayList();
        List<Integer> needCastSumIndex = Lists.newArrayList();
        for (int i = 0; i < aggCalls.size(); i++) {
            CastInfo curCastInfo;
            AggregateCall curAgg = aggCalls.get(i);
            if ((curCastInfo = getCastInfoForSum(curAgg, castInfos)) != null && !curCastInfo.isAllConstants()) {
                needCastSumIndex.add(i);
                newAggs.add(createAggCall(curAgg, curCastInfo));
            } else {
                newAggs.add(curAgg);
            }
        }
        RelBuilder.GroupKey groupKey = relBuilder.groupKey(logicalAggregate.getGroupSet(),
                logicalAggregate.getGroupSets());
        relBuilder.aggregate(groupKey, newAggs);

        // step 3. if needed, build top project
        if (!needCastSumIndex.isEmpty()) {
            relBuilder.project(newProjectRexNodes(logicalAggregate, relBuilder, castInfos));
        }
        call.transformTo(relBuilder.build());
    }

    private AggregateCall createAggCall(AggregateCall curAgg, CastInfo curCastInfo) {
        return AggregateCall.create(curAgg.getAggregation(), curAgg.isDistinct(), curAgg.isApproximate(),
                curAgg.getArgList(), curAgg.filterArg, curCastInfo.getColumnType(), curAgg.name);
    }

    private CastInfo getCastInfoForSum(AggregateCall call, List<CastInfo> castInfos) {
        if (!AggExpressionUtil.isSum(call.getAggregation().getKind())) {
            return null;
        }
        int input = call.getArgList().get(0);
        for (CastInfo castInfo : castInfos) {
            if (castInfo.getIndex() == input) {
                return castInfo;
            }
        }
        return null;
    }

    private boolean checkAggNeedToRewrite(AggregateCall call, List<Integer> castIndexs) {
        return AggExpressionUtil.isSum(call.getAggregation().getKind())
                && castIndexs.contains(call.getArgList().get(0));
    }

    private List<RexNode> newProjectRexNodes(Aggregate logicalAggregate, RelBuilder relBuilder,
            List<CastInfo> castInfos) {
        RexBuilder rexBuilder = relBuilder.getRexBuilder();

        List<RexNode> projectRexNodes = Lists.newArrayList();
        int offset = 0;
        List<Integer> groups = logicalAggregate.getGroupSet().asList();
        List<RelDataTypeField> fieldList = logicalAggregate.getRowType().getFieldList();
        for (int i = 0; i < groups.size(); i++) {
            int index = groups.get(i);
            projectRexNodes.add(rexBuilder.makeInputRef(fieldList.get(i).getType(), index));
            offset++;
        }

        List<AggregateCall> aggCalls = logicalAggregate.getAggCallList();
        RelNode peekedRelNodes = relBuilder.peek();
        CastInfo castInfo;
        for (int i = 0; i < aggCalls.size(); i++) {
            if ((castInfo = needCastForSum(i + offset, castInfos)) != null) {
                projectRexNodes.add(rexBuilder.makeCall(castInfo.getCastType(), new SqlCastFunction(),
                        Lists.newArrayList(rexBuilder.makeInputRef(peekedRelNodes, castInfo.getIndex()))));
            } else {
                projectRexNodes.add(rexBuilder.makeInputRef(peekedRelNodes, i + offset));
            }
        }
        return projectRexNodes;
    }

    private CastInfo needCastForSum(int i, List<CastInfo> castInfos) {
        for (CastInfo castInfo : castInfos) {
            if (castInfo.getIndex() == i && !castInfo.isAllConstants()) {
                return castInfo;
            }
        }
        return null;
    }

    private List<RexNode> getCastedOperands(List<RexNode> operands, InnerCastType curCastType, RelDataType castType,
            RexBuilder rexBuilder) {
        List<RexNode> castedOperands = Lists.newArrayList();
        for (int i = 0; i < operands.size() - 1; i += 2) {
            castedOperands.add(operands.get(i));
            castedOperands.add(transRexNode(operands.get(i + 1), curCastType, castType, rexBuilder));
        }
        if (operands.size() % 2 == 1) {
            castedOperands.add(transRexNode(operands.get(operands.size() - 1), curCastType, castType, rexBuilder));
        }
        return castedOperands;
    }

    private RexNode transRexNode(RexNode valueRexNode, InnerCastType curCastType, RelDataType castType,
            RexBuilder rexBuilder) {
        if (valueRexNode instanceof RexLiteral && curCastType == InnerCastType.OTHER) {
            return rexBuilder.makeCall(castType, new SqlCastFunction(), Lists.newArrayList(valueRexNode));
        }
        return valueRexNode;
    }

    private boolean canCaseType(Project logicalProject) {
        List<RexNode> childExps = logicalProject.getChildExps();
        Set<RelDataType> columnsDataType;
        RelDataType castReturnType;
        for (RexNode rexNode : childExps) {
            if (isCastCase(rexNode)) {
                castReturnType = rexNode.getType();
                RexNode caseWhenRexNode = ((RexCall) rexNode).getOperands().get(0);
                List<RexNode> operands = ((RexCall) caseWhenRexNode).getOperands();
                columnsDataType = getAllColumnType(operands);
                if (columnsDataType.isEmpty()) {
                    continue;
                }
                if (columnsDataType.size() != 1) {
                    return false;
                }
                if (!SqlTypeUtil.canCastFrom(columnsDataType.iterator().next(), castReturnType, true)) {
                    return false;
                }
            }
        }
        return true;
    }

    private Set<RelDataType> getAllColumnType(List<RexNode> operands) {
        RexNode valueRexNode;
        Set<RelDataType> columnsDataType = Sets.newHashSet();
        for (int i = 0; i < operands.size() - 1; i += 2) {
            valueRexNode = operands.get(i + 1);
            if (isNumericColumn(valueRexNode)) {
                columnsDataType.add(valueRexNode.getType());
            }
        }
        if (operands.size() % 2 == 1) {
            valueRexNode = operands.get(operands.size() - 1);
            if (isNumericColumn(valueRexNode)) {
                columnsDataType.add(valueRexNode.getType());
            }
        }
        return columnsDataType;
    }

    private boolean isNumericColumn(RexNode valueRexNode) {
        if (valueRexNode == null) {
            return false;
        }
        return valueRexNode instanceof RexInputRef && SqlTypeUtil.isNumeric(valueRexNode.getType());
    }

    private InnerCastType getCastType(Project logicalProject) {
        List<RexNode> childExps = logicalProject.getChildExps();
        InnerCastType castType = InnerCastType.OTHER;
        InnerCastType cur;
        for (RexNode rexNode : childExps) {
            if (!isCastCase(rexNode)) {
                continue;
            }
            cur = getCurCastType(getOperandsFromCaseWhen(rexNode));
            castType = castType.weight > cur.weight ? castType : cur;
        }
        return castType;
    }

    private InnerCastType getCurCastType(List<RexNode> operands) {
        InnerCastType castType = InnerCastType.OTHER;
        InnerCastType cur;
        for (int i = 0; i < operands.size() - 1; i += 2) {
            cur = getValueRexNodeType(operands.get(i + 1));
            castType = castType.weight > cur.weight ? castType : cur;
        }
        if (operands.size() % 2 == 1) {
            cur = getValueRexNodeType(operands.get(operands.size() - 1));
            castType = castType.weight > cur.weight ? castType : cur;
        }
        return castType;
    }

    private InnerCastType getValueRexNodeType(RexNode valueRexNode) {
        InnerCastType cur;
        if (valueRexNode instanceof RexInputRef) {
            cur = SqlTypeUtil.isNumeric(valueRexNode.getType()) ? InnerCastType.HAS_COLUMN_NUMBER
                    : InnerCastType.HAS_COLUMN_NOT_NUMBER;
        } else {
            cur = InnerCastType.OTHER;
        }
        return cur;
    }

    public enum InnerCastType {
        HAS_COLUMN_NOT_NUMBER(3), HAS_COLUMN_NUMBER(2), OTHER(1);

        private int weight;

        InnerCastType(int weight) {
            this.weight = weight;
        }
    }

    public static class CastInfo {
        private int index;
        private RelDataType columnType;
        private RelDataType castType;
        private boolean allConstants;

        public CastInfo(int index, RelDataType columnType, RelDataType castType, boolean allConstants) {
            this.index = index;
            this.columnType = columnType;
            this.castType = castType;
            this.allConstants = allConstants;
        }

        public int getIndex() {
            return index;
        }

        public RelDataType getColumnType() {
            return columnType;
        }

        public RelDataType getCastType() {
            return castType;
        }

        public boolean isAllConstants() {
            return allConstants;
        }
    }
}
