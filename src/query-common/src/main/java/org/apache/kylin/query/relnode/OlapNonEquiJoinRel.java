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

package org.apache.kylin.query.relnode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.apache.kylin.query.util.RexToTblColRefTranslator;
import org.apache.kylin.query.util.RexUtils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapNonEquiJoinRel extends EnumerableNestedLoopJoin implements OlapRel {

    private ColumnRowType columnRowType;
    private OlapContext context;
    @Setter
    private Set<OlapContext> subContexts = Sets.newHashSet();

    @Getter(AccessLevel.PRIVATE)
    private boolean isPreCalJoin = true;
    @Getter(AccessLevel.PRIVATE)
    private boolean aboveContextPreCalcJoin = false;

    // record left input size before rewrite for runtime join expression parsing
    private int leftInputSizeBeforeRewrite = -1;
    private final boolean isScd2Rel;

    public OlapNonEquiJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean isScd2Rel) throws InvalidRelException {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
        leftInputSizeBeforeRewrite = left.getRowType().getFieldList().size();
        rowType = getRowType();
        this.isScd2Rel = isScd2Rel;
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        ContextVisitorState leftState = ContextVisitorState.init();
        contextImpl.fixSharedOlapTableScanOnTheLeft(this);
        contextImpl.visitChild(getInput(0), this, leftState);

        ContextVisitorState rightState = ContextVisitorState.init();
        contextImpl.fixSharedOlapTableScanOnTheRight(this);
        contextImpl.visitChild(getInput(1), this, rightState);

        allocateContext(leftState, rightState, contextImpl);

        state.merge(leftState).merge(rightState);
        subContexts.addAll(ContextUtil.collectSubContext(this.left));
        subContexts.addAll(ContextUtil.collectSubContext(this.right));
    }

    private void allocateContext(ContextVisitorState leftState, ContextVisitorState rightState,
            ContextImpl contextImpl) {
        if (!isScd2Rel) {
            if (leftState.hasFreeTable()) {
                contextImpl.allocateContext((OlapRel) left, this);
                leftState.setHasFreeTable(false);
            }
            if (rightState.hasFreeTable()) {
                contextImpl.allocateContext((OlapRel) right, this);
                rightState.setHasFreeTable(false);
            }
            return;
        }

        if (rightState.hasFreeTable() && rightState.hasFilter()) {
            contextImpl.allocateContext((OlapRel) right, this);
            rightState.setHasFreeTable(false);
        }

        if (!leftState.hasFreeTable() && !rightState.hasFreeTable()) {
            // no free table, return directly
            return;
        }
        if (leftState.hasFreeTable() && !rightState.hasFreeTable()) {
            // left has free tbl, alloc ctx to left only
            contextImpl.allocateContext((OlapRel) left, this);
            leftState.setHasFreeTable(false);
        } else if (rightState.hasFreeTable() && !leftState.hasFreeTable()) {
            // right has free tbl, alloc ctx to right only
            contextImpl.allocateContext((OlapRel) right, this);
            rightState.setHasFreeTable(false);
        } else {
            // both has free tbl, leave ctx alloc for higher rel node
            // except the following situations
            if (rightState.hasIncrementalTable() || hasSameFirstTable(leftState, rightState)
                    || RexUtils.joinMoreThanOneTable(this)) {
                contextImpl.allocateContext((OlapRel) left, this);
                contextImpl.allocateContext((OlapRel) right, this);
                leftState.setHasFreeTable(false);
                rightState.setHasFreeTable(false);
            }
        }
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        if (isPreCalJoin) {
            this.context = null;
            this.columnRowType = null;
            contextCutImpl.allocateContext((OlapRel) getInput(0), this);
            contextCutImpl.allocateContext((OlapRel) getInput(1), this);
        } else {
            RelNode input = ((OlapRel) this.left).getContext() == null ? this.left : this.right;
            contextCutImpl.visitChild(input);
            this.context = null;
            this.columnRowType = null;
        }
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        if (context != null) {
            this.aboveContextPreCalcJoin = !this.isPreCalJoin || !this.context.isHasPreCalcJoin();
            this.context.setHasJoin(true);
            this.context.setHasPreCalcJoin(this.context.isHasPreCalcJoin() || this.isPreCalJoin);
        }

        olapImpl.visitChild(this.left, this);
        olapImpl.visitChild(this.right, this);

        columnRowType = buildColumnRowType();

        Set<TblColRef> joinCols = collectColumnsInJoinCondition(this.getCondition());
        if (context != null) {
            if (isPreCalJoin) {
                // for pre calc join
                buildAndUpdateContextJoin(condition);
            } else {
                for (TblColRef joinCol : joinCols) {
                    if (this.context.belongToContextTables(joinCol)) {
                        this.context.getSubqueryJoinParticipants().add(joinCol);
                        this.context.getAllColumns().add(joinCol);
                    }
                }
                pushDownJoinColsToSubContexts(joinCols);
            }
        } else {
            pushDownJoinColsToSubContexts(joinCols);
        }
    }

    private void buildAndUpdateContextJoin(RexNode condition) {
        condition = preTransferCastColumn(condition);
        JoinDesc.JoinDescBuilder joinDescBuilder = new JoinDesc.JoinDescBuilder();
        JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        Set<TblColRef> leftCols = new HashSet<>();
        joinInfo.leftKeys.forEach(key -> leftCols.addAll(getColFromLeft(key).getSourceColumns()));
        joinDescBuilder.addForeignKeys(leftCols);
        Set<TblColRef> rightCols = new HashSet<>();
        joinInfo.rightKeys.forEach(key -> rightCols.addAll(getColFromRight(key).getSourceColumns()));
        joinDescBuilder.addPrimaryKeys(rightCols);

        String joinType = this.getJoinType() == JoinRelType.INNER || this.getJoinType() == JoinRelType.LEFT
                ? this.getJoinType().name()
                : null;
        joinDescBuilder.setType(joinType);

        RexNode neqCond = RexUtil.composeConjunction(new RexBuilder(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT)),
                joinInfo.nonEquiConditions);

        //by default, extract foreign table form equi-join keys
        if (CollectionUtils.isNotEmpty(leftCols) && CollectionUtils.isNotEmpty(rightCols)) {
            joinDescBuilder.setForeignTableRef(leftCols.iterator().next().getTableRef());
            joinDescBuilder.setPrimaryTableRef(rightCols.iterator().next().getTableRef());
        } else {
            joinDescBuilder.setForeignTableRef(((OlapRel) left).getColumnRowType().getColumnByIndex(0).getTableRef());
            joinDescBuilder.setPrimaryTableRef(((OlapRel) right).getColumnRowType().getColumnByIndex(0).getTableRef());
        }

        NonEquiJoinCondition nonEquiJoinCondition = doBuildJoin(neqCond);
        nonEquiJoinCondition
                .setExpr(RexToTblColRefTranslator.translateRexNode(condition, columnRowType).getParserDescription());
        joinDescBuilder.setNonEquiJoinCondition(nonEquiJoinCondition);

        JoinDesc joinDesc = joinDescBuilder.build();

        context.getJoins().add(joinDesc);
    }

    //cast(col1 as ...) = col2 with col1 = col2
    private RexNode preTransferCastColumn(RexNode condition) {
        if (condition instanceof RexCall) {
            RexCall conditionCall = ((RexCall) condition);
            List<RexNode> rexNodes = conditionCall.getOperands().stream()
                    .map(RexUtils::stripOffCastInColumnEqualPredicate).collect(Collectors.toList());

            return conditionCall.clone(conditionCall.getType(), rexNodes);
        }
        return condition;

    }

    private NonEquiJoinCondition doBuildJoin(RexNode condition) {
        if (condition instanceof RexCall) {
            List<NonEquiJoinCondition> nonEquiJoinConditions = new LinkedList<>();
            for (RexNode operand : ((RexCall) condition).getOperands()) {
                nonEquiJoinConditions.add(doBuildJoin(operand));
            }
            return new NonEquiJoinCondition(((RexCall) condition).getOperator(),
                    nonEquiJoinConditions.toArray(new NonEquiJoinCondition[0]), condition.getType());
        } else if (condition instanceof RexInputRef) {
            final int colIdx = ((RexInputRef) condition).getIndex();
            Set<TblColRef> sourceCols = getColByIndex(colIdx).getSourceColumns();
            Preconditions.checkArgument(sourceCols.size() == 1);
            TblColRef sourceCol = sourceCols.iterator().next();
            return new NonEquiJoinCondition(sourceCol, condition.getType());
        } else if (condition instanceof RexLiteral) {
            return new NonEquiJoinCondition(((RexLiteral) condition), condition.getType());
        }
        throw new IllegalStateException("Invalid join condition " + condition);
    }

    private TblColRef getColByIndex(int idx) {
        final int leftColumnsSize = ((OlapRel) this.left).getColumnRowType().getAllColumns().size();
        if (idx < leftColumnsSize) {
            return getColFromLeft(idx);
        } else {
            return getColFromRight(idx - leftColumnsSize);
        }
    }

    private TblColRef getColFromLeft(int idx) {
        return ((OlapRel) this.left).getColumnRowType().getAllColumns().get(idx);
    }

    private TblColRef getColFromRight(int idx) {
        return ((OlapRel) this.right).getColumnRowType().getAllColumns().get(idx);
    }

    private void pushDownJoinColsToSubContexts(Set<TblColRef> joinColumns) {
        for (OlapContext subContext : subContexts) {
            for (TblColRef joinCol : joinColumns) {
                if (subContext.belongToContextTables(joinCol)) {
                    subContext.getAllColumns().add(joinCol);
                }
            }
        }
    }

    private Set<TblColRef> collectColumnsInJoinCondition(RexNode condition) {
        return RexUtils.getAllInputRefs(condition).stream()
                .map(inRef -> columnRowType.getColumnByIndex(inRef.getIndex()))
                .flatMap(col -> col.getSourceColumns().stream()).collect(Collectors.toSet());
    }

    private ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = new ArrayList<>();

        OlapRel leftRel = (OlapRel) this.left;
        OlapRel rightRel = (OlapRel) this.right;

        ColumnRowType leftColumnRowType = leftRel.getColumnRowType();
        ColumnRowType rightColumnRowType = rightRel.getColumnRowType();
        columns.addAll(leftColumnRowType.getAllColumns());
        columns.addAll(rightColumnRowType.getAllColumns());

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        rewriteImpl.visitChild(this, left);
        rewriteImpl.visitChild(this, right);

        if (context != null) {
            this.rowType = this.deriveRowType();
            // for runtime join, add rewrite fields anyway
            if (this.context.hasPrecalculatedFields() && RewriteImpl.needRewrite(this.context)
                    && aboveContextPreCalcJoin) {
                int paramIndex = this.rowType.getFieldList().size();
                List<RelDataTypeField> newFieldList = Lists.newLinkedList();
                for (Map.Entry<String, RelDataType> rewriteField : this.context.getRewriteFields().entrySet()) {
                    String fieldName = rewriteField.getKey();
                    if (this.rowType.getField(fieldName, true, false) == null) {
                        RelDataType fieldType = rewriteField.getValue();
                        RelDataTypeField newField = new RelDataTypeFieldImpl(fieldName, paramIndex++, fieldType);
                        newFieldList.add(newField);
                    }
                }

                // rebuild row type
                List<RelDataTypeField> fieldList = Stream.of(rowType.getFieldList(), newFieldList) //
                        .flatMap(List::stream).collect(Collectors.toList());
                this.rowType = getCluster().getTypeFactory().createStructType(fieldList);
                // rebuild columns
                this.columnRowType = this.rebuildColumnRowType(newFieldList);
            }
        }
    }

    private ColumnRowType rebuildColumnRowType(List<RelDataTypeField> missingFields) {
        List<TblColRef> columns = Lists.newArrayList();
        OlapRel olapLeft = (OlapRel) this.left;
        OlapRel olapRight = (OlapRel) this.right;
        columns.addAll(olapLeft.getColumnRowType().getAllColumns());
        columns.addAll(olapRight.getColumnRowType().getAllColumns());

        for (RelDataTypeField dataTypeField : missingFields) {
            String fieldName = dataTypeField.getName();
            TblColRef aggOutCol = TblColRef.newInnerColumn(fieldName, TblColRef.InnerDataTypeEnum.LITERAL);
            aggOutCol.getColumnDesc().setId(String.valueOf(dataTypeField.getIndex()));
            columns.add(aggOutCol);
        }

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        return super.copy(traitSet, condition, inputs.get(0), inputs.get(1), joinType, isSemiJoinDone());
    }

    @Override
    public boolean pushRelInfoToContext(OlapContext context) {
        if (this.context != null)
            return false;
        // if non-equiv-join is the direct parent of the context,
        // there is no need to push context further down
        // otherwise try push context down to both side
        if (this == context.getParentOfTopNode() || ((OlapRel) getLeft()).pushRelInfoToContext(context)
                || ((OlapRel) getRight()).pushRelInfoToContext(context)) {
            this.context = context;
            isPreCalJoin = false;
            return true;
        }
        return false;
    }

    @Override
    public void setContext(OlapContext context) {
        this.context = context;
        for (RelNode input : getInputs()) {
            ((OlapRel) input).setContext(context);
            subContexts.addAll(ContextUtil.collectSubContext(input));
        }
    }

    @Override
    public boolean hasSubQuery() {
        throw new UnsupportedOperationException("hasSubQuery is not implemented yet");
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return joinType == JoinRelType.RIGHT //
                ? super.computeSelfCost(planner, mq).multiplyBy(OlapJoinRel.LARGE_JOIN_FACTOR)
                : super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR);
    }

    @Override
    public EnumerableNestedLoopJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right,
            JoinRelType joinType, boolean semiJoinDone) {
        try {
            return new OlapNonEquiJoinRel(getCluster(), traitSet, left, right, condition, variablesSet, joinType,
                    isScd2Rel);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return super.estimateRowCount(mq) * 0.1;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx", displayCtxId(context));
    }

    public boolean isRuntimeJoin() {
        if (context != null) {
            context.setReturnTupleInfo(rowType, columnRowType);
        }
        return this.context == null || ((OlapRel) left).getContext() != ((OlapRel) right).getContext();
    }

    private boolean hasSameFirstTable(ContextVisitorState leftState, ContextVisitorState rightState) {
        // both sides have the same first table, each side should allocate a context
        return !leftState.hasIncrementalTable() && !rightState.hasIncrementalTable() && leftState.hasFirstTable()
                && rightState.hasFirstTable();
    }

    public int getLeftInputSizeBeforeRewrite() {
        return leftInputSizeBeforeRewrite;
    }
}
