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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.adapter.enumerable.EnumerableJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.apache.kylin.query.util.RexUtils;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;

public class KapJoinRel extends OLAPJoinRel implements KapRel {
    private Set<OLAPContext> subContexts = Sets.newHashSet();
    private boolean isPreCalJoin = true;
    private boolean aboveTopPreCalcJoin = false;
    private boolean joinCondEqualNullSafe = false;

    public KapJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
            ImmutableIntList leftKeys, ImmutableIntList rightKeys, Set<CorrelationId> variablesSet,
            JoinRelType joinType) throws InvalidRelException {
        super(cluster, traits, left, right, condition, leftKeys, rightKeys, variablesSet, joinType);
    }

    @Override
    public EnumerableJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, //
            JoinRelType joinType, boolean semiJoinDone) {

        final JoinInfo joinInfo = JoinInfo.of(left, right, conditionExpr);
        assert joinInfo.isEqui();
        try {
            return new KapJoinRel(getCluster(), traitSet, left, right, conditionExpr, joinInfo.leftKeys,
                    joinInfo.rightKeys, variablesSet, joinType);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError(e);
        }
    }

    public boolean isRuntimeJoin() {
        if (context != null) {
            context.setReturnTupleInfo(rowType, columnRowType);
        }
        return this.context == null || ((KapRel) left).getContext() != ((KapRel) right).getContext();
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        ContextVisitorState leftState = ContextVisitorState.init();
        olapContextImplementor.fixSharedOlapTableScanOnTheLeft(this);
        olapContextImplementor.visitChild(getInput(0), this, leftState);

        ContextVisitorState rightState = ContextVisitorState.init();
        olapContextImplementor.fixSharedOlapTableScanOnTheRight(this);
        olapContextImplementor.visitChild(getInput(1), this, rightState);

        if (leftState.hasModelView() || rightState.hasModelView()) {
            if (leftState.hasFreeTable()) {
                olapContextImplementor.allocateContext((KapRel) getInput(0), this);
                leftState.setHasFreeTable(false);
            }

            if (rightState.hasFreeTable()) {
                olapContextImplementor.allocateContext((KapRel) getInput(1), this);
                rightState.setHasFreeTable(false);
            }
        }

        // special case for left join
        if (getJoinType() == JoinRelType.LEFT && rightState.hasFilter() && rightState.hasFreeTable()) {
            olapContextImplementor.allocateContext((KapRel) getInput(1), this);
            rightState.setHasFreeTable(false);
        }

        if (getJoinType() == JoinRelType.INNER || getJoinType() == JoinRelType.LEFT) {

            // if one side of join has no free table, the other side should have separate context
            if (!leftState.hasFreeTable() && rightState.hasFreeTable()) {
                olapContextImplementor.allocateContext((KapRel) right, this);
                rightState.setHasFreeTable(false);
            } else if (leftState.hasFreeTable() && !rightState.hasFreeTable()) {
                olapContextImplementor.allocateContext((KapRel) left, this);
                leftState.setHasFreeTable(false);
            } else if (leftState.hasFreeTable() && rightState.hasFreeTable()
                    && (isCrossJoin() || hasSameFirstTable(leftState, rightState)
                            || isRightSideIncrementalTable(rightState) || RexUtils.joinMoreThanOneTable(this)
                            || !RexUtils.isMerelyTableColumnReference(this, condition) || joinCondEqualNullSafe)) {
                olapContextImplementor.allocateContext((KapRel) left, this);
                olapContextImplementor.allocateContext((KapRel) right, this);
                leftState.setHasFreeTable(false);
                rightState.setHasFreeTable(false);
            }

            state.merge(leftState).merge(rightState);
            subContexts.addAll(ContextUtil.collectSubContext(this.left));
            subContexts.addAll(ContextUtil.collectSubContext(this.right));
            return;
        }

        // other join types (RIGHT or FULL), two sides two contexts
        if (leftState.hasFreeTable()) {
            olapContextImplementor.allocateContext((KapRel) left, this);
            leftState.setHasFreeTable(false);
        }

        if (rightState.hasFreeTable()) {
            olapContextImplementor.allocateContext((KapRel) right, this);
            rightState.setHasFreeTable(false);
        }

        state.merge(leftState).merge(rightState);
        subContexts.addAll(ContextUtil.collectSubContext(this.left));
        subContexts.addAll(ContextUtil.collectSubContext(this.right));
    }

    private boolean isRightSideIncrementalTable(ContextVisitorState rightState) {
        // if right side is incremental table, each side should allocate a context
        return rightState.hasIncrementalTable();
    }

    private boolean hasSameFirstTable(ContextVisitorState leftState, ContextVisitorState rightState) {
        // both sides have the same first table, each side should allocate a context
        return !leftState.hasIncrementalTable() && !rightState.hasIncrementalTable() && leftState.hasFirstTable()
                && rightState.hasFirstTable();
    }

    private boolean isCrossJoin() {
        // each side of cross join should allocate a context
        return leftKeys.isEmpty() || rightKeys.isEmpty();
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        if (!this.isPreCalJoin) {
            RelNode input = this.context == ((KapRel) this.left).getContext() ? this.left : this.right;
            implementor.visitChild(input);
            this.context = null;
            this.columnRowType = null;
        } else {
            this.context = null;
            this.columnRowType = null;
            implementor.allocateContext((KapRel) getInput(0), this);
            implementor.allocateContext((KapRel) getInput(1), this);
        }

    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        for (RelNode input : getInputs()) {
            ((KapRel) input).setContext(context);
            subContexts.addAll(ContextUtil.collectSubContext(input));
        }
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        if (this.context != null)
            return false;
        if (this == context.getParentOfTopNode() || ((KapRel) getLeft()).pushRelInfoToContext(context)
                || ((KapRel) getRight()).pushRelInfoToContext(context)) {
            this.context = context;
            this.isPreCalJoin = false;
            return true;
        }
        return false;
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        if (context != null) {
            this.context.allOlapJoins.add(this);
            this.aboveTopPreCalcJoin = !this.isPreCalJoin || !this.context.isHasPreCalcJoin();

            this.context.setHasJoin(true);
            this.context.setHasPreCalcJoin(this.context.isHasPreCalcJoin() || this.isPreCalJoin);
        }

        // as we keep the first table as fact table, we need to visit from left to right
        olapContextImplementor.visitChild(this.left, this);
        olapContextImplementor.visitChild(this.right, this);
        //parent context
        Preconditions.checkState(!this.hasSubQuery, "there should be no subquery in context");

        this.columnRowType = buildColumnRowType();
        if (context != null) {
            collectCtxOlapInfoIfExist();
        } else {
            Map<TblColRef, TblColRef> joinColumns = translateJoinColumn(this.getCondition());
            pushDownJoinColsToSubContexts(joinColumns.entrySet().stream()
                    .flatMap(e -> Stream.of(e.getKey(), e.getValue())).collect(Collectors.toSet()));
        }
    }

    private void collectCtxOlapInfoIfExist() {
        if (isPreCalJoin || this.context.getParentOfTopNode() instanceof OLAPRel
                && ((OLAPRel) this.context.getParentOfTopNode()).getContext() != this.context) {
            // build JoinDesc for pre-calculate join
            JoinDesc join = buildJoin((RexCall) this.getCondition());
            String joinType = this.getJoinType() == JoinRelType.INNER || this.getJoinType() == JoinRelType.LEFT
                    ? this.getJoinType().name()
                    : null;

            join.setType(joinType);
            this.context.joins.add(join);

        } else {
            Map<TblColRef, TblColRef> joinColumnsMap = translateJoinColumn(this.getCondition());
            Collection<TblColRef> joinCols = joinColumnsMap.entrySet().stream()
                    .flatMap(e -> Stream.of(e.getKey(), e.getValue())).collect(Collectors.toSet());
            joinCols.stream().flatMap(e -> e.getSourceColumns().stream()).filter(context::belongToContextTables)
                    .forEach(colRef -> {
                        context.getSubqueryJoinParticipants().add(colRef);
                        context.allColumns.add(colRef);
                    });
            pushDownJoinColsToSubContexts(joinCols);
        }
        if (this == context.getTopNode() && !context.isHasAgg())
            KapContext.amendAllColsIfNoAgg(this);
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, this.left);
        implementor.visitChild(this, this.right);

        if (context != null) {
            this.rowType = deriveRowType();

            if (this.context.hasPrecalculatedFields() && this.aboveTopPreCalcJoin
                    && RewriteImplementor.needRewrite(this.context)) {
                // find missed rewrite fields
                int paramIndex = this.rowType.getFieldList().size();
                List<RelDataTypeField> newFieldList = Lists.newLinkedList();
                for (Map.Entry<String, RelDataType> rewriteField : this.context.rewriteFields.entrySet()) {
                    String fieldName = rewriteField.getKey();
                    if (this.rowType.getField(fieldName, true, false) == null) {
                        RelDataType fieldType = rewriteField.getValue();
                        RelDataTypeField newField = new RelDataTypeFieldImpl(fieldName, paramIndex++, fieldType);
                        newFieldList.add(newField);
                    }
                }

                // rebuild row type
                RelDataTypeFactory.FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
                fieldInfo.addAll(this.rowType.getFieldList());
                fieldInfo.addAll(newFieldList);
                this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);
                // rebuild columns
                this.columnRowType = this.rebuildColumnRowType(newFieldList, context);
            }

        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        if (isRuntimeJoin()) {
            try {
                return EnumerableJoin.create(inputs.get(0), inputs.get(1), condition, leftKeys, rightKeys, variablesSet,
                        joinType);
            } catch (Exception e) {
                throw new IllegalStateException("Can't create EnumerableJoin!", e);
            }
        } else {
            return this;
        }
    }

    private void pushDownJoinColsToSubContexts(Collection<TblColRef> joinColumns) {
        for (OLAPContext context : subContexts) {
            collectJoinColsToContext(joinColumns, context);
        }
    }

    private void collectJoinColsToContext(Collection<TblColRef> joinColumns, OLAPContext context) {
        val sourceJoinKeyCols = joinColumns.stream().flatMap(col -> col.getSourceColumns().stream())
                .filter(context::belongToContextTables).collect(Collectors.toSet());
        context.allColumns.addAll(sourceJoinKeyCols);

        if (context.getOuterJoinParticipants().isEmpty() && isDirectOuterJoin(this, context)) {
            context.getOuterJoinParticipants().addAll(sourceJoinKeyCols);
        }
    }

    /**
     * only jon, filter, project is allowed
     * @param currentNode
     * @param context
     * @return
     */
    private boolean isDirectOuterJoin(RelNode currentNode, OLAPContext context) {
        if (currentNode == this && currentNode instanceof Join) {
            for (RelNode input : currentNode.getInputs()) {
                if (isDirectOuterJoin(input, context)) {
                    return true;
                }
            }
            return false;
        } else if (((KapRel) currentNode).getContext() == context) {
            return true;
        } else if (currentNode instanceof Project || currentNode instanceof Filter) {
            return isDirectOuterJoin(currentNode.getInput(0), context);
        } else {
            return false;
        }
    }

    @Override
    public Set<OLAPContext> getSubContext() {
        return subContexts;
    }

    @Override
    public void setSubContexts(Set<OLAPContext> contexts) {
        this.subContexts = contexts;
    }

    private ColumnRowType rebuildColumnRowType(List<RelDataTypeField> missingFields, OLAPContext context) {
        List<TblColRef> columns = Lists.newArrayList();
        OLAPRel olapLeft = (OLAPRel) this.left;
        OLAPRel olapRight = (OLAPRel) this.right;
        columns.addAll(olapLeft.getColumnRowType().getAllColumns());
        columns.addAll(olapRight.getColumnRowType().getAllColumns());

        for (RelDataTypeField dataTypeField : missingFields) {
            String fieldName = dataTypeField.getName();
            TblColRef aggOutCol = null;
            for (OLAPTableScan tableScan : context.allTableScans) {
                aggOutCol = tableScan.getColumnRowType().getColumnByName(fieldName);
                if (aggOutCol != null) {
                    break;
                }
            }
            if (aggOutCol == null) {
                aggOutCol = TblColRef.newInnerColumn(fieldName, TblColRef.InnerDataTypeEnum.LITERAL);
            }
            aggOutCol.getColumnDesc().setId("" + dataTypeField.getIndex());
            columns.add(aggOutCol);
        }

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }

    public boolean isJoinCondEqualNullSafe() {
        return joinCondEqualNullSafe;
    }

    public void setJoinCondEqualNullSafe(boolean joinCondEqualNullSafe) {
        this.joinCondEqualNullSafe = joinCondEqualNullSafe;
    }
}
