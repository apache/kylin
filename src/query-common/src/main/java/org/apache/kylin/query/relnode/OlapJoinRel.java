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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.schema.OlapTable;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.apache.kylin.query.util.RexUtils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Getter
public class OlapJoinRel extends EnumerableHashJoin implements OlapRel {

    static final double LARGE_JOIN_FACTOR = 100.0;
    static final String[] COLUMN_ARRAY_MARKER = new String[0];

    private OlapContext context;
    private Set<OlapContext> subContexts = Sets.newHashSet();
    private ColumnRowType columnRowType;
    /** The pivot used to distinguish columns from left or right relNode. */
    @Getter(AccessLevel.PRIVATE)
    private int columnRowTypePivot;
    @Getter(AccessLevel.PRIVATE)
    private boolean isPreCalJoin = true;
    @Getter(AccessLevel.PRIVATE)
    private boolean aboveTopPreCalcJoin = false;
    @Setter
    private boolean joinCondEqualNullSafe = false;

    public OlapJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType) throws InvalidRelException {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        this.rowType = getRowType();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // assign a huge cost on right join and cross join so that the swapped
        // left join and inner join will win in the optimization
        return joinType == JoinRelType.RIGHT || condition.isAlwaysTrue()
                ? super.computeSelfCost(planner, mq).multiplyBy(OlapJoinRel.LARGE_JOIN_FACTOR)
                : super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return super.estimateRowCount(mq) * 0.1;
    }

    //when OLAPJoinPushThroughJoinRule is applied, a "MerelyPermutation" project rel will be created
    protected boolean isParentMerelyPermutation(OlapImpl olapImpl) {
        if (olapImpl.getParentNode() instanceof OlapProjectRel) {
            return ((OlapProjectRel) olapImpl.getParentNode()).isMerelyPermutation();
        }
        return false;
    }

    protected ColumnRowType buildColumnRowType() {
        List<Set<TblColRef>> sourceColumns = new ArrayList<>();
        OlapRel leftRel = (OlapRel) this.left;
        OlapRel rightRel = (OlapRel) this.right;

        ColumnRowType leftColumnRowType = leftRel.getColumnRowType();
        ColumnRowType rightColumnRowType = rightRel.getColumnRowType();

        this.columnRowTypePivot = leftColumnRowType.getAllColumns().size();
        List<TblColRef> columns = new ArrayList<>(leftColumnRowType.getAllColumns());
        columns.addAll(rightColumnRowType.getAllColumns());
        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        columns.forEach(col -> sourceColumns.add(col.getSourceColumns()));
        return new ColumnRowType(columns, sourceColumns);
    }

    protected JoinDesc buildJoin(RexCall condition) {
        Map<TblColRef, Set<TblColRef>> joinColumns = translateJoinColumn(condition);

        List<String> pks = new ArrayList<>();
        List<String> fks = new ArrayList<>();
        List<TblColRef> pkCols = new ArrayList<>();
        List<TblColRef> fkCols = new ArrayList<>();
        for (Map.Entry<TblColRef, Set<TblColRef>> columnPair : joinColumns.entrySet()) {
            TblColRef fromCol = columnPair.getKey();
            Set<TblColRef> toCols = columnPair.getValue();
            for (TblColRef toCol : toCols) {
                fks.add(fromCol.getName());
                pks.add(toCol.getName());
                fkCols.add(fromCol);
                pkCols.add(toCol);
            }
        }

        JoinDesc join = new JoinDesc();
        join.setForeignKey(fks.toArray(COLUMN_ARRAY_MARKER));
        join.setPrimaryKey(pks.toArray(COLUMN_ARRAY_MARKER));
        join.setForeignKeyColumns(fkCols.toArray(new TblColRef[0]));
        join.setPrimaryKeyColumns(pkCols.toArray(new TblColRef[0]));
        join.sortByFK();
        return join;
    }

    protected Map<TblColRef, Set<TblColRef>> translateJoinColumn(RexNode condition) {
        Map<TblColRef, Set<TblColRef>> joinColumns = new HashMap<>();
        if (condition instanceof RexCall) {
            translateJoinColumn((RexCall) condition, joinColumns);
        }
        return joinColumns;
    }

    void translateJoinColumn(RexCall condition, Map<TblColRef, Set<TblColRef>> joinColumns) {
        SqlKind kind = condition.getOperator().getKind();
        if (kind == SqlKind.AND) {
            for (RexNode operand : condition.getOperands()) {
                RexCall subCond = (RexCall) operand;
                translateJoinColumn(subCond, joinColumns);
            }
        } else if (kind == SqlKind.EQUALS) {
            List<RexNode> operands = condition.getOperands();
            RexInputRef op0 = (RexInputRef) operands.get(0);
            TblColRef col0 = columnRowType.getColumnByIndex(op0.getIndex());
            RexInputRef op1 = (RexInputRef) operands.get(1);
            TblColRef col1 = columnRowType.getColumnByIndex(op1.getIndex());
            // map left => right
            if (op0.getIndex() < columnRowTypePivot) {
                joinColumns.computeIfAbsent(col0, key -> new HashSet<>()).add(col1);
            } else {
                joinColumns.computeIfAbsent(col1, key -> new HashSet<>()).add(col0);
            }
        }
    }

    /**
     * Implement of CalcitePlanExec, corresponding to SparderPlanExec, pay less attention
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        String execFunc = context.genExecFunc(this);
        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
        RelOptTable factTable = context.getFirstTableScan().getTable();
        MethodCallExpression exprCall = Expressions.call(
                Objects.requireNonNull(factTable.getExpression(OlapTable.class)), execFunc,
                implementor.getRootExpression(), Expressions.constant(context.getId()));
        return implementor.result(physType, Blocks.toBlock(exprCall));
    }

    @Override
    public boolean hasSubQuery() {
        // there should be no subQuery in context
        return false;
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx", displayCtxId(context));
    }

    @Override
    public EnumerableHashJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right,
            JoinRelType joinType, boolean semiJoinDone) {

        // Calcite no longer distinguishes between equal and non-equal joins,
        // so the assertion here is no longer needed. For example: the join condition
        // `=(CAST($3):BIGINT, $8)` is indeed an equal join condition in logical.
        try {
            return new OlapJoinRel(getCluster(), traitSet, left, right, conditionExpr, variablesSet, joinType);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError(e);
        }
    }

    public boolean isRuntimeJoin() {
        if (context != null) {
            context.setReturnTupleInfo(rowType, columnRowType);
        }
        return this.context == null || ((OlapRel) left).getContext() != ((OlapRel) right).getContext();
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        ContextVisitorState leftState = ContextVisitorState.init();
        contextImpl.fixSharedOlapTableScanOnTheLeft(this);
        contextImpl.visitChild(getInput(0), this, leftState);

        ContextVisitorState rightState = ContextVisitorState.init();
        contextImpl.fixSharedOlapTableScanOnTheRight(this);
        contextImpl.visitChild(getInput(1), this, rightState);

        if (leftState.hasModelView() || rightState.hasModelView()) {
            if (leftState.hasFreeTable()) {
                contextImpl.allocateContext((OlapRel) getInput(0), this);
                leftState.setHasFreeTable(false);
            }

            if (rightState.hasFreeTable()) {
                contextImpl.allocateContext((OlapRel) getInput(1), this);
                rightState.setHasFreeTable(false);
            }
        }

        // special case for left join
        if (getJoinType() == JoinRelType.LEFT && rightState.hasFilter() && rightState.hasFreeTable()) {
            contextImpl.allocateContext((OlapRel) getInput(1), this);
            rightState.setHasFreeTable(false);
        }

        if (getJoinType() == JoinRelType.INNER || getJoinType() == JoinRelType.LEFT) {

            // if one side of join has no free table, the other side should have separate context
            if (!leftState.hasFreeTable() && rightState.hasFreeTable()) {
                contextImpl.allocateContext((OlapRel) right, this);
                rightState.setHasFreeTable(false);
            } else if (leftState.hasFreeTable() && !rightState.hasFreeTable()) {
                contextImpl.allocateContext((OlapRel) left, this);
                leftState.setHasFreeTable(false);
            } else if (leftState.hasFreeTable() && rightState.hasFreeTable()
                    && (isCrossJoin() || hasSameFirstTable(leftState, rightState)
                            || isRightSideIncrementalTable(rightState) || RexUtils.joinMoreThanOneTable(this)
                            || !RexUtils.isMerelyTableColumnReference(this, condition) || joinCondEqualNullSafe)) {
                contextImpl.allocateContext((OlapRel) left, this);
                contextImpl.allocateContext((OlapRel) right, this);
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
            contextImpl.allocateContext((OlapRel) left, this);
            leftState.setHasFreeTable(false);
        }

        if (rightState.hasFreeTable()) {
            contextImpl.allocateContext((OlapRel) right, this);
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
        return joinInfo.leftKeys.isEmpty() || joinInfo.rightKeys.isEmpty();
    }

    public ImmutableIntList getLeftKeys() {
        return joinInfo.leftKeys;
    }

    public ImmutableIntList getRightKeys() {
        return joinInfo.rightKeys;
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        if (!this.isPreCalJoin) {
            RelNode input = this.context == ((OlapRel) this.left).getContext() ? this.left : this.right;
            contextCutImpl.visitChild(input);
            this.context = null;
            this.columnRowType = null;
        } else {
            this.context = null;
            this.columnRowType = null;
            contextCutImpl.allocateContext((OlapRel) getInput(0), this);
            contextCutImpl.allocateContext((OlapRel) getInput(1), this);
        }

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
    public boolean pushRelInfoToContext(OlapContext context) {
        if (this.context != null)
            return false;
        if (this == context.getParentOfTopNode() || ((OlapRel) getLeft()).pushRelInfoToContext(context)
                || ((OlapRel) getRight()).pushRelInfoToContext(context)) {
            this.context = context;
            this.isPreCalJoin = false;
            return true;
        }
        return false;
    }

    @Override
    public void implementOlap(OlapImpl implementor) {
        if (context != null) {
            this.context.getAllOlapJoins().add(this);
            this.aboveTopPreCalcJoin = !this.isPreCalJoin || !this.context.isHasPreCalcJoin();

            this.context.setHasJoin(true);
            this.context.setHasPreCalcJoin(this.context.isHasPreCalcJoin() || this.isPreCalJoin);
        }

        // as we keep the first table as fact table, we need to visit from left to right
        implementor.visitChild(this.left, this);
        implementor.visitChild(this.right, this);

        this.columnRowType = buildColumnRowType();
        if (context != null) {
            collectCtxOlapInfoIfExist();
        } else {
            Map<TblColRef, Set<TblColRef>> joinColumns = translateJoinColumn(this.getCondition());
            pushDownJoinColsToSubContexts(joinColumns.entrySet().stream()
                    .flatMap(e -> Stream.concat(Stream.of(e.getKey()), e.getValue().stream()))
                    .collect(Collectors.toSet()));
        }
    }

    private void collectCtxOlapInfoIfExist() {
        if (isPreCalJoin || this.context.getParentOfTopNode() instanceof OlapRel
                && ((OlapRel) this.context.getParentOfTopNode()).getContext() != this.context) {
            // build JoinDesc for pre-calculate join
            JoinDesc join = buildJoin((RexCall) this.getCondition());
            String joinType = this.getJoinType() == JoinRelType.INNER || this.getJoinType() == JoinRelType.LEFT
                    ? this.getJoinType().name()
                    : null;

            join.setType(joinType);
            this.context.getJoins().add(join);

        } else {
            Map<TblColRef, Set<TblColRef>> joinColumnsMap = translateJoinColumn(this.getCondition());
            Collection<TblColRef> joinCols = joinColumnsMap.entrySet().stream()
                    .flatMap(e -> Stream.concat(Stream.of(e.getKey()), e.getValue().stream()))
                    .collect(Collectors.toSet());
            joinCols.stream().flatMap(e -> e.getSourceColumns().stream()).filter(context::belongToContextTables)
                    .forEach(colRef -> {
                        context.getSubqueryJoinParticipants().add(colRef);
                        context.getAllColumns().add(colRef);
                    });
            pushDownJoinColsToSubContexts(joinCols);
        }
        if (this == context.getTopNode() && !context.isHasAgg()) {
            ContextUtil.amendAllColsIfNoAgg(this);
        }
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        rewriteImpl.visitChild(this, this.left);
        rewriteImpl.visitChild(this, this.right);

        if (context != null) {
            this.rowType = deriveRowType();

            if (this.context.hasPrecalculatedFields() && this.aboveTopPreCalcJoin
                    && RewriteImpl.needRewrite(this.context)) {
                // find missed rewrite fields
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
                this.columnRowType = this.rebuildColumnRowType(newFieldList, context);
            }
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        if (isRuntimeJoin()) {
            try {
                return EnumerableHashJoin.create(inputs.get(0), inputs.get(1), condition, variablesSet, joinType);
            } catch (Exception e) {
                throw new IllegalStateException("Can't create EnumerableHashJoin!", e);
            }
        } else {
            return this;
        }
    }

    private void pushDownJoinColsToSubContexts(Collection<TblColRef> joinColumns) {
        for (OlapContext subContext : subContexts) {
            collectJoinColsToContext(joinColumns, subContext);
        }
    }

    private void collectJoinColsToContext(Collection<TblColRef> joinColumns, OlapContext context) {
        val sourceJoinKeyCols = joinColumns.stream().flatMap(col -> col.getSourceColumns().stream())
                .filter(context::belongToContextTables).collect(Collectors.toSet());
        context.getAllColumns().addAll(sourceJoinKeyCols);

        if (context.getOuterJoinParticipants().isEmpty() && isDirectOuterJoin(this, context)) {
            context.getOuterJoinParticipants().addAll(sourceJoinKeyCols);
        }
    }

    /**
     * only jon, filter, project is allowed
     */
    private boolean isDirectOuterJoin(RelNode currentNode, OlapContext context) {
        if (currentNode == this) {
            for (RelNode input : currentNode.getInputs()) {
                if (isDirectOuterJoin(input, context)) {
                    return true;
                }
            }
            return false;
        } else if (((OlapRel) currentNode).getContext() == context) {
            return true;
        } else if (currentNode instanceof Project || currentNode instanceof Filter) {
            return isDirectOuterJoin(currentNode.getInput(0), context);
        } else {
            return false;
        }
    }

    @Override
    public void setSubContexts(Set<OlapContext> contexts) {
        this.subContexts = contexts;
    }

    private ColumnRowType rebuildColumnRowType(List<RelDataTypeField> missingFields, OlapContext context) {
        List<TblColRef> columns = Lists.newArrayList();
        OlapRel olapLeft = (OlapRel) this.left;
        OlapRel olapRight = (OlapRel) this.right;
        columns.addAll(olapLeft.getColumnRowType().getAllColumns());
        columns.addAll(olapRight.getColumnRowType().getAllColumns());

        for (RelDataTypeField dataTypeField : missingFields) {
            String fieldName = dataTypeField.getName();
            TblColRef aggOutCol = null;
            for (OlapTableScan tableScan : context.getAllTableScans()) {
                aggOutCol = tableScan.getColumnRowType().getColumnByName(fieldName);
                if (aggOutCol != null) {
                    break;
                }
            }
            if (aggOutCol == null) {
                aggOutCol = TblColRef.newInnerColumn(fieldName, TblColRef.InnerDataTypeEnum.LITERAL);
            }
            aggOutCol.getColumnDesc().setId(String.valueOf(dataTypeField.getIndex()));
            columns.add(aggOutCol);
        }

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }
}
