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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableJoin;
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
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.schema.OLAPTable;

import com.google.common.base.Preconditions;

/**
 *
 */
public class OLAPJoinRel extends EnumerableJoin implements OLAPRel {

    final static String[] COLUMN_ARRAY_MARKER = new String[0];

    protected OLAPContext context;
    protected ColumnRowType columnRowType;
    protected int columnRowTypeLeftRightCut;
    protected boolean isTopJoin;
    protected boolean hasSubQuery;

    public OLAPJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, //
            RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, //
            Set<CorrelationId> variablesSet, JoinRelType joinType) throws InvalidRelException {
        super(cluster, traits, left, right, condition, leftKeys, rightKeys, variablesSet, joinType);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        this.rowType = getRowType();
        this.isTopJoin = false;
    }

    @Override
    public EnumerableJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, //
            JoinRelType joinType, boolean semiJoinDone) {

        final JoinInfo joinInfo = JoinInfo.of(left, right, conditionExpr);
        assert joinInfo.isEqui();
        try {
            return new OLAPJoinRel(getCluster(), traitSet, left, right, conditionExpr, joinInfo.leftKeys,
                    joinInfo.rightKeys, variablesSet, joinType);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // assign a huge cost on right join and cross join so that the swapped left join and inner join will win in the optimization
        return joinType == JoinRelType.RIGHT || condition.isAlwaysTrue()
                ? super.computeSelfCost(planner, mq).multiplyBy(100)
                : super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return super.estimateRowCount(mq) * 0.1;
    }

    //when OLAPJoinPushThroughJoinRule is applied, a "MerelyPermutation" project rel will be created
    protected boolean isParentMerelyPermutation(OLAPImplementor implementor) {
        if (implementor.getParentNode() instanceof OLAPProjectRel) {
            return ((OLAPProjectRel) implementor.getParentNode()).isMerelyPermutation();
        }
        return false;
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {

        // create context for root join
        if (!(implementor.getParentNode() instanceof OLAPJoinRel) && !isParentMerelyPermutation(implementor)) {
            implementor.allocateContext();
        }
        //parent context
        this.context = implementor.getContext();
        this.context.allOlapJoins.add(this);
        this.isTopJoin = !this.context.isHasJoin();
        this.context.setHasJoin(true);

        boolean leftHasSubquery = false;
        boolean rightHasSubquery = false;

        // as we keep the first table as fact table, we need to visit from left to right
        implementor.fixSharedOlapTableScanOnTheLeft(this);
        implementor.visitChild(this.left, this);

        //current  has another context
        if (this.context != implementor.getContext() || ((OLAPRel) this.left).hasSubQuery()) {
            this.hasSubQuery = true;
            leftHasSubquery = true;
            // if child is also an OLAPJoin, then the context has already been popped
            if (this.context != implementor.getContext()) {
                implementor.freeContext();
            }
        }

        if (leftHasSubquery) {
            // After KYLIN-2579, leftHasSubquery means right side have to be separate olap context 
            implementor.setNewOLAPContextRequired(true);
        }

        implementor.fixSharedOlapTableScanOnTheRight(this);
        implementor.visitChild(this.right, this);
        if (this.context != implementor.getContext() || ((OLAPRel) this.right).hasSubQuery()) {
            this.hasSubQuery = true;
            rightHasSubquery = true;
            // if child is also an OLAPJoin, then the context has already been popped

            if (leftHasSubquery) {
                Preconditions.checkState(!implementor.isNewOLAPContextRequired());//should have been satisfied
                Preconditions.checkState(this.context != implementor.getContext(), "missing a new olapcontext");
            }

            if (this.context != implementor.getContext()) {
                implementor.freeContext();
            }
        }

        this.columnRowType = buildColumnRowType();

        if (isTopJoin) {
            this.context.afterTopJoin = true;
        }

        if (!this.hasSubQuery) {
            // build JoinDesc
            Preconditions.checkState(this.getCondition() instanceof RexCall, "Cartesian Join is not supported.");

            RexCall condition = (RexCall) this.getCondition();
            JoinDesc join = buildJoin(condition);

            JoinRelType joinRelType = this.getJoinType();
            String joinType = joinRelType == JoinRelType.INNER ? "INNER"
                    : joinRelType == JoinRelType.LEFT ? "LEFT" : joinRelType == JoinRelType.RIGHT ? "RIGHT" : "FULL";
            join.setType(joinType);

            this.context.joins.add(join);
        } else {
            //When join contains subquery, the join-condition fields of fact_table will add into context.
            Map<TblColRef, TblColRef> joinCol = translateJoinColumn(this.getCondition());

            for (Map.Entry<TblColRef, TblColRef> columnPair : joinCol.entrySet()) {
                TblColRef fromCol = (rightHasSubquery ? columnPair.getKey() : columnPair.getValue());
                this.context.getSubqueryJoinParticipants().add(fromCol);
            }
            joinCol.clear();
        }
    }

    protected ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = new ArrayList<>();
        List<Set<TblColRef>> sourceColumns = new ArrayList<>();

        OLAPRel olapLeft = (OLAPRel) this.left;
        ColumnRowType leftColumnRowType = olapLeft.getColumnRowType();
        columns.addAll(leftColumnRowType.getAllColumns());

        this.columnRowTypeLeftRightCut = columns.size();

        OLAPRel olapRight = (OLAPRel) this.right;
        ColumnRowType rightColumnRowType = olapRight.getColumnRowType();
        columns.addAll(rightColumnRowType.getAllColumns());

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }

        columns.stream().forEach(col -> sourceColumns.add(col.getSourceColumns()));

        return new ColumnRowType(columns, sourceColumns);
    }

    protected JoinDesc buildJoin(RexCall condition) {
        Map<TblColRef, TblColRef> joinColumns = translateJoinColumn(condition);

        List<String> pks = new ArrayList<>();
        List<TblColRef> pkCols = new ArrayList<>();
        List<String> fks = new ArrayList<>();
        List<TblColRef> fkCols = new ArrayList<>();
        for (Map.Entry<TblColRef, TblColRef> columnPair : joinColumns.entrySet()) {
            TblColRef fromCol = columnPair.getKey();
            TblColRef toCol = columnPair.getValue();
            fks.add(fromCol.getName());
            fkCols.add(fromCol);
            pks.add(toCol.getName());
            pkCols.add(toCol);
        }

        JoinDesc join = new JoinDesc();
        join.setForeignKey(fks.toArray(COLUMN_ARRAY_MARKER));
        join.setForeignKeyColumns(fkCols.toArray(new TblColRef[fkCols.size()]));
        join.setPrimaryKey(pks.toArray(COLUMN_ARRAY_MARKER));
        join.setPrimaryKeyColumns(pkCols.toArray(new TblColRef[pkCols.size()]));
        join.sortByFK();
        return join;
    }

    protected Map<TblColRef, TblColRef> translateJoinColumn(RexNode condition) {
        Map<TblColRef, TblColRef> joinColumns = new HashMap<>();
        if (condition instanceof RexCall) {
            translateJoinColumn((RexCall) condition, joinColumns);
        }
        return joinColumns;
    }

    void translateJoinColumn(RexCall condition, Map<TblColRef, TblColRef> joinColumns) {
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
            if (op0.getIndex() < columnRowTypeLeftRightCut)
                joinColumns.put(col0, col1);
            else
                joinColumns.put(col1, col0);
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        if (this.hasSubQuery) {
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

    /**
     * belongs to legacy "calcite query engine" (compared to current "sparder query engine"), pay less attention
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        context.setReturnTupleInfo(rowType, columnRowType);

        PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
        RelOptTable factTable = context.firstTableScan.getTable();
        // query result is error like select min(2+2), max(2) from EmptyTable
        String execFunc = context.isConstantQueryWithAggregations() ? "executeSimpleAggregationQuery" : "executeOLAPQuery";
        MethodCallExpression exprCall = Expressions.call(factTable.getExpression(OLAPTable.class), execFunc,
                implementor.getRootExpression(), Expressions.constant(context.id));
        return implementor.result(physType, Blocks.toBlock(exprCall));
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return columnRowType;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, this.left);
        implementor.visitChild(this, this.right);

        this.rowType = this.deriveRowType();

        if (RewriteImplementor.needRewrite(this.context)) {
            if (this.context.hasPrecalculatedFields()) {

                // find missed rewrite fields
                int paramIndex = this.rowType.getFieldList().size();
                List<RelDataTypeField> newFieldList = new LinkedList<>();
                for (Map.Entry<String, RelDataType> rewriteField : this.context.rewriteFields.entrySet()) {
                    String fieldName = rewriteField.getKey();
                    if (this.rowType.getField(fieldName, true, false) == null) {
                        RelDataType fieldType = rewriteField.getValue();
                        RelDataTypeField newField = new RelDataTypeFieldImpl(fieldName, paramIndex++, fieldType);
                        newFieldList.add(newField);
                    }
                }

                // rebuild row type
                FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
                fieldInfo.addAll(this.rowType.getFieldList());
                fieldInfo.addAll(newFieldList);
                this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);

                // rebuild columns
                this.columnRowType = this.buildColumnRowType();
            }
        }
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public boolean hasSubQuery() {
        return this.hasSubQuery;
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx",
                context == null ? "" : String.valueOf(context.id) + "@" + context.realization);
    }
}
