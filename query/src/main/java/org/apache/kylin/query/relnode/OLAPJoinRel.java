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

import net.hydromatic.linq4j.expressions.Blocks;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableJoinRel;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;

import org.apache.kylin.query.schema.OLAPTable;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinInfo;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory.FieldInfoBuilder;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.util.ImmutableIntList;

import com.google.common.base.Preconditions;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author xjiang
 */
public class OLAPJoinRel extends EnumerableJoinRel implements OLAPRel {

    private final static String[] COLUMN_ARRAY_MARKER = new String[0];

    private OLAPContext context;
    private ColumnRowType columnRowType;
    private boolean isTopJoin;
    private boolean hasSubQuery;

    public OLAPJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, //
            RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys, //
            JoinRelType joinType, Set<String> variablesStopped) throws InvalidRelException {
        super(cluster, traits, left, right, condition, leftKeys, rightKeys, joinType, variablesStopped);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        this.rowType = getRowType();
        this.isTopJoin = false;
    }

    @Override
    public EnumerableJoinRel copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, //
            JoinRelType joinType, boolean semiJoinDone) {

        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        assert joinInfo.isEqui();
        try {
            return new OLAPJoinRel(getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys, joinType, variablesStopped);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.05);
    }

    @Override
    public double getRows() {
        return super.getRows() * 0.1;
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {

        // create context for root join
        if (!(implementor.getParentNode() instanceof OLAPJoinRel)) {
            implementor.allocateContext();
        }
        this.context = implementor.getContext();
        this.isTopJoin = !this.context.hasJoin;
        this.context.hasJoin = true;

        // as we keep the first table as fact table, we need to visit from left
        // to right
        implementor.visitChild(this.left, this);
        if (this.context != implementor.getContext() || ((OLAPRel) this.left).hasSubQuery()) {
            this.hasSubQuery = true;
            implementor.freeContext();
        }
        implementor.visitChild(this.right, this);
        if (this.context != implementor.getContext() || ((OLAPRel) this.right).hasSubQuery()) {
            this.hasSubQuery = true;
            implementor.freeContext();
        }

        this.columnRowType = buildColumnRowType();
        if (isTopJoin) {
            this.context.afterJoin = true;
        }

        if (!this.hasSubQuery) {
            this.context.allColumns.clear();
            this.context.olapRowType = getRowType();
            buildAliasMap();

            // build JoinDesc
            RexCall condition = (RexCall) this.getCondition();
            JoinDesc join = buildJoin(condition);

            JoinRelType joinRelType = this.getJoinType();
            String joinType = joinRelType == JoinRelType.INNER ? "INNER" : joinRelType == JoinRelType.LEFT ? "LEFT" : null;
            join.setType(joinType);

            this.context.joins.add(join);
        }
    }

    private ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = new ArrayList<TblColRef>();

        OLAPRel olapLeft = (OLAPRel) this.left;
        ColumnRowType leftColumnRowType = olapLeft.getColumnRowType();
        columns.addAll(leftColumnRowType.getAllColumns());

        OLAPRel olapRight = (OLAPRel) this.right;
        ColumnRowType rightColumnRowType = olapRight.getColumnRowType();
        columns.addAll(rightColumnRowType.getAllColumns());

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException("RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }

    private JoinDesc buildJoin(RexCall condition) {
        Map<TblColRef, TblColRef> joinColumns = new HashMap<TblColRef, TblColRef>();
        translateJoinColumn(condition, joinColumns);

        List<String> pks = new ArrayList<String>();
        List<TblColRef> pkCols = new ArrayList<TblColRef>();
        List<String> fks = new ArrayList<String>();
        List<TblColRef> fkCols = new ArrayList<TblColRef>();
        String factTable = context.firstTableScan.getTableName();
        for (Map.Entry<TblColRef, TblColRef> columnPair : joinColumns.entrySet()) {
            TblColRef fromCol = columnPair.getKey();
            TblColRef toCol = columnPair.getValue();
            if (factTable.equalsIgnoreCase(fromCol.getTable())) {
                fks.add(fromCol.getName());
                fkCols.add(fromCol);
                pks.add(toCol.getName());
                pkCols.add(toCol);
            } else {
                fks.add(toCol.getName());
                fkCols.add(toCol);
                pks.add(fromCol.getName());
                pkCols.add(fromCol);
            }
        }

        JoinDesc join = new JoinDesc();
        join.setForeignKey(fks.toArray(COLUMN_ARRAY_MARKER));
        join.setForeignKeyColumns(fkCols.toArray(new TblColRef[fkCols.size()]));
        join.setPrimaryKey(pks.toArray(COLUMN_ARRAY_MARKER));
        join.setPrimaryKeyColumns(pkCols.toArray(new TblColRef[pkCols.size()]));
        return join;
    }

    private void translateJoinColumn(RexCall condition, Map<TblColRef, TblColRef> joinColumns) {
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
            joinColumns.put(col0, col1);
        }
    }

    private void buildAliasMap() {
        int size = this.rowType.getFieldList().size();

        for (int i = 0; i < size; i++) {
            RelDataTypeField field = this.rowType.getFieldList().get(i);
            TblColRef column = this.columnRowType.getColumnByIndex(i);
            context.storageContext.addAlias(column, field.getName());
        }
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        Result result = null;
        if (this.hasSubQuery) {
            result = super.implement(implementor, pref);
        } else {
            PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());

            RelOptTable factTable = context.firstTableScan.getTable();
            result = implementor.result(physType, Blocks.toBlock(Expressions.call(factTable.getExpression(OLAPTable.class), "executeOLAPQuery", implementor.getRootExpression(), Expressions.constant(context.id))));
        }

        return result;
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

        if (this.isTopJoin && RewriteImplementor.needRewrite(this.context)) {
            // find missed rewrite fields
            int paramIndex = this.rowType.getFieldList().size();
            List<RelDataTypeField> newFieldList = new LinkedList<RelDataTypeField>();
            for (Map.Entry<String, RelDataType> rewriteField : this.context.rewriteFields.entrySet()) {
                String fieldName = rewriteField.getKey();
                if (this.rowType.getField(fieldName, true) == null) {
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
            this.context.olapRowType = this.rowType;

            // rebuild columns
            this.columnRowType = this.buildColumnRowType();
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
}
