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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableThetaJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class OLAPNonEquiJoinRel extends EnumerableThetaJoin implements OLAPRel {

    private OLAPContext context;
    private ColumnRowType columnRowType;
    private boolean hasSubQuery;

    private boolean isTopJoin;

    public OLAPNonEquiJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
                             Set<CorrelationId> variablesSet, JoinRelType joinType) throws InvalidRelException {
        super(cluster, traits, left, right, condition, variablesSet, joinType);
        rowType = getRowType();
    }

    @Override
    public OLAPContext getContext() {
        return context;
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return columnRowType;
    }

    @Override
    public boolean hasSubQuery() {
        return hasSubQuery;
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    protected boolean isParentMerelyPermutation(OLAPImplementor implementor) {
        if (implementor.getParentNode() instanceof OLAPProjectRel) {
            return ((OLAPProjectRel) implementor.getParentNode()).isMerelyPermutation();
        }
        return false;
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        // create context for root join
        if (!(implementor.getParentNode() instanceof OLAPJoinRel)
                && !(implementor.getParentNode() instanceof OLAPNonEquiJoinRel)
                && !isParentMerelyPermutation(implementor)) {
            implementor.allocateContext();
        }

        //parent context
        this.context = implementor.getContext();
        this.isTopJoin = !context.hasJoin;
        this.context.hasJoin = true;
        this.hasSubQuery = true;

        // visit and allocate context for left input
        implementor.fixSharedOlapTableScanOnTheLeft(this);
        implementor.setNewOLAPContextRequired(true);
        implementor.visitChild(this.left, this);
        if (this.context != implementor.getContext()) {
            implementor.freeContext();
        }

        // visit and allocate context for right input
        implementor.fixSharedOlapTableScanOnTheRight(this);
        implementor.setNewOLAPContextRequired(true);
        implementor.visitChild(this.right, this);
        if (this.context != implementor.getContext()) {
            implementor.freeContext();
        }

        this.columnRowType = buildColumnRowType();

        if (isTopJoin) {
            this.context.afterJoin = true;
        }

        this.context.subqueryJoinParticipants.addAll(collectJoinColumns(condition));
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, this.left);
        implementor.visitChild(this, this.right);

        this.rowType = this.deriveRowType();
        if (this.isTopJoin) {

            // add dynamic field
            Map<TblColRef, RelDataType> dynFields = this.context.dynamicFields;
            if (!dynFields.isEmpty()) {
                List<TblColRef> newCols = Lists.newArrayList(this.columnRowType.getAllColumns());
                List<RelDataTypeField> newFieldList = Lists.newArrayList();
                int paramIndex = this.rowType.getFieldList().size();
                for (TblColRef fieldCol : dynFields.keySet()) {
                    RelDataType fieldType = dynFields.get(fieldCol);

                    RelDataTypeField newField = new RelDataTypeFieldImpl(fieldCol.getName(), paramIndex++, fieldType);
                    newFieldList.add(newField);

                    newCols.add(fieldCol);
                }

                // rebuild row type
                RelDataTypeFactory.FieldInfoBuilder fieldInfo = getCluster().getTypeFactory().builder();
                fieldInfo.addAll(this.rowType.getFieldList());
                fieldInfo.addAll(newFieldList);
                this.rowType = getCluster().getTypeFactory().createStructType(fieldInfo);

                this.columnRowType = new ColumnRowType(newCols);
            }
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        return super.copy(traitSet, condition, inputs.get(0), inputs.get(1), joinType, isSemiJoinDone());
    }

    protected ColumnRowType buildColumnRowType() {
        List<TblColRef> columns = new ArrayList<TblColRef>();

        OLAPRel olapLeft = (OLAPRel) this.left;
        ColumnRowType leftColumnRowType = olapLeft.getColumnRowType();
        columns.addAll(leftColumnRowType.getAllColumns());

        OLAPRel olapRight = (OLAPRel) this.right;
        ColumnRowType rightColumnRowType = olapRight.getColumnRowType();
        columns.addAll(rightColumnRowType.getAllColumns());

        if (columns.size() != this.rowType.getFieldCount()) {
            throw new IllegalStateException(
                    "RowType=" + this.rowType.getFieldCount() + ", ColumnRowType=" + columns.size());
        }
        return new ColumnRowType(columns);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return super.estimateRowCount(mq) * 0.1;
    }

    @Override
    public EnumerableThetaJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        try {
            return new OLAPNonEquiJoinRel(this.getCluster(), traitSet, left, right, condition, this.variablesSet, joinType);
        } catch (InvalidRelException var8) {
            throw new AssertionError(var8);
        }
    }

    private Collection<TblColRef> collectJoinColumns(RexNode condition) {
        Set<TblColRef> joinColumns = Sets.newHashSet();
        doCollectJoinColumns(condition, joinColumns);
        return joinColumns;
    }

    private void doCollectJoinColumns(RexNode node, Set<TblColRef> joinColumns) {
        if (node instanceof RexCall) {
            ((RexCall) node).getOperands().forEach(operand -> doCollectJoinColumns(operand, joinColumns));
        } else if (node instanceof RexInputRef) {
            joinColumns.add(columnRowType.getColumnByIndex(((RexInputRef) node).getIndex()));
        }
    }

}
