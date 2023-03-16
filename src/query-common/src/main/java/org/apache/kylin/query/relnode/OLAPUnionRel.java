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
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableUnion;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

/**
 */
public class OLAPUnionRel extends Union implements OLAPRel {

    final boolean localAll; // avoid same name in parent class
    protected ColumnRowType columnRowType;
    protected OLAPContext context;

    public OLAPUnionRel(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        for (RelNode child : inputs) {
            Preconditions.checkArgument(getConvention() == child.getConvention());
        }
        this.localAll = all;
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new OLAPUnionRel(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("ctx", context == null ? "" : String.valueOf(context.id) + "@" + context.realization)
                .itemIf("all", all, true);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        // Always create new OlapContext to combine columns from all children contexts.
        implementor.allocateContext();
        this.context = implementor.getContext();
        for (int i = 0, n = getInputs().size(); i < n; i++) {
            implementor.fixSharedOlapTableScanAt(this, i);
            implementor.visitChild(getInputs().get(i), this);
            if (implementor.getContext() != this.context) {
                implementor.freeContext();
            }
        }

        this.columnRowType = buildColumnRowType();
    }

    /**
     * Fake ColumnRowType for Union, all the columns are inner columns.
     */
    protected ColumnRowType buildColumnRowType() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput(0)).getColumnRowType();
        List<TblColRef> columns = new ArrayList<>();
        for (TblColRef tblColRef : inputColumnRowType.getAllColumns()) {
            columns.add(TblColRef.newInnerColumn(tblColRef.getName(), TblColRef.InnerDataTypeEnum.LITERAL));
        }

        return new ColumnRowType(columns, inputColumnRowType.getSourceColumns());
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        for (RelNode child : getInputs()) {
            implementor.visitChild(this, child);
        }

        this.rowType = this.deriveRowType();
        this.columnRowType = buildColumnRowType();
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        ArrayList<RelNode> relInputs = new ArrayList<>(inputs.size());
        for (EnumerableRel input : inputs) {
            if (input instanceof OLAPRel) {
                ((OLAPRel) input).replaceTraitSet(EnumerableConvention.INSTANCE);
            }
            relInputs.add(input);
        }
        return new EnumerableUnion(getCluster(), traitSet.replace(EnumerableConvention.INSTANCE), relInputs, localAll);
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
        for (RelNode child : getInputs()) {
            if (((OLAPRel) child).hasSubQuery()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }
}
