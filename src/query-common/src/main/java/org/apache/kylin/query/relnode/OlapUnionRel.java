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
import java.util.Set;

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
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.ICutContextStrategy;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapUnionRel extends Union implements OlapRel {

    @Getter(AccessLevel.PRIVATE)
    private final boolean localAll; // avoid same name in parent class
    private ColumnRowType columnRowType;
    private OlapContext context = null;
    @Setter
    private Set<OlapContext> subContexts = Sets.newHashSet();

    public OlapUnionRel(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        for (RelNode child : inputs) {
            Preconditions.checkArgument(getConvention() == child.getConvention());
        }
        this.localAll = all;
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new OlapUnionRel(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx", displayCtxId(context)) //
                .itemIf("all", all, true);
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        for (RelNode input : getInputs()) {
            olapImpl.visitChild(input, this);
        }
        this.columnRowType = buildColumnRowType();
        if (context != null && this == context.getTopNode() && !context.isHasAgg()) {
            ContextUtil.amendAllColsIfNoAgg(this);
        }
    }

    /**
     * Fake ColumnRowType for Union, all the columns are inner columns.
     */
    private ColumnRowType buildColumnRowType() {
        ColumnRowType inputColumnRowType = ((OlapRel) getInput(0)).getColumnRowType();
        List<TblColRef> columns = new ArrayList<>();
        for (TblColRef tblColRef : inputColumnRowType.getAllColumns()) {
            columns.add(TblColRef.newInnerColumn(tblColRef.getName(), TblColRef.InnerDataTypeEnum.LITERAL));
        }

        return new ColumnRowType(columns, inputColumnRowType.getSourceColumns());
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        for (RelNode child : getInputs()) {
            rewriteImpl.visitChild(this, child);
        }

        if (context != null) {
            this.rowType = this.deriveRowType();
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        ArrayList<RelNode> relInputs = new ArrayList<>(inputs.size());
        for (EnumerableRel input : inputs) {
            if (input instanceof OlapRel) {
                ((OlapRel) input).replaceTraitSet(EnumerableConvention.INSTANCE);
            }
            relInputs.add(input);
        }
        return new EnumerableUnion(getCluster(), traitSet.replace(EnumerableConvention.INSTANCE), relInputs, localAll);
    }

    @Override
    public boolean hasSubQuery() {
        for (RelNode child : getInputs()) {
            if (((OlapRel) child).hasSubQuery()) {
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

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        throw new KylinRuntimeException("Union rel should not be re-cut from outside");
    }

    /**
     * Should not be called in Union
     *
     * @param context The context to be set.
     */
    @Override
    public void setContext(OlapContext context) {
        if (QueryContext.current().getQueryTagInfo().isConstantQuery()) {
            return;
        }
        throw new KylinRuntimeException("Union should not be set context from outside");
    }

    @Override
    public boolean pushRelInfoToContext(OlapContext context) {
        return context == this.context;
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        // Because all children should have their own context(s), no free table exists after visit.
        ContextVisitorState accumulateState = ContextVisitorState.init();
        for (int i = 0; i < getInputs().size(); i++) {
            contextImpl.fixSharedOlapTableScanAt(this, i);
            ContextVisitorState tempState = ContextVisitorState.init();
            RelNode input = getInput(i);
            contextImpl.visitChild(input, this, tempState);
            if (tempState.hasFreeTable()) {
                // any input containing free table should be assigned a context
                contextImpl.allocateContext((OlapRel) input, this);
            }
            tempState.setHasFreeTable(false);
            accumulateState.merge(tempState);
        }
        state.merge(accumulateState);

        for (RelNode subRel : getInputs()) {
            subContexts.addAll(ContextUtil.collectSubContext(subRel));
        }
    }
}
