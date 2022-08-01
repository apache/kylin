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

import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.util.ICutContextStrategy;

import com.google.common.collect.Sets;

/**
 */
public class KapUnionRel extends OLAPUnionRel implements KapRel {
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapUnionRel(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        throw new RuntimeException("Union rel should not be re-cut from outside");
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new KapUnionRel(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    /**
     * Should not be called in Union
     *
     * @param context The context to be set.
     */
    @Override
    public void setContext(OLAPContext context) {
        if (QueryContext.current().getQueryTagInfo().isConstantQuery()) {
            return;
        }
        throw new RuntimeException("Union should not be set context from outside");
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        return context == this.context;
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        // Because all children should have their own context(s), no free table exists after visit.
        ContextVisitorState accumulateState = ContextVisitorState.init();
        for (int i = 0; i < getInputs().size(); i++) {
            olapContextImplementor.fixSharedOlapTableScanAt(this, i);
            ContextVisitorState tempState = ContextVisitorState.init();
            RelNode input = getInput(i);
            olapContextImplementor.visitChild(input, this, tempState);
            if (tempState.hasFreeTable()) {
                // any input containing free table should be assigned a context
                olapContextImplementor.allocateContext((KapRel) input, this);
            }
            tempState.setHasFreeTable(false);
            accumulateState.merge(tempState);
        }
        state.merge(accumulateState);

        for (RelNode subRel : getInputs()) {
            subContexts.addAll(ContextUtil.collectSubContext((KapRel) subRel));
        }
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        for (int i = 0, n = getInputs().size(); i < n; i++) {
            olapContextImplementor.visitChild(getInputs().get(i), this);
        }
        this.columnRowType = buildColumnRowType();
        if (context != null && this == context.getTopNode() && !context.isHasAgg())
            KapContext.amendAllColsIfNoAgg(this);
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        for (RelNode child : getInputs()) {
            implementor.visitChild(this, child);
        }

        if (context != null) {
            this.rowType = this.deriveRowType();
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
}
