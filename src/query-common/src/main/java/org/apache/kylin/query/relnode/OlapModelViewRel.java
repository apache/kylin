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

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.query.util.ICutContextStrategy;

import lombok.Getter;

/**
 * placeholder for model view
 */
public class OlapModelViewRel extends SingleRel implements OlapRel, EnumerableRel {

    private final String modelAlias;
    @Getter
    private OlapContext context;

    public OlapModelViewRel(RelOptCluster cluster, RelTraitSet traits, RelNode input, String modelAlias) {
        super(cluster, traits, input);
        this.modelAlias = modelAlias;
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        ((OlapRel) getInput(0)).implementContext(contextImpl, state);
        state.setHasModelView(true);
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        ((OlapRel) getInput(0)).implementOlap(olapImpl);
        if (this == context.getTopNode() && !context.isHasAgg()) {
            ContextUtil.amendAllColsIfNoAgg(this);
        }
        this.context.setBoundedModelAlias(modelAlias);
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        ((OlapRel) getInput(0)).implementRewrite(rewriteImpl);
        rowType = deriveRowType();
    }

    @Override
    public boolean pushRelInfoToContext(OlapContext context) {
        return ((OlapRel) getInput(0)).pushRelInfoToContext(context);
    }

    @Override
    public Set<OlapContext> getSubContexts() {
        return ((OlapRel) getInput(0)).getSubContexts();
    }

    @Override
    public void setSubContexts(Set<OlapContext> contexts) {
        ((OlapRel) getInput(0)).setSubContexts(contexts);
    }

    @Override
    public void setContext(OlapContext context) {
        this.context = context;
        ((OlapRel) getInput(0)).setContext(context);
    }

    @Override
    public ColumnRowType getColumnRowType() {
        return ((OlapRel) getInput(0)).getColumnRowType();
    }

    @Override
    public boolean hasSubQuery() {
        return ((OlapRel) getInput(0)).hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public RelDataType deriveRowType() {
        return getInput(0).getRowType();

    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OlapModelViewRel(getCluster(), traitSet, inputs.get(0), modelAlias);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        throw new KylinException(QueryErrorCode.UNSUPPORTED_OPERATION, "Not Implemented");
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        throw new KylinException(QueryErrorCode.UNSUPPORTED_OPERATION, "Not Implemented");
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        throw new KylinException(QueryErrorCode.UNSUPPORTED_OPERATION,
                "OlapStarTableRel should not be re-cut from outside");
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // cost nothing
        return planner.getCostFactory().makeCost(0, 0, 0);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.input("input", getInput());
        pw.item("model", modelAlias);
        pw.item("ctx", displayCtxId(context));
        return pw;
    }
}
