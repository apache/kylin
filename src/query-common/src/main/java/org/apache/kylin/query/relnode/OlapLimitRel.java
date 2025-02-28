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

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.query.util.ICutContextStrategy;

import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapLimitRel extends SingleRel implements OlapRel {

    private final RexNode localOffset; // avoid same name in parent class
    private final RexNode localFetch; // avoid same name in parent class
    private ColumnRowType columnRowType;
    private OlapContext context;
    @Setter
    private Set<OlapContext> subContexts = Sets.newHashSet();

    public OlapLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
        this.localOffset = offset;
        this.localFetch = fetch;
    }

    @Override
    public OlapLimitRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OlapLimitRel(getCluster(), traitSet, AbstractRelNode.sole(inputs), localOffset, localFetch);
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        this.context = null;
        this.columnRowType = null;
        contextCutImpl.visitChild(getInput());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx", displayCtxId(context)) //
                .itemIf("offset", localOffset, localOffset != null) //
                .itemIf("fetch", localFetch, localFetch != null);
    }

    protected Integer translateRexToValue(RexNode rexNode, int defaultValue) {
        if (rexNode instanceof RexLiteral) {
            RexLiteral rexLiteral = (RexLiteral) rexNode;
            Number number = (Number) rexLiteral.getValue();
            return number.intValue();
        } else if (rexNode instanceof RexDynamicParam) {
            return defaultValue;
        } else {
            throw new IllegalStateException("Unsupported RexNode for limit Rel " + rexNode);
        }
    }

    protected ColumnRowType buildColumnRowType() {
        OlapRel olapChild = (OlapRel) getInput();
        return olapChild.getColumnRowType();
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        rewriteImpl.visitChild(this, getInput());

        if (context != null) {
            this.rowType = this.deriveRowType();
            this.columnRowType = buildColumnRowType();
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        EnumerableRel input = AbstractRelNode.sole(inputs);
        if (input instanceof OlapRel) {
            ((OlapRel) input).replaceTraitSet(EnumerableConvention.INSTANCE);
        }
        return EnumerableLimit.create(input, localOffset, localFetch);
    }

    @Override
    public boolean hasSubQuery() {
        OlapRel olapChild = (OlapRel) getInput();
        return olapChild.hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }

    @Override
    public void setContext(OlapContext context) {
        this.context = context;
        ((OlapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OlapContext context) {
        if (this.context == null && ((OlapRel) getInput()).pushRelInfoToContext(context)) {
            this.context = context;
            return true;
        }
        return false;
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        contextImpl.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        contextImpl.visitChild(getInput(), this, tempState);
        if (tempState.hasFreeTable()) {
            contextImpl.allocateContext(this, null);
            tempState.setHasFreeTable(false);
        }
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));

        if (context == null && subContexts.size() == 1
                && this.getInput() == Lists.newArrayList(this.subContexts).get(0).getTopNode()) {
            this.context = Lists.newArrayList(this.subContexts).get(0);
            this.context.setTopNode(this);
        }
        state.merge(tempState);
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        olapImpl.visitChild(getInput(), this);

        // ignore limit after having clause
        // ignore limit after another limit, for example:
        //     select A, count(*) from (select A,B from fact group by A,B limit 100) limit 10
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            if (!context.isAfterHavingClauseFilter() && !context.isAfterLimit()) {
                int limit = translateRexToValue(localFetch, Integer.MAX_VALUE);
                this.context.setLimit(limit);
                context.setAfterLimit(true);
            }
            if (this == context.getTopNode() && !context.isHasAgg()) {
                ContextUtil.amendAllColsIfNoAgg(this);
            }
        }
    }
}
