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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.util.ICutContextStrategy;

import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapSortRel extends Sort implements OlapRel {

    private ColumnRowType columnRowType;
    private OlapContext context;
    @Setter
    private Set<OlapContext> subContexts = Sets.newHashSet();
    @Setter
    private boolean needPushToSubCtx;

    public OlapSortRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation,
            RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child, collation, offset, fetch);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
    }

    @Override
    public OlapSortRel copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset,
            RexNode fetch) {
        return new OlapSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR);
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        olapImpl.visitChild(getInput(), this);
        this.columnRowType = buildColumnRowType();
        if (context != null && this == context.getTopNode() && !context.isHasAgg()) {
            ContextUtil.amendAllColsIfNoAgg(this);
        }

        if (context != null) {
            for (RelFieldCollation fieldCollation : this.collation.getFieldCollations()) {
                int index = fieldCollation.getFieldIndex();
                SQLDigest.OrderEnum order = getOrderEnum(fieldCollation.getDirection());
                OlapRel olapChild = (OlapRel) this.getInput();
                TblColRef orderCol = olapChild.getColumnRowType().getAllColumns().get(index);
                this.context.addSort(orderCol, order);
                this.context.getAllColumns().addAll(orderCol.getSourceColumns());
            }
        } else if (needPushToSubCtx) {
            List<Set<TblColRef>> sourceColumns = this.columnRowType.getSourceColumns();
            if (CollectionUtils.isNotEmpty(sourceColumns)) {
                Set<TblColRef> colRefs = sourceColumns.stream().flatMap(Collection::stream)
                        .filter(tblColRef -> !tblColRef.getName().startsWith("_KY_")) //
                        .collect(Collectors.toSet());
                ContextUtil.updateSubContexts(colRefs, subContexts);
            }
        }
    }

    protected ColumnRowType buildColumnRowType() {
        return ((OlapRel) getInput()).getColumnRowType();
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        rewriteImpl.visitChild(this, getInput());

        if (context != null) {
            // No need to rewrite "order by" applied on non-olap context.
            // Occurs in sub-query like "select ... from (...) inner join (...) order by ..."
            if (this.context.getRealization() == null)
                return;

            this.rowType = this.deriveRowType();
            this.columnRowType = buildColumnRowType();
        }
    }

    protected SQLDigest.OrderEnum getOrderEnum(RelFieldCollation.Direction direction) {
        if (direction == RelFieldCollation.Direction.DESCENDING) {
            return SQLDigest.OrderEnum.DESCENDING;
        } else {
            return SQLDigest.OrderEnum.ASCENDING;
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        return new EnumerableSort(getCluster(),
                getCluster().traitSetOf(EnumerableConvention.INSTANCE).replace(collation), //
                sole(inputs), collation, offset, fetch);
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
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx", displayCtxId(context));
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        this.context = null;
        this.columnRowType = null;
        contextCutImpl.visitChild(getInput());
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
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));

        if (context == null && subContexts.size() == 1
                && this.getInput() == Lists.newArrayList(this.subContexts).get(0).getTopNode()) {
            this.context = Lists.newArrayList(this.subContexts).get(0);
            this.context.setTopNode(this);
        }
        state.merge(tempState);
    }
}
