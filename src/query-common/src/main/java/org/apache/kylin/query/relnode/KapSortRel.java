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

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.util.ICutContextStrategy;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class KapSortRel extends OLAPSortRel implements KapRel {
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapSortRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation,
            RexNode offset, RexNode fetch) {
        super(cluster, traitSet, child, collation, offset, fetch);
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        this.columnRowType = null;
        implementor.visitChild(getInput());
    }

    @Override
    public KapSortRel copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset,
            RexNode fetch) {
        return new KapSortRel(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        ((KapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        if (this.context == null && ((KapRel) getInput()).pushRelInfoToContext(context)) {
            this.context = context;
            return true;
        }
        return false;
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        olapContextImplementor.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        olapContextImplementor.visitChild(getInput(), this, tempState);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));

        if (context == null && subContexts.size() == 1
                && this.getInput() == Lists.newArrayList(this.subContexts).get(0).getTopNode()) {
            this.context = Lists.newArrayList(this.subContexts).get(0);
            this.context.setTopNode(this);
        }
        state.merge(tempState);
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        olapContextImplementor.visitChild(getInput(), this);
        this.columnRowType = buildColumnRowType();
        if (context != null && this == context.getTopNode() && !context.isHasAgg())
            KapContext.amendAllColsIfNoAgg(this);

        if (context != null) {
            for (RelFieldCollation fieldCollation : this.collation.getFieldCollations()) {
                int index = fieldCollation.getFieldIndex();
                SQLDigest.OrderEnum order = getOrderEnum(fieldCollation.getDirection());
                OLAPRel olapChild = (OLAPRel) this.getInput();
                TblColRef orderCol = olapChild.getColumnRowType().getAllColumns().get(index);
                this.context.addSort(orderCol, order);
                this.context.allColumns.addAll(orderCol.getSourceColumns());
            }
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        if (context != null) {
            // No need to rewrite "order by" applied on non-olap context.
            // Occurs in sub-query like "select ... from (...) inner join (...) order by ..."
            if (this.context.realization == null)
                return;

            this.rowType = this.deriveRowType();
            this.columnRowType = buildColumnRowType();
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
