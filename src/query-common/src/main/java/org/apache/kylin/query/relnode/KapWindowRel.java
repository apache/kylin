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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.apache.kylin.query.util.RexUtils;

import com.google.common.collect.Sets;

/**
 */
public class KapWindowRel extends OLAPWindowRel implements KapRel {
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    public KapWindowRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<RexLiteral> constants,
            RelDataType rowType, List<Window.Group> groups) {
        super(cluster, traitSet, input, constants, rowType, groups);
    }

    @Override
    public Window copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new KapWindowRel(getCluster(), traitSet, inputs.get(0), constants, rowType, groups);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        olapContextImplementor.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        olapContextImplementor.visitChild(getInput(), this, tempState);

        // window rel need a separate context
        if (tempState.hasFreeTable()) {
            olapContextImplementor.allocateContext(this, this);
            tempState.setHasFreeTable(false);
        }

        state.merge(tempState);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        implementor.visitChild(getInput());
    }

    @Override
    public void setContext(OLAPContext context) {
        this.context = context;
        ((KapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OLAPContext context) {
        return true;
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        olapContextImplementor.visitChild(getInput(), this);

        this.columnRowType = buildColumnRowType();
        if (context != null) {
            this.context.hasWindow = true;
            if (this == context.getTopNode() && !context.isHasAgg())
                KapContext.amendAllColsIfNoAgg(this);
        } else {
            ContextUtil.updateSubContexts(getGroupingColumns(), subContexts);
        }
    }

    public Collection<TblColRef> getGroupingColumns() {
        ColumnRowType inputColumnRowType = ((KapRel) getInput()).getColumnRowType();
        Set<TblColRef> tblColRefs = new HashSet<>();
        for (Window.Group group : groups) {
            group.keys.forEach(grpKey -> tblColRefs.addAll(inputColumnRowType.getSourceColumnsByIndex(grpKey)));
            group.orderKeys.getFieldCollations()
                    .forEach(f -> tblColRefs.addAll(inputColumnRowType.getSourceColumnsByIndex(f.getFieldIndex())));
            group.aggCalls.stream().flatMap(call -> RexUtils.getAllInputRefs(call).stream())
                    .filter(inRef -> inRef.getIndex() < inputColumnRowType.size()) // if idx >= input column cnt, it is referencing to come constants
                    .flatMap(inRef -> inputColumnRowType.getSourceColumnsByIndex(inRef.getIndex()).stream())
                    .forEach(tblColRefs::add);
        }
        return tblColRefs;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        for (RelNode child : getInputs()) {
            implementor.visitChild(this, child);
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
