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
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableWindowBridge;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.apache.kylin.query.util.RexUtils;

import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapWindowRel extends Window implements OlapRel {
    private ColumnRowType columnRowType;
    private OlapContext context;
    @Setter
    private Set<OlapContext> subContexts = Sets.newHashSet();

    private boolean existParentProjectNeedPushInfo;

    public OlapWindowRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<RexLiteral> constants,
            RelDataType rowType, List<Window.Group> groups) {
        super(cluster, traitSet, input, constants, rowType, groups);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == input.getConvention());
    }

    @Override
    public Window copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OlapWindowRel(getCluster(), traitSet, inputs.get(0), constants, rowType, groups);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx", displayCtxId(context)) //
                .itemIf("constants", constants, !constants.isEmpty()) //
                .itemIf("groups", groups, !groups.isEmpty());
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        olapImpl.visitChild(getInput(), this);

        this.columnRowType = buildColumnRowType();
        if (context != null) {
            this.context.setHasWindow(true);
            if (this == context.getTopNode() && !context.isHasAgg())
                amendContextColumns(olapImpl);
        } else {
            ContextUtil.updateSubContexts(getGroupingColumns(), subContexts);
        }
    }

    private void amendContextColumns(OlapImpl olapImpl) {
        this.existParentProjectNeedPushInfo = checkParentProjectNeedPushInfo(olapImpl.getParentNodeStack());
        ContextUtil.amendAllColsIfNoAgg(this);
    }

    public boolean checkParentProjectNeedPushInfo(Deque<RelNode> allParents) {
        for (RelNode parent : allParents) {
            if (parent instanceof OlapProjectRel) {
                OlapProjectRel parentProject = (OlapProjectRel) parent;
                if (parentProject.isNeedPushInfoToSubCtx()) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isExistParentProjectNeedPushInfo() {
        return existParentProjectNeedPushInfo;
    }

    protected ColumnRowType buildColumnRowType() {
        OlapRel olapChild = (OlapRel) getInput(0);
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();

        List<TblColRef> columns = new ArrayList<>();
        // the input col always be collected by left
        columns.addAll(inputColumnRowType.getAllColumns());

        // add window aggregate calls column
        for (Group group : groups) {
            for (AggregateCall aggrCall : group.getAggregateCalls(this)) {
                TblColRef aggrCallCol = TblColRef.newInnerColumn(aggrCall.getName(),
                        TblColRef.InnerDataTypeEnum.LITERAL);
                columns.add(aggrCallCol);
            }
        }
        return new ColumnRowType(columns);
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        for (RelNode child : getInputs()) {
            rewriteImpl.visitChild(this, child);
        }
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        for (EnumerableRel input : inputs) {
            if (input instanceof OlapRel) {
                ((OlapRel) input).replaceTraitSet(EnumerableConvention.INSTANCE);
            }
        }
        return EnumerableWindowBridge.createEnumerableWindow(getCluster(),
                getCluster().traitSetOf(EnumerableConvention.INSTANCE), inputs.get(0), constants, rowType, groups);
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
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        contextImpl.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        contextImpl.visitChild(getInput(), this, tempState);

        // window rel need a separate context
        if (tempState.hasFreeTable()) {
            contextImpl.allocateContext(this, this);
            tempState.setHasFreeTable(false);
        }

        state.merge(tempState);
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        this.context = null;
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
        return true;
    }

    public Collection<TblColRef> getGroupingColumns() {
        ColumnRowType inputColumnRowType = ((OlapRel) getInput()).getColumnRowType();
        Set<TblColRef> tblColRefs = new HashSet<>();
        for (Window.Group group : groups) {
            group.keys.forEach(grpKey -> tblColRefs.addAll(inputColumnRowType.getSourceColumnsByIndex(grpKey)));
            group.orderKeys.getFieldCollations()
                    .forEach(f -> tblColRefs.addAll(inputColumnRowType.getSourceColumnsByIndex(f.getFieldIndex())));
            group.aggCalls.stream().flatMap(call -> RexUtils.getAllInputRefs(call).stream())
                    .filter(inRef -> inRef.getIndex() < inputColumnRowType.size())
                    // if idx >= input column cnt, it is referencing to come constants
                    .flatMap(inRef -> inputColumnRowType.getSourceColumnsByIndex(inRef.getIndex()).stream())
                    .forEach(tblColRefs::add);
        }
        return tblColRefs;
    }
}
