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

import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.filter.FilterOptimizeTransformer;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.visitor.TupleFilterVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 */
public class OLAPFilterRel extends Filter implements OLAPRel {

    ColumnRowType columnRowType;
    OLAPContext context;
    boolean autoJustTimezone = KylinConfig.getInstanceFromEnv().getStreamingDerivedTimeTimezone().length() > 0;

    public OLAPFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
        this.rowType = getRowType();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.05);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OLAPFilterRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.fixSharedOlapTableScan(this);
        implementor.visitChild(getInput(), this);

        this.columnRowType = buildColumnRowType();
        this.context = implementor.getContext();

        // only translate where clause and don't translate having clause
        if (!context.afterAggregate) {
            translateFilter(context);
        } else {
            context.afterHavingClauseFilter = true;

            if (context.havingFilter == null) {
                TupleFilterVisitor visitor = new TupleFilterVisitor(this.columnRowType);
                RexNode condition = getHavingFilterCondition();
                if (condition != null) {
                    context.havingFilter = condition.accept(visitor);
                }
            }
        }
    }

    // In case that there's (is not null) for some dimension in the having filter,
    //      which may not utilize the FilterAggregateTransposeRule to do filter decomposition,
    // we need to extract filters on aggregations here which may be used in GTCubeStorageQueryBase.
    // Otherwise, the logic in GTCubeStorageQueryBase may not be correct and may throw exceptions
    private RexNode getHavingFilterCondition() {
        if (!(getInput() instanceof OLAPAggregateRel)) {
            return this.condition;
        }

        List<RexNode> remainingConditions = Lists.newArrayList();

        OLAPAggregateRel aggRel = (OLAPAggregateRel) getInput();
        final List<RexNode> conditions = RelOptUtil.conjunctions(this.condition);
        for (RexNode condition : conditions) {
            ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
            if (!canPush(aggRel, rCols)) {
                remainingConditions.add(condition);
            }
        }

        return remainingConditions.isEmpty() ? null
                : RexUtil.composeDisjunction(getCluster().getRexBuilder(), remainingConditions);
    }

    private boolean canPush(OLAPAggregateRel aggregate, ImmutableBitSet rCols) {
        // If the filter references columns not in the group key, we cannot push
        final ImmutableBitSet groupKeys = ImmutableBitSet.range(0, aggregate.getGroupSet().cardinality());
        if (!groupKeys.contains(rCols)) {
            return false;
        }

        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            // If grouping sets are used, the filter can be pushed if
            // the columns referenced in the predicate are present in
            // all the grouping sets.
            for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
                if (!groupingSet.contains(rCols)) {
                    return false;
                }
            }
        }
        return true;
    }

    ColumnRowType buildColumnRowType() {
        OLAPRel olapChild = (OLAPRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        return inputColumnRowType;
    }

    void translateFilter(OLAPContext context) {
        if (this.condition == null) {
            return;
        }

        TupleFilterVisitor visitor = new TupleFilterVisitor(this.columnRowType);
        boolean isRealtimeTable = columnRowType.getColumnByIndex(0).getColumnDesc().getTable().isStreamingTable() ;
        autoJustTimezone = isRealtimeTable && autoJustTimezone;
        visitor.setAutoJustByTimezone(autoJustTimezone);
        TupleFilter filter = this.condition.accept(visitor);

        // optimize the filter, the optimization has to be segment-irrelevant
        filter = new FilterOptimizeTransformer().transform(filter);

        Set<TblColRef> filterColumns = Sets.newHashSet();
        TupleFilter.collectColumns(filter, filterColumns);
        for (TblColRef tblColRef : filterColumns) {
            if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                context.allColumns.add(tblColRef);
                context.filterColumns.add(tblColRef);
            }
        }

        context.filter = and(context.filter, filter);
    }
    
    private TupleFilter and(TupleFilter f1, TupleFilter f2) {
        if (f1 == null)
            return f2;
        if (f2 == null)
            return f1;

        if (f1.getOperator() == FilterOperatorEnum.AND) {
            f1.addChild(f2);
            return f1;
        }

        if (f2.getOperator() == FilterOperatorEnum.AND) {
            f2.addChild(f1);
            return f2;
        }

        LogicalTupleFilter and = new LogicalTupleFilter(FilterOperatorEnum.AND);
        and.addChild(f1);
        and.addChild(f2);
        return and;
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        // keep it for having clause
        RexBuilder rexBuilder = getCluster().getRexBuilder();
        RelDataType inputRowType = getInput().getRowType();
        RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        programBuilder.addIdentity();
        programBuilder.addCondition(this.condition);
        RexProgram program = programBuilder.getProgram();

        return new EnumerableCalc(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                sole(inputs), program);
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        this.rowType = this.deriveRowType();
        this.columnRowType = buildColumnRowType();
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
        OLAPRel olapChild = (OLAPRel) getInput();
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
        return super.explainTerms(pw).item("ctx",
                context == null ? "" : String.valueOf(context.id) + "@" + context.realization);
    }
}
