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
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.apache.kylin.query.util.RexToTblColRefTranslator;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.apache.kylin.query.util.RexUtils;
import org.apache.kylin.util.FilterConditionExpander;

import com.google.common.collect.Sets;

public class KapFilterRel extends OLAPFilterRel implements KapRel {
    private Set<OLAPContext> subContexts = Sets.newHashSet();

    private boolean belongToPreAggContext = false;

    public KapFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new KapFilterRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void implementCutContext(ICutContextStrategy.CutContextImplementor implementor) {
        this.context = null;
        this.columnRowType = null;
        this.belongToPreAggContext = false;
        implementor.visitChild(getInput());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost c = super.computeSelfCost(planner, mq);
        return c;
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
            this.belongToPreAggContext = true;
            return true;
        }

        return false;
    }

    @Override
    public void implementContext(OLAPContextImplementor olapContextImplementor, ContextVisitorState state) {
        olapContextImplementor.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        olapContextImplementor.visitChild(getInput(), this, tempState);
        state.merge(ContextVisitorState.of(true, false)).merge(tempState);
        subContexts.addAll(ContextUtil.collectSubContext((KapRel) this.getInput()));
    }

    @Override
    public void implementOLAP(OLAPImplementor olapContextImplementor) {
        olapContextImplementor.visitChild(getInput(), this);
        if (RexUtils.countOperatorCall(condition, SqlLikeOperator.class) > 0) {
            QueryContext.current().getQueryTagInfo().setHasLike(true);
        }
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            // only translate where clause and don't translate having clause
            if (!context.afterAggregate) {
                updateContextFilter();
            } else {
                context.afterHavingClauseFilter = true;
            }
            if (this == context.getTopNode() && !context.isHasAgg())
                KapContext.amendAllColsIfNoAgg(this);
        } else {
            pushDownColsInfo(subContexts);
        }
    }

    private boolean isHeterogeneousSegmentOrMultiPartEnabled(OLAPContext context) {
        if (context.olapSchema == null) {
            return false;
        }
        String projectName = context.olapSchema.getProjectName();
        KylinConfig kylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(projectName)
                .getConfig();
        return kylinConfig.isHeterogeneousSegmentEnabled() || kylinConfig.isMultiPartitionEnabled();
    }

    private boolean isJoinMatchOptimizationEnabled() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        return kylinConfig.isJoinMatchOptimizationEnabled();
    }

    private void collectNotNullTableWithFilterCondition(OLAPContext context) {
        if (context == null || CollectionUtils.isEmpty(context.allTableScans)) {
            return;
        }

        RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new KylinRelDataTypeSystem()));
        // Convert to Disjunctive Normal Form(DNF), i.e., only root node's op could be OR
        RexNode newDnf = RexUtil.toDnf(rexBuilder, condition);
        Set<TableRef> leftOrInnerTables = context.allTableScans.stream().map(OLAPTableScan::getTableRef)
                .collect(Collectors.toSet());
        Set<TableRef> orNotNullTables = Sets.newHashSet();
        MatchWithFilterVisitor visitor = new MatchWithFilterVisitor(this.columnRowType, orNotNullTables);

        if (SqlStdOperatorTable.OR.equals(((RexCall) newDnf).getOperator())) {
            for (RexNode rexNode : ((RexCall) newDnf).getOperands()) {
                rexNode.accept(visitor);
                leftOrInnerTables.retainAll(orNotNullTables);
                orNotNullTables.clear();
            }
        } else {
            newDnf.accept(visitor);
            leftOrInnerTables.retainAll(orNotNullTables);
        }
        context.getNotNullTables().addAll(leftOrInnerTables);
    }

    private void updateContextFilter() {
        // optimize the filter, the optimization has to be segment-irrelevant
        Set<TblColRef> filterColumns = Sets.newHashSet();
        FilterVisitor visitor = new FilterVisitor(this.columnRowType, filterColumns);
        this.condition.accept(visitor);
        if (isHeterogeneousSegmentOrMultiPartEnabled(this.context)) {
            context.getExpandedFilterConditions()
                    .addAll(new FilterConditionExpander(context, this).convert(this.condition));
        }
        if (isJoinMatchOptimizationEnabled()) {
            collectNotNullTableWithFilterCondition(context);
        }
        for (TblColRef tblColRef : filterColumns) {
            if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                context.allColumns.add(tblColRef);
                context.filterColumns.add(tblColRef);
            }
        }
        // collect inner col condition
        context.getInnerFilterColumns().addAll(collectInnerColumnInFilter());
    }

    private Collection<TblColRef> collectInnerColumnInFilter() {
        Collection<TblColRef> resultSet = new HashSet<>();
        if (condition instanceof RexCall) {
            // collection starts from the sub rexNodes
            for (RexNode childCondition : ((RexCall) condition).getOperands()) {
                doCollectInnerColumnInFilter(childCondition, resultSet);
            }
        }
        return resultSet;
    }

    private void doCollectInnerColumnInFilter(RexNode rexNode, Collection<TblColRef> resultSet) {
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            // for comparison operators, continue with its operands
            // otherwise, try translating rexCall into inner column
            SqlKind sqlKind = rexCall.getOperator().kind;
            if (sqlKind == SqlKind.AND || sqlKind == SqlKind.OR || // AND, OR
                    SqlKind.COMPARISON.contains(sqlKind) || sqlKind == SqlKind.NOT_IN || // COMPARISON
                    sqlKind == SqlKind.LIKE || sqlKind == SqlKind.SIMILAR || sqlKind == SqlKind.BETWEEN
                    || sqlKind.name().startsWith("IS_") // IS_TRUE, IS_FALSE, iS_NOT_TRUE...
            ) {
                rexCall.getOperands().forEach(childRexNode -> doCollectInnerColumnInFilter(childRexNode, resultSet));
            } else {
                TblColRef colRef;
                try {
                    colRef = RexToTblColRefTranslator.translateRexNode(rexCall, ((OLAPRel) input).getColumnRowType());
                } catch (IllegalStateException e) {
                    // if translation failed (encountered unrecognized rex node), simply return
                    return;
                }
                // inner column and contains any actual cols
                if (colRef.isInnerColumn() && !colRef.getSourceColumns().isEmpty()) {
                    resultSet.add(colRef);
                }
            }
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        if (context != null) {
            this.rowType = this.deriveRowType();
            this.columnRowType = buildColumnRowType();
        }
    }

    private void pushDownColsInfo(Set<OLAPContext> subContexts) {
        for (OLAPContext context : subContexts) {
            if (this.condition == null)
                return;
            Set<TblColRef> filterColumns = Sets.newHashSet();
            FilterVisitor visitor = new FilterVisitor(this.columnRowType, filterColumns);
            this.condition.accept(visitor);
            if (isHeterogeneousSegmentOrMultiPartEnabled(context)) {
                context.getExpandedFilterConditions()
                        .addAll(new FilterConditionExpander(context, this).convert(this.condition));
            }
            if (isJoinMatchOptimizationEnabled()) {
                collectNotNullTableWithFilterCondition(context);
            }
            // optimize the filter, the optimization has to be segment-irrelevant
            for (TblColRef tblColRef : filterColumns) {
                if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                    context.allColumns.add(tblColRef);
                    context.filterColumns.add(tblColRef);
                    if (belongToPreAggContext)
                        context.getGroupByColumns().add(tblColRef);
                }
            }
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

    private class MatchWithFilterVisitor extends RexVisitorImpl<RexNode> {

        private ColumnRowType columnRowType;
        private Set<TableRef> notNullTables;

        protected MatchWithFilterVisitor(ColumnRowType columnRowType, Set<TableRef> notNullTables) {
            super(true);
            this.columnRowType = columnRowType;
            this.notNullTables = notNullTables;
        }

        @Override
        public RexCall visitCall(RexCall call) {
            if (!deep) {
                return null;
            }

            RexCall r = null;

            // only support `is not distinct from` as not null condition
            // i.e., CASE(IS NULL(DEFAULT.TEST_MEASURE.NAME2), false, =(DEFAULT.TEST_MEASURE.NAME2, '123'))
            // TODO: support `CASE WHEN`
            if (SqlStdOperatorTable.CASE.equals(call.getOperator())) {
                List<RexNode> rexNodes = call.getOperands();
                boolean isOpNull = SqlStdOperatorTable.IS_NULL.equals(((RexCall) rexNodes.get(0)).getOperator());
                boolean isSecondFalse = call.getOperands().get(1).isAlwaysFalse();
                if (isOpNull && isSecondFalse) {
                    r = (RexCall) call.getOperands().get(2).accept(this);
                    return r;
                }
                return null;
            }

            if (SqlStdOperatorTable.IS_NULL.equals(call.getOperator())) {
                return null;
            }

            for (RexNode operand : call.operands) {
                r = (RexCall) operand.accept(this);
            }
            return r;
        }

        @Override
        public RexCall visitInputRef(RexInputRef inputRef) {
            TableRef notNullTable = columnRowType.getColumnByIndex(inputRef.getIndex()).getTableRef();
            notNullTables.add(notNullTable);
            return null;
        }
    }
}
