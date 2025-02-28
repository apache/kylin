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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.apache.kylin.query.util.ICutContextStrategy;
import org.apache.kylin.query.util.RexToTblColRefTranslator;
import org.apache.kylin.query.util.RexUtils;

import lombok.AccessLevel;
import lombok.Getter;

@Getter
public class OlapFilterRel extends Filter implements OlapRel {

    private static final Map<SqlKind, SqlKind> REVERSE_OP_MAP = Maps.newHashMap();

    static {
        REVERSE_OP_MAP.put(SqlKind.EQUALS, SqlKind.NOT_EQUALS);
        REVERSE_OP_MAP.put(SqlKind.NOT_EQUALS, SqlKind.EQUALS);
        REVERSE_OP_MAP.put(SqlKind.GREATER_THAN, SqlKind.LESS_THAN_OR_EQUAL);
        REVERSE_OP_MAP.put(SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN);
        REVERSE_OP_MAP.put(SqlKind.LESS_THAN, SqlKind.GREATER_THAN_OR_EQUAL);
        REVERSE_OP_MAP.put(SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN);
        REVERSE_OP_MAP.put(SqlKind.IS_NULL, SqlKind.IS_NOT_NULL);
        REVERSE_OP_MAP.put(SqlKind.IS_NOT_NULL, SqlKind.IS_NULL);
        REVERSE_OP_MAP.put(SqlKind.AND, SqlKind.OR);
        REVERSE_OP_MAP.put(SqlKind.OR, SqlKind.AND);
    }

    private ColumnRowType columnRowType;
    private OlapContext context;
    private Set<OlapContext> subContexts = Sets.newHashSet();
    @Getter(AccessLevel.PRIVATE)
    private boolean belongToPreAggContext = false;

    public OlapFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        this.rowType = getRowType();
    }

    private ColumnRowType buildColumnRowType() {
        OlapRel olapChild = (OlapRel) getInput();
        return olapChild.getColumnRowType();
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

        return new EnumerableCalc(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), sole(inputs),
                program);
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

    private static class FilterVisitor extends RexVisitorImpl<Void> {

        final ColumnRowType inputRowType;
        final Set<TblColRef> filterColumns;
        final Deque<TblColRef.FilterColEnum> tmpLevels;
        final Deque<Boolean> reverses;

        public FilterVisitor(ColumnRowType inputRowType, Set<TblColRef> filterColumns) {
            super(true);
            this.inputRowType = inputRowType;
            this.filterColumns = filterColumns;
            this.tmpLevels = new ArrayDeque<>();
            this.tmpLevels.offerLast(TblColRef.FilterColEnum.NONE);
            this.reverses = new ArrayDeque<>();
            this.reverses.offerLast(false);
        }

        @Override
        public Void visitCall(RexCall call) {
            SqlOperator op = call.getOperator();
            SqlKind kind = op.getKind();
            if (kind == SqlKind.CAST || kind == SqlKind.REINTERPRET) {
                for (RexNode operand : call.operands) {
                    operand.accept(this);
                }
                return null;
            }
            if (op.getKind() == SqlKind.NOT) {
                reverses.offerLast(Boolean.FALSE.equals(reverses.peekLast()));
            } else {
                assert reverses.peekLast() != null;
                if (reverses.peekLast()) {
                    kind = REVERSE_OP_MAP.get(kind);
                    reverses.offerLast(kind == SqlKind.AND || kind == SqlKind.OR);
                } else {
                    reverses.offerLast(false);
                }
            }
            TblColRef.FilterColEnum tmpLevel = getFilterColEnum(kind);
            tmpLevels.offerLast(tmpLevel);
            call.operands.forEach(operand -> operand.accept(this));
            tmpLevels.pollLast();
            reverses.pollLast();
            return null;
        }

        private TblColRef.FilterColEnum getFilterColEnum(SqlKind kind) {
            TblColRef.FilterColEnum tmpLevel;
            if (kind == SqlKind.EQUALS) {
                tmpLevel = TblColRef.FilterColEnum.EQUAL_FILTER;
            } else if (kind == SqlKind.IS_NULL) {
                tmpLevel = TblColRef.FilterColEnum.INFERIOR_EQUAL_FILTER;
            } else if (isRangeFilter(kind)) {
                tmpLevel = TblColRef.FilterColEnum.RANGE_FILTER;
            } else if (kind == SqlKind.LIKE) {
                tmpLevel = TblColRef.FilterColEnum.LIKE_FILTER;
            } else {
                tmpLevel = TblColRef.FilterColEnum.OTHER_FILTER;
            }
            return tmpLevel;
        }

        boolean isRangeFilter(SqlKind sqlKind) {
            return sqlKind == SqlKind.NOT_EQUALS || sqlKind == SqlKind.GREATER_THAN || sqlKind == SqlKind.LESS_THAN
                    || sqlKind == SqlKind.GREATER_THAN_OR_EQUAL || sqlKind == SqlKind.LESS_THAN_OR_EQUAL
                    || sqlKind == SqlKind.IS_NOT_NULL;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            TblColRef column = inputRowType.getColumnByIndex(inputRef.getIndex());
            TblColRef.FilterColEnum tmpLevel = tmpLevels.peekLast();
            collect(column, tmpLevel);
            return null;
        }

        private void collect(TblColRef column, TblColRef.FilterColEnum tmpLevel) {
            if (!column.isInnerColumn()) {
                filterColumns.add(column);
                if (tmpLevel.getPriority() > column.getFilterLevel().getPriority()) {
                    column.setFilterLevel(tmpLevel);
                }
                return;
            }
            if (column.isAggregationColumn()) {
                return;
            }
            List<TblColRef> children = column.getOperands();
            if (children == null) {
                return;
            }
            for (TblColRef child : children) {
                collect(child, tmpLevel);
            }
        }
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OlapFilterRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl contextCutImpl) {
        this.context = null;
        this.columnRowType = null;
        this.belongToPreAggContext = false;
        contextCutImpl.visitChild(getInput());
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return Objects.requireNonNull(super.computeSelfCost(planner, mq)).multiplyBy(OlapRel.OLAP_COST_FACTOR)
                .multiplyBy(calcComplexity());
    }

    // For example:
    // 1) AND(>=($11, 2021-10-28 00:00:00), <($11, 2021-11-05 00:00:00))
    // 2) AND(>=($11, CAST('2021-10-28'):TIMESTAMP(3) NOT NULL), <($11, CAST('2021-11-05'):TIMESTAMP(3) NOT NULL))
    // the first condition is better than the second condition.
    private double calcComplexity() {
        int complexity = 1 + getCondition().toString().length();
        return Math.exp(-1.0 / complexity);
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
            this.belongToPreAggContext = true;
            return true;
        }

        return false;
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        contextImpl.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        contextImpl.visitChild(getInput(), this, tempState);
        state.merge(ContextVisitorState.of(true, false)).merge(tempState);
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        olapImpl.visitChild(getInput(), this);
        if (RexUtils.countOperatorCall(condition, SqlLikeOperator.class) > 0) {
            QueryContext.current().getQueryTagInfo().setHasLike(true);
        }
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            // only translate where clause and don't translate having clause
            if (!context.isAfterAggregate()) {
                collectContextFilter();
            } else {
                context.setAfterHavingClauseFilter(true);
            }
            if (this == context.getTopNode() && !context.isHasAgg()) {
                ContextUtil.amendAllColsIfNoAgg(this);
            }
        } else {
            pushDownColsInfo(subContexts);
        }
    }

    private boolean isHeterogeneousSegmentOrMultiPartEnabled(OlapContext context) {
        if (context.getOlapSchema() == null) {
            return false;
        }
        KylinConfig kylinConfig = context.getOlapSchema().getConfig();
        return kylinConfig.isHeterogeneousSegmentEnabled() || kylinConfig.isMultiPartitionEnabled();
    }

    private boolean isJoinMatchOptimizationEnabled() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (this.context != null && this.context.getOlapSchema() != null) {
            kylinConfig = context.getOlapSchema().getConfig();
        }
        return kylinConfig.isJoinMatchOptimizationEnabled();
    }

    private void collectNotNullTableWithFilterCondition(OlapContext context) {
        if (context == null || CollectionUtils.isEmpty(context.getAllTableScans())) {
            return;
        }

        RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new KylinRelDataTypeSystem()));
        // Convert to Disjunctive Normal Form(DNF), i.e., only root node's op could be OR
        RexNode newDnf = RexUtil.toDnf(rexBuilder, condition);
        Set<TableRef> leftOrInnerTables = context.getAllTableScans().stream().map(OlapTableScan::getTableRef)
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

    private void collectContextFilter() {
        // optimize the filter, the optimization has to be segment-irrelevant
        Set<TblColRef> filterColumns = Sets.newHashSet();
        FilterVisitor visitor = new FilterVisitor(this.columnRowType, filterColumns);
        this.condition.accept(visitor);
        if (isHeterogeneousSegmentOrMultiPartEnabled(this.context)) {
            context.getAllFilterRels().add(this);
        }
        if (isJoinMatchOptimizationEnabled()) {
            collectNotNullTableWithFilterCondition(context);
        }
        for (TblColRef tblColRef : filterColumns) {
            if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                context.getAllColumns().add(tblColRef);
                context.getFilterColumns().add(tblColRef);
            }
        }
        // collect inner col condition
        context.getInnerFilterColumns().addAll(collectInnerColumnInFilter());
    }

    private Collection<TableColRefWithRel> collectInnerColumnInFilter() {
        Collection<TableColRefWithRel> resultSet = new HashSet<>();
        if (condition instanceof RexCall) {
            // collection starts from the sub rexNodes
            for (RexNode childCondition : ((RexCall) condition).getOperands()) {
                doCollectInnerColumnInFilter(childCondition, resultSet);
            }
        }
        return resultSet;
    }

    private void doCollectInnerColumnInFilter(RexNode rexNode, Collection<TableColRefWithRel> resultSet) {
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            // for comparison operators, continue with its operands
            // otherwise, try translating rexCall into inner column
            SqlKind sqlKind = rexCall.getOperator().kind;
            if (sqlKind == SqlKind.AND || sqlKind == SqlKind.OR // AND, OR
                    || SqlKind.COMPARISON.contains(sqlKind) || sqlKind == SqlKind.NOT_IN // COMPARISON
                    || sqlKind == SqlKind.LIKE || sqlKind == SqlKind.SIMILAR || sqlKind == SqlKind.BETWEEN
                    || sqlKind.name().startsWith("IS_") // IS_TRUE, IS_FALSE, iS_NOT_TRUE...
            ) {
                rexCall.getOperands().forEach(childRexNode -> doCollectInnerColumnInFilter(childRexNode, resultSet));
            } else {
                TblColRef colRef;
                try {
                    colRef = RexToTblColRefTranslator.translateRexNode(rexCall, ((OlapRel) input).getColumnRowType());
                } catch (IllegalStateException e) {
                    // if translation failed (encountered unrecognized rex node), simply return
                    return;
                }
                // inner column and contains any actual cols
                if (colRef.isInnerColumn() && !colRef.getSourceColumns().isEmpty()) {
                    resultSet.add(new TableColRefWithRel(this, colRef));
                }
            }
        }
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        rewriteImpl.visitChild(this, getInput());
        if (context != null) {
            this.rowType = this.deriveRowType();
            this.columnRowType = buildColumnRowType();
        }
    }

    private void pushDownColsInfo(Set<OlapContext> subContexts) {
        for (OlapContext subCtx : subContexts) {
            if (this.condition == null)
                return;
            Set<TblColRef> filterColumns = Sets.newHashSet();
            FilterVisitor visitor = new FilterVisitor(this.columnRowType, filterColumns);
            this.condition.accept(visitor);
            if (isHeterogeneousSegmentOrMultiPartEnabled(subCtx)) {
                subCtx.getAllFilterRels().add(this);
            }
            if (isJoinMatchOptimizationEnabled()) {
                collectNotNullTableWithFilterCondition(subCtx);
            }
            // optimize the filter, the optimization has to be segment-irrelevant
            for (TblColRef tblColRef : filterColumns) {
                if (!tblColRef.isInnerColumn() && subCtx.belongToContextTables(tblColRef)) {
                    subCtx.getAllColumns().add(tblColRef);
                    subCtx.getFilterColumns().add(tblColRef);
                    if (belongToPreAggContext) {
                        subCtx.getGroupByColumns().add(tblColRef);
                    }
                }
            }
        }
    }

    @Override
    public void setSubContexts(Set<OlapContext> contexts) {
        this.subContexts = contexts;
    }

    private static class MatchWithFilterVisitor extends RexVisitorImpl<RexNode> {

        static final Set<SqlOperator> NULL_OPERATORS = ImmutableSet.of(SqlStdOperatorTable.IS_NULL,
                SqlStdOperatorTable.IS_NOT_TRUE);

        private final ColumnRowType columnRowType;
        private final Set<TableRef> notNullTables;

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
            // Need to support `CASE WHEN` ?
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

            if (NULL_OPERATORS.contains(call.getOperator())) {
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
