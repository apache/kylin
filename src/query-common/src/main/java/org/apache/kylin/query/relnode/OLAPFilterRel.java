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
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
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
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 */
public class OLAPFilterRel extends Filter implements OLAPRel {

    static final Map<SqlKind, SqlKind> REVERSE_OP_MAP = Maps.newHashMap();

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

    protected ColumnRowType columnRowType;
    protected OLAPContext context;
    public OLAPFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        //        Preconditions.checkArgument(getConvention() == child.getConvention());
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
        }
    }

    protected ColumnRowType buildColumnRowType() {
        OLAPRel olapChild = (OLAPRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        return inputColumnRowType;
    }

    protected void translateFilter(OLAPContext context) {
        if (this.condition == null) {
            return;
        }
        Set<TblColRef> filterColumns = Sets.newHashSet();
        FilterVisitor visitor = new FilterVisitor(this.columnRowType, filterColumns);
        this.condition.accept(visitor);
        for (TblColRef tblColRef : filterColumns) {
            if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                context.allColumns.add(tblColRef);
                context.filterColumns.add(tblColRef);
            }
        }
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

    protected static class FilterVisitor extends RexVisitorImpl<Void> {

        final ColumnRowType inputRowType;
        final Set<TblColRef> filterColumns;
        final Stack<TblColRef.FilterColEnum> tmpLevels;
        final Stack<Boolean> reverses;

        public FilterVisitor(ColumnRowType inputRowType, Set<TblColRef> filterColumns) {
            super(true);
            this.inputRowType = inputRowType;
            this.filterColumns = filterColumns;
            this.tmpLevels = new Stack<>();
            this.tmpLevels.add(TblColRef.FilterColEnum.NONE);
            this.reverses = new Stack<>();
            this.reverses.add(false);
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
                reverses.add(!reverses.peek());
            } else {
                if (reverses.peek()) {
                    kind = REVERSE_OP_MAP.get(kind);
                    if (kind == SqlKind.AND || kind == SqlKind.OR) {
                        reverses.add(true);
                    } else {
                        reverses.add(false);
                    }
                } else {
                    reverses.add(false);
                }
            }
            TblColRef.FilterColEnum tmpLevel = TblColRef.FilterColEnum.NONE;
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
            tmpLevels.add(tmpLevel);
            for (RexNode operand : call.operands) {
                operand.accept(this);
            }
            tmpLevels.pop();
            reverses.pop();
            return null;
        }

        boolean isRangeFilter(SqlKind sqlKind) {
            return sqlKind == SqlKind.NOT_EQUALS || sqlKind == SqlKind.GREATER_THAN || sqlKind == SqlKind.LESS_THAN
                    || sqlKind == SqlKind.GREATER_THAN_OR_EQUAL || sqlKind == SqlKind.LESS_THAN_OR_EQUAL
                    || sqlKind == SqlKind.IS_NOT_NULL;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            TblColRef column = inputRowType.getColumnByIndex(inputRef.getIndex());
            TblColRef.FilterColEnum tmpLevel = tmpLevels.peek();
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
}
