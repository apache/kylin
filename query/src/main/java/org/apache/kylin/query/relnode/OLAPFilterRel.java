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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableCalcRel;
import net.hydromatic.optiq.runtime.SqlFunctions;
import org.apache.kylin.metadata.filter.*;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.util.NlsString;

import java.util.*;

/**
 * @author xjiang
 */
public class OLAPFilterRel extends FilterRelBase implements OLAPRel, EnumerableRel {

    private static class TupleFilterVisitor extends RexVisitorImpl<TupleFilter> {

        private final ColumnRowType inputRowType;
        private final OLAPContext context;

        public TupleFilterVisitor(ColumnRowType inputRowType, OLAPContext context) {
            super(true);
            this.inputRowType = inputRowType;
            this.context = context;
        }

        @Override
        public TupleFilter visitCall(RexCall call) {
            TupleFilter filter = null;
            SqlOperator op = call.getOperator();
            switch (op.getKind()) {
            case AND:
                filter = new LogicalTupleFilter(FilterOperatorEnum.AND);
                break;
            case OR:
                filter = new LogicalTupleFilter(FilterOperatorEnum.OR);
                break;
            case NOT:
                filter = new LogicalTupleFilter(FilterOperatorEnum.NOT);
                break;
            case EQUALS:
                filter = new CompareTupleFilter(FilterOperatorEnum.EQ);
                break;
            case GREATER_THAN:
                filter = new CompareTupleFilter(FilterOperatorEnum.GT);
                break;
            case LESS_THAN:
                filter = new CompareTupleFilter(FilterOperatorEnum.LT);
                break;
            case GREATER_THAN_OR_EQUAL:
                filter = new CompareTupleFilter(FilterOperatorEnum.GTE);
                break;
            case LESS_THAN_OR_EQUAL:
                filter = new CompareTupleFilter(FilterOperatorEnum.LTE);
                break;
            case NOT_EQUALS:
                filter = new CompareTupleFilter(FilterOperatorEnum.NEQ);
                break;
            case IS_NULL:
                filter = new CompareTupleFilter(FilterOperatorEnum.ISNULL);
                break;
            case IS_NOT_NULL:
                filter = new CompareTupleFilter(FilterOperatorEnum.ISNOTNULL);
                break;
            case CAST:
            case REINTERPRET:
                // NOTE: use child directly
                break;
            case CASE:
                filter = new CaseTupleFilter();
                break;
            case OTHER:
                if (op.getName().equalsIgnoreCase("extract_date")) {
                    filter = new ExtractTupleFilter(FilterOperatorEnum.EXTRACT);
                } else {
                    throw new UnsupportedOperationException(op.getName());
                }
                break;
            default:
                throw new UnsupportedOperationException(op.getName());
            }

            for (RexNode operand : call.operands) {
                TupleFilter childFilter = operand.accept(this);
                if (filter == null) {
                    filter = childFilter;
                } else {
                    filter.addChild(childFilter);
                }
            }

            if (op.getKind() == SqlKind.OR) {
                CompareTupleFilter inFilter = mergeToInClause(filter);
                if (inFilter != null) {
                    filter = inFilter;
                }
            }
            return filter;
        }

        private CompareTupleFilter mergeToInClause(TupleFilter filter) {
            List<? extends TupleFilter> children = filter.getChildren();
            TblColRef inColumn = null;
            List<Object> inValues = new LinkedList<Object>();
            for (TupleFilter child : children) {
                if (child.getOperator() == FilterOperatorEnum.EQ) {
                    CompareTupleFilter compFilter = (CompareTupleFilter) child;
                    TblColRef column = compFilter.getColumn();
                    if (inColumn == null) {
                        inColumn = column;
                    }

                    if (column == null || !column.equals(inColumn)) {
                        return null;
                    }
                    inValues.addAll(compFilter.getValues());
                } else {
                    return null;
                }
            }

            children.clear();

            CompareTupleFilter inFilter = new CompareTupleFilter(FilterOperatorEnum.IN);
            inFilter.addChild(new ColumnTupleFilter(inColumn));
            inFilter.addChild(new ConstantTupleFilter(inValues));
            return inFilter;
        }

        @Override
        public TupleFilter visitLocalRef(RexLocalRef localRef) {
            throw new UnsupportedOperationException("local ref:" + localRef);
        }

        @Override
        public TupleFilter visitInputRef(RexInputRef inputRef) {
            TblColRef column = inputRowType.getColumnByIndex(inputRef.getIndex());
            context.allColumns.add(column);
            ColumnTupleFilter filter = new ColumnTupleFilter(column);
            return filter;
        }

        private String normToTwoDigits(int i) {
            if (i < 10)
                return "0" + i;
            else
                return "" + i;
        }

        @Override
        public TupleFilter visitLiteral(RexLiteral literal) {
            String strValue = null;
            Object literalValue = literal.getValue();
            if (literalValue instanceof NlsString) {
                strValue = ((NlsString) literalValue).getValue();
            } else if (literalValue instanceof GregorianCalendar) {
                GregorianCalendar g = (GregorianCalendar) literalValue;
                strValue = "" + g.get(Calendar.YEAR) + "-" + normToTwoDigits(g.get(Calendar.MONTH) + 1) + "-" + normToTwoDigits(g.get(Calendar.DAY_OF_MONTH));
            } else if (literalValue instanceof SqlFunctions.TimeUnitRange) {
                // Extract(x from y) in where clause
                strValue = ((SqlFunctions.TimeUnitRange) literalValue).name();
            } else if (literalValue == null) {
                strValue = null;
            } else {
                strValue = literalValue.toString();
            }
            TupleFilter filter = new ConstantTupleFilter(strValue);
            return filter;
        }

        @Override
        public TupleFilter visitDynamicParam(RexDynamicParam dynamicParam) {
            String name = dynamicParam.getName();
            TupleFilter filter = new DynamicTupleFilter(name);
            return filter;
        }
    }

    private ColumnRowType columnRowType;
    private OLAPContext context;

    public OLAPFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
        this.rowType = getRowType();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.05);
    }

    @Override
    public FilterRelBase copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OLAPFilterRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.visitChild(getChild(), this);

        this.columnRowType = buildColumnRowType();
        this.context = implementor.getContext();

        // only translate where clause and don't translate having clause
        if (!context.afterAggregate) {
            translateFilter(context);
        }
    }

    private ColumnRowType buildColumnRowType() {
        OLAPRel olapChild = (OLAPRel) getChild();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        return inputColumnRowType;
    }

    private void translateFilter(OLAPContext context) {
        if (this.condition == null) {
            return;
        }

        TupleFilterVisitor visitor = new TupleFilterVisitor(this.columnRowType, context);
        context.filter = this.condition.accept(visitor);
        context.filterColumns = collectColumns(context.filter);
    }

    private Set<TblColRef> collectColumns(TupleFilter filter) {
        Set<TblColRef> ret = Sets.newHashSet();
        collectColumnsRecursively(filter, ret);
        return ret;
    }

    private void collectColumnsRecursively(TupleFilter filter, Set<TblColRef> collector) {
        if (filter instanceof ColumnTupleFilter) {
            collector.add(((ColumnTupleFilter) filter).getColumn());
        }
        for (TupleFilter child : filter.getChildren()) {
            collectColumnsRecursively(child, collector);
        }
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        // keep it for having clause
        RexBuilder rexBuilder = getCluster().getRexBuilder();
        RelDataType inputRowType = getChild().getRowType();
        RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
        programBuilder.addIdentity();
        programBuilder.addCondition(this.condition);
        RexProgram program = programBuilder.getProgram();

        EnumerableCalcRel enumCalcRel = new EnumerableCalcRel(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), getChild(), this.rowType, program, ImmutableList.<RelCollation> of());

        return enumCalcRel.implement(implementor, pref);
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getChild());

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
        OLAPRel olapChild = (OLAPRel) getChild();
        return olapChild.hasSubQuery();
    }

    @Override
    public RelTraitSet replaceTraitSet(RelTrait trait) {
        RelTraitSet oldTraitSet = this.traitSet;
        this.traitSet = this.traitSet.replace(trait);
        return oldTraitSet;
    }
}
