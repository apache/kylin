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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.sqlfunc.HLLDistinctCountAggFunc;

import com.google.common.base.Preconditions;

/**
 */
public class OLAPAggregateRel extends Aggregate implements OLAPRel {

    private final static Map<String, String> AGGR_FUNC_MAP = new HashMap<String, String>();

    static {
        AGGR_FUNC_MAP.put("SUM", "SUM");
        AGGR_FUNC_MAP.put("$SUM0", "SUM");
        AGGR_FUNC_MAP.put("COUNT", "COUNT");
        AGGR_FUNC_MAP.put("COUNT_DISTINCT", "COUNT_DISTINCT");
        AGGR_FUNC_MAP.put("HLL_COUNT", "COUNT_DISTINCT");
        AGGR_FUNC_MAP.put("MAX", "MAX");
        AGGR_FUNC_MAP.put("MIN", "MIN");
    }

    private static String getFuncName(AggregateCall aggCall) {
        String aggName = aggCall.getAggregation().getName();
        if (aggCall.isDistinct()) {
            aggName = aggName + "_DISTINCT";
        }
        String funcName = AGGR_FUNC_MAP.get(aggName);
        if (funcName == null) {
            throw new IllegalStateException("Don't suppoprt aggregation " + aggName);
        }
        return funcName;
    }

    private OLAPContext context;
    private ColumnRowType columnRowType;
    private boolean afterAggregate;
    private List<AggregateCall> rewriteAggCalls;
    private List<TblColRef> groups;
    private List<FunctionDesc> aggregations;

    public OLAPAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, ImmutableBitSet groupSet, List<AggregateCall> aggCalls) throws InvalidRelException {
        super(cluster, traits, child, false, groupSet, asList(groupSet), aggCalls);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        this.afterAggregate = false;
        this.rewriteAggCalls = aggCalls;
        this.rowType = getRowType();
    }

    private static List<ImmutableBitSet> asList(ImmutableBitSet groupSet) {
        ArrayList<ImmutableBitSet> l = new ArrayList<ImmutableBitSet>(1);
        l.add(groupSet);
        return l;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        try {
            return new OLAPAggregateRel(getCluster(), traitSet, input, groupSet, aggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OLAPAggregateRel!", e);
        }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.05);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {

        implementor.visitChild(getInput(), this);

        this.context = implementor.getContext();
        this.columnRowType = buildColumnRowType();
        this.afterAggregate = this.context.afterAggregate;

        // only translate the first aggregation
        if (!this.afterAggregate) {
            translateGroupBy();
            fillbackOptimizedColumn();
            translateAggregation();
            this.context.afterAggregate = true;
        } else {
            for (AggregateCall aggCall : aggCalls) {
                // check if supported by kylin
                if (aggCall.isDistinct()) {
                    throw new IllegalStateException("Distinct count is only allowed in innermost sub-query.");
                }
            }
        }
    }

    private ColumnRowType buildColumnRowType() {
        buildGroups();
        buildAggregations();

        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        List<TblColRef> columns = new ArrayList<TblColRef>(this.rowType.getFieldCount());
        columns.addAll(this.groups);

        for (int i = 0; i < this.aggregations.size(); i++) {
            FunctionDesc aggFunc = this.aggregations.get(i);
            TblColRef aggCol = null;
            if (aggFunc.needRewrite()) {
                aggCol = buildRewriteColumn(aggFunc);
            } else {
                AggregateCall aggCall = this.rewriteAggCalls.get(i);
                if (!aggCall.getArgList().isEmpty()) {
                    int index = aggCall.getArgList().get(0);
                    aggCol = inputColumnRowType.getColumnByIndex(index);
                }
            }
            columns.add(aggCol);
        }
        return new ColumnRowType(columns);
    }

    private TblColRef buildRewriteColumn(FunctionDesc aggFunc) {
        TblColRef colRef;
        if (aggFunc.needRewrite()) {
            ColumnDesc column = new ColumnDesc();
            column.setName(aggFunc.getRewriteFieldName());
            TableDesc table = this.context.firstTableScan.getOlapTable().getSourceTable();
            column.setTable(table);
            colRef = new TblColRef(column);
        } else {
            throw new IllegalStateException("buildRewriteColumn on a aggrFunc that does not need rewrite " + aggFunc);
        }
        return colRef;
    }

    private void buildGroups() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        this.groups = new ArrayList<TblColRef>();
        for (int i = getGroupSet().nextSetBit(0); i >= 0; i = getGroupSet().nextSetBit(i + 1)) {
            Set<TblColRef> columns = inputColumnRowType.getSourceColumnsByIndex(i);
            this.groups.addAll(columns);
        }
    }

    private void buildAggregations() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        this.aggregations = new ArrayList<FunctionDesc>();
        for (AggregateCall aggCall : this.rewriteAggCalls) {
            ParameterDesc parameter = null;
            if (!aggCall.getArgList().isEmpty()) {
                int index = aggCall.getArgList().get(0);
                TblColRef column = inputColumnRowType.getColumnByIndex(index);
                if (!column.isInnerColumn()) {
                    parameter = new ParameterDesc();
                    parameter.setValue(column.getName());
                    parameter.setType("column");
                    parameter.setColRefs(Arrays.asList(column));
                }
            }
            FunctionDesc aggFunc = new FunctionDesc();
            String funcName = getFuncName(aggCall);
            aggFunc.setExpression(funcName);
            aggFunc.setParameter(parameter);
            this.aggregations.add(aggFunc);
        }
    }

    private void translateGroupBy() {
        context.groupByColumns.addAll(this.groups);
    }

    private void translateAggregation() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        for (int i = 0; i < this.aggregations.size(); i++) {
            FunctionDesc aggFunc = this.aggregations.get(i);
            context.aggregations.add(aggFunc);
            if (aggFunc.needRewrite()) {
                String rewriteFieldName = aggFunc.getRewriteFieldName();
                context.rewriteFields.put(rewriteFieldName, null);

                TblColRef column = buildRewriteColumn(aggFunc);
                this.context.metricsColumns.add(column);
            }

            AggregateCall aggCall = this.rewriteAggCalls.get(i);
            if (!aggCall.getArgList().isEmpty()) {
                int index = aggCall.getArgList().get(0);
                TblColRef column = inputColumnRowType.getColumnByIndex(index);
                if (!column.isInnerColumn()) {
                    this.context.metricsColumns.add(column);
                }
            }
        }
    }

    private void fillbackOptimizedColumn() {
        // some aggcall will be optimized out in sub-query (e.g. tableau generated sql), we need to fill them back
        RelDataType inputAggRow = getInput().getRowType();
        RelDataType outputAggRow = getRowType();
        if (inputAggRow.getFieldCount() != outputAggRow.getFieldCount()) {
            for (RelDataTypeField inputField : inputAggRow.getFieldList()) {
                String inputFieldName = inputField.getName();
                if (outputAggRow.getField(inputFieldName, true, false) == null) {
                    TblColRef column = this.columnRowType.getColumnByIndex(inputField.getIndex());
                    this.context.metricsColumns.add(column);
                }
            }
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        // only rewrite the first aggregation
        if (!this.afterAggregate && RewriteImplementor.needRewrite(this.context)) {
            // rewrite the aggCalls
            this.rewriteAggCalls = new ArrayList<AggregateCall>(aggCalls.size());
            for (int i = 0; i < this.aggCalls.size(); i++) {
                AggregateCall aggCall = this.aggCalls.get(i);
                FunctionDesc cubeFunc = this.context.aggregations.get(i);
                if (cubeFunc.needRewrite()) {
                    aggCall = rewriteAggregateCall(aggCall, cubeFunc);
                }
                this.rewriteAggCalls.add(aggCall);
            }
        }

        // rebuild rowType & columnRowType
        //ClassUtil.updateFinalField(Aggregate.class, "aggCalls", this, rewriteAggCalls);
        this.rowType = this.deriveRowType(); // this does not work coz super.aggCalls is final
        this.columnRowType = this.buildColumnRowType();

    }

    private AggregateCall rewriteAggregateCall(AggregateCall aggCall, FunctionDesc func) {

        // rebuild parameters
        List<Integer> newArgList = new ArrayList<Integer>(1);
        String fieldName = func.getRewriteFieldName();
        RelDataTypeField field = getInput().getRowType().getField(fieldName, true, false);
        newArgList.add(field.getIndex());

        // rebuild function
        RelDataType fieldType = aggCall.getType();
        SqlAggFunction newAgg = aggCall.getAggregation();
        if (func.isCountDistinct()) {
            newAgg = createHyperLogLogAggFunction(fieldType);
        } else if (func.isCount()) {
            newAgg = SqlStdOperatorTable.SUM0;
        }

        // rebuild aggregate call
        AggregateCall newAggCall = new AggregateCall(newAgg, false, newArgList, fieldType, newAgg.getName());
        return newAggCall;
    }

    private SqlAggFunction createHyperLogLogAggFunction(RelDataType returnType) {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        SqlIdentifier sqlIdentifier = new SqlIdentifier("HLL_COUNT", new SqlParserPos(1, 1));
        AggregateFunction aggFunction = AggregateFunctionImpl.create(HLLDistinctCountAggFunc.class);
        List<RelDataType> argTypes = new ArrayList<RelDataType>();
        List<SqlTypeFamily> typeFamilies = new ArrayList<SqlTypeFamily>();
        for (FunctionParameter o : aggFunction.getParameters()) {
            final RelDataType type = o.getType(typeFactory);
            argTypes.add(type);
            typeFamilies.add(Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
        }
        return new SqlUserDefinedAggFunction(sqlIdentifier, ReturnTypes.explicit(returnType), InferTypes.explicit(argTypes), OperandTypes.family(typeFamilies), aggFunction);
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        try {
            return new EnumerableAggregate(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                    sole(inputs), false, this.groupSet, this.groupSets, rewriteAggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create EnumerableAggregate!", e);
        }
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
}
