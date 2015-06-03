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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.AggregateFunction;
import net.hydromatic.optiq.FunctionParameter;
import net.hydromatic.optiq.impl.AggregateFunctionImpl;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.JavaRules.EnumerableAggregateRel;

import org.apache.kylin.query.sqlfunc.HLLDistinctCountAggFunc;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.Aggregation;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTrait;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.SqlAggFunction;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.InferTypes;
import org.eigenbase.sql.type.OperandTypes;
import org.eigenbase.sql.type.ReturnTypes;
import org.eigenbase.sql.type.SqlTypeFamily;
import org.eigenbase.sql.validate.SqlUserDefinedAggFunction;
import org.eigenbase.util.Util;

import com.google.common.base.Preconditions;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author xjiang
 */
public class OLAPAggregateRel extends AggregateRelBase implements OLAPRel, EnumerableRel {

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

    public OLAPAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, BitSet groupSet, List<AggregateCall> aggCalls) throws InvalidRelException {
        super(cluster, traits, child, groupSet, aggCalls);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        this.afterAggregate = false;
        this.rewriteAggCalls = aggCalls;
        this.rowType = getRowType();
    }

    @Override
    public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet, List<AggregateCall> aggCalls) {
        try {
            return new OLAPAggregateRel(getCluster(), traitSet, input, groupSet, aggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OLAPAggregateRel!", e);
        }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        double factor = .5;
        for (AggregateCall aggCall : aggCalls) {
            if ("$SUM0".equals(aggCall.getAggregation().getName())) {
                factor = .2;
            }
        }
        return super.computeSelfCost(planner).multiplyBy(factor);
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {

        implementor.visitChild(getChild(), this);

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

        ColumnRowType inputColumnRowType = ((OLAPRel) getChild()).getColumnRowType();
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
        TblColRef colRef = null;
        if (aggFunc.needRewrite()) {
            ColumnDesc column = new ColumnDesc();
            column.setName(aggFunc.getRewriteFieldName());
            TableDesc table = this.context.firstTableScan.getOlapTable().getSourceTable();
            column.setTable(table);
            colRef = new TblColRef(column);
        }
        return colRef;
    }

    private void buildGroups() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getChild()).getColumnRowType();
        this.groups = new ArrayList<TblColRef>();
        for (int i = getGroupSet().nextSetBit(0); i >= 0; i = getGroupSet().nextSetBit(i + 1)) {
            Set<TblColRef> columns = inputColumnRowType.getSourceColumnsByIndex(i);
            this.groups.addAll(columns);
        }
    }

    private void buildAggregations() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getChild()).getColumnRowType();
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
        ColumnRowType inputColumnRowType = ((OLAPRel) getChild()).getColumnRowType();
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
        // some aggcall will be optimized out in sub-query (e.g. tableau generated sql)
        // we need to fill them back
        RelDataType inputAggRow = getChild().getRowType();
        RelDataType outputAggRow = getRowType();
        if (inputAggRow.getFieldCount() != outputAggRow.getFieldCount()) {
            for (RelDataTypeField inputField : inputAggRow.getFieldList()) {
                String inputFieldName = inputField.getName();
                if (outputAggRow.getField(inputFieldName, true) == null) {
                    TblColRef column = this.columnRowType.getColumnByIndex(inputField.getIndex());
                    this.context.metricsColumns.add(column);
                }
            }
        }
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getChild());

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
        this.rowType = this.deriveRowType();
        this.columnRowType = this.buildColumnRowType();

    }

    private AggregateCall rewriteAggregateCall(AggregateCall aggCall, FunctionDesc func) {

        // rebuild parameters
        List<Integer> newArgList = new ArrayList<Integer>(1);
        String fieldName = func.getRewriteFieldName();
        RelDataTypeField field = getChild().getRowType().getField(fieldName, true);
        newArgList.add(field.getIndex());

        // rebuild function
        RelDataType fieldType = aggCall.getType();
        Aggregation newAgg = aggCall.getAggregation();
        if (func.isCountDistinct()) {
            newAgg = createHyperLogLogAggFunction(fieldType);
        } else if (func.isCount()) {
            //newAgg = new SqlSumEmptyIsZeroAggFunction(fieldType);
            newAgg = SqlStdOperatorTable.SUM0;
        }

        // rebuild aggregate call
        AggregateCall newAggCall = new AggregateCall(newAgg, false, newArgList, fieldType, newAgg.getName());

        // To make sure specified type matches the inferReturnType, or otherwise
        // there will be assertion failure in optiq
        // The problem is BIGINT != BIGINT NOT NULL
        // Details see https://github.scm.corp.ebay.com/Kylin/Kylin/issues/323
        SqlAggFunction aggFunction = (SqlAggFunction) newAggCall.getAggregation();
        AggCallBinding callBinding = newAggCall.createBinding(this);
        RelDataType inferReturnType = aggFunction.inferReturnType(callBinding);

        return new AggregateCall(newAgg, false, newArgList, inferReturnType, newAgg.getName());
    }

    private Aggregation createHyperLogLogAggFunction(RelDataType returnType) {
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
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {

        EnumerableAggregateRel enumAggRel;
        try {
            enumAggRel = new EnumerableAggregateRel(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), getChild(), this.groupSet, rewriteAggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create EnumerableAggregateRel!", e);
        }

        return enumAggRel.implement(implementor, pref);
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
