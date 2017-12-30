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

import java.lang.reflect.Method;
import java.util.ArrayList;
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
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.ParamAsMeasureCount;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest.SQLCall;
import org.apache.kylin.query.schema.OLAPTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
public class OLAPAggregateRel extends Aggregate implements OLAPRel {

    final static Map<String, String> AGGR_FUNC_MAP = new HashMap<String, String>();
    final static Map<String, Integer> AGGR_FUNC_PARAM_AS_MEASURE_MAP = new HashMap<String, Integer>();

    static {
        AGGR_FUNC_MAP.put("SUM", "SUM");
        AGGR_FUNC_MAP.put("$SUM0", "SUM");
        AGGR_FUNC_MAP.put("COUNT", "COUNT");
        AGGR_FUNC_MAP.put("COUNT_DISTINCT", "COUNT_DISTINCT");
        AGGR_FUNC_MAP.put("MAX", "MAX");
        AGGR_FUNC_MAP.put("MIN", "MIN");

        Map<String, MeasureTypeFactory> udafFactories = MeasureTypeFactory.getUDAFFactories();
        for (Map.Entry<String, MeasureTypeFactory> entry : udafFactories.entrySet()) {
            AGGR_FUNC_MAP.put(entry.getKey(), entry.getValue().getAggrFunctionName());
        }

        Map<String, Class<?>> udafs = MeasureTypeFactory.getUDAFs();
        for (String func : udafs.keySet()) {
            try {
                AGGR_FUNC_PARAM_AS_MEASURE_MAP.put(func,
                        ((ParamAsMeasureCount) (udafs.get(func).newInstance())).getParamAsMeasureCount());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    static String getSqlFuncName(AggregateCall aggCall) {
        String sqlName = aggCall.getAggregation().getName();
        if (aggCall.isDistinct()) {
            sqlName = sqlName + "_DISTINCT";
        }
        return sqlName;
    }

    public static String getAggrFuncName(AggregateCall aggCall) {
        String sqlName = getSqlFuncName(aggCall);
        String funcName = AGGR_FUNC_MAP.get(sqlName);
        if (funcName == null) {
            throw new IllegalStateException("Non-support aggregation " + sqlName);
        }
        return funcName;
    }

    OLAPContext context;
    ColumnRowType columnRowType;
    private boolean afterAggregate;
    private List<AggregateCall> rewriteAggCalls;
    private List<TblColRef> groups;
    private List<FunctionDesc> aggregations;

    public OLAPAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator,
            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
            throws InvalidRelException {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        this.afterAggregate = false;
        this.rewriteAggCalls = aggCalls;
        this.rowType = getRowType();
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        try {
            return new OLAPAggregateRel(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OLAPAggregateRel!", e);
        }
    }

    /**
     * Since the grouping aggregate will be expanded by {@link org.apache.kylin.query.optrule.AggregateMultipleExpandRule},
     * made the cost of grouping aggregate more expensive to use the expanded aggregates
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost cost;
        if (getGroupType() == Group.SIMPLE) {
            cost = super.computeSelfCost(planner, mq).multiplyBy(.05);
        } else {
            cost = super.computeSelfCost(planner, mq).multiplyBy(.05).plus(planner.getCost(getInput(), mq))
                    .multiplyBy(groupSets.size() * 1.5);
        }
        return cost;
    }

    @Override
    public void implementOLAP(OLAPImplementor implementor) {
        implementor.fixSharedOlapTableScan(this);
        implementor.visitChild(getInput(), this);

        this.context = implementor.getContext();
        this.columnRowType = buildColumnRowType();
        this.afterAggregate = this.context.afterAggregate;

        // only translate the innermost aggregation
        if (!this.afterAggregate) {
            addToContextGroupBy(this.groups);
            this.context.aggregations.addAll(this.aggregations);
            this.context.aggrOutCols
                    .addAll(columnRowType.getAllColumns().subList(groups.size(), columnRowType.getAllColumns().size()));
            this.context.afterAggregate = true;

            if (this.context.afterLimit) {
                this.context.limitPrecedesAggr = true;
            }
        } else {
            for (AggregateCall aggCall : aggCalls) {
                // check if supported by kylin
                if (aggCall.isDistinct()) {
                    throw new IllegalStateException("Distinct count is only allowed in innermost sub-query.");
                }
            }
        }
    }

    ColumnRowType buildColumnRowType() {
        buildGroups();
        buildAggregations();

        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        List<TblColRef> columns = new ArrayList<TblColRef>(this.rowType.getFieldCount());
        columns.addAll(this.groups);

        // Add group column indicators
        if (indicator) {
            final Set<String> containedNames = Sets.newHashSet();
            for (TblColRef groupCol : groups) {
                String base = "i$" + groupCol.getName();
                String name = base;
                int i = 0;
                while (containedNames.contains(name)) {
                    name = base + "_" + i++;
                }
                containedNames.add(name);
                TblColRef indicatorCol = TblColRef.newInnerColumn(name, TblColRef.InnerDataTypeEnum.LITERAL);
                columns.add(indicatorCol);
            }
        }

        for (int i = 0; i < this.aggregations.size(); i++) {
            FunctionDesc aggFunc = this.aggregations.get(i);
            String aggOutName;
            if (aggFunc != null) {
                aggOutName = aggFunc.getRewriteFieldName();
            } else {
                AggregateCall aggCall = this.rewriteAggCalls.get(i);
                int index = aggCall.getArgList().get(0);
                aggOutName = getSqlFuncName(aggCall) + "_"
                        + inputColumnRowType.getColumnByIndex(index).getIdentity().replace('.', '_') + "_";
            }
            TblColRef aggOutCol = TblColRef.newInnerColumn(aggOutName, TblColRef.InnerDataTypeEnum.LITERAL);
            aggOutCol.getColumnDesc().setId("" + (i + 1)); // mark the index of aggregation
            columns.add(aggOutCol);
        }
        return new ColumnRowType(columns);
    }

    TblColRef buildRewriteColumn(FunctionDesc aggFunc) {
        TblColRef colRef;
        if (aggFunc.needRewriteField()) {
            String colName = aggFunc.getRewriteFieldName();
            colRef = this.context.firstTableScan.makeRewriteColumn(colName);
        } else {
            throw new IllegalStateException("buildRewriteColumn on a aggrFunc that does not need rewrite " + aggFunc);
        }
        return colRef;
    }

    void buildGroups() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        this.groups = new ArrayList<TblColRef>();
        for (int i = getGroupSet().nextSetBit(0); i >= 0; i = getGroupSet().nextSetBit(i + 1)) {
            Set<TblColRef> columns = inputColumnRowType.getSourceColumnsByIndex(i);
            this.groups.addAll(columns);
        }
    }

    void buildAggregations() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        this.aggregations = new ArrayList<FunctionDesc>();
        for (AggregateCall aggCall : this.rewriteAggCalls) {
            ParameterDesc parameter = null;
            // By default all args are included, UDFs can define their own in getParamAsMeasureCount method.
            if (!aggCall.getArgList().isEmpty()) {
                List<TblColRef> columns = Lists.newArrayList();
                String funcName = getSqlFuncName(aggCall);
                int columnsCount = aggCall.getArgList().size();
                if (AGGR_FUNC_PARAM_AS_MEASURE_MAP.containsKey(funcName)) {
                    int asMeasureCnt = AGGR_FUNC_PARAM_AS_MEASURE_MAP.get(funcName);
                    if (asMeasureCnt > 0) {
                        columnsCount = asMeasureCnt;
                    } else {
                        columnsCount += asMeasureCnt;
                    }
                }
                for (Integer index : aggCall.getArgList().subList(0, columnsCount)) {
                    TblColRef column = inputColumnRowType.getColumnByIndex(index);
                    columns.add(column);
                }
                if (!columns.isEmpty()) {
                    parameter = ParameterDesc.newInstance(columns.toArray(new TblColRef[columns.size()]));
                }
            }
            String expression = getAggrFuncName(aggCall);
            FunctionDesc aggFunc = FunctionDesc.newInstance(expression, parameter, null);
            this.aggregations.add(aggFunc);
        }
    }

    public boolean needRewrite() {
        boolean hasRealization = (null != this.context.realization);
        return hasRealization && !this.afterAggregate;
    }

    @Override
    public void implementRewrite(RewriteImplementor implementor) {
        // only rewrite the innermost aggregation
        if (needRewrite()) {
            translateAggregation();
            buildRewriteFieldsAndMetricsColumns();
        }

        implementor.visitChild(this, getInput());
        // only rewrite the innermost aggregation
        if (needRewrite()) {
            // rewrite the aggCalls
            this.rewriteAggCalls = new ArrayList<AggregateCall>(aggCalls.size());
            for (int i = 0; i < this.aggCalls.size(); i++) {
                AggregateCall aggCall = this.aggCalls.get(i);
                FunctionDesc cubeFunc = this.context.aggregations.get(i);
                // filter needn,t rewrite aggfunc
                // if it's not a cube, then the "needRewriteField func" should not resort to any rewrite fields,
                // which do not exist at all
                if (!(noPrecaculatedFieldsAvailable() && cubeFunc.needRewriteField())) {
                    if (cubeFunc.needRewrite()) {
                        aggCall = rewriteAggregateCall(aggCall, cubeFunc);
                    }

                    //if not dim as measure (using some measure), differentiate it with a new class
                    if (cubeFunc.getMeasureType() != null &&
                    // DimCountDistinct case
                            cubeFunc.getMeasureType().needRewriteField()) {
                        aggCall = new KylinAggregateCall(aggCall, cubeFunc);
                    }
                } else {
                    logger.info(aggCall + "skip rewriteAggregateCall because no pre-aggregated field available");
                }

                this.rewriteAggCalls.add(aggCall);
                this.context.aggrSqlCalls.add(toSqlCall(aggCall));
            }
        }

        // rebuild rowType & columnRowType
        this.rowType = this.deriveRowType();
        this.columnRowType = this.buildColumnRowType();

    }

    SQLCall toSqlCall(AggregateCall aggCall) {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();

        String function = getSqlFuncName(aggCall);
        List<Object> args = Lists.newArrayList();
        for (Integer index : aggCall.getArgList()) {
            TblColRef col = inputColumnRowType.getColumnByIndexNullable(index);
            args.add(col);
        }
        return new SQLCall(function, args);
    }

    void translateAggregation() {
        if (!noPrecaculatedFieldsAvailable()) {
            // now the realization is known, replace aggregations with what's defined on MeasureDesc
            List<MeasureDesc> measures = this.context.realization.getMeasures();
            List<FunctionDesc> newAggrs = Lists.newArrayList();
            for (FunctionDesc aggFunc : this.aggregations) {
                newAggrs.add(findInMeasures(aggFunc, measures));
            }
            this.aggregations.clear();
            this.aggregations.addAll(newAggrs);
            this.context.aggregations.clear();
            this.context.aggregations.addAll(newAggrs);
        } else {
            //the realization is not contributing pre-calculated fields at all
        }
    }

    FunctionDesc findInMeasures(FunctionDesc aggFunc, List<MeasureDesc> measures) {
        for (MeasureDesc m : measures) {
            if (aggFunc.equals(m.getFunction()))
                return m.getFunction();
        }
        return aggFunc;
    }

    void buildRewriteFieldsAndMetricsColumns() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        for (int i = 0; i < this.aggregations.size(); i++) {
            FunctionDesc aggFunc = this.aggregations.get(i);

            if (aggFunc.isDimensionAsMetric()) {
                addToContextGroupBy(aggFunc.getParameter().getColRefs());
                continue; // skip rewrite, let calcite handle
            }

            if (aggFunc.needRewriteField()) {
                String rewriteFieldName = aggFunc.getRewriteFieldName();
                RelDataType rewriteFieldType = OLAPTable.createSqlType(typeFactory, aggFunc.getRewriteFieldType(),
                        true);
                this.context.rewriteFields.put(rewriteFieldName, rewriteFieldType);

                TblColRef column = buildRewriteColumn(aggFunc);
                this.context.metricsColumns.add(column);
            }

            AggregateCall aggCall = this.rewriteAggCalls.get(i);
            if (!aggCall.getArgList().isEmpty()) {
                for (Integer index : aggCall.getArgList()) {
                    TblColRef column = inputColumnRowType.getColumnByIndex(index);
                    if (!column.isInnerColumn() && this.context.belongToContextTables(column)) {
                        this.context.metricsColumns.add(column);
                    }
                }
            }
        }
    }

    void addToContextGroupBy(List<TblColRef> colRefs) {
        for (TblColRef col : colRefs) {
            if (col.isInnerColumn() == false && this.context.belongToContextTables(col))
                this.context.groupByColumns.add(col);
        }
    }

    public boolean noPrecaculatedFieldsAvailable() {
        return !this.context.hasPrecalculatedFields() || !RewriteImplementor.needRewrite(this.context);
    }

    @SuppressWarnings("deprecation")
    private AggregateCall rewriteAggregateCall(AggregateCall aggCall, FunctionDesc func) {
        // rebuild function
        String callName = getSqlFuncName(aggCall);
        RelDataType fieldType = aggCall.getType();
        SqlAggFunction newAgg = aggCall.getAggregation();
        
        Map<String, Class<?>> udafMap = func.getMeasureType().getRewriteCalciteAggrFunctions();
        if (func.isCount()) {
            newAgg = SqlStdOperatorTable.SUM0;
        } else if (udafMap != null && udafMap.containsKey(callName)) {
            newAgg = createCustomAggFunction(callName, fieldType, udafMap.get(callName));
        }

        // rebuild parameters
        List<Integer> newArgList = Lists.newArrayList(aggCall.getArgList());
        if (udafMap != null && udafMap.containsKey(callName)) {
            newArgList = truncArgList(newArgList, udafMap.get(callName));
        }
        if (func.needRewriteField()) {
            RelDataTypeField field = getInput().getRowType().getField(func.getRewriteFieldName(), true, false);
            if (newArgList.isEmpty()) {
                newArgList.add(field.getIndex());
            } else {
                // TODO: only the first column got overwritten
                newArgList.set(0, field.getIndex());
            }
        }

        // rebuild aggregate call
        AggregateCall newAggCall = new AggregateCall(newAgg, false, newArgList, fieldType, callName);

        return newAggCall;
    }

    /**
     * truncate Arg List according to UDAF's "add" method parameter count
     */
    List<Integer> truncArgList(List<Integer> argList, Class<?> udafClazz) {
        int argListLength = argList.size();
        for (Method method : udafClazz.getMethods()) {
            if (method.getName().equals("add")) {
                argListLength = Math.min(method.getParameterTypes().length - 1, argListLength);
            }
        }
        return argList.subList(0, argListLength);
    }

    SqlAggFunction createCustomAggFunction(String funcName, RelDataType returnType, Class<?> customAggFuncClz) {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        SqlIdentifier sqlIdentifier = new SqlIdentifier(funcName, new SqlParserPos(1, 1));
        AggregateFunction aggFunction = AggregateFunctionImpl.create(customAggFuncClz);
        List<RelDataType> argTypes = new ArrayList<RelDataType>();
        List<SqlTypeFamily> typeFamilies = new ArrayList<SqlTypeFamily>();
        for (FunctionParameter o : aggFunction.getParameters()) {
            final RelDataType type = o.getType(typeFactory);
            argTypes.add(type);
            typeFamilies.add(Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
        }
        return new SqlUserDefinedAggFunction(sqlIdentifier, ReturnTypes.explicit(returnType),
                InferTypes.explicit(argTypes), OperandTypes.family(typeFamilies), aggFunction, false, false);
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        try {
            return new EnumerableAggregate(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                    sole(inputs), indicator, this.groupSet, this.groupSets, rewriteAggCalls);
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

    public List<AggregateCall> getRewriteAggCalls() {
        return rewriteAggCalls;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("ctx",
                context == null ? "" : String.valueOf(context.id) + "@" + context.realization);
    }
}
