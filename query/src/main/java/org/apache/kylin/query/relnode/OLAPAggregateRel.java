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

import static org.apache.kylin.metadata.expression.TupleExpression.ExpressionOperatorEnum.COLUMN;

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
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.ParamAsMeasureCount;
import org.apache.kylin.metadata.expression.CaseTupleExpression;
import org.apache.kylin.metadata.expression.ColumnTupleExpression;
import org.apache.kylin.metadata.expression.ExpressionColCollector;
import org.apache.kylin.metadata.expression.ExpressionCountDistributor;
import org.apache.kylin.metadata.expression.NumberTupleExpression;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.DynamicFunctionDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.SumDynamicFunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest.SQLCall;
import org.apache.kylin.query.schema.OLAPTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
        AGGR_FUNC_MAP.put("GROUPING", "GROUPING");

        Map<String, MeasureTypeFactory> udafFactories = MeasureTypeFactory.getUDAFFactories();
        for (Map.Entry<String, MeasureTypeFactory> entry : udafFactories.entrySet()) {
            AGGR_FUNC_MAP.put(entry.getKey(), entry.getValue().getAggrFunctionName());
        }

        Map<String, Class<?>> udafs = MeasureTypeFactory.getUDAFs();
        for (String func : udafs.keySet()) {
            try {
                AGGR_FUNC_PARAM_AS_MEASURE_MAP.put(func,
                        ((ParamAsMeasureCount) (udafs.get(func).getDeclaredConstructor().newInstance()))
                                .getParamAsMeasureCount());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    OLAPContext context;
    ColumnRowType columnRowType;
    private boolean afterAggregate;
    private Map<Integer, AggregateCall> hackAggCalls;
    private List<AggregateCall> rewriteAggCalls;
    private List<TblColRef> groups;
    private List<FunctionDesc> aggregations;
    private boolean rewriting;

    public OLAPAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator,
            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls)
            throws InvalidRelException {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        Preconditions.checkArgument(getConvention() == OLAPRel.CONVENTION);
        this.afterAggregate = false;
        this.rewriteAggCalls = aggCalls;
        this.rowType = getRowType();
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
            this.context.afterOuterAggregate = true;
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
        this.groups = Lists.newArrayList();
        for (int i = getGroupSet().nextSetBit(0); i >= 0; i = getGroupSet().nextSetBit(i + 1)) {
            TupleExpression tupleExpression = inputColumnRowType.getTupleExpressionByIndex(i);

            // group by column with operator
            if (this.context.groupByExpression == false
                    && !(COLUMN.equals(tupleExpression.getOperator()) && tupleExpression.getChildren().isEmpty())) {
                this.context.groupByExpression = true;
            }

            TblColRef groupOutCol = inputColumnRowType.getColumnByIndex(i);
            if (tupleExpression instanceof ColumnTupleExpression) {
                this.groups.add(((ColumnTupleExpression) tupleExpression).getColumn());
            } else if (this.context.isDynamicColumnEnabled() && tupleExpression.ifForDynamicColumn()) {
                Pair<Set<TblColRef>, Set<TblColRef>> cols = ExpressionColCollector.collectColumnsPair(tupleExpression);

                // push down only available for the innermost aggregation
                boolean ifPushDown = !afterAggregate;

                // if measure columns exist, don't do push down
                if (ifPushDown && !cols.getSecond().isEmpty()) {
                    ifPushDown = false;
                }

                // if existing a dimension which is a derived column, don't do push down
                if (ifPushDown) {
                    for (TblColRef dimCol : cols.getFirst()) {
                        if (!this.context.belongToFactTableDims(dimCol)) {
                            ifPushDown = false;
                            break;
                        }
                    }
                }

                if (ifPushDown) {
                    this.groups.add(groupOutCol);
                    this.context.dynGroupBy.put(groupOutCol, tupleExpression);
                } else {
                    this.groups.addAll(cols.getFirst());
                    this.groups.addAll(cols.getSecond());
                    this.context.dynamicFields.remove(groupOutCol);
                }
            } else {
                Set<TblColRef> srcCols = ExpressionColCollector.collectColumns(tupleExpression);
                // if no source columns, use target column instead
                if (srcCols.isEmpty()) {
                    srcCols.add(groupOutCol);
                }
                this.groups.addAll(srcCols);
            }
        }
    }

    void buildAggregations() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        this.aggregations = Lists.newArrayList();
        this.hackAggCalls = Maps.newHashMap();
        for (int i = 0; i < this.rewriteAggCalls.size(); i++) {
            AggregateCall aggCall = this.rewriteAggCalls.get(i);
            ParameterDesc parameter = null;
            List<Integer> argList = aggCall.getArgList();
            // By default all args are included, UDFs can define their own in getParamAsMeasureCount method.
            if (!argList.isEmpty()) {
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
            // Check dynamic aggregation
            if (this.context.isDynamicColumnEnabled() && !afterAggregate && !rewriting && argList.size() == 1) {
                int iRowIdx = argList.get(0);
                TupleExpression tupleExpr = inputColumnRowType.getTupleExpressionByIndex(iRowIdx);
                if (aggCall.getAggregation() instanceof SqlSumAggFunction
                        || aggCall.getAggregation() instanceof SqlSumEmptyIsZeroAggFunction) {
                    // sum (expression)
                    if (!(tupleExpr instanceof NumberTupleExpression || tupleExpr instanceof ColumnTupleExpression)) {
                        ColumnTupleExpression cntExpr = new ColumnTupleExpression(SumDynamicFunctionDesc.mockCntCol);
                        ExpressionCountDistributor cntDistributor = new ExpressionCountDistributor(cntExpr);
                        tupleExpr = tupleExpr.accept(cntDistributor);
                        SumDynamicFunctionDesc sumDynFunc = new SumDynamicFunctionDesc(parameter, tupleExpr);
                        this.aggregations.add(sumDynFunc);
                        continue;
                    }
                } else if (aggCall.getAggregation() instanceof SqlCountAggFunction && !aggCall.isDistinct()) {
                    // count column
                    if (tupleExpr instanceof ColumnTupleExpression) {
                        TblColRef srcCol = ((ColumnTupleExpression) tupleExpr).getColumn();
                        if (this.context.belongToFactTableDims(srcCol)) {
                            tupleExpr = getCountColumnExpression(srcCol);

                            TblColRef column = TblColRef.newInnerColumn(tupleExpr.getDigest(),
                                    TblColRef.InnerDataTypeEnum.LITERAL);

                            SumDynamicFunctionDesc sumDynFunc = new SumDynamicFunctionDesc(
                                    ParameterDesc.newInstance(column), tupleExpr);

                            inputColumnRowType.replaceColumnByIndex(iRowIdx, column, tupleExpr);

                            AggregateCall newAggCall = AggregateCall.create(SqlStdOperatorTable.SUM, false,
                                    aggCall.getArgList(), -1, aggCall.getType(), aggCall.getName());
                            this.hackAggCalls.put(i, newAggCall);

                            this.context.dynamicFields.put(column, aggCall.getType());

                            this.aggregations.add(sumDynFunc);
                            continue;
                        }
                    }
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

        this.rewriting = true;

        // only rewrite the innermost aggregation
        if (needRewrite()) {
            // rewrite the aggCalls
            this.rewriteAggCalls = Lists.newArrayListWithExpectedSize(aggCalls.size());
            for (int i = 0; i < this.aggCalls.size(); i++) {
                AggregateCall aggCall = this.hackAggCalls.get(i) != null ? this.hackAggCalls.get(i)
                        : this.aggCalls.get(i);
                if (SqlStdOperatorTable.GROUPING == aggCall.getAggregation()) {
                    this.rewriteAggCalls.add(aggCall);
                    continue;
                }

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

        this.rewriting = false;
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
                if (aggFunc instanceof DynamicFunctionDesc) {
                    DynamicFunctionDesc rtAggFunc = (DynamicFunctionDesc) aggFunc;
                    Map<TblColRef, FunctionDesc> innerOldAggrs = rtAggFunc.getRuntimeFuncMap();
                    Map<TblColRef, FunctionDesc> innerNewAggrs = Maps.newHashMapWithExpectedSize(innerOldAggrs.size());
                    for (TblColRef key : innerOldAggrs.keySet()) {
                        innerNewAggrs.put(key, findInMeasures(innerOldAggrs.get(key), measures));
                    }
                    rtAggFunc.setRuntimeFuncMap(innerNewAggrs);
                    newAggrs.add(rtAggFunc);
                } else {
                    newAggrs.add(findInMeasures(aggFunc, measures));
                }
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
            if (aggFunc.equals(m.getFunction())) {
                return m.getFunction();
            }
        }

        // no count(col) measure found, use count(1) to replace it.
        if (aggFunc.isCount()) {
            FunctionDesc func = findCountConstantFunc(measures);
            if (func != null)
                return func;
        }

        return aggFunc;
    }

    private FunctionDesc findCountConstantFunc(List<MeasureDesc> measures) {
        for (MeasureDesc measure : measures) {
            if (measure.getFunction().isCountConstant()) {
                return measure.getFunction();
            }
        }
        return null;
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
            if (!col.isInnerColumn() && this.context.belongToContextTables(col))
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
                InferTypes.explicit(argTypes), OperandTypes.family(typeFamilies), aggFunction, false, false,
                typeFactory);
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

    private TupleExpression getCountColumnExpression(TblColRef colRef) {
        List<Pair<TupleFilter, TupleExpression>> whenList = Lists.newArrayListWithExpectedSize(1);
        TupleFilter whenFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.ISNULL);
        whenFilter.addChild(new ColumnTupleFilter(colRef));
        whenList.add(new Pair<TupleFilter, TupleExpression>(whenFilter, new NumberTupleExpression(0)));

        TupleExpression elseExpr = new ColumnTupleExpression(SumDynamicFunctionDesc.mockCntCol);
        TupleExpression ret = new CaseTupleExpression(whenList, elseExpr);
        ret.setDigest("_KY_COUNT(" + colRef.getName() + ")");
        return ret;
    }

    public List<TblColRef> getGroups() {
        return groups;
    }

    public void setGroups(List<TblColRef> groups) {
        this.groups = groups;
    }
}
