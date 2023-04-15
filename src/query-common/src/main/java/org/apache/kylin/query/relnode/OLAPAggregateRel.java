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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.schema.OLAPTable;

/**
 */
public class OLAPAggregateRel extends Aggregate implements OLAPRel {

    final static Map<String, String> AGGR_FUNC_MAP = new HashMap<>();

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
        AGGR_FUNC_MAP.put("BITMAP_UUID", "BITMAP_UUID");
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_BITMAP_BUILD, FunctionDesc.FUNC_BITMAP_BUILD);
    }

    protected OLAPContext context;
    protected ColumnRowType columnRowType;
    protected boolean afterAggregate;
    protected List<AggregateCall> rewriteAggCalls;
    protected List<TblColRef> groups;
    protected List<FunctionDesc> aggregations;

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
        // issue 4337
        if (SqlKind.SINGLE_VALUE == aggCall.getAggregation().kind) {
            return SqlKind.SINGLE_VALUE.sql;
        }
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
     * Since the grouping aggregate will be expanded by {org.apache.kylin.query.optrule.AggregateMultipleExpandRule},
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

    public ColumnRowType buildColumnRowType() {
        buildGroups();
        buildAggregations();

        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        List<TblColRef> columns = Lists.newArrayListWithCapacity(this.rowType.getFieldCount());
        columns.addAll(getGroupColsOfColumnRowType());

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
            List<TblColRef> operands = Lists.newArrayList();
            if (aggFunc != null) {
                operands.addAll(aggFunc.getColRefs());
                aggOutName = aggFunc.getRewriteFieldName();
            } else {
                AggregateCall aggCall = this.rewriteAggCalls.get(i);
                int index = aggCall.getArgList().get(0);
                aggOutName = getSqlFuncName(aggCall) + "_"
                        + inputColumnRowType.getColumnByIndex(index).getIdentity().replace('.', '_') + "_";
                aggCall.getArgList().forEach(argIndex -> operands.add(inputColumnRowType.getColumnByIndex(argIndex)));
            }
            TblColRef aggOutCol = TblColRef.newInnerColumn(aggOutName, TblColRef.InnerDataTypeEnum.AGGREGATION_TYPE);
            aggOutCol.setOperator(this.rewriteAggCalls.get(i).getAggregation());
            aggOutCol.setOperands(operands);
            aggOutCol.getColumnDesc().setId("" + (i + 1)); // mark the index of aggregation
            columns.add(aggOutCol);
        }
        Preconditions.checkState(columns.size() == this.rowType.getFieldCount());
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

    protected void buildGroups() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        this.groups = new ArrayList<>();
        for (int i = getGroupSet().nextSetBit(0); i >= 0; i = getGroupSet().nextSetBit(i + 1)) {
            Set<TblColRef> columns = inputColumnRowType.getSourceColumnsByIndex(i);
            this.groups.addAll(columns);
        }
    }

    public List<TblColRef> getGroupColsOfColumnRowType() {
        List<TblColRef> allColumns = Lists.newArrayList();
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        for (int i = getGroupSet().nextSetBit(0); i >= 0; i = getGroupSet().nextSetBit(i + 1)) {
            TblColRef tblColRef = inputColumnRowType.getColumnByIndex(i);
            allColumns.add(tblColRef);
        }
        return allColumns;
    }

    void buildAggregations() {

        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        this.aggregations = new ArrayList<>();
        for (AggregateCall aggCall : this.rewriteAggCalls) {
            List<ParameterDesc> parameters = Lists.newArrayList();
            // By default all args are included, UDFs can define their own in getParamAsMeasureCount method.
            if (!aggCall.getArgList().isEmpty()) {
                List<TblColRef> columns = Lists.newArrayList();
                List<Integer> args = Lists.newArrayList();
                if (PercentileMeasureType.FUNC_PERCENTILE.equals(getSqlFuncName(aggCall))
                        || PercentileMeasureType.FUNC_PERCENTILE_APPROX.equals(getSqlFuncName(aggCall))) {
                    args.add(aggCall.getArgList().get(0));
                } else {
                    args = aggCall.getArgList();
                }
                for (Integer index : args) {
                    TblColRef column = inputColumnRowType.getColumnByIndex(index);
                    if (FunctionDesc.FUNC_SUM.equals(getSqlFuncName(aggCall))) {
                        column = rewriteCastInSumIfNecessary(aggCall, inputColumnRowType, index);
                    }
                    columns.add(column);
                }
                if (!columns.isEmpty()) {
                    columns.forEach(column -> parameters.add(ParameterDesc.newInstance(column)));
                }
            }

            String expression = getAggrFuncName(aggCall);
            FunctionDesc aggFunc = FunctionDesc.newInstance(expression, parameters, null);
            this.aggregations.add(aggFunc);
        }
    }

    private TblColRef rewriteCastInSumIfNecessary(AggregateCall aggCall, ColumnRowType inputColumnRowType,
            Integer index) {
        // ISSUE #7294, for case like sum({fn CONVERT(ITEM_COUNT, SQL_BIGINT)})
        // remove the cast by rewriting input project, such that the sum can hit cube
        TblColRef column = inputColumnRowType.getColumnByIndex(index);
        if (getInput() instanceof OLAPProjectRel && SqlTypeUtil.isBigint(aggCall.type) && column.isCastInnerColumn()) {
            TblColRef innerColumn = column.getOperands().get(0);
            if (!innerColumn.isInnerColumn() && innerColumn.getType().isIntegerFamily()) {
                inputColumnRowType.getAllColumns().set(index, innerColumn);
                column = inputColumnRowType.getColumnByIndex(index);
            }
        }
        return column;
    }

    public boolean needRewrite() {
        return this.context.realization != null && !this.afterAggregate;
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
            this.rewriteAggCalls = new ArrayList<>(aggCalls.size());
            for (int i = 0; i < this.aggCalls.size(); i++) {
                AggregateCall aggCall = this.aggCalls.get(i);
                if (SqlStdOperatorTable.GROUPING == aggCall.getAggregation()) {
                    this.rewriteAggCalls.add(aggCall);
                    continue;
                }

                FunctionDesc cubeFunc = this.context.aggregations.get(i);
                aggCall = rewriteAggCall(aggCall, cubeFunc);
                this.rewriteAggCalls.add(aggCall);
            }
        }

        // rebuild rowType & columnRowType
        this.rowType = this.deriveRowType();
        this.columnRowType = this.buildColumnRowType();
    }

    protected AggregateCall rewriteAggCall(AggregateCall aggCall, FunctionDesc cubeFunc) {
        // filter needn,t rewrite aggfunc
        // if it's not a cube, then the "needRewriteField func" should not resort to any rewrite fields,
        // which do not exist at all
        if (!(noPrecaculatedFieldsAvailable() && cubeFunc.needRewriteField())) {
            if (cubeFunc.needRewrite()) {
                aggCall = rewriteAggregateCall(aggCall, cubeFunc);
            }

            //if not dim as measure (using some measure), differentiate it with a new class
            if (cubeFunc.getMeasureType() != null
                    // DimCountDistinct case
                    && cubeFunc.getMeasureType().needRewriteField()) {
                aggCall = new KylinAggregateCall(aggCall, cubeFunc);
            }
        } else {
            logger.info(aggCall + "skip rewriteAggregateCall because no pre-aggregated field available");
        }

        return aggCall;
    }

    protected void translateAggregation() {
        if (!noPrecaculatedFieldsAvailable()) {
            // now the realization is known, replace aggregations with what's defined on MeasureDesc
            List<MeasureDesc> measures = this.context.realization.getMeasures();
            List<FunctionDesc> newAggrs = Lists.newArrayList();
            for (FunctionDesc aggFunc : this.aggregations) {
                if (aggFunc.isDimensionAsMetric()) {
                    newAggrs.add(aggFunc);
                    continue;
                }
                FunctionDesc newAgg = findInMeasures(aggFunc, measures);
                if (newAgg == null && aggFunc.isCountOnColumn()
                        && this.context.realization.getConfig().isReplaceColCountWithCountStar()) {
                    newAgg = FunctionDesc.newCountOne();
                }
                if (newAgg == null) {
                    newAgg = aggFunc;
                }
                newAggrs.add(newAgg);
            }
            this.aggregations.clear();
            this.aggregations.addAll(newAggrs);
            this.context.aggregations.clear();
            for (FunctionDesc agg : aggregations) {
                if (!agg.isAggregateOnConstant())
                    this.context.aggregations.add(agg);
            }
        } else {
            //the realization is not contributing pre-calculated fields at all
        }
    }

    private FunctionDesc findInMeasures(FunctionDesc aggFunc, List<MeasureDesc> measures) {
        for (MeasureDesc m : measures) {
            if (aggFunc.equals(m.getFunction()))
                return m.getFunction();
        }
        // try replace advance measure, like topn
        for (MeasureDesc m : measures) {
            if (m.getFunction().getMeasureType() instanceof BasicMeasureType)
                continue;

            if (m.getFunction().getMeasureType() instanceof TopNMeasureType) {
                FunctionDesc internalTopn = TopNMeasureType.getTopnInternalMeasure(m.getFunction());
                if (aggFunc.equals(internalTopn))
                    return internalTopn;
            }
            if (FunctionDesc.FUNC_INTERSECT_COUNT.equalsIgnoreCase(aggFunc.getExpression())
                    || FunctionDesc.FUNC_BITMAP_UUID.equalsIgnoreCase(aggFunc.getExpression())
                    || FunctionDesc.FUNC_BITMAP_BUILD.equalsIgnoreCase(aggFunc.getExpression())) {
                if (m.getFunction().getReturnType().equals("bitmap")
                        && aggFunc.getParameters().get(0).equals(m.getFunction().getParameters().get(0)))
                    return m.getFunction();
            }
        }
        return null;
    }

    protected void buildRewriteFieldsAndMetricsColumns() {
        ColumnRowType inputColumnRowType = ((OLAPRel) getInput()).getColumnRowType();
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        for (int i = 0; i < this.aggregations.size(); i++) {
            FunctionDesc aggFunc = this.aggregations.get(i);
            if (aggFunc.isDimensionAsMetric()) {
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

    protected void addToContextGroupBy(List<TblColRef> colRefs) {
        for (TblColRef col : colRefs) {
            if (!col.isInnerColumn() && this.context.belongToContextTables(col))
                this.context.getGroupByColumns().add(col);
        }
    }

    public boolean noPrecaculatedFieldsAvailable() {
        return !this.context.hasPrecalculatedFields() || !RewriteImplementor.needRewrite(this.context);
    }

    @SuppressWarnings("deprecation")
    protected AggregateCall rewriteAggregateCall(AggregateCall aggCall, FunctionDesc func) {
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
        return new AggregateCall(newAgg, false, newArgList, fieldType, callName);
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
        List<RelDataType> argTypes = new ArrayList<>();
        List<SqlTypeFamily> typeFamilies = new ArrayList<>();
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
        return super.explainTerms(pw).item("ctx", context == null ? "" : context.id + "@" + context.realization);
    }

    public List<TblColRef> getGroups() {
        return groups;
    }

    public void setGroups(List<TblColRef> groups) {
        this.groups = groups;
    }
}
