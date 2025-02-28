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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Hints;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.measure.corr.CorrMeasureType;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.schema.OlapTable;
import org.apache.kylin.query.util.ICutContextStrategy;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
public class OlapAggregateRel extends Aggregate implements OlapRel {

    private static final Map<String, String> AGGR_FUNC_MAP = new HashMap<>();
    // why PERCENTILE_APPROX also be considered?
    public static final Set<String> SUPPORTED_FUNCTIONS = ImmutableSet.of(FunctionDesc.FUNC_SUM, FunctionDesc.FUNC_MIN,
            FunctionDesc.FUNC_MAX, FunctionDesc.FUNC_COUNT_DISTINCT, FunctionDesc.FUNC_BITMAP_UUID,
            FunctionDesc.FUNC_PERCENTILE, FunctionDesc.FUNC_BITMAP_BUILD, FunctionDesc.FUNC_SUM_LC);

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
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_BITMAP_UUID, FunctionDesc.FUNC_BITMAP_UUID);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_BITMAP_BUILD, FunctionDesc.FUNC_BITMAP_BUILD);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_DISTINCT,
                FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_DISTINCT);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_COUNT, FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_COUNT);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_VALUE, FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_VALUE);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_VALUE_ALL,
                FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_VALUE_ALL);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_UNION_BITMAP_UUID_DISTINCT, FunctionDesc.FUNC_UNION_BITMAP_UUID_DISTINCT);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_UNION_BITMAP_UUID_COUNT, FunctionDesc.FUNC_UNION_BITMAP_UUID_COUNT);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_UNION_BITMAP_UUID_VALUE, FunctionDesc.FUNC_UNION_BITMAP_UUID_VALUE);
        AGGR_FUNC_MAP.put(FunctionDesc.FUNC_UNION_BITMAP_UUID_VALUE_ALL, FunctionDesc.FUNC_UNION_BITMAP_UUID_VALUE_ALL);
    }

    private OlapContext context;
    private ColumnRowType columnRowType;
    @Setter
    private Set<OlapContext> subContexts = Sets.newHashSet();
    @Setter
    private List<TblColRef> groups;
    /** innerColumns in group keys, used to generate ComputedColumn. */
    @Getter(AccessLevel.PRIVATE)
    private final Set<TblColRef> groupByInnerColumns = new HashSet<>();
    /** Preserve the ordering of group keys after CC replacement. */
    private ImmutableList<Integer> rewriteGroupKeys;
    /** group sets with cc replaced. */
    private List<ImmutableBitSet> rewriteGroupSets;
    private final List<AggregateCall> aggregateCalls;
    private List<AggregateCall> rewriteAggCalls;
    @Getter(AccessLevel.PRIVATE)
    private List<FunctionDesc> aggregations;
    @Getter(AccessLevel.PRIVATE)
    private boolean afterAggregate;

    public OlapAggregateRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) throws InvalidRelException {
        super(cluster, traits, child, groupSet, groupSets, aggregateCalls);
        Preconditions.checkArgument(getConvention() == OlapRel.CONVENTION);
        this.afterAggregate = false;
        this.rewriteAggCalls = aggregateCalls;
        this.rowType = getRowType();
        this.rewriteGroupKeys = ImmutableList.copyOf(groupSet.toList());
        this.aggregateCalls = aggregateCalls;
        this.rewriteGroupSets = groupSets;
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
            throw new IllegalStateException("Not-support aggregation: " + sqlName);
        }
        return funcName;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        try {
            return new OlapAggregateRel(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create OlapAggregateRel!", e);
        }
    }

    @Override
    public void implementContext(ContextImpl contextImpl, ContextVisitorState state) {
        contextImpl.fixSharedOlapTableScan(this);
        ContextVisitorState tempState = ContextVisitorState.init();
        contextImpl.visitChild(getInput(), this, tempState);
        if (tempState.hasFreeTable()) {
            // since SINGLE_VALUE agg doesn't participant in any computation,
            // context is allocated to the input rel
            if (CollectionUtils.exists(aggregateCalls,
                    aggCall -> ((AggregateCall) aggCall).getAggregation().getKind() == SqlKind.SINGLE_VALUE)) {
                contextImpl.allocateContext((OlapRel) this.getInput(), this);
            } else {
                contextImpl.allocateContext(this, null);
            }
            tempState.setHasFreeTable(false);
        }
        state.merge(tempState);

        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));
    }

    @Override
    public void implementCutContext(ICutContextStrategy.ContextCutImpl implementor) {
        this.context = null;
        this.columnRowType = null;
        implementor.visitChild(getInput());
    }

    /**
     * Since the grouping aggregate will be expanded by {org.apache.kylin.query.optrule.AggregateMultipleExpandRule},
     * made the cost of grouping aggregate more expensive to use the expanded aggregates
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        for (AggregateCall call : aggregateCalls) {
            // skip corr expansion during model suggestion
            if (!KylinConfig.getInstanceFromEnv().getSkipCorrReduceRule()
                    && CorrMeasureType.FUNC_CORR.equalsIgnoreCase(call.getAggregation().getName())) {
                return planner.getCostFactory().makeInfiniteCost();
            }
        }
        RelOptCost cost;
        if (getGroupType() == Group.SIMPLE) {
            cost = super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR);
        } else {
            cost = super.computeSelfCost(planner, mq).multiplyBy(OlapRel.OLAP_COST_FACTOR)
                    .plus(planner.getCost(getInput(), mq)).multiplyBy(groupSets.size() * 1.5);
        }
        return cost;
    }

    @Override
    public void implementOlap(OlapImpl olapImpl) {
        olapImpl.visitChild(getInput(), this);
        for (AggregateCall aggCall : aggregateCalls) {
            if (FunctionDesc.NOT_SUPPORTED_FUNCTION.contains(aggCall.getAggregation().getName())) {
                context.getContainedNotSupportedFunc().add(aggCall.getAggregation().getName());
            }
        }
        this.columnRowType = buildColumnRowType();
        if (context != null) {
            this.context.setHasAgg(true);
            this.afterAggregate = this.context.isAfterAggregate();
            // only translate the innermost aggregation
            if (!this.afterAggregate) {
                updateContextGroupByColumns();
                addAggFunctions();
                this.context.setAfterAggregate(true);
                if (this.context.isAfterLimit()) {
                    this.context.setLimitPrecedesAggr(true);
                }

                addSourceColsToContext();
                return;
            }

            checkAggCallAfterAggRel();
        }
    }

    private void addAggFunctions() {
        for (FunctionDesc agg : aggregations) {
            if (agg.isAggregateOnConstant()) {
                this.context.getConstantAggregations().add(agg);
            } else {
                this.context.getAggregations().add(agg);
            }
        }
    }

    public ColumnRowType buildColumnRowType() {
        buildGroups();
        buildAggregations();

        ColumnRowType colRowType = ((OlapRel) getInput()).getColumnRowType();
        List<TblColRef> columns = Lists.newArrayListWithCapacity(this.rowType.getFieldCount());
        columns.addAll(getGroupColsOfColumnRowType());

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
                        + colRowType.getColumnByIndex(index).getIdentity().replace('.', '_') + "_";
                aggCall.getArgList().forEach(argIndex -> operands.add(colRowType.getColumnByIndex(argIndex)));
            }
            TblColRef aggOutCol = TblColRef.newInnerColumn(aggOutName, TblColRef.InnerDataTypeEnum.AGGREGATION_TYPE);
            aggOutCol.setOperator(this.rewriteAggCalls.get(i).getAggregation());
            aggOutCol.setOperands(operands);
            aggOutCol.getColumnDesc().setId(String.valueOf(i + 1)); // mark the index of aggregation
            columns.add(aggOutCol);
        }
        Preconditions.checkState(columns.size() == this.rowType.getFieldCount());
        return new ColumnRowType(columns);
    }

    private TblColRef buildRewriteColumn(FunctionDesc aggFunc) {
        TblColRef colRef;
        if (aggFunc.needRewriteField()) {
            String colName = aggFunc.getRewriteFieldName();
            colRef = this.context.getFirstTableScan().makeRewriteColumn(colName);
        } else {
            throw new IllegalStateException("buildRewriteColumn on a aggrFunc that does not need rewrite " + aggFunc);
        }
        return colRef;
    }

    private void buildGroups() {
        buildGroupSet();
        buildGroupSets();
    }

    private void buildGroupSet() {
        List<TblColRef> groupCols = new ArrayList<>();
        List<Integer> groupKeys = new LinkedList<>();
        doBuildGroupSet(getGroupSet(), groupCols, groupKeys);
        this.groups = groupCols;
        this.rewriteGroupKeys = ImmutableList.copyOf(groupKeys);
    }

    private void buildGroupSets() {
        List<ImmutableBitSet> newRewriteGroupSets = new LinkedList<>();
        for (ImmutableBitSet subGroup : this.groupSets) {
            List<TblColRef> groupCols = new ArrayList<>();
            List<Integer> groupKeys = new LinkedList<>();
            doBuildGroupSet(subGroup, groupCols, groupKeys);
            ImmutableBitSet rewriteGroupSet = ImmutableBitSet.of(groupKeys);
            newRewriteGroupSets.add(rewriteGroupSet);
        }
        this.rewriteGroupSets = newRewriteGroupSets;
    }

    private void doBuildGroupSet(ImmutableBitSet groupSet, List<TblColRef> groups, List<Integer> groupKeys) {
        ColumnRowType inputColumnRowType = ((OlapRel) getInput()).getColumnRowType();
        for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
            TblColRef originalColumn = inputColumnRowType.getColumnByIndex(i);
            if (null != this.context && this.context.getGroupCCColRewriteMapping().containsKey(originalColumn)) {
                groups.add(this.context.getGroupCCColRewriteMapping().get(originalColumn));
                String colName = this.context.getGroupCCColRewriteMapping().get(originalColumn).getName();
                groupKeys.add(inputColumnRowType.getIndexByName(colName));
            } else {
                Set<TblColRef> sourceColumns = inputColumnRowType.getSourceColumnsByIndex(i);
                groups.addAll(sourceColumns);
                groupKeys.add(i);
            }

            if (originalColumn.isInnerColumn()) {
                this.groupByInnerColumns.add(originalColumn);
            }
        }
    }

    public void reBuildGroups(Map<TblColRef, TblColRef> colReplacementMapping) {
        this.context.setGroupCCColRewriteMapping(colReplacementMapping);
        ColumnRowType inputColumnRowType = ((OlapRel) this.getInput()).getColumnRowType();
        Set<TblColRef> groupCols = new HashSet<>();
        for (int i = this.getGroupSet().nextSetBit(0); i >= 0; i = this.getGroupSet().nextSetBit(i + 1)) {
            TblColRef originalColumn = inputColumnRowType.getColumnByIndex(i);
            Set<TblColRef> sourceColumns = inputColumnRowType.getSourceColumnsByIndex(i);

            if (colReplacementMapping.containsKey(originalColumn)) {
                groupCols.add(colReplacementMapping.get(originalColumn));
            } else {
                groupCols.addAll(sourceColumns);
            }
        }
        this.setGroups(new ArrayList<>(groupCols));
        updateContextGroupByColumns();
    }

    private void updateContextGroupByColumns() {
        context.getGroupByColumns().clear();
        for (TblColRef col : groups) {
            if (!col.isInnerColumn() && context.belongToContextTables(col)) {
                context.getGroupByColumns().add(col);
            }
        }

        for (TblColRef tblColRef : groupByInnerColumns) {
            context.getInnerGroupByColumns().add(new TableColRefWithRel(this, tblColRef));
        }
    }

    public List<TblColRef> getGroupColsOfColumnRowType() {
        List<TblColRef> allColumns = Lists.newArrayList();
        ColumnRowType inputColumnRowType = ((OlapRel) getInput()).getColumnRowType();
        for (int i = getGroupSet().nextSetBit(0); i >= 0; i = getGroupSet().nextSetBit(i + 1)) {
            TblColRef tblColRef = inputColumnRowType.getColumnByIndex(i);
            allColumns.add(tblColRef);
        }
        return allColumns;
    }

    @Override
    public void setContext(OlapContext context) {
        this.context = context;
        ((OlapRel) getInput()).setContext(context);
        subContexts.addAll(ContextUtil.collectSubContext(this.getInput()));
    }

    @Override
    public boolean pushRelInfoToContext(OlapContext context) {
        if (this.context != null)
            return false;
        if (((OlapRel) getInput()).pushRelInfoToContext(context)) {
            this.context = context;
            return true;
        }
        return false;
    }

    private void buildAggregations() {
        ColumnRowType inputColumnRowType = ((OlapRel) getInput()).getColumnRowType();
        this.aggregations = new ArrayList<>();
        for (AggregateCall aggCall : this.rewriteAggCalls) {
            List<ParameterDesc> parameters = Lists.newArrayList();
            // By default, all args are included, UDFs can define their own in getParamAsMeasureCount method.
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

    private TblColRef rewriteCastInSumIfNecessary(AggregateCall aggCall, ColumnRowType colRowType, Integer index) {
        // ISSUE #7294, for case like sum({fn CONVERT(ITEM_COUNT, SQL_BIGINT)})
        // remove the cast by rewriting input project, such that the sum can hit cube
        TblColRef column = colRowType.getColumnByIndex(index);
        if (getInput() instanceof OlapProjectRel && SqlTypeUtil.isBigint(aggCall.type) && column.isCastInnerColumn()) {
            TblColRef innerColumn = column.getOperands().get(0);
            if (!innerColumn.isInnerColumn() && innerColumn.getType().isIntegerFamily()) {
                colRowType.getAllColumns().set(index, innerColumn);
                column = colRowType.getColumnByIndex(index);
            }
        }
        return column;
    }

    public boolean needRewrite() {
        return this.context.getRealization() != null && !this.afterAggregate;
    }

    @Override
    public void implementRewrite(RewriteImpl rewriteImpl) {
        if (context == null) {
            QueryContext.current().getQueryTagInfo().setHasRuntimeAgg(true);
        } else if (needRewrite()) {
            translateAggregation();
            buildRewriteFieldsAndMetricsColumns();
        }

        rewriteImpl.visitChild(this, getInput());

        if (context == null) {
            return;
        }
        // only rewrite the innermost aggregation
        if (needRewrite()) {
            // rewrite the aggCalls
            this.rewriteAggCalls = new ArrayList<>(aggregateCalls.size());
            for (int i = 0; i < this.aggregateCalls.size(); i++) {
                AggregateCall aggCall = this.aggregateCalls.get(i);
                if (SqlStdOperatorTable.GROUPING == aggCall.getAggregation()
                        || this.aggregations.get(i).isAggregateOnConstant()) {
                    this.rewriteAggCalls.add(aggCall);
                    continue;
                }

                aggCall = rewriteAggCall(aggCall, this.aggregations.get(i));
                this.rewriteAggCalls.add(aggCall);
            }
            getContext().setExactlyAggregate(isExactlyMatched());
            if (getContext().isExactlyAggregate()) {
                boolean fastBitmapEnabled = getContext().getStorageContext().getBatchCandidate().getLayoutEntity()
                        .getIndex().getIndexPlan().isFastBitmapEnabled();
                getContext().setExactlyFastBitmap(fastBitmapEnabled && getContext().isHasBitmapMeasure());
            }
        }

        // rebuild rowType & columnRowType
        this.rowType = this.deriveRowType();
        this.columnRowType = this.buildColumnRowType();
    }

    private Boolean isExactlyMatched() {
        if (!KapConfig.getInstanceFromEnv().needReplaceAggWhenExactlyMatched()) {
            return false;
        }
        if (getSubContexts().size() > 1) {
            return false;
        }
        NLayoutCandidate candidate = getContext().getStorageContext().getBatchCandidate();
        if (candidate.isEmpty()) {
            return false;
        }

        NDataModel model = candidate.getLayoutEntity().getModel();
        if (model.getStorageType().isInvalidStorage()) {
            return false;
        }
        if (model.getModelType() != NDataModel.ModelType.BATCH) {
            return false;
        }

        if (!checkAggCall()) {
            return false;
        }
        Set<String> cuboidDimSet = new HashSet<>();
        if (getContext() != null && getContext().getStorageContext().getBatchCandidate() != null) {
            cuboidDimSet = getContext().getStorageContext().getBatchCandidate().getLayoutEntity().getOrderedDimensions()
                    .values().stream().map(TblColRef::getIdentity).collect(Collectors.toSet());

        }
        Set<String> groupByCols = getGroups().stream().map(TblColRef::getIdentity).collect(Collectors.toSet());

        OlapRel.logger.info("group by cols:{}", groupByCols);
        OlapRel.logger.info("cuboid dimensions: {}", cuboidDimSet);

        boolean isDimensionMatch = isDimExactlyMatch(groupByCols, cuboidDimSet);
        if (KylinConfig.getInstanceFromEnv().isRouteToMetadataEnabled()) {
            isDimensionMatch = isDimensionMatch && !groupByCols.isEmpty();
        }
        if (!isDimensionMatch) {
            return false;
        }

        NDataflow dataflow = (NDataflow) getContext().getRealization();
        PartitionDesc partitionDesc = dataflow.getModel().getPartitionDesc();
        MultiPartitionDesc multiPartitionDesc = dataflow.getModel().getMultiPartitionDesc();
        if (groupbyContainMultiPartitions(multiPartitionDesc) && groupbyContainSegmentPartition(partitionDesc)) {
            OlapRel.logger.info("Find partition column. skip agg");
            return true;
        }

        return dataflow.getQueryableSegments().size() == 1
                && dataflow.getQueryableSegments().get(0).getMultiPartitions().size() <= 1;
    }

    private boolean checkAggCall() {
        for (AggregateCall call : getRewriteAggCalls()) {
            String aggFuncName = getAggrFuncName(call);
            if (!SUPPORTED_FUNCTIONS.contains(aggFuncName)) {
                return false;
            }
            // bitmap uuid is fine with exactly matched cube as what we need to query
            // from the cube is exactly the binary bitmap
            if (aggFuncName.equals(FunctionDesc.FUNC_BITMAP_UUID)
                    || aggFuncName.equals(FunctionDesc.FUNC_BITMAP_BUILD)) {
                continue;
            }
            if (call instanceof KylinAggregateCall) {
                FunctionDesc func = ((KylinAggregateCall) call).getFunc();
                boolean hasBitmap = func.getReturnDataType() != null
                        && func.getReturnDataType().getName().equals("bitmap");
                if (hasBitmap) {
                    getContext().setHasBitmapMeasure(true);
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private boolean groupbyContainSegmentPartition(PartitionDesc partitionDesc) {
        return partitionDesc != null && partitionDesc.getPartitionDateColumnRef() != null
                && getGroups().stream().map(TblColRef::getIdentity).collect(Collectors.toSet())
                        .contains(partitionDesc.getPartitionDateColumnRef().getIdentity());
    }

    private boolean groupbyContainMultiPartitions(MultiPartitionDesc multiPartitionDesc) {
        if (multiPartitionDesc == null || CollectionUtils.isEmpty(multiPartitionDesc.getPartitions()))
            return true;

        return getGroups().stream().map(TblColRef::getIdentity).collect(Collectors.toSet()).containsAll(
                multiPartitionDesc.getColumnRefs().stream().map(TblColRef::getIdentity).collect(Collectors.toSet()));
    }

    private boolean isDimExactlyMatch(Set<String> groupByCols, Set<String> cuboidDimSet) {
        return groupByCols.equals(cuboidDimSet) && isSimpleGroupType()
                && (this.context.getInnerGroupByColumns().isEmpty()
                        || !this.context.getGroupCCColRewriteMapping().isEmpty());

    }

    private AggregateCall rewriteAggCall(AggregateCall aggCall, FunctionDesc cubeFunc) {
        // filter needn't rewrite aggFunc
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
            logger.info("{} skip rewriteAggregateCall because no pre-aggregated field available", aggCall);
        }

        return aggCall;
    }

    private void translateAggregation() {
        if (noPrecaculatedFieldsAvailable()) {
            //the realization is not contributing pre-calculated fields at all
            return;
        }
        // now the realization is known, replace aggregations with what's defined on MeasureDesc
        List<MeasureDesc> measures = this.context.getRealization().getMeasures();
        List<FunctionDesc> newAggrs = Lists.newArrayList();
        for (FunctionDesc aggFunc : this.aggregations) {
            if (aggFunc.isDimensionAsMetric()) {
                newAggrs.add(aggFunc);
                continue;
            }
            FunctionDesc newAgg = findInMeasures(aggFunc, measures);
            if (newAgg == null && aggFunc.isCountOnColumn()
                    && this.context.getRealization().getConfig().isReplaceColCountWithCountStar()) {
                newAgg = FunctionDesc.newCountOne();
            }
            if (newAgg == null) {
                newAgg = aggFunc;
            }
            newAggrs.add(newAgg);
        }
        this.aggregations.clear();
        this.aggregations.addAll(newAggrs);
        this.context.getAggregations().clear();
        aggregations.stream().filter(agg -> !agg.isAggregateOnConstant())
                .forEach(agg -> this.context.getAggregations().add(agg));
    }

    private FunctionDesc findInMeasures(FunctionDesc aggFunc, List<MeasureDesc> measures) {
        for (MeasureDesc m : measures) {
            if (aggFunc.equals(m.getFunction()))
                return m.getFunction();
        }
        // try to replace advance measure, like topN
        for (MeasureDesc m : measures) {
            if (m.getFunction().getMeasureType() instanceof BasicMeasureType)
                continue;

            if (m.getFunction().getMeasureType() instanceof TopNMeasureType) {
                FunctionDesc internalTopn = TopNMeasureType.getTopnInternalMeasure(m.getFunction());
                if (aggFunc.equals(internalTopn))
                    return internalTopn;
            }
            if (isBitmapFunction(aggFunc.getExpression()) && m.getFunction().getReturnType().equals("bitmap")
                    && aggFunc.getParameters().get(0).equals(m.getFunction().getParameters().get(0))) {
                return m.getFunction();
            }
        }
        return null;
    }

    private static boolean isBitmapFunction(String function) {
        return FunctionDesc.FUNC_INTERSECT_COUNT.equalsIgnoreCase(function)
                || FunctionDesc.FUNC_BITMAP_UUID.equalsIgnoreCase(function)
                || FunctionDesc.FUNC_BITMAP_BUILD.equalsIgnoreCase(function);
    }

    private void buildRewriteFieldsAndMetricsColumns() {
        ColumnRowType inputColumnRowType = ((OlapRel) getInput()).getColumnRowType();
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        for (int i = 0; i < this.aggregations.size(); i++) {
            FunctionDesc aggFunc = this.aggregations.get(i);
            if (aggFunc.isDimensionAsMetric()) {
                continue; // skip rewrite, let calcite handle
            }

            if (aggFunc.needRewriteField()) {
                String rewriteFieldName = aggFunc.getRewriteFieldName();
                RelDataType rewriteFieldType = OlapTable.createSqlType(typeFactory, aggFunc.getRewriteFieldType(),
                        true);
                this.context.getRewriteFields().put(rewriteFieldName, rewriteFieldType);

                TblColRef column = buildRewriteColumn(aggFunc);
                this.context.getMetricsColumns().add(column);
            }

            AggregateCall aggCall = this.rewriteAggCalls.get(i);
            if (!aggCall.getArgList().isEmpty()) {
                for (Integer index : aggCall.getArgList()) {
                    TblColRef column = inputColumnRowType.getColumnByIndex(index);
                    if (!column.isInnerColumn() && this.context.belongToContextTables(column)) {
                        this.context.getMetricsColumns().add(column);
                    }
                }
            }
        }
    }

    /**
     * optimize its Context Rel after context cut off according some rules
     * 1. push through the Agg Above Join Rel
     */
    public void optimizeContextCut() {
        // case 1: Agg push through Join
        if (context == null) {
            for (OlapContext subContext : subContexts) {
                if (!subContext.getAggregations().isEmpty())
                    continue;
                if (ContextUtil.qualifiedForAggInfoPushDown(this, subContext)) {
                    subContext.setTopNode(this);
                    pushRelInfoToContext(subContext);
                }
            }
        }
    }

    private void addSourceColsToContext() {
        if (this.context == null)
            return;

        this.context.getGroupByColumns().stream().filter(this::isSuitableForContextColumn)
                .forEach(colRef -> this.context.getAllColumns().add(colRef));

        RelNode input = getInput();
        if (!(input instanceof OlapProjectRel)) {
            // see https://olapio.atlassian.net/browse/KE-42047
            // Calcite 1.30 replaces the input RelNode with the current RelNode when the fields in the aggregate is 0
            if (!(input instanceof OlapFilterRel) && CollectionUtils.isEmpty(columnRowType.getAllColumns().stream()
                    .filter(this::isSuitableForContextColumn).collect(Collectors.toList()))) {
                context.getAllColumns().clear();
                return;
            }
            for (TblColRef colRef : ((OlapRel) input).getColumnRowType().getAllColumns()) {
                if (isSuitableForContextColumn(colRef))
                    context.getAllColumns().add(colRef);
            }
            return;
        }

        // Specifically designed to deal with count(*) issues caused by push down ProjectRel by Calcite RelFieldTrimmer
        if (getGroupCount() == 0 && aggregations.size() == 1 && aggregations.get(0).isCountConstant()) {
            return;
        }

        for (Set<TblColRef> colRefs : ((OlapProjectRel) getInput()).getColumnRowType().getSourceColumns()) {
            for (TblColRef colRef : colRefs) {
                if (isSuitableForContextColumn((colRef)))
                    context.getAllColumns().add(colRef);
            }
        }
    }

    private boolean isSuitableForContextColumn(TblColRef colRef) {
        return !colRef.getName().startsWith("_KY_") && context.belongToContextTables(colRef);
    }

    private void checkAggCallAfterAggRel() {
        for (AggregateCall aggCall : aggregateCalls) {
            // check if supported by kylin
            if (aggCall.isDistinct()) {
                throw new IllegalStateException("Distinct count is only allowed in innermost sub-query.");
            }
        }
    }

    public boolean noPrecaculatedFieldsAvailable() {
        return !this.context.hasPrecalculatedFields() || !RewriteImpl.needRewrite(this.context);
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
            newAgg = createCustomAggFunction(callName, udafMap.get(callName));
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
                // only the first column got overwritten
                newArgList.set(0, field.getIndex());
            }
        }

        // rebuild aggregate call
        return AggregateCall.create(newAgg, false, false, false, newArgList, -1, null, RelCollations.EMPTY, fieldType,
                callName);
    }

    /**
     * truncate Arg List according to UDAF's "add" method parameter count
     */
    private List<Integer> truncArgList(List<Integer> argList, Class<?> udafClazz) {
        int argListLength = argList.size();
        for (Method method : udafClazz.getMethods()) {
            if (method.getName().equals("add")) {
                argListLength = Math.min(method.getParameterTypes().length - 1, argListLength);
            }
        }
        return argList.subList(0, argListLength);
    }

    private SqlAggFunction createCustomAggFunction(String funcName, Class<?> customAggFuncClz) {
        SqlIdentifier sqlIdentifier = new SqlIdentifier(funcName, new SqlParserPos(1, 1));
        AggregateFunction aggFunction = AggregateFunctionImpl.create(customAggFuncClz);
        return (SqlAggFunction) toOp(sqlIdentifier, aggFunction);
    }

    @Override
    public EnumerableRel implementEnumerable(List<EnumerableRel> inputs) {
        try {
            return new EnumerableAggregate(getCluster(), getCluster().traitSetOf(EnumerableConvention.INSTANCE), //
                    sole(inputs), this.groupSet, this.groupSets, rewriteAggCalls);
        } catch (InvalidRelException e) {
            throw new IllegalStateException("Can't create EnumerableAggregate!", e);
        }
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
        pw.input("input", getInput());
        pw.item("group-set", rewriteGroupKeys)
                .itemIf("group-sets", rewriteGroupSets, getGroupType() != Aggregate.Group.SIMPLE) //
                .item("groups", groups).itemIf("aggs", rewriteAggCalls, pw.nest());
        if (!pw.nest()) {
            for (Ord<AggregateCall> ord : Ord.zip(rewriteAggCalls)) {
                pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
            }
        }
        pw.item("ctx", displayCtxId(context));
        return pw;
    }

    public boolean isSimpleGroupType() {
        return getGroupType() == Aggregate.Group.SIMPLE;
    }

    public boolean isContainCountDistinct() {
        return aggregateCalls.stream()
                .anyMatch(agg -> agg.getAggregation().getKind() == SqlKind.COUNT && agg.isDistinct());
    }

    /**
     * Copy toOp implementations of {@link org.apache.calcite.prepare.CalciteCatalogReader}
     *  to create user defined aggregation functions
     */

    /** Converts a function to a {@link org.apache.calcite.sql.SqlOperator}. */
    private static SqlOperator toOp(SqlIdentifier name, final org.apache.calcite.schema.Function function) {
        final Function<RelDataTypeFactory, List<RelDataType>> argTypesFactory = typeFactory -> function.getParameters()
                .stream().map(o -> o.getType(typeFactory)).collect(Util.toImmutableList());
        final Function<RelDataTypeFactory, List<SqlTypeFamily>> typeFamiliesFactory = typeFactory -> argTypesFactory
                .apply(typeFactory).stream()
                .map(type -> Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY))
                .collect(Util.toImmutableList());
        final Function<RelDataTypeFactory, List<RelDataType>> paramTypesFactory = typeFactory -> argTypesFactory
                .apply(typeFactory).stream().map(type -> toSql(typeFactory, type)).collect(Util.toImmutableList());

        // Use a short-lived type factory to populate "typeFamilies" and "argTypes".
        // SqlOperandMetadata.paramTypes will use the real type factory, during
        // validation.
        final RelDataTypeFactory dummyTypeFactory = new JavaTypeFactoryImpl();
        final List<RelDataType> argTypes = argTypesFactory.apply(dummyTypeFactory);
        final List<SqlTypeFamily> typeFamilies = typeFamiliesFactory.apply(dummyTypeFactory);

        final SqlOperandTypeInference operandTypeInference = InferTypes.explicit(argTypes);

        final SqlOperandMetadata operandMetadata = OperandTypes.operandMetadata(typeFamilies, paramTypesFactory,
                i -> function.getParameters().get(i).getName(), i -> function.getParameters().get(i).isOptional());

        final SqlKind kind = kind(function);
        if (function instanceof ScalarFunction) {
            final SqlReturnTypeInference returnTypeInference = infer((ScalarFunction) function);
            return new SqlUserDefinedFunction(name, kind, returnTypeInference, operandTypeInference, operandMetadata,
                    function);
        } else if (function instanceof AggregateFunction) {
            final SqlReturnTypeInference returnTypeInference = infer((AggregateFunction) function);
            return new SqlUserDefinedAggFunction(name, kind, returnTypeInference, operandTypeInference, operandMetadata,
                    (AggregateFunction) function, false, false, Optionality.FORBIDDEN);
        } else if (function instanceof TableMacro) {
            return new SqlUserDefinedTableMacro(name, kind, ReturnTypes.CURSOR, operandTypeInference, operandMetadata,
                    (TableMacro) function);
        } else if (function instanceof TableFunction) {
            return new SqlUserDefinedTableFunction(name, kind, ReturnTypes.CURSOR, operandTypeInference,
                    operandMetadata, (TableFunction) function);
        } else {
            throw new AssertionError("unknown function type " + function);
        }
    }

    /** Deduces the {@link org.apache.calcite.sql.SqlKind} of a user-defined
     * function based on a {@link Hints} annotation, if present. */
    private static SqlKind kind(org.apache.calcite.schema.Function function) {
        if (function instanceof ScalarFunctionImpl) {
            Hints hints = ((ScalarFunctionImpl) function).method.getAnnotation(Hints.class);
            if (hints != null) {
                for (String hint : hints.value()) {
                    if (hint.startsWith("SqlKind:")) {
                        return SqlKind.valueOf(hint.substring("SqlKind:".length()));
                    }
                }
            }
        }
        return SqlKind.OTHER_FUNCTION;
    }

    private static SqlReturnTypeInference infer(final ScalarFunction function) {
        return opBinding -> {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            final RelDataType type;
            if (function instanceof ScalarFunctionImpl) {
                type = ((ScalarFunctionImpl) function).getReturnType(typeFactory, opBinding);
            } else {
                type = function.getReturnType(typeFactory);
            }
            return toSql(typeFactory, type);
        };
    }

    private static SqlReturnTypeInference infer(final AggregateFunction function) {
        return opBinding -> {
            final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
            final RelDataType type = function.getReturnType(typeFactory);
            return toSql(typeFactory, type);
        };
    }

    private static RelDataType toSql(RelDataTypeFactory typeFactory, RelDataType type) {
        if (type instanceof RelDataTypeFactoryImpl.JavaType
                && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass() == Object.class) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);
        }
        return JavaTypeFactoryImpl.toSql(typeFactory, type);
    }
}
