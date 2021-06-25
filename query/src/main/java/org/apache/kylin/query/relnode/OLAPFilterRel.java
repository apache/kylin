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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.avatica.util.TimeUnitRange;
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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.filter.FilterOptimizeTransformer;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.visitor.TupleFilterVisitor;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
public class OLAPFilterRel extends Filter implements OLAPRel {

    ColumnRowType columnRowType;
    OLAPContext context;
    boolean autoJustTimezone = KylinConfig.getInstanceFromEnv().getStreamingDerivedTimeTimezone().length() > 0;
    boolean doOptimizePartition = true;
    public Set<TblColRef> filterColRefs = new HashSet<>();
    public List<CubeSegment> preCalculatedSegment = new ArrayList<>();
    FastDateFormat format;
    private RexBuilder builder = new RexBuilder(new JavaTypeFactoryImpl());

    public OLAPFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
        Preconditions.checkArgument(getConvention() == CONVENTION);
        Preconditions.checkArgument(getConvention() == child.getConvention());
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

            TupleFilterVisitor visitor = new TupleFilterVisitor(this.columnRowType);
            TupleFilter havingFilter = this.condition.accept(visitor);
            if (context.havingFilter == null)
                context.havingFilter = havingFilter;
        }
    }

    ColumnRowType buildColumnRowType() {
        OLAPRel olapChild = (OLAPRel) getInput();
        ColumnRowType inputColumnRowType = olapChild.getColumnRowType();
        return inputColumnRowType;
    }

    void translateFilter(OLAPContext context) {
        if (this.condition == null) {
            return;
        }

        TupleFilterVisitor visitor = new TupleFilterVisitor(this.columnRowType);
        boolean isRealtimeTable = columnRowType.getColumnByIndex(0).getColumnDesc().getTable().isStreamingTable() ;
        autoJustTimezone = isRealtimeTable && autoJustTimezone;
        visitor.setAutoJustByTimezone(autoJustTimezone);
        TupleFilter filter = this.condition.accept(visitor);

        // optimize the filter, the optimization has to be segment-irrelevant
        filter = new FilterOptimizeTransformer().transform(filter);

        Set<TblColRef> filterColumns = Sets.newHashSet();
        TupleFilter.collectColumns(filter, filterColumns);
        for (TblColRef tblColRef : filterColumns) {
            if (!tblColRef.isInnerColumn() && context.belongToContextTables(tblColRef)) {
                context.allColumns.add(tblColRef);
                context.filterColumns.add(tblColRef);
            }
        }

        context.filter = and(context.filter, filter);
    }
    
    private TupleFilter and(TupleFilter f1, TupleFilter f2) {
        if (f1 == null)
            return f2;
        if (f2 == null)
            return f1;

        if (f1.getOperator() == FilterOperatorEnum.AND) {
            f1.addChild(f2);
            return f1;
        }

        if (f2.getOperator() == FilterOperatorEnum.AND) {
            f2.addChild(f1);
            return f2;
        }

        LogicalTupleFilter and = new LogicalTupleFilter(FilterOperatorEnum.AND);
        and.addChild(f1);
        and.addChild(f2);
        return and;
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
    public void implementRewrite(RelNode parent, RewriteImplementor implementor) {
        if (doOptimizePartition &&
                (this.getContext().realization).getConfig().removePartitionDimensionDynamically()) {
            try {
                boolean optimizeFiler = shouldOptimizePartition(this.getContext());
                if (optimizeFiler) {
                    this.format = DateFormat.getDateFormat(this.getContext().realization.getModel().getPartitionDesc().getPartitionDateFormat());
                    EquivalentFilter result = splitFilterAndSegments((RexCall) this.getCondition(), (CubeInstance) this.getContext().realization);

                    Pair<RexNode, RexNode> splits = generateSplitFilters(result);

                    boolean fallback = true;
                    if (splits.getFirst() == null) {   //if full matched segment is empty, no need to split filter
                        fallback = true;
                    } else {
                        for (DetailedSegments querySegment : result.querySegments) {
                            if (querySegment.fullyUsed()) {
                                fallback = false;
                                break;
                            }
                        }
                    }
                    if (fallback) {
                        implementor.visitChild(this, getInput());
                        this.rowType = this.deriveRowType();
                        this.columnRowType = buildColumnRowType();
                        return;
                    }

                    OLAPFilterRel fullMatchedRel = null;
                    OLAPFilterRel partialMatchedRel = null;
                    if (splits.getFirst() != null) {    //full matched filter is not empty, we need to split filter
                        fullMatchedRel = (OLAPFilterRel)this.copy(traitSet, this.getInput(), splits.getFirst());
                        if (null != context.filterColumns) {
                            fullMatchedRel.filterColRefs.addAll(context.filterColumns);
                        }
                        fullMatchedRel.context = this.getContext();
                        fullMatchedRel.doOptimizePartition = false;

                        List<CubeSegment> fullyUsed = new LinkedList<>();
                        for (DetailedSegments querySegment : result.querySegments) {
                            if (querySegment.fullyUsed()) {
                                fullyUsed.add(querySegment.segment);
                            }
                        }
                        fullMatchedRel.preCalculatedSegment = fullyUsed;
                        fullMatchedRel.filterColRefs.addAll(context.filterColumns);
                        fullMatchedRel.filterColRefs.remove(context.realization.getModel().getPartitionDesc().getPartitionDateColumnRef());
                    }
                    if (splits.getSecond() != null) {
                        partialMatchedRel = (OLAPFilterRel)this.copy(traitSet, this.getInput(),
                                ((RexCall) this.getCondition()).clone(this.getCondition().getType(), Lists.newArrayList(splits.getSecond())));
                        partialMatchedRel.context = this.getContext();
                        partialMatchedRel.doOptimizePartition = false;
                    }

                    int i = 0;
                    for (; i < parent.getInputs().size(); i++) {
                        if (parent.getInputs().get(i).getId() == this.getId()) {
                            break;
                        }
                    }
                    if (partialMatchedRel == null) {
                        RelNode toReplace = fullMatchedRel != null ? fullMatchedRel : partialMatchedRel;
                        parent.replaceInput(i, toReplace);
                        this.context.splitFilters.add(fullMatchedRel);
                        implementor.visitChild(parent, toReplace);
                    } else {
                        OLAPUnionRel rel = new OLAPUnionRel(this.getCluster(), this.getTraitSet(),
                                Lists.newArrayList(fullMatchedRel, partialMatchedRel), true);
                        parent.replaceInput(i, rel);
                        this.context.splitFilters.add(fullMatchedRel);
                        this.context.splitFilters.add(partialMatchedRel);
                        implementor.visitChild(parent, rel);
                    }
                } else {
                    implementor.visitChild(this, getInput());
                    this.rowType = this.deriveRowType();
                    this.columnRowType = buildColumnRowType();
                }
            } catch (Exception e) {
                logger.warn("failed to remove partition dimension dynamically will fall back", e);
                implementor.visitChild(this, getInput());
                this.rowType = this.deriveRowType();
                this.columnRowType = buildColumnRowType();
            }
        } else {
            implementor.visitChild(this, getInput());
            this.rowType = this.deriveRowType();
            this.columnRowType = buildColumnRowType();
        }
    }

    private boolean shouldOptimizePartition(OLAPContext context) {
        SQLDigest sqlDigest = context.sqlDigest;
        CubeInstance cubeInstance = (CubeInstance) context.realization;
        TblColRef partitionCol = cubeInstance.getModel().getPartitionDesc().getPartitionDateColumnRef();
        return !sqlDigest.groupbyColumns.contains(partitionCol)
                &&  sqlDigest.filterColumns.contains(partitionCol);
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


    /*
    *
    */
    public EquivalentFilter splitFilterAndSegments(RexCall call, CubeInstance cubeInstance) throws Exception{
        Set<TblColRef> allColumns = cubeInstance.getDescriptor().listAllColumns();
        Segments<CubeSegment> segments = cubeInstance.getSegments(SegmentStatusEnum.READY);
        List<DetailedSegments> querySegments = cubeInstance.getSegments().stream().map(seg -> new DetailedSegments(seg, false)).collect(Collectors.toList());

        //partition column index
        int parIdx = 0;
        for (TblColRef col: allColumns) {
            if (col.equals(cubeInstance.getModel().getPartitionDesc().getPartitionDateColumnRef())) {
                break;
            }
            parIdx++;
        }

        switch (call.getKind()) {
            case AND:
                List<EquivalentFilter> andOperands = new ArrayList<>();
                RexInputRef rexInputRef = null;
                for (RexNode operand : call.getOperands()) {
                    EquivalentFilter equivalentSplit = splitFilterAndSegments((RexCall) operand, cubeInstance);
                    if (rexInputRef == null) {
                        rexInputRef = equivalentSplit.parInputRef;
                    }
                    andOperands.add(equivalentSplit);
                }
                return convertAndOperands(andOperands, call, rexInputRef);
            case OR:
                List<EquivalentFilter> orOperands = new ArrayList<>();
                RexInputRef parInputRef = null;
                for (RexNode rexNode : call.getOperands()) {
                    EquivalentFilter filterAndSegments = splitFilterAndSegments((RexCall) rexNode, cubeInstance);
                    if (parInputRef == null) {
                        parInputRef = filterAndSegments.parInputRef;
                    }
                    orOperands.add(filterAndSegments);
                }
                return convertOrOperands(orOperands, call, parInputRef);
            case EQUALS:
                RexInputRef inputRef = (RexInputRef) call.getOperands().get(0);
                if (parIdx == inputRef.getIndex()) {
                    Object value = getValue(call.getOperands().get(1));
                    long time;
                    if (value instanceof String) {
                        time = format.parse(value.toString()).getTime();
                    } else {
                        time = Long.valueOf(value.toString());
                    }
                    List<CubeSegment> result = segments.stream().filter(seg ->
                            time >= (long) seg.getSegRange().start.v && time < (long) seg.getSegRange().end.v)
                            .collect(Collectors.toList());
                    EquivalentFilter equivalentFilter = new EquivalentFilter(SqlStdOperatorTable.EQUALS, call, inputRef);
                    if (!result.isEmpty()) {
                        DetailedSegments detailedSegments = new DetailedSegments(result.get(0), false);
                        detailedSegments.addSegmentEquals(time);
                        equivalentFilter.querySegments.add(detailedSegments);
                    }
                    return equivalentFilter;
                } else {
                    return new EquivalentFilter(SqlStdOperatorTable.EQUALS, call, null);
                }
            case GREATER_THAN:
                RexInputRef gtInputRef = (RexInputRef) call.getOperands().get(0);
                if (parIdx == gtInputRef.getIndex()) {
                    EquivalentFilter equivalentFilter = new EquivalentFilter(SqlStdOperatorTable.GREATER_THAN, call, gtInputRef);
                    long time = getTimeMillisFromLiteral(call.getOperands().get(1));
                    //partly matched
                    querySegments.forEach(seg -> {
                        if (seg.start > time) {
                            equivalentFilter.querySegments.add(new DetailedSegments(seg.segment, true));
                        } else if (seg.start <= time && (seg.end - seg.segmentUnit) > time) {
                            DetailedSegments partial = new DetailedSegments(seg.segment, false);
                            partial.addSegmentGreaterThan(time);
                            equivalentFilter.querySegments.add(partial);
                        }
                    });
                    return equivalentFilter;
                } else {
                    return new EquivalentFilter(SqlStdOperatorTable.EQUALS, call, null);
                }
            case GREATER_THAN_OR_EQUAL:
                RexInputRef gteInputRef = (RexInputRef) call.getOperands().get(0);
                if (parIdx == gteInputRef.getIndex()) {
                    EquivalentFilter equivalentFilter = new EquivalentFilter(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, call, gteInputRef);
                    long time = getTimeMillisFromLiteral(call.getOperands().get(1));

                    querySegments.forEach(seg -> {
                        if (seg.start >= time) {
                            equivalentFilter.querySegments.add(new DetailedSegments(seg.segment, true));
                        } else if (seg.start < time && seg.end > time) {
                            DetailedSegments partial = new DetailedSegments(seg.segment, false);
                            partial.addSegmentGreaterOrEqualThan(time);
                            equivalentFilter.querySegments.add(partial);
                        }
                    });
                    return equivalentFilter;
                } else {
                    return new EquivalentFilter(SqlStdOperatorTable.EQUALS, call, null);
                }
            case LESS_THAN:
                RexInputRef leInputRef = (RexInputRef) call.getOperands().get(0);
                if (parIdx == leInputRef.getIndex()) {
                    EquivalentFilter equivalentFilter = new EquivalentFilter(SqlStdOperatorTable.LESS_THAN, call, leInputRef);
                    long time = getTimeMillisFromLiteral(call.getOperands().get(1));
                    //partly matched
                    querySegments.forEach(seg -> {
                        if (seg.end <= time) {
                            equivalentFilter.querySegments.add(new DetailedSegments(seg.segment, true));
                        } else if (seg.start < time && seg.end > time) {
                            DetailedSegments partial = new DetailedSegments(seg.segment, false);
                            partial.addSegmentLessThan(time);
                            equivalentFilter.querySegments.add(partial);
                        }
                    });
                    return equivalentFilter;
                } else {
                    return new EquivalentFilter(SqlStdOperatorTable.EQUALS, call, null);
                }
            case LESS_THAN_OR_EQUAL:
                RexInputRef lteInputRef = (RexInputRef) call.getOperands().get(0);
                if (parIdx == lteInputRef.getIndex()) {
                    EquivalentFilter equivalentFilter = new EquivalentFilter(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, call, lteInputRef);
                    long time = getTimeMillisFromLiteral(call.getOperands().get(1));

                    querySegments.forEach(seg -> {
                        if (seg.end <= time + 86400 * 1000) {
                            equivalentFilter.querySegments.add(new DetailedSegments(seg.segment, true));
                        } else if (seg.start < time && seg.end > time + 86400 * 100) {
                            DetailedSegments partial = new DetailedSegments(seg.segment, false);
                            partial.addSegmentLessOrEqualThan(time);
                            equivalentFilter.querySegments.add(partial);
                        }
                    });
                    return equivalentFilter;
                } else {
                    return new EquivalentFilter(SqlStdOperatorTable.EQUALS, call, null);
                }
            default:
                throw new UnsupportedOperationException("unsupported");
        }
    }

    public RexNode createPartitionIrrelevantFilter(EquivalentFilter equivalentFilter) {
        if (equivalentFilter.children.isEmpty()) {
            if (equivalentFilter.parInputRef == null) {
                return equivalentFilter.origin;
            } else {
                return null;
            }
        } else {
            List<RexNode> filter = new ArrayList<>();
            for (EquivalentFilter child : equivalentFilter.children) {
                RexNode childRex = createPartitionIrrelevantFilter(child);
                if (null != childRex) {
                    filter.add(childRex);
                }
            }
            if (filter.size() == 0) {
                return null;
            } else if (filter.size() == 1) {
                return filter.get(0);
            } else {
                return builder.makeCall(equivalentFilter.op, filter);
            }
        }
    }

    private RexNode createSegmentFilter(LinkedList<DetailedSegments> segments, RexInputRef parInputRef) {
        long rangeStart = 0;
        long rangeEnd = 0;
        if (segments.isEmpty()) {
            return null;
        }
        long segmentUnit = segments.get(0).segmentUnit;
        List<RexNode> operands = new ArrayList<>();
        for (DetailedSegments segment : segments) {
            if (segment.fullyUsed()) {//skip fully used segment
                continue;
            }
            for (int i = 0; i < segment.querySegments.length(); i ++) {
                if (rangeStart == 0) {
                    if (segment.querySegments.get(i)) {
                        rangeStart = segment.start + i * segmentUnit;
                        rangeEnd = rangeStart;
                    }
                    continue;
                }
                if (!segment.querySegments.get(i)) { //query time range is discontinuous
                    operands.add(createTimeRangeRexNode(parInputRef, rangeStart, rangeEnd, segmentUnit));
                    rangeStart = segment.start + i * segmentUnit;
                    rangeEnd = segment.start + i * segmentUnit;
                } else {
                    if ((rangeEnd + segmentUnit) == (segment.start + i * segmentUnit)) { //query time range is continuous
                        rangeEnd = segment.start + i * segmentUnit;
                    } else {
                        operands.add(createTimeRangeRexNode(parInputRef, rangeStart, rangeEnd, segmentUnit));
                        rangeStart = segment.start + i * segmentUnit;
                        rangeEnd = segment.start + i * segmentUnit;
                    }
                }
            }
        }
        if (rangeStart != 0) {
            operands.add(createTimeRangeRexNode(parInputRef, rangeStart, rangeEnd, segmentUnit));
        }
        if (operands.size() == 0) {
            return null;
        }else if (operands.size() == 1) {
            return operands.get(0);
        } else {
            return builder.makeCall(SqlStdOperatorTable.OR, operands);
        }
    }

    private RexNode createTimeRangeRexNode(RexInputRef parInputRef, long rangeStart, long rangeEnd, long segmentUnit) {
        if (rangeEnd == rangeStart) {
            return builder.makeCall(SqlStdOperatorTable.EQUALS,
                    Lists.newArrayList(parInputRef, builder.makeLiteral(format.format(rangeStart))));
        } else {
            RexNode gte = builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    Lists.newArrayList(parInputRef, builder.makeLiteral(format.format(rangeStart))));
            RexNode lte = builder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    Lists.newArrayList(parInputRef, builder.makeLiteral(format.format(rangeEnd))));
            return builder.makeCall(SqlStdOperatorTable.AND, Lists.newArrayList(gte, lte));
        }
    }

    private long getTimeMillisFromLiteral(RexNode rexNode) throws ParseException {
        Object value = getValue(rexNode);
        if (value instanceof String) {
            return format.parse(value.toString()).getTime();
        } else {
            return Long.valueOf(value.toString());
        }
    }

    /*
     * segments: [20200101, 20200104), [20200104, 20200105), [20200105, 20200107)
     * filter: partition > 20200102 and partition <= 20200107 and ops_user = 'ADMIN'
     *
     * In this case, we generate 3 EquivalentSplitFilters:
     *   Filter1:
     *     fullMatched: [20200104, 20200105), [20200105, 20200107)
     *     partlyUsedSegments: [20200101, 20200104)
     *   Filter2:
     *     fullMatched: [20200104, 20200105), [20200105, 20200107), [20200101, 20200104)
     *     partlyUsedSegments: Empty
     *   Filter3：
     *     filterIncludesPar: false
     *     RexNode origin: ops_user = 'ADMIN'
     *
     * Merged Filter
     *   Merged Filter
     *     fullMatched: [20200104, 20200105), [20200105, 20200107)
     *     partlyUsedSegments: [20200101, 20200104)
     *     otherFilters: ops_user = 'ADMIN'
     *     SqlOperator op: AND
     */
    private EquivalentFilter convertAndOperands(List<EquivalentFilter> operands,
                                                 RexNode origin, RexInputRef inputRef) {
        EquivalentFilter result = new EquivalentFilter(SqlStdOperatorTable.AND, origin, inputRef);

        LinkedList<DetailedSegments> segmentsToQuery = null;
        for (EquivalentFilter eqFilter : operands) {
            if (!eqFilter.includePartition()) {
                continue;
            }
            if (segmentsToQuery == null) {
                segmentsToQuery =  new LinkedList<>();
                segmentsToQuery.addAll(eqFilter.querySegments);
            } else {
                segmentsToQuery = intersectSegments(segmentsToQuery, eqFilter.querySegments);
            }
        }
        result.querySegments = segmentsToQuery;

        for (EquivalentFilter  operand : operands) {
            LinkedList<DetailedSegments> updatedSegments = new LinkedList<>();
            if (operand.includePartition()) {
                for (int i = 0; i < segmentsToQuery.size(); i++) {
                    updatedSegments.add(segmentsToQuery.get(i).clone());
                }
                operand.querySegments = updatedSegments;
            }
        }
        result.children = operands;
        return result;
    }

    /*
     * segments: [20200101, 20200104), [20200104, 20200105), [20200105, 20200107)
     * filter: (partition >= 20200101 and partition <= 20200107 and ops_user = 'ADMIN')
     *         or
     *         (partition > 20200103 and partition <= 20200107 and ops_user = 'ANALYST')
     *
     * In this case, we generate 2 EquivalentSplitFilters:
     *   Filter1:
     *     fullMatched: [20200101, 20200104), [20200104, 20200105), [20200105, 20200107)
     *     partlyUsedSegments:
     *     otherFilters: ops_user = 'ADMIN'
     *   Filter2:
     *     fullMatched: [20200104, 20200105), [20200105, 20200107),
     *     partlyUsedSegments: Empty
     *     otherFilters: ops_user = 'ANALYST'
     *
     * Transformed Filter
     *   Filter1:
     *     fullMatched: [20200104, 20200105), [20200105, 20200107)
     *     partlyUsedSegments: [20200101, 20200104)
     *     otherFilters: ops_user = 'ADMIN'
     *   Filter2:
     *     fullMatched: [20200104, 20200105), [20200105, 20200107)
     *     partlyUsedSegments: [20200101, 20200104)
     *     otherFilters: ops_user = 'ANALYST'
     */
    private EquivalentFilter convertOrOperands(List<EquivalentFilter> splitOperands,
                                                RexNode origin, RexInputRef parInputRef) {
        EquivalentFilter result = new EquivalentFilter(SqlStdOperatorTable.OR, origin, parInputRef);

        LinkedList<DetailedSegments> segmentsToQuery = null;
        for (EquivalentFilter eqFilter : splitOperands) {
            if (!eqFilter.includePartition()) {
                continue;
            }
            if (eqFilter.querySegments.isEmpty()) { // get no data, equal to ALWAYS_FALSE, discard this condition
                continue;
            }
            if(null == segmentsToQuery) {
                segmentsToQuery = new LinkedList<>();
                for (DetailedSegments detailedSegment : eqFilter.querySegments) {
                    segmentsToQuery.add(detailedSegment.clone());
                }
            } else {
                segmentsToQuery = unionSegments(segmentsToQuery, eqFilter.querySegments);
            }
        }

        if (segmentsToQuery.isEmpty()) {
            return null;
        }

        //update child filter's segments
        for (EquivalentFilter splitOperand : splitOperands) {
            if (splitOperand.querySegments.isEmpty()) {
                continue;
            }
            convertFullyUsedSegmentToPartlyUsed(splitOperand.querySegments, segmentsToQuery);
        }
        result.querySegments = segmentsToQuery;
        result.children = splitOperands;
        return result;
    }

    private void convertFullyUsedSegmentToPartlyUsed(LinkedList<DetailedSegments> source,
                 LinkedList<DetailedSegments> targetFullyUsed) {
        for (int i =0 , j = 0; i < source.size() && j < targetFullyUsed.size();) {
            DetailedSegments sourceSegment = source.get(i);
            DetailedSegments fullyUsedSegment = targetFullyUsed.get(j);
            if (sourceSegment.start == fullyUsedSegment.start) {
                if (sourceSegment.fullyUsed() && !fullyUsedSegment.fullyUsed()) {
                    sourceSegment.convertToPartlyUsed = true;
                }
                i++;
                j++;
            } else if (source.get(i).start < targetFullyUsed.get(j).start) {
                if (source.get(i).fullyUsed()) {
                    source.get(i).convertToPartlyUsed = true;
                }
                i++;
            } else {
                j++;
            }
        }
    }

    public Object getValue(RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            return getRexLiteral((RexLiteral)rexNode);
        }
        if (rexNode instanceof  RexCall) {
            switch (rexNode.getKind()) {
                case CAST: {
                    RexLiteral literal = (RexLiteral) ((RexCall)rexNode).getOperands().get(0);
                    return getRexLiteral(literal);
                }
                default:
                    throw new UnsupportedOperationException("unsupported");
            }
        }  else {
            throw new UnsupportedOperationException("unsupported");
        }
    }

    public Object getRexLiteral(RexLiteral rexLiteral) {
        Comparable literalValue = rexLiteral.getValue();
        if (literalValue instanceof NlsString) {
            return ((NlsString) literalValue).getValue();
        } else if (literalValue instanceof GregorianCalendar) {
            GregorianCalendar g = (GregorianCalendar) literalValue;
            return g.getTimeInMillis();
        } else if (literalValue instanceof TimeUnitRange) {
            // Extract(x from y) in where clause
            throw new UnsupportedOperationException();
        } else if (literalValue == null) {
            return null;
        } else {
            return literalValue.toString();
        }
    }

    static class EquivalentFilter {
        //original RexNode information
        RexNode origin;
        SqlOperator op;
        RexInputRef parInputRef;

        List<EquivalentFilter> children = new ArrayList<>();

        LinkedList<DetailedSegments> querySegments =  new LinkedList<>();

        public EquivalentFilter(SqlOperator op, RexNode origin, RexInputRef parInputRef) {
            this.origin = origin;
            this.op = op;
            this.parInputRef = parInputRef;
        }

        public boolean includePartition() {
            return parInputRef != null;
        }
    }


    static class DetailedSegments {
        CubeSegment segment;
        BitSet querySegments;
        //inclusive
        long start;
        //exclusive
        long end;
        int segmentSize;
        boolean convertToPartlyUsed = false;
        private long segmentUnit = 86400 * 1000;

        public DetailedSegments(CubeSegment segment, boolean fullyUsed) {
            this.segment = segment;
            this.end = (long)segment.getSegRange().end.v;
            this.start = (long)segment.getSegRange().start.v;
            this.segmentSize = (int) ((this.end - this.start) / segmentUnit);
            this.querySegments = new BitSet(segmentSize);
            if (fullyUsed) {
                querySegments.set(0, segmentSize, true);
            }
        }

        public DetailedSegments(CubeSegment segment, BitSet bitSet) {
            this.segment = segment;
            this.end = (long)segment.getSegRange().end.v;
            this.start = (long)segment.getSegRange().start.v;
            this.segmentSize = (int) ((this.end - this.start) / segmentUnit);
            this.querySegments = bitSet;
        }

        public DetailedSegments intersect(DetailedSegments seg) {
            if (!this.segment.equals(seg.segment)) {
                throw new RuntimeException("can not intersect different segments");
            }
            BitSet bs = new BitSet(segmentSize);
            for (int i = 0; i < segmentSize; i++) {
                bs.set(i, this.querySegments.get(i) & seg.querySegments.get(i));
            }
            return new DetailedSegments(this.segment, bs);
        }

        public DetailedSegments union(DetailedSegments target) {
            if (!this.segment.equals(target.segment)) {
                throw new RuntimeException("can not union different segments");
            }
            BitSet bs = new BitSet(segmentSize);
            for (int i = 0; i < segmentSize; i++) {
                bs.set(i, this.querySegments.get(i) | target.querySegments.get(i));
            }
            DetailedSegments result = new DetailedSegments(this.segment, bs);
            if (result.fullyUsed() && (!this.fullyUsed() || !target.fullyUsed())) {
                result.convertToPartlyUsed = true;
            }
            return result;
        }

        public boolean fullyUsed() {
            return !convertToPartlyUsed && querySegments.cardinality() == segmentSize;
        }

        public DetailedSegments clone() {
            return new DetailedSegments(this.segment, (BitSet) this.querySegments.clone());
        }

        public void addSegmentGreaterThan(long timeStamp) {
            if (timeStamp >= end - segmentUnit) {
                return;
            }
            if (timeStamp < start) {
                querySegments.set(0, segmentSize, true);
                return;
            }
            // (start + index * segmentUnit) <= timeStamp
            int index = (int) ((timeStamp - start) / segmentUnit);
            index = index + 1;
            querySegments.set(index, segmentSize, true);
        }

        public void addSegmentGreaterOrEqualThan(long timeStamp) {
            if (timeStamp > end - segmentUnit) {
                return;
            }
            if (timeStamp < start) {
                querySegments.set(0, segmentSize - 1, true);
                return;
            }
            int index = (int) ((timeStamp - start) / segmentUnit);
            // (start + index * segmentUnit) <= timeStamp
            if ((start + index * segmentUnit) < timeStamp) {
                index = index + 1;
            }
            querySegments.set(index, segmentSize - 1, true);
        }

        public void addSegmentLessThan(long timeStamp) {
            if (timeStamp < start) {
                return;
            }
            if (timeStamp > end - segmentUnit) {
                querySegments.set(0, segmentSize - 1, true);
                return;
            }
            int index = (int) ((timeStamp - start) / segmentUnit);
            // (start + index * segmentUnit) <= timeStamp
            if ((start + index * segmentUnit) == timeStamp) {
                index = index - 1;
            }
            querySegments.set(0, index + 1, true);
        }

        public void addSegmentLessOrEqualThan(long timeStamp) {
            if (timeStamp < start) {
                return;
            }
            if (timeStamp > end - segmentUnit) {
                querySegments.set(0, segmentSize - 1, true);
                return;
            }
            int index = (int) ((timeStamp - start) / segmentUnit);
            querySegments.set(0, index + 1, true);
        }

        public void addSegmentEquals(long timeStamp) {
            int index = (int) ((timeStamp - start) / segmentUnit);
            if ((start + index * segmentUnit) == timeStamp) {
                querySegments.set(0, index, true);
            }
        }

        public boolean merged() {
            return end - start != 86400 * 1000;
        }
    }

    public LinkedList<DetailedSegments> intersectSegments(LinkedList<DetailedSegments> seg1, LinkedList<DetailedSegments> seg2) {
        LinkedList<DetailedSegments> result = new LinkedList<>();
        if (seg1.isEmpty() || seg2.isEmpty()) {
            return result;
        }
        for (int i =0 , j = 0; i < seg1.size() && j < seg2.size();) {
            if (seg1.get(i).start == seg2.get(j).start) {
                DetailedSegments detailedSegments = seg1.get(i).intersect(seg2.get(j));
                result.add(detailedSegments);
                i++;
                j++;
            } else if (seg1.get(i).start < seg2.get(j).start) {
                i++;
            } else {
                j++;
            }
        }
        return result;
    }

    private LinkedList<DetailedSegments> unionSegments(LinkedList<DetailedSegments> segList1, LinkedList<DetailedSegments> segList2) {
        LinkedList<DetailedSegments> result = new LinkedList<>();
        int i =0;
        int j = 0;
        DetailedSegments seg1 = null;
        DetailedSegments seg2 = null;
        for (; i < segList1.size() && j < segList2.size();) {
            if (null == seg1 || null == seg2) {
                seg1 = segList1.get(i);
                seg2 = segList2.get(j);
            }
            if (seg1.start == seg2.start) {
                DetailedSegments detailedSegments = seg1.union(seg2);
                result.add(detailedSegments);
                if (++i < segList1.size()) {
                    seg1 = segList1.get(i);
                }
                if (++j < segList2.size()) {
                    seg2 = segList2.get(j);
                }
            } else if (seg1.start < seg2.start) {
                if (seg1.fullyUsed()) {
                    seg1.convertToPartlyUsed = true;
                }
                result.add(seg1);
                if (++i < segList1.size()) {
                    seg1 = segList1.get(i);
                }
            } else {
                if (seg2.fullyUsed()) {
                    seg1.convertToPartlyUsed = true;
                }
                result.add(seg2);
                if (++j < segList2.size()) {
                    seg2 = segList2.get(j);
                }
            }
        }
        if (i < segList1.size()) {
            for (; i < segList1.size(); i++) {
                seg1 = segList1.get(i);
                if (seg1.fullyUsed()) {
                    seg1.convertToPartlyUsed = true;
                }
                result.add(seg1);
            }
        } else if (j < segList2.size()) {
            for (; j < segList2.size(); j++) {
                seg2 = segList2.get(j);
                if (seg2.fullyUsed()) {
                    seg2.convertToPartlyUsed = true;
                }
                result.add(seg2);
            }
        }
        return result;
    }

    //return full matched filter and partial matched filter
    private Pair<RexNode, RexNode> generateSplitFilters(EquivalentFilter result) {
        RexNode partitionIrrelevantFilter = createPartitionIrrelevantFilter(result);
        RexNode filterWithPartialPar = createPartialMatchedFilter(result);
        return new Pair<>(partitionIrrelevantFilter, filterWithPartialPar);
    }

    private RexNode createPartialMatchedFilter(EquivalentFilter equivalentFilter) {
        //unary operator
        if (equivalentFilter.children.isEmpty()) {
            if (equivalentFilter.parInputRef == null) { //filter without partition column
                return equivalentFilter.origin;
            } else if (equivalentFilter.parInputRef != null && equivalentFilter.querySegments.isEmpty()) { //skip filter that read no data
                return null;
            } else {
                RexNode partialSegmentFilter = createSegmentFilter(equivalentFilter.querySegments, equivalentFilter.parInputRef);
                return partialSegmentFilter;
            }
        } else {
            if (equivalentFilter.includePartition()) {
                boolean containsPartlyUsedSegments = false;
                for (DetailedSegments querySegment : equivalentFilter.querySegments) {
                    if (querySegment.fullyUsed()) {
                        continue;
                    } else {
                        containsPartlyUsedSegments = true;
                    }
                }
                if (!containsPartlyUsedSegments) {
                    return null;
                }
            }
            List<RexNode> children = new ArrayList<>();
            boolean queryPartialSegments = false;
            for (DetailedSegments sg : equivalentFilter.querySegments) {
                if (sg.fullyUsed()) {
                    queryPartialSegments = true;
                    break;
                }
            }
            if (!queryPartialSegments) {
                return null;
            }
            for (EquivalentFilter child : equivalentFilter.children) {
                RexNode rexNode =  createPartialMatchedFilter(child);
                if (null != rexNode) {
                    children.add(rexNode);
                }
            }
            if (children.isEmpty()) {
                return null;
            } else if (children.size() == 1) {
                return children.get(0);
            } else {
                return builder.makeCall(equivalentFilter.op, children);
            }
        }
    }

}
