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

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
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
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.visitor.TupleFilterVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
public class OLAPFilterRel extends Filter implements OLAPRel {

    public static final int ONE_DAY_MS = 86400 * 1000;
    private static final Set<String> DAY_FORMATE = ImmutableSet.of("yyyyMMdd", "yyyy-MM-dd");

    ColumnRowType columnRowType;
    OLAPContext context;
    boolean autoJustTimezone = KylinConfig.getInstanceFromEnv().getStreamingDerivedTimeTimezone().length() > 0;
    boolean doOptimizePartition = true;
    public Set<TblColRef> filterColRefs = new HashSet<>();
    public List<CubeSegment> preCalculatedSegment = new ArrayList<>();
    private FastDateFormat format;
    private RexBuilder builder = new RexBuilder(new JavaTypeFactoryImpl());
    private InputRefVisitor inputRefVisitor = new InputRefVisitor();

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
        boolean needOptimizeFilter = false;
        boolean canOptimizeFilter = true;
        List<DetailedSegments> querySegments = null;
        SegmentFilter segmentFilter = null;

        if (doOptimizePartition && checkQueryCanOpt(this.getContext().realization)) {
            try {
                List<ColumnDesc> columnDescs = null;
                if (this.getInput() instanceof OLAPTableScan) {
                    OLAPTableScan olapTableScan = (OLAPTableScan) this.getInput();
                    columnDescs = olapTableScan.olapTable.getSourceColumns();
                } else {
                    canOptimizeFilter = false;
                }
                //partition column index
                int parIdx = getParIdx(columnDescs);
                segmentFilter = extractSegmentFilter((RexCall) this.getCondition(), parIdx);
                if (canOptimizeFilter && segmentFilter.segmentFilter != null) {
                    PartitionDesc partitionDesc = this.getContext().realization.getModel().getPartitionDesc();
                    this.format = DateFormat.getDateFormat(partitionDesc.getPartitionDateFormat());
                    querySegments = calSegments(segmentFilter.segmentFilter,
                            (CubeInstance) this.getContext().realization);
                    for (DetailedSegments querySegment : querySegments) {
                        if (querySegment.fullyUsed()) {
                            needOptimizeFilter = true;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("failed to remove partition dimension dynamically will fall back", e);
                canOptimizeFilter = false;
            }
        }

        if (canOptimizeFilter && needOptimizeFilter) {
            /**
             * first是全匹配的，不需要分区过滤
             * second是需要进行分区过滤
             */
            Pair<RexNode, RexNode> splits = generateSplitFilters(querySegments, segmentFilter);
            rewriteNodeWithPartitonSplit(parent, implementor, querySegments, splits);
        } else {
            originFilterRewrite(implementor);
        }
    }

    private boolean checkQueryCanOpt(IRealization realization) {
        try {
            if (realization == null || realization.getModel() == null
                    || !realization.getConfig().removePartitionDimensionDynamically())
                return false;
            final PartitionDesc partitionDesc = realization.getModel().getPartitionDesc();
            if (partitionDesc != null && partitionDesc.getPartitionDateColumnRef() != null
                    && partitionDesc.getPartitionTimeColumnRef() == null) {
                final String partitionDateFormat = partitionDesc.getPartitionDateFormat();
                if (DAY_FORMATE.contains(partitionDateFormat)) {
                    SQLDigest sqlDigest = context.sqlDigest;
                    CubeInstance cubeInstance = (CubeInstance) context.realization;
                    TblColRef partitionCol = cubeInstance.getModel().getPartitionDesc().getPartitionDateColumnRef();
                    return !sqlDigest.groupbyColumns.contains(partitionCol)
                            && sqlDigest.filterColumns.contains(partitionCol);
                }
            }
        }catch (Exception e){
            logger.warn("check filter optimize fail {}", e);
            return false;
        }
        return false;
    }

    private int getParIdx(List<ColumnDesc> columnDescs) {
        int parIdx = 0;
        CubeInstance realization = (CubeInstance) this.getContext().realization;
        for (ColumnDesc col : columnDescs) {
            if (col.equals(realization.getModel().getPartitionDesc().getPartitionDateColumnRef().getColumnDesc())) {
                break;
            }
            parIdx++;
        }
        return parIdx;
    }

    private void originFilterRewrite(RewriteImplementor implementor) {
        implementor.visitChild(this, getInput());

        this.rowType = this.deriveRowType();
        this.columnRowType = buildColumnRowType();
    }

    private void rewriteNodeWithPartitonSplit(RelNode parent, RewriteImplementor implementor,
            List<DetailedSegments> segments, Pair<RexNode, RexNode> splits) {
        OLAPFilterRel fullMatchedRel = null;
        OLAPFilterRel partialMatchedRel = null;
        if (splits.getFirst() != null) { //full matched filter is not empty, we need to split filter
            fullMatchedRel = (OLAPFilterRel) this.copy(traitSet, this.getInput(), splits.getFirst());
            if (null != context.filterColumns) {
                fullMatchedRel.filterColRefs.addAll(context.filterColumns);
            }
            fullMatchedRel.context = this.getContext();
            fullMatchedRel.doOptimizePartition = false;

            List<CubeSegment> fullyUsed = new LinkedList<>();
            for (DetailedSegments querySegment : segments) {
                if (querySegment.fullyUsed()) {
                    fullyUsed.add(querySegment.segment);
                }
            }
            fullMatchedRel.preCalculatedSegment = fullyUsed;
            fullMatchedRel.filterColRefs.addAll(context.filterColumns);
            fullMatchedRel.filterColRefs
                    .remove(context.realization.getModel().getPartitionDesc().getPartitionDateColumnRef());
        }
        if (splits.getSecond() != null) {
            partialMatchedRel = (OLAPFilterRel) this.copy(traitSet, this.getInput(), ((RexCall) this.getCondition())
                    .clone(this.getCondition().getType(), Lists.newArrayList(splits.getSecond())));
            partialMatchedRel.context = this.getContext();
            partialMatchedRel.doOptimizePartition = false;
        }

        int i = 0;
        for (; i < parent.getInputs().size(); i++) {
            if (parent.getInputs().get(i).getId() == this.getId()) {
                break;
            }
        }
        logger.info("Filter rewrite :\n all matched:{} ,segment:{} .\n partition filter {} ",
                RelOptUtil.toString(fullMatchedRel), fullMatchedRel.preCalculatedSegment,
                RelOptUtil.toString(partialMatchedRel));

        if (partialMatchedRel == null) {
            RelNode toReplace = fullMatchedRel;
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

    private List<DetailedSegments> calSegments(RexCall call, CubeInstance cubeInstance) throws Exception {
        List<DetailedSegments> segments = cubeInstance.getSegments(SegmentStatusEnum.READY).stream()
                .map(seg -> new DetailedSegments(seg, false)).collect(Collectors.toList());
        switch (call.getKind()) {
        case AND: {
            List<DetailedSegments> querySegments = null;
            for (RexNode operand : call.getOperands()) {
                List<DetailedSegments> childSegments = calSegments((RexCall) operand, cubeInstance);
                if (querySegments == null) {
                    querySegments = childSegments;
                } else {
                    querySegments = intersectSegments(querySegments, childSegments);
                }
            }
            return querySegments == null ? new LinkedList<>() : querySegments;
        }
        case OR: {
            List<DetailedSegments> querySegments = new LinkedList<>();
            for (RexNode operand : call.getOperands()) {
                List<DetailedSegments> childSegments = calSegments((RexCall) operand, cubeInstance);
                querySegments = unionSegments(querySegments, childSegments);
            }
            return querySegments;
        }
        case EQUALS: {
            long time = getTimeMillisFromLiteral(call.getOperands().get(1));
            LinkedList<DetailedSegments> realQuerySegments = new LinkedList<>();
            segments.forEach(seg -> {
                if (seg.start <= time && time < seg.end) {
                    DetailedSegments partial = new DetailedSegments(seg.segment, false);
                    partial.addSegmentEquals(time);
                    realQuerySegments.add(partial);
                }
            });
            return realQuerySegments;
        }
        case GREATER_THAN: {
            long time = getTimeMillisFromLiteral(call.getOperands().get(1));
            LinkedList<DetailedSegments> realQuerySegments = new LinkedList<>();
            segments.forEach(seg -> {
                if (seg.start > time) {
                    realQuerySegments.add(new DetailedSegments(seg.segment, true));
                } else if (seg.start <= time && (seg.end - seg.segmentUnit) > time) {
                    DetailedSegments partial = new DetailedSegments(seg.segment, false);
                    partial.addSegmentGreaterThan(time);
                    realQuerySegments.add(partial);
                }
            });
            return realQuerySegments;
        }
        case GREATER_THAN_OR_EQUAL: {
            long time = getTimeMillisFromLiteral(call.getOperands().get(1));
            LinkedList<DetailedSegments> realQuerySegments = new LinkedList<>();
            segments.forEach(seg -> {
                if (seg.start >= time) {
                    realQuerySegments.add(new DetailedSegments(seg.segment, true));
                } else if (seg.start <= time && seg.end > time) {
                    DetailedSegments partial = new DetailedSegments(seg.segment, false);
                    partial.addSegmentGreaterOrEqualThan(time);
                    realQuerySegments.add(partial);
                }
            });
            return realQuerySegments;
        }
        case LESS_THAN: {
            long time = getTimeMillisFromLiteral(call.getOperands().get(1));
            LinkedList<DetailedSegments> realQuerySegments = new LinkedList<>();
            segments.forEach(seg -> {
                if (seg.end <= time) {
                    realQuerySegments.add(new DetailedSegments(seg.segment, true));
                } else if (seg.start < time && seg.end > time) {
                    DetailedSegments partial = new DetailedSegments(seg.segment, false);
                    partial.addSegmentLessThan(time);
                    realQuerySegments.add(partial);
                }
            });
            return realQuerySegments;
        }
        case LESS_THAN_OR_EQUAL: {
            long time = getTimeMillisFromLiteral(call.getOperands().get(1));
            LinkedList<DetailedSegments> realQuerySegments = new LinkedList<>();
            segments.forEach(seg -> {
                if (seg.end - ONE_DAY_MS <= time) {
                    realQuerySegments.add(new DetailedSegments(seg.segment, true));
                } else if (seg.start <= time && seg.end - ONE_DAY_MS > time) {
                    DetailedSegments partial = new DetailedSegments(seg.segment, false);
                    partial.addSegmentLessOrEqualThan(time);
                    realQuerySegments.add(partial);
                }
            });
            return realQuerySegments;
        }
        default:
            throw new UnsupportedOperationException("unsupported");
        }
    }

    /**
     *
     * @param filter
     * @param parIdx
     * @return
     * If segment column expression can be extract in one group return true,else return false
     * e.g. "where partition >= xxx and (a = xxx or partition <= xxx) " will return false
     * "where partition >= xxx and (partition = xxx or partition <= xxx)  and a=xxx " will return false
     * because or is not support "
     */
    private SegmentFilter extractSegmentFilter(RexCall filter, int parIdx) {
        List<SegmentFilter> children = new LinkedList<>();
        switch (filter.getKind()) {
        case AND: {
            RexInputRef parRef = null;
            for (RexNode child : filter.getOperands()) {
                SegmentFilter childFilter = extractSegmentFilter((RexCall) child, parIdx);
                if (!childFilter.canOpt)
                    return SegmentFilter.createNoOptSegment();
                if (childFilter.segmentFilter != null) {
                    parRef = childFilter.parInputRef;
                }
                children.add(childFilter);
            }
            List<RexCall> segmentFilterNodes = children.stream().map(e -> e.segmentFilter).filter(e -> e != null)
                    .collect(Collectors.toList());
            if (segmentFilterNodes.size() > 1) {
                return SegmentFilter.createOptSegment(filter,
                        (RexCall) builder.makeCall(SqlStdOperatorTable.AND, segmentFilterNodes), parRef, children);
            } else if (segmentFilterNodes.size() == 1) {
                return SegmentFilter.createOptSegment(filter, segmentFilterNodes.get(0), parRef, children);
            } else {
                return SegmentFilter.createOptSegment(filter, null, null, children);
            }
        }
        case OR: {
            for (RexNode child : filter.getOperands()) {
                SegmentFilter childFilter = extractSegmentFilter((RexCall) child, parIdx);
                if (!childFilter.canOpt)
                    return SegmentFilter.createNoOptSegment();
                if (childFilter.segmentFilter != null) {
                    return SegmentFilter.createNoOptSegment();
                }
                children.add(childFilter);
            }
            return SegmentFilter.createOptSegment(filter, null, null, children);
        }
        default:
            List<RexInputRef> inputs = filter.accept(inputRefVisitor);
            if (inputs == null) {
                return SegmentFilter.createNoOptSegment();
            } else {
                Set<Integer> inputIdxs = inputs.stream().map(e -> e.getIndex()).collect(Collectors.toSet());
                if (inputIdxs.contains(parIdx)) {
                    if (inputIdxs.size() == 1) {
                        return SegmentFilter.createOptSegment(filter, parseCastFilter(filter), inputs.get(0),
                                new LinkedList<>());
                    } else {
                        return SegmentFilter.createNoOptSegment();
                    }
                } else
                    return SegmentFilter.createOptSegment(filter, null, null, new LinkedList<>());
            }
        }
    }

    private RexCall parseCastFilter(RexCall filter) {
        RexLiteral literal = null;
        RexNode castValue = null;
        if (filter.getOperands().size() == 2) {
            final RexNode first = filter.getOperands().get(0);
            final RexNode second = filter.getOperands().get(1);
            if (first.getKind() == SqlKind.LITERAL && second.getKind() == SqlKind.CAST
                    && ((RexCall) second).getOperands().get(0).getKind() == SqlKind.INPUT_REF) {
                literal = (RexLiteral) first;
                castValue = ((RexCall) second).getOperands().get(0);
            } else if (second.getKind() == SqlKind.LITERAL && first.getKind() == SqlKind.CAST
                    && ((RexCall) first).getOperands().get(0).getKind() == SqlKind.INPUT_REF) {
                castValue = ((RexCall) first).getOperands().get(0);
                literal = (RexLiteral) second;
            }

        }
        if (literal != null && castValue != null)
            return (RexCall) builder.makeCall(filter.getOperator(), Lists.newArrayList(castValue, literal));
        else
            return filter;
    }

    public RexNode createPartitionIrrelevantFilter(SegmentFilter equivalentFilter) {
        if (equivalentFilter.children.isEmpty()) {
            if (equivalentFilter.parInputRef == null) {
                return equivalentFilter.origin;
            } else {
                return null;
            }
        } else {
            List<RexNode> filter = new ArrayList<>();
            for (SegmentFilter child : equivalentFilter.children) {
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

    private RexNode createTimeRangeRexNode(RexInputRef parInputRef, long rangeStart, long rangeEnd) {
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
        if (value instanceof Date) {
            return DateFormat.stringToMillis(value.toString());
        }
        if (value instanceof String || value instanceof Integer || value instanceof Long) {
            return format.parse(value.toString()).getTime();
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime();
        }
        throw new UnsupportedOperationException("unsupport time");
    }

    public Object getValue(RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            return getRexLiteral((RexLiteral) rexNode);
        }
        if (rexNode instanceof RexCall) {
            switch (rexNode.getKind()) {
            case CAST: {
                RexLiteral literal = (RexLiteral) ((RexCall) rexNode).getOperands().get(0);
                return getRexLiteral(literal);
            }
            default:
                throw new UnsupportedOperationException("unsupported");
            }
        } else {
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

    static class SegmentFilter {
        //original RexNode information
        RexNode origin;
        RexCall segmentFilter;
        SqlOperator op;
        RexInputRef parInputRef;
        boolean canOpt;
        List<SegmentFilter> children;

        private SegmentFilter(RexCall origin, RexCall segmentFilter, SqlOperator op, RexInputRef inputRef,
                Boolean canOpt, List<SegmentFilter> children) {
            this.origin = origin;
            this.op = op;
            this.segmentFilter = segmentFilter;
            this.canOpt = canOpt;
            this.parInputRef = inputRef;
            this.children = children;
        }

        public static SegmentFilter createNoOptSegment() {
            return new SegmentFilter(null, null, null, null, false, new LinkedList<>());
        }

        public static SegmentFilter createOptSegment(RexCall origin, RexCall segmentFilter, RexInputRef inputRef,
                List<SegmentFilter> children) {
            return new SegmentFilter(origin, segmentFilter, origin.getOperator(), inputRef, true, children);
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
        private long segmentUnit = ONE_DAY_MS;

        public DetailedSegments(CubeSegment segment, boolean fullyUsed) {
            this.segment = segment;
            this.end = (long) segment.getSegRange().end.v;
            this.start = (long) segment.getSegRange().start.v;
            this.segmentSize = (int) ((this.end - this.start) / segmentUnit);
            this.querySegments = new BitSet(segmentSize);
            if (fullyUsed) {
                querySegments.set(0, segmentSize, true);
            }
        }

        public DetailedSegments(CubeSegment segment, BitSet bitSet) {
            this.segment = segment;
            this.end = (long) segment.getSegRange().end.v;
            this.start = (long) segment.getSegRange().start.v;
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
                querySegments.set(0, segmentSize, true);
                return;
            }
            int index = (int) ((timeStamp - start) / segmentUnit);
            // (start + index * segmentUnit) <= timeStamp
            if ((start + index * segmentUnit) < timeStamp) {
                index = index + 1;
            }
            querySegments.set(index, segmentSize, true);
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
            return end - start != ONE_DAY_MS;
        }
    }

    public List<DetailedSegments> intersectSegments(List<DetailedSegments> seg1, List<DetailedSegments> seg2) {
        LinkedList<DetailedSegments> result = new LinkedList<>();
        if (seg1.isEmpty() || seg2.isEmpty()) {
            return result;
        }
        Collections.sort(seg1, Comparator.comparingLong(a -> a.start));
        Collections.sort(seg2, Comparator.comparingLong(a -> a.start));
        for (int i = 0, j = 0; i < seg1.size() && j < seg2.size();) {
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

    private List<DetailedSegments> unionSegments(List<DetailedSegments> segList1, List<DetailedSegments> segList2) {
        LinkedList<DetailedSegments> result = new LinkedList<>();
        int i = 0;
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
    private Pair<RexNode, RexNode> generateSplitFilters(List<DetailedSegments> querySegments,
            SegmentFilter segmentFilter) {
        RexNode partitionIrrelevantFilter = createPartitionIrrelevantFilter(segmentFilter);
        RexNode filterWithPartialPar = null;
        if (preCheckRewrite(querySegments)) {
            RexNode segmentFilterNode = createSegmentFilter(querySegments, segmentFilter.parInputRef);
            filterWithPartialPar = builder.makeCall(SqlStdOperatorTable.AND,
                    Lists.newArrayList(segmentFilterNode, partitionIrrelevantFilter));
        }
        return new Pair<>(partitionIrrelevantFilter, filterWithPartialPar);
    }

    private RexNode createSegmentFilter(List<DetailedSegments> segments, RexInputRef parInputRef) {
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
            for (int i = 0; i < segment.querySegments.length(); i++) {
                if (rangeStart == 0) {
                    if (segment.querySegments.get(i)) {
                        rangeStart = segment.start + i * segmentUnit;
                        rangeEnd = rangeStart;
                    }
                    continue;
                }
                if (!segment.querySegments.get(i)) { //query time range is discontinuous
                    operands.add(createTimeRangeRexNode(parInputRef, rangeStart, rangeEnd));
                    rangeStart = segment.start + i * segmentUnit;
                    rangeEnd = segment.start + i * segmentUnit;
                } else {
                    if ((rangeEnd + segmentUnit) == (segment.start + i * segmentUnit)) { //query time range is continuous
                        rangeEnd = segment.start + i * segmentUnit;
                    } else {
                        operands.add(createTimeRangeRexNode(parInputRef, rangeStart, rangeEnd));
                        rangeStart = segment.start + i * segmentUnit;
                        rangeEnd = segment.start + i * segmentUnit;
                    }
                }
            }
        }
        if (rangeStart != 0) {
            operands.add(createTimeRangeRexNode(parInputRef, rangeStart, rangeEnd));
        }
        if (operands.size() == 0) {
            return null;
        } else if (operands.size() == 1) {
            return operands.get(0);
        } else {
            return builder.makeCall(SqlStdOperatorTable.OR, operands);
        }
    }

    private boolean preCheckRewrite(List<DetailedSegments> segments) {
        //全是Segment全匹配，部分partiton匹配将不存在，不改写
        boolean containsPartlyUsedSegments = false;
        for (DetailedSegments querySegment : segments) {
            if (!querySegment.fullyUsed()) {
                containsPartlyUsedSegments = true;
                break;
            }
        }

        //全是Segment部分匹配，partiton不需要改写
        boolean queryPartialSegments = false;
        for (DetailedSegments sg : segments) {
            if (sg.fullyUsed()) {
                queryPartialSegments = true;
                break;
            }
        }
        return containsPartlyUsedSegments && queryPartialSegments;
    }

    private class InputRefVisitor implements RexVisitor<List<RexInputRef>> {

        @Override
        public List<RexInputRef> visitInputRef(RexInputRef inputRef) {
            List<RexInputRef> rexIds = new LinkedList<>();
            rexIds.add(inputRef);
            return rexIds;
        }

        @Override
        public List<RexInputRef> visitLocalRef(RexLocalRef localRef) {
            return null;
        }

        @Override
        public List<RexInputRef> visitLiteral(RexLiteral literal) {
            return new LinkedList<>();
        }

        @Override
        public List<RexInputRef> visitCall(RexCall call) {
            List<RexInputRef> rexIds = new LinkedList<>();
            for (RexNode operand : call.getOperands()) {
                List<RexInputRef> child = operand.accept(this);
                if (child == null)
                    return null;
                rexIds.addAll(child);
            }
            return rexIds;
        }

        @Override
        public List<RexInputRef> visitOver(RexOver over) {
            return null;
        }

        @Override
        public List<RexInputRef> visitCorrelVariable(RexCorrelVariable correlVariable) {
            return null;
        }

        @Override
        public List<RexInputRef> visitDynamicParam(RexDynamicParam dynamicParam) {
            return null;
        }

        @Override
        public List<RexInputRef> visitRangeRef(RexRangeRef rangeRef) {
            return null;
        }

        @Override
        public List<RexInputRef> visitFieldAccess(RexFieldAccess fieldAccess) {
            return null;
        }

        @Override
        public List<RexInputRef> visitSubQuery(RexSubQuery subQuery) {
            return null;
        }

        @Override
        public List<RexInputRef> visitTableInputRef(RexTableInputRef fieldRef) {
            return null;
        }

        @Override
        public List<RexInputRef> visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            return null;
        }

    }
}
