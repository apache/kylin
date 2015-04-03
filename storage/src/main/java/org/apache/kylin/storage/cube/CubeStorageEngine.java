package org.apache.kylin.storage.cube;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.DerivedFilterTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CubeStorageEngine implements IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(CubeStorageEngine.class);

    private static final long MEM_BUDGET_PER_QUERY = 3L * 1024 * 1024 * 1024; // 3G

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;

    public CubeStorageEngine(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest) {
        Collection<TblColRef> groups = sqlDigest.groupbyColumns;
        TupleFilter filter = sqlDigest.filter;

        // build dimension & metrics
        Collection<TblColRef> dimensions = new HashSet<TblColRef>();
        Collection<FunctionDesc> metrics = new HashSet<FunctionDesc>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        // all dimensions = groups + filter dimensions
        Set<TblColRef> filterDims = Sets.newHashSet(dimensions);
        filterDims.removeAll(groups);

        // expand derived (xxxD means contains host columns only, derived columns were translated)
        Set<TblColRef> derivedPostAggregation = Sets.newHashSet();
        Set<TblColRef> groupsD = expandDerived(groups, derivedPostAggregation);
        Set<TblColRef> filterDimsD = expandDerived(filterDims, derivedPostAggregation);
        filterDimsD.removeAll(groupsD);
        derivedPostAggregation.removeAll(groups);

        // identify cuboid
        Set<TblColRef> dimensionsD = Sets.newHashSet();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(filterDimsD);
        Cuboid cuboid = identifyCuboid(dimensionsD);
        context.setCuboid(cuboid);

        // isExactAggregation? meaning: tuples returned from storage requires no further aggregation in query engine
        Set<TblColRef> singleValuesD = findSingleValueColumns(filter);
        boolean isExactAggregation = isExactAggregation(cuboid, groups, filterDimsD, singleValuesD, derivedPostAggregation);
        context.setExactAggregation(isExactAggregation);

        // replace derived columns in filter with host columns; columns on loosened condition must be added to group by
        TupleFilter filterD = translateDerived(filter, groupsD);

        setThreshold(dimensionsD, metrics, context); // set cautious threshold to prevent out of memory
        // TODO enable coprocessor
//        setCoprocessor(groupsCopD, valueDecoders, context); // enable coprocessor if beneficial
        setLimit(filter, context);

        List<CubeScanner> scanners = Lists.newArrayList();
        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            scanners.add(new CubeScanner(cubeSeg, dimensionsD, groupsD, metrics, filterD, context));
        }
        
        return new SerializedCubeTupleIterator(scanners);
    }

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric()) {
                metrics.add(func);
            }
        }

        for (TblColRef column : sqlDigest.allColumns) {
            // skip measure columns
            if (sqlDigest.metricColumns.contains(column)) {
                continue;
            }
            dimensions.add(column);
        }
    }

    private Set<TblColRef> expandDerived(Collection<TblColRef> cols, Set<TblColRef> derivedPostAggregation) {
        Set<TblColRef> expanded = Sets.newHashSet();
        for (TblColRef col : cols) {
            if (cubeDesc.isDerived(col)) {
                DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
                for (TblColRef hostCol : hostInfo.columns) {
                    expanded.add(hostCol);
                    if (hostInfo.isOneToOne == false)
                        derivedPostAggregation.add(hostCol);
                }
            } else {
                expanded.add(col);
            }
        }
        return expanded;
    }

    private Cuboid identifyCuboid(Set<TblColRef> dimensions) {
        long cuboidID = 0;
        for (TblColRef column : dimensions) {
            int index = cubeDesc.getRowkey().getColumnBitIndex(column);
            cuboidID |= 1L << index;
        }
        return Cuboid.findById(cubeDesc, cuboidID);
    }

    @SuppressWarnings("unchecked")
    private Set<TblColRef> findSingleValueColumns(TupleFilter filter) {
        Collection<? extends TupleFilter> toCheck;
        if (filter instanceof CompareTupleFilter) {
            toCheck = Collections.singleton(filter);
        } else if (filter instanceof LogicalTupleFilter && filter.getOperator() == FilterOperatorEnum.AND) {
            toCheck = filter.getChildren();
        } else {
            return (Set<TblColRef>) Collections.EMPTY_SET;
        }

        Set<TblColRef> result = Sets.newHashSet();
        for (TupleFilter f : toCheck) {
            if (f instanceof CompareTupleFilter) {
                CompareTupleFilter compFilter = (CompareTupleFilter) f;
                // is COL=const ?
                if (compFilter.getOperator() == FilterOperatorEnum.EQ && compFilter.getValues().size() == 1 && compFilter.getColumn() != null) {
                    result.add(compFilter.getColumn());
                }
            }
        }

        // expand derived
        Set<TblColRef> resultD = Sets.newHashSet();
        for (TblColRef col : result) {
            if (cubeDesc.isDerived(col)) {
                DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
                if (hostInfo.isOneToOne) {
                    for (TblColRef hostCol : hostInfo.columns) {
                        resultD.add(hostCol);
                    }
                }
                //if not one2one, it will be pruned
            } else {
                resultD.add(col);
            }
        }
        return resultD;
    }

    private boolean isExactAggregation(Cuboid cuboid, Collection<TblColRef> groups, Set<TblColRef> othersD, Set<TblColRef> singleValuesD, Set<TblColRef> derivedPostAggregation) {
        boolean exact = true;

        if (cuboid.requirePostAggregation()) {
            exact = false;
            logger.info("exactAggregation is false because cuboid " + cuboid.getInputID() + "=> " + cuboid.getId());
        }

        // derived aggregation is bad, unless expanded columns are already in group by
        if (groups.containsAll(derivedPostAggregation) == false) {
            exact = false;
            logger.info("exactAggregation is false because derived column require post aggregation: " + derivedPostAggregation);
        }

        // other columns (from filter) is bad, unless they are ensured to have single value
        if (singleValuesD.containsAll(othersD) == false) {
            exact = false;
            logger.info("exactAggregation is false because some column not on group by: " + othersD //
                    + " (single value column: " + singleValuesD + ")");
        }

        if (exact) {
            logger.info("exactAggregation is true");
        }
        return exact;
    }

    @SuppressWarnings("unchecked")
    private TupleFilter translateDerived(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null)
            return filter;

        if (filter instanceof CompareTupleFilter) {
            return translateDerivedInCompare((CompareTupleFilter) filter, collector);
        }

        List<TupleFilter> children = (List<TupleFilter>) filter.getChildren();
        List<TupleFilter> newChildren = Lists.newArrayListWithCapacity(children.size());
        boolean modified = false;
        for (TupleFilter child : children) {
            TupleFilter translated = translateDerived(child, collector);
            newChildren.add(translated);
            if (child != translated)
                modified = true;
        }
        if (modified) {
            filter = replaceChildren(filter, newChildren);
        }
        return filter;
    }

    private TupleFilter replaceChildren(TupleFilter filter, List<TupleFilter> newChildren) {
        if (filter instanceof LogicalTupleFilter) {
            LogicalTupleFilter r = new LogicalTupleFilter(filter.getOperator());
            r.addChildren(newChildren);
            return r;
        } else
            throw new IllegalStateException("Cannot replaceChildren on " + filter);
    }

    private TupleFilter translateDerivedInCompare(CompareTupleFilter compf, Set<TblColRef> collector) {
        if (compf.getColumn() == null || compf.getValues().isEmpty())
            return compf;

        TblColRef derived = compf.getColumn();
        if (cubeDesc.isDerived(derived) == false)
            return compf;

        DeriveInfo hostInfo = cubeDesc.getHostInfo(derived);
        CubeManager cubeMgr = CubeManager.getInstance(this.cubeInstance.getConfig());
        CubeSegment seg = cubeInstance.getLatestReadySegment();
        LookupStringTable lookup = cubeMgr.getLookupTable(seg, hostInfo.dimension);
        Pair<TupleFilter, Boolean> translated = DerivedFilterTranslator.translate(lookup, hostInfo, compf);
        TupleFilter translatedFilter = translated.getFirst();
        boolean loosened = translated.getSecond();
        if (loosened) {
            collectColumnsRecursively(translatedFilter, collector);
        }
        return translatedFilter;
    }

    private void collectColumnsRecursively(TupleFilter filter, Set<TblColRef> collector) {
        if (filter instanceof ColumnTupleFilter) {
            collectColumns(((ColumnTupleFilter) filter).getColumn(), collector);
        }
        for (TupleFilter child : filter.getChildren()) {
            collectColumnsRecursively(child, collector);
        }
    }

    private void collectColumns(TblColRef col, Set<TblColRef> collector) {
        if (cubeDesc.isDerived(col)) {
            DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
            for (TblColRef h : hostInfo.columns)
                collector.add(h);
        } else {
            collector.add(col);
        }
    }

    private void setThreshold(Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics, StorageContext context) {
        boolean hasMemHungryCountDistinct = false;
        for (FunctionDesc func : metrics) {
            if (func.isCountDistinct() && !func.isHolisticCountDistinct()) {
                hasMemHungryCountDistinct = true;
            }
        }

        // need to limit the memory usage for memory hungry count distinct
        if (hasMemHungryCountDistinct == false) {
            return;
        }

        int rowSizeEst = dimensions.size() * 3;
        for (FunctionDesc func : metrics) {
            rowSizeEst += func.getReturnDataType().getSpaceEstimate();
        }

        long rowEst = MEM_BUDGET_PER_QUERY / rowSizeEst;
        context.setThreshold((int) rowEst);
    }

    private void setLimit(TupleFilter filter, StorageContext context) {
        boolean goodAggr = context.isExactAggregation();
        boolean goodFilter = filter == null || (TupleFilter.isEvaluableRecursively(filter) && context.isCoprocessorEnabled());
        boolean goodSort = context.hasSort() == false;
        if (goodAggr && goodFilter && goodSort) {
            logger.info("Enable limit " + context.getLimit());
            context.enableLimit();
        }
    }

}
