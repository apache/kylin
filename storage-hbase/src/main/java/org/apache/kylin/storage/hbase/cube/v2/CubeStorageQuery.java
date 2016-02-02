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

package org.apache.kylin.storage.hbase.cube.v2;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.NotEnoughGTInfoException;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.translate.DerivedFilterTranslator;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

public class CubeStorageQuery implements ICachableStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(CubeStorageQuery.class);

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;

    public CubeStorageQuery(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        // check whether this is a TopN query
        checkAndRewriteTopN(sqlDigest);

        Collection<TblColRef> groups = sqlDigest.groupbyColumns;
        TupleFilter filter = sqlDigest.filter;

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
        Set<FunctionDesc> metrics = new LinkedHashSet<FunctionDesc>();
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
        Set<TblColRef> dimensionsD = new LinkedHashSet<TblColRef>();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(filterDimsD);
        Cuboid cuboid = identifyCuboid(dimensionsD);
        context.setCuboid(cuboid);

        // isExactAggregation? meaning: tuples returned from storage requires no further aggregation in query engine
        Set<TblColRef> singleValuesD = findSingleValueColumns(filter);
        boolean isExactAggregation = isExactAggregation(cuboid, groups, filterDimsD, singleValuesD, derivedPostAggregation);
        context.setExactAggregation(isExactAggregation);

        if (isExactAggregation) {
            metrics = replaceHolisticCountDistinct(metrics);
        }

        // replace derived columns in filter with host columns; columns on loosened condition must be added to group by
        TupleFilter filterD = translateDerived(filter, groupsD);

        setThreshold(dimensionsD, metrics, context); // set cautious threshold to prevent out of memory
        setLimit(filter, context);

        List<CubeSegmentScanner> scanners = Lists.newArrayList();
        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            CubeSegmentScanner scanner;
            try {
                scanner = new CubeSegmentScanner(cubeSeg, cuboid, dimensionsD, groupsD, metrics, filterD, !isExactAggregation);
            } catch (NotEnoughGTInfoException e) {
                //deal with empty cube segment
                logger.info("Cannot construct Segment {}'s GTInfo, this may due to empty segment or broken metadata", cubeSeg);
                logger.info("error stack", e);
                continue;
            }
            scanners.add(scanner);
        }

        if (scanners.isEmpty())
            return ITupleIterator.EMPTY_TUPLE_ITERATOR;

        return newSequentialCubeTupleIterator(scanners, cuboid, dimensionsD, metrics, returnTupleInfo, context);
    }

    private ITupleIterator newSequentialCubeTupleIterator(List<CubeSegmentScanner> scanners, Cuboid cuboid, Set<TblColRef> dimensionsD, Set<FunctionDesc> metrics, TupleInfo returnTupleInfo, StorageContext context) {
        TblColRef topNCol = null;
        for (FunctionDesc func : metrics) {
            if (func.isTopN()) {
                topNCol = func.getTopNLiteralColumn();
                break;
            }
        }

        if (topNCol != null)
            return new SequentialCubeTopNTupleIterator(scanners, cuboid, dimensionsD, topNCol, metrics, returnTupleInfo, context);
        else
            return new SequentialCubeTupleIterator(scanners, cuboid, dimensionsD, metrics, returnTupleInfo, context);
    }

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision
                metrics.add(findAggrFuncFromCubeDesc(func));
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

    private FunctionDesc findAggrFuncFromCubeDesc(FunctionDesc aggrFunc) {
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        return aggrFunc;
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

    private Set<FunctionDesc> replaceHolisticCountDistinct(Set<FunctionDesc> metrics) {
        // for count distinct, try use its holistic version if possible
        Set<FunctionDesc> result = new LinkedHashSet<FunctionDesc>();
        for (FunctionDesc metric : metrics) {
            if (metric.isCountDistinct() == false) {
                result.add(metric);
                continue;
            }

            FunctionDesc holisticVersion = null;
            for (MeasureDesc measure : cubeDesc.getMeasures()) {
                FunctionDesc measureFunc = measure.getFunction();
                if (measureFunc.equals(metric) && measureFunc.isHolisticCountDistinct()) {
                    holisticVersion = measureFunc;
                }
            }
            result.add(holisticVersion == null ? metric : holisticVersion);
        }
        return result;
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
        if (filter == null)
            return;

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
            // FIXME getStorageBytesEstimate() is not appropriate as here we want size in memory (not in storage)
            rowSizeEst += func.getReturnDataType().getStorageBytesEstimate();
        }

        long rowEst = this.cubeInstance.getConfig().getQueryMemBudget() / rowSizeEst;
        if (rowEst > 0) {
            logger.info("Memory budget is set to: " + rowEst);
            context.setThreshold((int) rowEst);
        } else {
            logger.info("Memory budget is not set.");
        }
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

    // ============================================================================

    @Override
    public Range<Long> getVolatilePeriod() {
        return null;
    }

    @Override
    public String getStorageUUID() {
        return cubeInstance.getUuid();
    }

    @Override
    public boolean isDynamic() {
        return false;
    }

    private void checkAndRewriteTopN(SQLDigest sqlDigest) {
        FunctionDesc topnFunc = null;
        TblColRef topnLiteralCol = null;
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            FunctionDesc func = measure.getFunction();
            if (func.isTopN() && sqlDigest.groupbyColumns.contains(func.getTopNLiteralColumn())) {
                topnFunc = func;
                topnLiteralCol = func.getTopNLiteralColumn();
            }
        }

        // if TopN is not involved
        if (topnFunc == null)
            return;

        if (sqlDigest.aggregations.size() != 1) {
            throw new IllegalStateException("When query with topN, only one metrics is allowed.");
        }

        FunctionDesc origFunc = sqlDigest.aggregations.iterator().next();
        if (origFunc.isSum() == false) {
            throw new IllegalStateException("When query with topN, only SUM function is allowed.");
        }

        sqlDigest.aggregations = Lists.newArrayList(topnFunc);
        sqlDigest.groupbyColumns.remove(topnLiteralCol);
        sqlDigest.metricColumns.add(topnLiteralCol);
        logger.info("Rewrite function " + origFunc + " to " + topnFunc);
    }
}
