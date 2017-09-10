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

package org.apache.kylin.storage.gtrecord;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.RawQueryLastHacker;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.gridtable.StorageLimitLevel;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.filter.CaseTupleFilter;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.translate.DerivedFilterTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class GTCubeStorageQueryBase implements IStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(GTCubeStorageQueryBase.class);

    protected final CubeInstance cubeInstance;
    protected final CubeDesc cubeDesc;

    public GTCubeStorageQueryBase(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        GTCubeStorageQueryRequest request = getStorageQueryRequest(context, sqlDigest, returnTupleInfo);

        List<CubeSegmentScanner> scanners = Lists.newArrayList();
        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            CubeSegmentScanner scanner;

            if (cubeDesc.getConfig().isSkippingEmptySegments() && cubeSeg.getInputRecords() == 0) {
                logger.info("Skip cube segment {} because its input record is 0", cubeSeg);
                continue;
            }

            scanner = new CubeSegmentScanner(cubeSeg, request.getCuboid(), request.getDimensions(), request.getGroups(),
                    request.getMetrics(), request.getFilter(), request.getHavingFilter(), request.getContext());
            if (!scanner.isSegmentSkipped())
                scanners.add(scanner);
        }

        if (scanners.isEmpty())
            return ITupleIterator.EMPTY_TUPLE_ITERATOR;

        return new SequentialCubeTupleIterator(scanners, request.getCuboid(), request.getDimensions(),
                request.getGroups(), request.getMetrics(), returnTupleInfo, request.getContext(), sqlDigest);
    }

    protected GTCubeStorageQueryRequest getStorageQueryRequest(StorageContext context, SQLDigest sqlDigest,
            TupleInfo returnTupleInfo) {
        context.setStorageQuery(this);

        //cope with queries with no aggregations
        RawQueryLastHacker.hackNoAggregations(sqlDigest, cubeDesc, returnTupleInfo);

        // Customized measure taking effect: e.g. allow custom measures to help raw queries
        notifyBeforeStorageQuery(sqlDigest);

        Collection<TblColRef> groups = sqlDigest.groupbyColumns;
        TupleFilter filter = sqlDigest.filter;

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
        Set<FunctionDesc> metrics = new LinkedHashSet<FunctionDesc>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        // all dimensions = groups + other(like filter) dimensions
        Set<TblColRef> otherDims = Sets.newHashSet(dimensions);
        otherDims.removeAll(groups);

        // expand derived (xxxD means contains host columns only, derived columns were translated)
        Set<TblColRef> derivedPostAggregation = Sets.newHashSet();
        Set<TblColRef> groupsD = expandDerived(groups, derivedPostAggregation);
        Set<TblColRef> otherDimsD = expandDerived(otherDims, derivedPostAggregation);
        otherDimsD.removeAll(groupsD);

        // identify cuboid
        Set<TblColRef> dimensionsD = new LinkedHashSet<TblColRef>();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(otherDimsD);
        Cuboid cuboid = findCuboid(cubeInstance, dimensionsD, metrics);
        context.setCuboid(cuboid);

        // set whether to aggr at storage
        Set<TblColRef> singleValuesD = findSingleValueColumns(filter);
        context.setNeedStorageAggregation(isNeedStorageAggregation(cuboid, groupsD, singleValuesD));

        // exactAggregation mean: needn't aggregation at storage and query engine both.
        boolean exactAggregation = isExactAggregation(context, cuboid, groups, otherDimsD, singleValuesD,
                derivedPostAggregation, sqlDigest.aggregations);
        context.setExactAggregation(exactAggregation);

        // replace derived columns in filter with host columns; columns on loosened condition must be added to group by
        Set<TblColRef> loosenedColumnD = Sets.newHashSet();
        Set<TblColRef> filterColumnD = Sets.newHashSet();
        TupleFilter filterD = translateDerived(filter, loosenedColumnD);
        groupsD.addAll(loosenedColumnD);
        TupleFilter.collectColumns(filterD, filterColumnD);

        // set limit push down
        enableStorageLimitIfPossible(cuboid, groups, derivedPostAggregation, groupsD, filterD, loosenedColumnD,
                sqlDigest.aggregations, context);
        // set whether to aggregate results from multiple partitions
        enableStreamAggregateIfBeneficial(cuboid, groupsD, context);
        // set query deadline
        context.setDeadline(cubeInstance);

        // push down having clause filter if possible
        TupleFilter havingFilter = checkHavingCanPushDown(sqlDigest.havingFilter, groupsD, sqlDigest.aggregations,
                metrics);

        logger.info(
                "Cuboid identified: cube={}, cuboidId={}, groupsD={}, filterD={}, limitPushdown={}, limitLevel={}, storageAggr={}",
                cubeInstance.getName(), cuboid.getId(), groupsD, filterColumnD, context.getFinalPushDownLimit(),
                context.getStorageLimitLevel(), context.isNeedStorageAggregation());

        return new GTCubeStorageQueryRequest(cuboid, dimensionsD, groupsD, filterColumnD, metrics, filterD,
                havingFilter, context);
    }

    protected abstract String getGTStorage();

    protected Cuboid findCuboid(CubeInstance cubeInstance, Set<TblColRef> dimensionsD, Set<FunctionDesc> metrics) {
        return Cuboid.identifyCuboid(cubeInstance, dimensionsD, metrics);
    }

    protected ITupleConverter newCubeTupleConverter(CubeSegment cubeSeg, Cuboid cuboid,
            Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics, int[] gtColIdx, TupleInfo tupleInfo) {
        return new CubeTupleConverter(cubeSeg, cuboid, selectedDimensions, selectedMetrics, gtColIdx, tupleInfo);
    }

    protected void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions,
            Collection<FunctionDesc> metrics) {
        for (FunctionDesc func : sqlDigest.aggregations) {
            if (!func.isDimensionAsMetric()) {
                // use the FunctionDesc from cube desc as much as possible, that has more info such as HLLC precision
                metrics.add(findAggrFuncFromCubeDesc(func));
            }
        }

        for (TblColRef column : sqlDigest.allColumns) {
            // skip measure columns
            if (sqlDigest.metricColumns.contains(column)
                    && !(sqlDigest.groupbyColumns.contains(column) || sqlDigest.filterColumns.contains(column))) {
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

    protected Set<TblColRef> expandDerived(Collection<TblColRef> cols, Set<TblColRef> derivedPostAggregation) {
        Set<TblColRef> expanded = Sets.newHashSet();
        for (TblColRef col : cols) {
            if (cubeDesc.hasHostColumn(col)) {
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
                if (compFilter.getOperator() == FilterOperatorEnum.EQ && compFilter.getValues().size() == 1
                        && compFilter.getColumn() != null) {
                    result.add(compFilter.getColumn());
                }
            }
        }

        // expand derived
        Set<TblColRef> resultD = Sets.newHashSet();
        for (TblColRef col : result) {
            if (cubeDesc.isExtendedColumn(col)) {
                throw new CubeDesc.CannotFilterExtendedColumnException(col);
            }
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

    public boolean isNeedStorageAggregation(Cuboid cuboid, Collection<TblColRef> groupD,
            Collection<TblColRef> singleValueD) {
        HashSet<TblColRef> temp = Sets.newHashSet();
        temp.addAll(groupD);
        temp.addAll(singleValueD);
        if (cuboid.getColumns().size() == temp.size()) {
            logger.debug("Does not need storage aggregation");
            return false;
        } else {
            logger.debug("Need storage aggregation");
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    protected TupleFilter translateDerived(TupleFilter filter, Set<TblColRef> collector) {
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
        } else if (filter instanceof CaseTupleFilter) {
            CaseTupleFilter r = new CaseTupleFilter();
            r.addChildren(newChildren);
            return r;
        } else {
            throw new IllegalStateException("Cannot replaceChildren on " + filter);
        }
    }

    private TupleFilter translateDerivedInCompare(CompareTupleFilter compf, Set<TblColRef> collector) {
        if (compf.getColumn() == null)
            return compf;

        TblColRef derived = compf.getColumn();
        if (cubeDesc.isExtendedColumn(derived)) {
            throw new CubeDesc.CannotFilterExtendedColumnException(derived);
        }
        if (cubeDesc.isDerived(derived) == false)
            return compf;

        DeriveInfo hostInfo = cubeDesc.getHostInfo(derived);
        LookupStringTable lookup = getLookupStringTableForDerived(derived, hostInfo);
        Pair<TupleFilter, Boolean> translated = DerivedFilterTranslator.translate(lookup, hostInfo, compf);
        TupleFilter translatedFilter = translated.getFirst();
        boolean loosened = translated.getSecond();
        if (loosened) {
            collectColumnsRecursively(translatedFilter, collector);
        }
        return translatedFilter;
    }

    @SuppressWarnings("unchecked")
    protected LookupStringTable getLookupStringTableForDerived(TblColRef derived, DeriveInfo hostInfo) {
        CubeManager cubeMgr = CubeManager.getInstance(this.cubeInstance.getConfig());
        CubeSegment seg = cubeInstance.getLatestReadySegment();
        return cubeMgr.getLookupTable(seg, hostInfo.join);
    }

    private void collectColumnsRecursively(TupleFilter filter, Set<TblColRef> collector) {
        if (filter == null)
            return;

        if (filter instanceof ColumnTupleFilter) {
            collector.add(((ColumnTupleFilter) filter).getColumn());
        }
        for (TupleFilter child : filter.getChildren()) {
            collectColumnsRecursively(child, collector);
        }
    }

    private void enableStorageLimitIfPossible(Cuboid cuboid, Collection<TblColRef> groups,
            Set<TblColRef> derivedPostAggregation, Collection<TblColRef> groupsD, TupleFilter filter,
            Set<TblColRef> loosenedColumnD, Collection<FunctionDesc> functionDescs, StorageContext context) {

        StorageLimitLevel storageLimitLevel = StorageLimitLevel.LIMIT_ON_SCAN;

        //if groupsD is clustered at "head" of the rowkey, then limit push down is possible
        int size = groupsD.size();
        if (!groupsD.containsAll(cuboid.getColumns().subList(0, size))) {
            storageLimitLevel = StorageLimitLevel.LIMIT_ON_RETURN_SIZE;
            logger.debug(
                    "storageLimitLevel set to LIMIT_ON_RETURN_SIZE because groupD is not clustered at head, groupsD: "
                            + groupsD //
                            + " with cuboid columns: " + cuboid.getColumns());
        }

        // derived aggregation is bad, unless expanded columns are already in group by
        if (!groups.containsAll(derivedPostAggregation)) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.debug("storageLimitLevel set to NO_LIMIT because derived column require post aggregation: "
                    + derivedPostAggregation);
        }

        if (!TupleFilter.isEvaluableRecursively(filter)) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.debug("storageLimitLevel set to NO_LIMIT because the filter isn't evaluable");
        }

        if (!loosenedColumnD.isEmpty()) { // KYLIN-2173
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.debug("storageLimitLevel set to NO_LIMIT because filter is loosened: " + loosenedColumnD);
        }

        if (context.hasSort()) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.debug("storageLimitLevel set to NO_LIMIT because the query has order by");
        }

        //if exists measures like max(cal_dt), then it's not a perfect cuboid match, cannot apply limit
        for (FunctionDesc functionDesc : functionDescs) {
            if (functionDesc.isDimensionAsMetric()) {
                storageLimitLevel = StorageLimitLevel.NO_LIMIT;
                logger.debug("storageLimitLevel set to NO_LIMIT because {} isDimensionAsMetric ", functionDesc);
            }
        }

        context.applyLimitPushDown(cubeInstance, storageLimitLevel);
    }

    private void enableStreamAggregateIfBeneficial(Cuboid cuboid, Set<TblColRef> groupsD, StorageContext context) {
        CubeDesc cubeDesc = cuboid.getCubeDesc();
        boolean enabled = cubeDesc.getConfig().isStreamAggregateEnabled();

        Set<TblColRef> shardByInGroups = Sets.newHashSet();
        for (TblColRef col : cubeDesc.getShardByColumns()) {
            if (groupsD.contains(col)) {
                shardByInGroups.add(col);
            }
        }
        if (!shardByInGroups.isEmpty()) {
            enabled = false;
            logger.debug("Aggregate partition results is not beneficial because shard by columns in groupD: "
                    + shardByInGroups);
        }

        if (!context.isNeedStorageAggregation()) {
            enabled = false;
            logger.debug("Aggregate partition results is not beneficial because no storage aggregation");
        }

        if (enabled) {
            context.enableStreamAggregate();
        }
    }

    protected void notifyBeforeStorageQuery(SQLDigest sqlDigest) {
        Map<String, List<MeasureDesc>> map = Maps.newHashMap();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            MeasureType<?> measureType = measure.getFunction().getMeasureType();

            String key = measureType.getClass().getCanonicalName();
            List<MeasureDesc> temp = null;
            if ((temp = map.get(key)) != null) {
                temp.add(measure);
            } else {
                map.put(key, Lists.<MeasureDesc> newArrayList(measure));
            }
        }

        for (List<MeasureDesc> sublist : map.values()) {
            sublist.get(0).getFunction().getMeasureType().adjustSqlDigest(sublist, sqlDigest);
        }
    }

    private TupleFilter checkHavingCanPushDown(TupleFilter havingFilter, Set<TblColRef> groupsD,
            List<FunctionDesc> aggregations, Set<FunctionDesc> metrics) {
        // must have only one segment
        Segments<CubeSegment> readySegs = cubeInstance.getSegments(SegmentStatusEnum.READY);
        if (readySegs.size() != 1)
            return null;

        // sharded-by column must on group by
        CubeDesc desc = cubeInstance.getDescriptor();
        Set<TblColRef> shardBy = desc.getShardByColumns();
        if (groupsD == null || shardBy.isEmpty() || !groupsD.containsAll(shardBy))
            return null;

        // OK, push down
        logger.info("Push down having filter " + havingFilter);

        // convert columns in the filter
        Set<TblColRef> aggrOutCols = new HashSet<>();
        TupleFilter.collectColumns(havingFilter, aggrOutCols);

        for (TblColRef aggrOutCol : aggrOutCols) {
            int aggrIdxOnSql = aggrOutCol.getColumnDesc().getZeroBasedIndex(); // aggr index marked in OLAPAggregateRel
            FunctionDesc aggrFunc = aggregations.get(aggrIdxOnSql);

            // calculate the index of this aggr among all the metrics that is sending to storage
            int aggrIdxAmongMetrics = 0;
            for (MeasureDesc m : cubeDesc.getMeasures()) {
                if (aggrFunc.equals(m.getFunction()))
                    break;
                if (metrics.contains(m.getFunction()))
                    aggrIdxAmongMetrics++;
            }
            aggrOutCol.getColumnDesc().setId("" + (aggrIdxAmongMetrics + 1));
        }
        return havingFilter;
    }

    private boolean isExactAggregation(StorageContext context, Cuboid cuboid, Collection<TblColRef> groups,
            Set<TblColRef> othersD, Set<TblColRef> singleValuesD, Set<TblColRef> derivedPostAggregation,
            Collection<FunctionDesc> functionDescs) {
        if (context.isNeedStorageAggregation()) {
            logger.info("exactAggregation is false because need storage aggregation");
            return false;
        }

        if (cuboid.requirePostAggregation()) {
            logger.info("exactAggregation is false because cuboid " + cuboid.getInputID() + "=> " + cuboid.getId());
            return false;
        }

        // derived aggregation is bad, unless expanded columns are already in group by
        if (groups.containsAll(derivedPostAggregation) == false) {
            logger.info("exactAggregation is false because derived column require post aggregation: "
                    + derivedPostAggregation);
            return false;
        }

        // other columns (from filter) is bad, unless they are ensured to have single value
        if (singleValuesD.containsAll(othersD) == false) {
            logger.info("exactAggregation is false because some column not on group by: " + othersD //
                    + " (single value column: " + singleValuesD + ")");
            return false;
        }

        //for DimensionAsMetric like max(cal_dt), the dimension column maybe not in real group by
        for (FunctionDesc functionDesc : functionDescs) {
            if (functionDesc.isDimensionAsMetric()) {
                logger.info("exactAggregation is false because has DimensionAsMetric");
                return false;
            }
        }

        // for partitioned cube, the partition column must belong to group by or has single value
        PartitionDesc partDesc = cuboid.getCubeDesc().getModel().getPartitionDesc();
        if (partDesc.isPartitioned()) {
            TblColRef col = partDesc.getPartitionDateColumnRef();
            if (!groups.contains(col) && !singleValuesD.contains(col)) {
                logger.info("exactAggregation is false because cube is partitioned and " + col + " is not on group by");
                return false;
            }
        }

        logger.info("exactAggregation is true, cuboid id is " + cuboid.getId());
        return true;
    }

}
