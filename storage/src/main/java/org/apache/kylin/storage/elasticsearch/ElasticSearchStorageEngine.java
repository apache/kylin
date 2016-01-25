

package org.apache.kylin.storage.elasticsearch;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

import java.util.*;

import org.apache.kylin.common.persistence.ElasticSearchClient;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowValueDecoder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.dict.lookup.LookupStringTable;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.ElasticSearchContext;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.ColumnValueRange;
import org.apache.kylin.storage.hbase.DerivedFilterTranslator;
import org.apache.kylin.storage.hbase.HBaseKeyRange;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Created by liuze on 2016/1/8 0008.
 */


public class ElasticSearchStorageEngine implements IStorageEngine {




    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchStorageEngine.class);

    private static final int MERGE_KEYRANGE_THRESHOLD = 100;
    private static final long MEM_BUDGET_PER_QUERY = 3L * 1024 * 1024 * 1024; // 3G

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;

    QueryBuilder qb = boolQuery();

    public ElasticSearchStorageEngine(CubeInstance cube) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest) {

        TupleFilter filter = sqlDigest.filter;
        if(filter!=null) {
            setLevel(filter, 1);
        }
        // build dimension & metrics
        Collection<TblColRef> dimension = new HashSet<TblColRef>();    //all dim colums except metrics
        Collection<FunctionDesc> metrics = new HashSet<FunctionDesc>();    //all func Desc
        buildDimensionsAndMetrics(dimension, metrics, sqlDigest);
        Collection<TblColRef> dimensions=sqlDigest.allColumns;
        //Collection<TblColRef> groupbyColumns=sqlDigest.groupbyColumns;


        Collection<FunctionDesc> aggregations=sqlDigest.aggregations;


        //setThreshold(dimensionsD, valueDecoders, context); // set cautious threshold to prevent out of memory
        setLimit(filter, context);

        Client client = ElasticSearchClient.get(context.getEsClusterUrl());
        if(aggregations==null||aggregations.size()==0) {
            return new SerializedElasticSearchTupleIterator(client, cubeInstance, dimensions, filter, context);
        }else{
            return new SerializedElasticAggregationTupleIterator(client, cubeInstance, filter, dimension, aggregations, context);

        }
    }

    private void buildDimensionsAndMetrics(Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics, SQLDigest sqlDigest) {

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

    private void  setLevel(TupleFilter filter, int j) {

        if(filter==null){
            filter=new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.AND);
        }
        filter.level=j;

        if (filter.hasChildren()) {
            j=j+1;
            List<? extends TupleFilter>  litf=filter.getChildren();
            for (TupleFilter f : litf) {
                if (f.getOperator().getValue() != 30 && f.getOperator().getValue() != 31) {
                    setLevel(f,j);
                }

            }

        }
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
            CubeDesc.DeriveInfo hostInfo = cubeDesc.getHostInfo(col);
            for (TblColRef h : hostInfo.columns)
                collector.add(h);
        } else {
            collector.add(col);
        }
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

        CubeDesc.DeriveInfo hostInfo = cubeDesc.getHostInfo(derived);
        CubeManager cubeMgr = CubeManager.getInstance(this.cubeInstance.getConfig());
        CubeSegment seg = cubeInstance.getLatestReadySegment();
        LookupStringTable lookup = cubeMgr.getLookupTable(seg, hostInfo.dimension);
        Pair<TupleFilter, Boolean> translated = DerivedFilterTranslator.translate(lookup, hostInfo, compf);
        TupleFilter translatedFilter = translated.getFirst();
        boolean loosened = translated.getSecond();
        if (loosened) {
            collectColumnsRecursively(compf, collector);
        }
        return translatedFilter;
    }

    private List<RowValueDecoder> translateAggregation(HBaseMappingDesc hbaseMapping, Collection<FunctionDesc> metrics, //
                                                       ElasticSearchContext context) {
        Map<HBaseColumnDesc, RowValueDecoder> codecMap = Maps.newHashMap();
        for (FunctionDesc aggrFunc : metrics) {
            Collection<HBaseColumnDesc> hbCols = hbaseMapping.findHBaseColumnByFunction(aggrFunc);
            if (hbCols.isEmpty()) {
                throw new IllegalStateException("can't find HBaseColumnDesc for function " + aggrFunc.getFullExpression());
            }
            HBaseColumnDesc bestHBCol = null;
            int bestIndex = -1;
            for (HBaseColumnDesc hbCol : hbCols) {
                bestHBCol = hbCol;
                bestIndex = hbCol.findMeasureIndex(aggrFunc);
                MeasureDesc measure = hbCol.getMeasures()[bestIndex];
                // criteria for holistic measure: Exact Aggregation && Exact Cuboid
                if (measure.isHolisticCountDistinct() && context.isExactAggregation()) {
                    logger.info("Holistic count distinct chosen for " + aggrFunc);
                    break;
                }
            }

            RowValueDecoder codec = codecMap.get(bestHBCol);
            if (codec == null) {
                codec = new RowValueDecoder(bestHBCol);
                codecMap.put(bestHBCol, codec);
            }
            codec.setIndex(bestIndex);
        }
        return new ArrayList<RowValueDecoder>(codecMap.values());
    }

    private TupleFilter flattenToOrAndFilter(TupleFilter filter) {
        if (filter == null)
            return null;

        TupleFilter flatFilter = filter.flatFilter();

        // normalize to OR-AND filter
        if (flatFilter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
            LogicalTupleFilter f = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
            f.addChild(flatFilter);
            flatFilter = f;
        }

        if (flatFilter.getOperator() != TupleFilter.FilterOperatorEnum.OR)
            throw new IllegalStateException();

        return flatFilter;
    }

    private List<HBaseKeyRange> buildScanRanges(TupleFilter flatFilter, Collection<TblColRef> dimensionColumns) {

        List<HBaseKeyRange> result = Lists.newArrayList();

        // build row key range for each cube segment
        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {

            // consider derived (lookup snapshot), filter on dimension may differ per segment
            List<Collection<ColumnValueRange>> orAndDimRanges = translateToOrAndDimRanges(flatFilter, cubeSeg);
            if (orAndDimRanges == null) { // has conflict
                continue;
            }

            List<HBaseKeyRange> scanRanges = Lists.newArrayListWithCapacity(orAndDimRanges.size());
            for (Collection<ColumnValueRange> andDimRanges : orAndDimRanges) {
                HBaseKeyRange rowKeyRange = new HBaseKeyRange(dimensionColumns, andDimRanges, cubeSeg, cubeDesc);
                scanRanges.add(rowKeyRange);
            }

            List<HBaseKeyRange> mergedRanges = mergeOverlapRanges(scanRanges);
            mergedRanges = mergeTooManyRanges(mergedRanges);
            result.addAll(mergedRanges);
        }

        dropUnhitSegments(result);

        return result;
    }

    private List<Collection<ColumnValueRange>> translateToOrAndDimRanges(TupleFilter flatFilter, CubeSegment cubeSegment) {
        List<Collection<ColumnValueRange>> result = Lists.newArrayList();

        if (flatFilter == null) {
            result.add(Collections.<ColumnValueRange> emptyList());
            return result;
        }

        for (TupleFilter andFilter : flatFilter.getChildren()) {
            if (andFilter.getOperator() != TupleFilter.FilterOperatorEnum.AND) {
                throw new IllegalStateException("Filter should be AND instead of " + andFilter);
            }

            Collection<ColumnValueRange> andRanges = translateToAndDimRanges(andFilter.getChildren(), cubeSegment);
            if (andRanges != null) {
                result.add(andRanges);
            }
        }

        return preprocessConstantConditions(result);
    }

    private List<Collection<ColumnValueRange>> preprocessConstantConditions(List<Collection<ColumnValueRange>> orAndRanges) {
        boolean globalAlwaysTrue = false;
        Iterator<Collection<ColumnValueRange>> iterator = orAndRanges.iterator();
        while (iterator.hasNext()) {
            Collection<ColumnValueRange> andRanges = iterator.next();
            Iterator<ColumnValueRange> iterator2 = andRanges.iterator();
            boolean hasAlwaysFalse = false;
            while (iterator2.hasNext()) {
                ColumnValueRange range = iterator2.next();
                if (range.satisfyAll())
                    iterator2.remove();
                else if (range.satisfyNone())
                    hasAlwaysFalse = true;
            }
            if (hasAlwaysFalse) {
                iterator.remove();
            } else if (andRanges.isEmpty()) {
                globalAlwaysTrue = true;
                break;
            }
        }
        if (globalAlwaysTrue) {
            orAndRanges.clear();
            orAndRanges.add(Collections.<ColumnValueRange> emptyList());
        }
        return orAndRanges;
    }

    // return empty collection to mean true; return null to mean false
    @SuppressWarnings("unchecked")
    private Collection<ColumnValueRange> translateToAndDimRanges(List<? extends TupleFilter> andFilters, CubeSegment cubeSegment) {
        Map<TblColRef, ColumnValueRange> rangeMap = new HashMap<TblColRef, ColumnValueRange>();
        for (TupleFilter filter : andFilters) {
            if ((filter instanceof CompareTupleFilter) == false) {
                continue;
            }

            CompareTupleFilter comp = (CompareTupleFilter) filter;
            if (comp.getColumn() == null) {
                continue;
            }

            ColumnValueRange range = new ColumnValueRange(comp.getColumn(), comp.getValues(), comp.getOperator());
            andMerge(range, rangeMap);
        }

        // a little pre-evaluation to remove invalid EQ/IN values and round start/end according to dictionary
        Iterator<ColumnValueRange> it = rangeMap.values().iterator();
        while (it.hasNext()) {
            ColumnValueRange range = it.next();
            range.preEvaluateWithDict((org.apache.kylin.dict.Dictionary<String>) cubeSegment.getDictionary(range.getColumn()));
            if (range.satisfyAll())
                it.remove();
            else if (range.satisfyNone())
                return null;
        }

        return rangeMap.values();
    }

    private void andMerge(ColumnValueRange range, Map<TblColRef, ColumnValueRange> rangeMap) {
        ColumnValueRange columnRange = rangeMap.get(range.getColumn());
        if (columnRange == null) {
            rangeMap.put(range.getColumn(), range);
        } else {
            columnRange.andMerge(range);
        }
    }

    private List<HBaseKeyRange> mergeOverlapRanges(List<HBaseKeyRange> keyRanges) {
        if (keyRanges.size() <= 1) {
            return keyRanges;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Merging key range from " + keyRanges.size());
        }

        // sort ranges by start key
        Collections.sort(keyRanges);

        // merge the overlap range
        List<HBaseKeyRange> mergedRanges = new LinkedList<HBaseKeyRange>();
        int beginIndex = 0;
        byte[] maxStopKey = keyRanges.get(0).getStopKey();
        for (int index = 0; index < keyRanges.size(); index++) {
            HBaseKeyRange keyRange = keyRanges.get(index);
            if (Bytes.compareTo(maxStopKey, keyRange.getStartKey()) < 0) {
                // merge the current key ranges
                HBaseKeyRange mergedRange = mergeKeyRange(keyRanges, beginIndex, index - 1);
                mergedRanges.add(mergedRange);
                // start new merge
                beginIndex = index;
            }
            if (Bytes.compareTo(maxStopKey, keyRange.getStopKey()) < 0) {
                // update the stop key
                maxStopKey = keyRange.getStopKey();
            }
        }
        // merge last range
        HBaseKeyRange mergedRange = mergeKeyRange(keyRanges, beginIndex, keyRanges.size() - 1);
        mergedRanges.add(mergedRange);
        if (logger.isDebugEnabled()) {
            logger.debug("Merging key range to " + mergedRanges.size());
        }
        return mergedRanges;
    }

    private HBaseKeyRange mergeKeyRange(List<HBaseKeyRange> keyRanges, int from, int to) {
        HBaseKeyRange keyRange = keyRanges.get(from);
        int mergeSize = to - from + 1;
        if (mergeSize > 1) {
            // merge range from mergeHeader to i - 1
            CubeSegment cubeSegment = keyRange.getCubeSegment();
            Cuboid cuboid = keyRange.getCuboid();
            byte[] startKey = keyRange.getStartKey();
            byte[] stopKey = keyRange.getStopKey();
            long partitionColumnStartDate = Long.MAX_VALUE;
            long partitionColumnEndDate = 0;

            List<Collection<ColumnValueRange>> newFlatOrAndFilter = Lists.newLinkedList();
            TreeSet<Pair<byte[], byte[]>> newFuzzyKeys = new TreeSet<>(new Comparator<Pair<byte[], byte[]>>() {
                @Override
                public int compare(Pair<byte[], byte[]> o1, Pair<byte[], byte[]> o2) {
                    int partialResult = Bytes.compareTo(o1.getFirst(), o2.getFirst());
                    if (partialResult != 0) {
                        return partialResult;
                    } else {
                        return Bytes.compareTo(o1.getSecond(), o2.getSecond());
                    }
                }
            });

            boolean hasNonFuzzyRange = false;
            for (int k = from; k <= to; k++) {
                HBaseKeyRange nextRange = keyRanges.get(k);
                hasNonFuzzyRange = hasNonFuzzyRange || nextRange.getFuzzyKeys().isEmpty();
                newFuzzyKeys.addAll(nextRange.getFuzzyKeys());
                newFlatOrAndFilter.addAll(nextRange.getFlatOrAndFilter());
                if (Bytes.compareTo(stopKey, nextRange.getStopKey()) < 0) {
                    stopKey = nextRange.getStopKey();
                }
                if (nextRange.getPartitionColumnStartDate() > 0 && nextRange.getPartitionColumnStartDate() < partitionColumnStartDate) {
                    partitionColumnStartDate = nextRange.getPartitionColumnStartDate();
                }
                if (nextRange.getPartitionColumnEndDate() < Long.MAX_VALUE && nextRange.getPartitionColumnEndDate() > partitionColumnEndDate) {
                    partitionColumnEndDate = nextRange.getPartitionColumnEndDate();
                }
            }

            // if any range is non-fuzzy, then all fuzzy keys must be cleared
            if (hasNonFuzzyRange) {
                newFuzzyKeys.clear();
            }

            partitionColumnStartDate = (partitionColumnStartDate == Long.MAX_VALUE) ? 0 : partitionColumnStartDate;
            partitionColumnEndDate = (partitionColumnEndDate == 0) ? Long.MAX_VALUE : partitionColumnEndDate;
            keyRange = new HBaseKeyRange(cubeSegment, cuboid, startKey, stopKey, Lists.newArrayList(newFuzzyKeys), newFlatOrAndFilter, partitionColumnStartDate, partitionColumnEndDate);
        }
        return keyRange;
    }

    private List<HBaseKeyRange> mergeTooManyRanges(List<HBaseKeyRange> keyRanges) {
        if (keyRanges.size() < MERGE_KEYRANGE_THRESHOLD) {
            return keyRanges;
        }
        // TODO: check the distance between range. and merge the large distance range
        List<HBaseKeyRange> mergedRanges = new LinkedList<HBaseKeyRange>();
        HBaseKeyRange mergedRange = mergeKeyRange(keyRanges, 0, keyRanges.size() - 1);
        mergedRanges.add(mergedRange);
        return mergedRanges;
    }

    private void dropUnhitSegments(List<HBaseKeyRange> scans) {
        if (cubeDesc.getModel().getPartitionDesc().isPartitioned()) {
            Iterator<HBaseKeyRange> iterator = scans.iterator();
            while (iterator.hasNext()) {
                HBaseKeyRange scan = iterator.next();
                if (scan.hitSegment() == false) {
                    iterator.remove();
                }
            }
        }
    }

    private void setThreshold(Collection<TblColRef> dimensions, List<RowValueDecoder> valueDecoders, ElasticSearchContext context) {
        if (RowValueDecoder.hasMemHungryCountDistinct(valueDecoders) == false) {
            return;
        }

        int rowSizeEst = dimensions.size() * 3;
        for (RowValueDecoder decoder : valueDecoders) {
            MeasureDesc[] measures = decoder.getMeasures();
            BitSet projectionIndex = decoder.getProjectionIndex();
            for (int i = projectionIndex.nextSetBit(0); i >= 0; i = projectionIndex.nextSetBit(i + 1)) {
                FunctionDesc func = measures[i].getFunction();
                rowSizeEst += func.getReturnDataType().getSpaceEstimate();
            }
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


