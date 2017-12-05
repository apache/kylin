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

package org.apache.kylin.storage.druid.read;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.RawQueryLastHacker;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterVisitor2;
import org.apache.kylin.metadata.filter.TupleFilterVisitor2Adaptor;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.druid.DruidSchema;
import org.apache.kylin.storage.druid.NameMapping;
import org.apache.kylin.storage.druid.NameMappingFactory;
import org.apache.kylin.storage.druid.read.cursor.RowCursor;
import org.apache.kylin.storage.druid.read.cursor.RowCursorFactory;
import org.apache.kylin.storage.druid.read.filter.FilterCondition;
import org.apache.kylin.storage.druid.read.filter.MoreDimFilters;
import org.apache.kylin.storage.druid.read.filter.PruneIntervalsProcessor;
import org.apache.kylin.storage.druid.read.filter.RemoveMarkerProcessor;
import org.apache.kylin.storage.druid.read.filter.ToDimFilterVisitor;
import org.apache.kylin.storage.druid.read.filter.TranslateDerivedVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.scan.ScanQuery;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;

public class DruidStorageQuery implements IStorageQuery {
    private static final Logger logger = LoggerFactory.getLogger(DruidStorageQuery.class);

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;
    private final RowCursorFactory cursorFactory;
    private final NameMapping nameMapping;

    public DruidStorageQuery(CubeInstance cube, RowCursorFactory cursorFactory) {
        this.cubeInstance = cube;
        this.cubeDesc = cube.getDescriptor();
        this.cursorFactory = cursorFactory;
        this.nameMapping = NameMappingFactory.getDefault(cubeDesc);
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        context.setStorageQuery(this);
        //context.getQueryContext().setDeadline(cubeInstance.getConfig().getQueryTimeoutSeconds() * 1000);

        final LookupTableCache lookupCache = new LookupTableCache(cubeInstance);

        final StorageRequest request = planRequest(context, sqlDigest, returnTupleInfo, lookupCache);

        // convert to druid filter
        List<DimFilter> dimFilters = Lists.newArrayList();
        dimFilters.add(DimFilters.dimEquals(DruidSchema.ID_COL, String.valueOf(request.cuboid.getId())));
        if (request.filter != null) {
            ToDimFilterVisitor dimFilterVisitor = new ToDimFilterVisitor(nameMapping, request.dimensions);
            DimFilter dimFilter = request.filter.accept(new TupleFilterVisitor2Adaptor<>(dimFilterVisitor));
            dimFilters.add(dimFilter);
        }
        DimFilter dimFilter = MoreDimFilters.and(dimFilters);

        // optimize filter condition
        FilterCondition condition = FilterCondition.of(ImmutableList.of(DruidSchema.ETERNITY_INTERVAL), dimFilter);
        condition = optimizeFilter(condition);
        if (condition.getIntervals().isEmpty()) {
            return ITupleIterator.EMPTY_TUPLE_ITERATOR;
        }

        DruidSchema schema = new DruidSchema(nameMapping, request.groups, request.measures);
        Query<?> query = makeDruidQuery(DruidSchema.getDataSource(cubeDesc), schema, condition, context);
        RowCursor cursor = cursorFactory.createRowCursor(schema, query, context);
        return new DruidTupleIterator(cubeInstance, lookupCache, schema, returnTupleInfo, cursor, context);
    }

    protected StorageRequest planRequest(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo, LookupTableCache lookupCache) {
        //deal with participant columns in subquery join
        sqlDigest.includeSubqueryJoinParticipants();

        //cope with queries with no aggregations
        RawQueryLastHacker.hackNoAggregations(sqlDigest, cubeDesc, returnTupleInfo);

        // Customized measure taking effect: e.g. allow custom measures to help raw queries
        notifyBeforeStorageQuery(sqlDigest);

        TupleFilter filter = sqlDigest.filter;
        List<MeasureDesc> measures = findMeasures(sqlDigest);

        // expand derived (xxxD means contains host columns only, derived columns were translated)
        Set<TblColRef> groupsD = translateDerivedToHost(sqlDigest.groupbyColumns);
        Set<TblColRef> filterD = Sets.newHashSet();
        if (filter != null) {
            TupleFilterVisitor2<TupleFilter> translateDerivedVisitor = new TranslateDerivedVisitor(cubeDesc, lookupCache);
            filter = filter.accept(new TupleFilterVisitor2Adaptor<>(translateDerivedVisitor));
            TupleFilter.collectColumns(filter, filterD);
        }

        // identify cuboid
        Set<TblColRef> dimensionsD = Sets.newLinkedHashSet();
        dimensionsD.addAll(groupsD);
        dimensionsD.addAll(filterD);
        Cuboid cuboid = Cuboid.findCuboid(cubeInstance.getCuboidScheduler(), dimensionsD, toFunctions(measures));
        context.setCuboid(cuboid);

        // determine whether push down aggregation to storage is beneficial
        Set<TblColRef> singleValuesD = findSingleValueColumns(filter);
        context.setNeedStorageAggregation(needStorageAggregation(cuboid, groupsD, singleValuesD));

        logger.info("Cuboid identified: cube={}, cuboidId={}, groupsD={}, filterD={}, limitPushdown={}, storageAggr={}", cubeInstance.getName(), cuboid.getId(), groupsD, filterD, context.getFinalPushDownLimit(), context.isNeedStorageAggregation());
        return new StorageRequest(cuboid, Lists.newArrayList(dimensionsD), Lists.newArrayList(groupsD), measures, filter);
    }

    private void notifyBeforeStorageQuery(SQLDigest sqlDigest) {
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

    private List<MeasureDesc> findMeasures(SQLDigest sqlDigest) {
        List<MeasureDesc> result = Lists.newArrayList();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            if (sqlDigest.aggregations.contains(measure.getFunction())) {
                result.add(measure);
            }
        }
        return result;
    }

    private List<FunctionDesc> toFunctions(Collection<MeasureDesc> measures) {
        List<FunctionDesc> result = Lists.newArrayListWithExpectedSize(measures.size());
        for (MeasureDesc measure : measures) {
            result.add(measure.getFunction());
        }
        return result;
    }

    private Set<TblColRef> translateDerivedToHost(Collection<TblColRef> columns) {
        Set<TblColRef> result = Sets.newLinkedHashSet();
        for (TblColRef column : columns) {
            if (cubeDesc.hasHostColumn(column)) {
                CubeDesc.DeriveInfo hostInfo = cubeDesc.getHostInfo(column);
                Collections.addAll(result, hostInfo.columns);
            } else {
                result.add(column);
            }
        }
        return result;
    }

    private Set<TblColRef> findSingleValueColumns(TupleFilter filter) {
        Set<TblColRef> result = Sets.newHashSet();

        List<CompareTupleFilter> toCheck = Lists.newArrayList();
        if (filter instanceof CompareTupleFilter) {
            toCheck.add((CompareTupleFilter) filter);
        }
        if (filter instanceof LogicalTupleFilter && filter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
            for (TupleFilter child : filter.getChildren()) {
                if (child instanceof CompareTupleFilter) {
                    toCheck.add((CompareTupleFilter) child);
                }
            }
        }

        for (CompareTupleFilter compare : toCheck) {
            if (compare.columnMatchSingleValue()) {
                result.add(compare.getColumn());
            }
        }
        return result;
    }

    private boolean needStorageAggregation(Cuboid cuboid, Collection<TblColRef> groupD, Collection<TblColRef> singleValueD) {
        HashSet<TblColRef> temp = Sets.newHashSet();
        temp.addAll(groupD);
        temp.addAll(singleValueD);
        return cuboid.getColumns().size() != temp.size();
    }

    @Override
    public boolean keepRuntimeFilter() {
        return false;
    }

    private FilterCondition optimizeFilter(FilterCondition condition) {
        List<Function<FilterCondition, FilterCondition>> processors = ImmutableList.of(
                RemoveMarkerProcessor.SINGLETON,
                new PruneIntervalsProcessor(cubeInstance, nameMapping)
        );

        for (Function<FilterCondition, FilterCondition> processor : processors) {
            condition = processor.apply(condition);
        }
        return condition;
    }

    private Query<?> makeDruidQuery(String dataSource, DruidSchema schema, FilterCondition condition, StorageContext context) {
        Map<String, Object> druidContext = new HashMap<>();
        druidContext.put("queryId", context.getQueryContext().getQueryId());
        // scan query has trouble with very large timeout value, so we only set druid timeout
        // when user configures a non-zero kylin timeout
//        if (context.getQueryContext().getDeadline() != Long.MAX_VALUE) {
//            druidContext.put(QueryContexts.TIMEOUT_KEY, context.getQueryContext().checkMillisBeforeDeadline());
//        }

        final Query<?> query;
        final DataSource ds = new TableDataSource(dataSource);
        final QuerySegmentSpec segmentSpec = new MultipleIntervalSegmentSpec(condition.getIntervals());

        if (context.isNeedStorageAggregation()) {
            druidContext.put("finalize", false);
            query = new GroupByQuery(
                    ds, segmentSpec, null,
                    condition.getDimFilter(),
                    Granularity.fromString("all"),
                    schema.getQueryDimensionSpec(schema.getDimensions()),
                    Lists.newArrayList(schema.getAggregators()),
                    null, null, null,
                    druidContext
            );

        } else {
            boolean hasMemoryHungryMetric = false;
            for (MeasureDesc metric : schema.getMeasures()) {
                hasMemoryHungryMetric |= metric.getFunction().getMeasureType().isMemoryHungry();
            }
            int batchSize = hasMemoryHungryMetric ? 10 : 1024;

            List<String> columns = new ArrayList<>();
            for (TblColRef dim : schema.getDimensions()) {
                columns.add(schema.getDimFieldName(dim));
            }
            for (MeasureDesc met : schema.getMeasures()) {
                columns.add(schema.getMeasureFieldName(met));
            }

            query = new ScanQuery(
                    ds,
                    segmentSpec,
                    null,
                    ScanQuery.RESULT_FORMAT_COMPACTED_LIST,
                    batchSize,
                    0,
                    condition.getDimFilter(),
                    columns,
                    false,
                    druidContext
            );
        }
        return query;
    }

    private static class StorageRequest {
        final Cuboid cuboid;
        final List<TblColRef> dimensions;
        final List<TblColRef> groups;
        final List<MeasureDesc> measures;
        final TupleFilter filter;

        public StorageRequest(Cuboid cuboid, List<TblColRef> dimensions, List<TblColRef> groups, List<MeasureDesc> measures, TupleFilter filter) {
            this.cuboid = cuboid;
            this.dimensions = dimensions;
            this.groups = groups;
            this.measures = measures;
            this.filter = filter;
        }
    }

}
