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

package org.apache.kylin.storage.parquet.cube;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.RawQueryLastHacker;
import org.apache.kylin.cube.common.SegmentPruner;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.ext.ClassLoaderUtils;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.measure.raw.RawMeasureType;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.datatype.DataType;
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
import org.apache.kylin.storage.parquet.NameMapping;
import org.apache.kylin.storage.parquet.NameMappingFactory;
import org.apache.kylin.storage.parquet.ParquetSchema;
import org.apache.kylin.storage.path.IStoragePathBuilder;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.manager.UdfManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ParquetStorageQuery implements IStorageQuery {
    private static final Logger logger = LoggerFactory.getLogger(ParquetStorageQuery.class);

    private final CubeInstance cubeInstance;
    private final CubeDesc cubeDesc;
    private final IStoragePathBuilder storagePathBuilder;
    private final NameMapping nameMapping;

    public ParquetStorageQuery(CubeInstance cubeInstance) {
        this.cubeInstance = cubeInstance;
        this.cubeDesc = cubeInstance.getDescriptor();
        this.storagePathBuilder = (IStoragePathBuilder)ClassUtil.newInstance(cubeInstance.getConfig().getStoragePathBuilder());
        this.nameMapping = NameMappingFactory.getDefault(cubeDesc);
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo tupleInfo) {
        try {
            context.setStorageQuery(this);
            final LookupTableCache lookupCache = new LookupTableCache(cubeInstance);
            final StorageRequest request = planRequest(context, sqlDigest, tupleInfo, lookupCache);

            SegmentPruner segPruner = new SegmentPruner(sqlDigest.filter);
            List<String> parquetFilePaths = Lists.newArrayList();
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();

            String prefix = "cuboid_" + request.cuboid.getId() + "_";

            for (CubeSegment cubeSeg : segPruner.listSegmentsForQuery(cubeInstance)) {
                List<List<Long>> layeredCuboids = cubeSeg.getCuboidScheduler().getCuboidsByLayer();
                int level = 0;
                for (List<Long> levelCuboids : layeredCuboids) {
                    if (levelCuboids.contains(request.cuboid.getId())) {
                        break;
                    }
                    level++;
                }
                String baseFolder = JobBuilderSupport.getCuboidOutputPathsByLevel(storagePathBuilder.getRealizationFinalDataPath(cubeSeg)  + "/", level);
                Path[] filePaths = HadoopUtil.getFilteredPath(fileSystem, new Path(baseFolder), prefix);
                for (int i = 0; i < filePaths.length; i++) {
                    parquetFilePaths.add(filePaths[i].toString());
                }
            }

            logger.debug("parquet file paths: {}", parquetFilePaths.toString());

            if (parquetFilePaths.isEmpty()) {
                return ITupleIterator.EMPTY_TUPLE_ITERATOR;
            } else {
                return new ParquetTupleIterator(cubeInstance, context, lookupCache, new ParquetSchema(nameMapping, request.groups, request.measures), tupleInfo, queryWithSpark(request, parquetFilePaths.toArray(new String[]{})));
            }

        } catch (Exception e) {
            throw new IllegalStateException("Query failed.", e);
        }
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
        Set<TblColRef> unevaluatableColumnCollector = Sets.newHashSet();
        if (filter != null) {
            TupleFilterVisitor2<TupleFilter> translateDerivedVisitor = new TranslateDerivedVisitor(cubeDesc, lookupCache, unevaluatableColumnCollector);
            filter = filter.accept(new TupleFilterVisitor2Adaptor<>(translateDerivedVisitor));
            groupsD.addAll(unevaluatableColumnCollector);
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

    @Override
    public boolean keepRuntimeFilter() {
        return false;
    }

    protected Iterator<Row> queryWithSpark(StorageRequest request, String[] parquetPaths) {
        logger.debug("Start to visit cube data with Spark <<<<<<");
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        SQLContext sqlContext = SparderEnv.getSparkSession().sqlContext();

        Dataset<Row> dataset = sqlContext.read().parquet(parquetPaths);
        List<MeasureDesc> measures = request.measures;
        List<TblColRef> groupBy = request.groups;

        // filter
        if (request.filter != null) {
            Column filter = getFilterColumn(request);
            dataset = dataset.filter(filter);
        }

        //groupby agg
        Column[] aggCols = getAggColumns(measures);
        Column[] tailCols;
        if (aggCols.length >= 1) {
            tailCols = new Column[aggCols.length - 1];
            System.arraycopy(aggCols, 1, tailCols, 0, tailCols.length);
            dataset = dataset.groupBy(getDimColumns(groupBy)).agg(aggCols[0], tailCols);
        }

        // select
        Column[] selectColumn = getSelectColumns(groupBy, measures);
        dataset = dataset.select(selectColumn);

        List<Row> list = dataset.collectAsList();
        return list.iterator();
    }

    private Column getFilterColumn(StorageRequest request) {
        ToSparkFilterVisitor sparkFilterVistor = new ToSparkFilterVisitor(this.nameMapping, request.dimensions);
        Column column = request.filter.accept(new TupleFilterVisitor2Adaptor<>(sparkFilterVistor));
        return column;
    }

    private Column[] getDimColumns(List<TblColRef> colRefs) {
        Column[] columns = new Column[colRefs.size()];
        for (int i = 0; i < colRefs.size(); i++) {
            columns[i] = col(nameMapping.getDimFieldName(colRefs.get(i)));
        }
        return columns;
    }

    private Column[] getMeaColumns(List<MeasureDesc> measures) {
        Column[] columns = new Column[measures.size()];
        for (int i = 0; i < measures.size(); i++) {
            columns[i] = col(nameMapping.getMeasureFieldName(measures.get(i)));
        }
        return columns;
    }

    private Column[] getSelectColumns(List<TblColRef> colRefs, List<MeasureDesc> measures) {
        Column[] dims = getDimColumns(colRefs);
        Column[] meas = getMeaColumns(measures);

        return ArrayUtils.addAll(dims, meas);
    }

    private Column[] getAggColumns(List<MeasureDesc> measures) {
        Column[] columns = new Column[measures.size()];
        for (int i = 0; i < measures.size(); i++) {
            MeasureDesc measure = measures.get(i);
            columns[i] = getAggColumn(nameMapping.getMeasureFieldName(measure), measure.getFunction().getExpression(), measure.getFunction().getReturnDataType());
        }
        return columns;
    }

    private Column getAggColumn(String meaName, String func, DataType dataType) {
        Column column;
        switch (func) {
            case FunctionDesc.FUNC_SUM:
                column = sum(meaName);
                break;
            case FunctionDesc.FUNC_MIN:
                column = min(meaName);
                break;
            case FunctionDesc.FUNC_MAX:
                column = max(meaName);
                break;
            case FunctionDesc.FUNC_COUNT:
                column = sum(meaName);
                break;
            case TopNMeasureType.FUNC_TOP_N:
            case FunctionDesc.FUNC_COUNT_DISTINCT:
            case ExtendedColumnMeasureType.FUNC_EXTENDED_COLUMN:
            case PercentileMeasureType.FUNC_PERCENTILE_APPROX:
            case RawMeasureType.FUNC_RAW:
                String udf = UdfManager.register(dataType, func);
                column = callUDF(udf, col(meaName));
                break;
            default:
                throw new IllegalArgumentException("Function " + func + " is not supported");

        }
        return column.alias(meaName);
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

    private List<FunctionDesc> toFunctions(Collection<MeasureDesc> measures) {
        List<FunctionDesc> result = Lists.newArrayListWithExpectedSize(measures.size());
        for (MeasureDesc measure : measures) {
            result.add(measure.getFunction());
        }
        return result;
    }

    public static class StorageRequest {
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
