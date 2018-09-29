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

package org.apache.kylin.storage.parquet.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparderEnv$;
import org.apache.spark.sql.manager.UdfManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.sum;

@SuppressWarnings("serial")
public class ParquetTask implements Serializable {
    public static final Logger logger = LoggerFactory.getLogger(org.apache.kylin.storage.parquet.spark.ParquetTask.class);

    private final transient JavaSparkContext sc;
    private final KylinConfig kylinConfig;
    private final transient Configuration conf;
    private final transient String[] parquetPaths;
    private final transient GTScanRequest scanRequest;
    private final transient CuboidToGridTableMapping mapping;

    ParquetTask(ParquetPayload request) {
        try {
            this.sc = JavaSparkContext.fromSparkContext(SparderEnv$.MODULE$.getSparkSession().sparkContext());
            this.kylinConfig = KylinConfig.getInstanceFromEnv();
            this.conf = HadoopUtil.getCurrentConfiguration();

            scanRequest = GTScanRequest.serializer
                    .deserialize(ByteBuffer.wrap(request.getGtScanRequest()));

            long startTime = System.currentTimeMillis();
            sc.setLocalProperty("spark.job.description", Thread.currentThread().getName());

            if (QueryContext.current().isHighPriorityQuery()) {
                sc.setLocalProperty("spark.scheduler.pool", "vip_tasks");
            } else {
                sc.setLocalProperty("spark.scheduler.pool", "lightweight_tasks");
            }

            String dataFolderName = request.getDataFolderName();

            String baseFolder = dataFolderName.substring(0, dataFolderName.lastIndexOf('/'));
            String cuboidId = dataFolderName.substring(dataFolderName.lastIndexOf("/") + 1);
            String prefix = "cuboid_" + cuboidId + "_";

            CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(request.getRealizationId());
            CubeSegment cubeSegment = cubeInstance.getSegmentById(request.getSegmentId());
            mapping = new CuboidToGridTableMapping(Cuboid.findById(cubeSegment.getCuboidScheduler(), Long.valueOf(cuboidId)));

            Path[] filePaths = HadoopUtil.getFilteredPath(HadoopUtil.getWorkingFileSystem(conf), new Path(baseFolder), prefix);
            parquetPaths = new String[filePaths.length];

            for (int i = 0; i < filePaths.length; i++) {
                parquetPaths[i] = filePaths[i].toString();
            }

            cleanHadoopConf(conf);

            logger.info("SparkVisit Init takes {} ms", System.currentTimeMillis() - startTime);

            StringBuilder pathBuilder = new StringBuilder();
            for (Path p : filePaths) {
                pathBuilder.append(p.toString()).append(";");
            }

            logger.info("Columnar path is " + pathBuilder.toString());
            logger.info("Required Measures: " + StringUtils.join(request.getParquetColumns(), ","));
            logger.info("Max GT length: " + request.getMaxRecordLength());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * the updatingResource part of Configuration will incur gzip compression in Configuration.write
     *  we cleaned them out to improve qps
     */
    private void cleanHadoopConf(Configuration c) {
        try {
            //updatingResource will get compressed by gzip, which is costly
            Field updatingResourceField = Configuration.class.getDeclaredField("updatingResource");
            updatingResourceField.setAccessible(true);
            Map<String, String[]> map = (Map<String, String[]>) updatingResourceField.get(c);
            map.clear();

        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterator<Object[]> executeTask() {
        logger.info("Start to visit cube data with Spark SQL <<<<<<");

        SQLContext sqlContext = new SQLContext(SparderEnv.getSparkSession().sparkContext());

        Dataset<Row> dataset = sqlContext.read().parquet(parquetPaths);
        ImmutableBitSet dimensions = scanRequest.getDimensions();
        ImmutableBitSet metrics = scanRequest.getAggrMetrics();
        ImmutableBitSet groupBy = scanRequest.getAggrGroupBy();

        // select
        Column[] selectColumn = getSelectColumn(dimensions, metrics, mapping);
        dataset = dataset.select(selectColumn);

        // where
        String where = scanRequest.getFilterPushDownSQL();
        if (where != null) {
            dataset = dataset.filter(where);
        }

        //groupby agg
        Column[] aggCols = getAggColumns(metrics, mapping);
        Column[] tailCols;

        if (aggCols.length >= 1) {
            tailCols = new Column[aggCols.length - 1];
            System.arraycopy(aggCols, 1, tailCols, 0, tailCols.length);
            dataset = dataset.groupBy(getGroupByColumn(dimensions, mapping)).agg(aggCols[0], tailCols);
        }

        // sort
        dataset = dataset.sort(getSortColumn(groupBy, mapping));

        JavaRDD<Row> rowRDD = dataset.javaRDD();

        JavaRDD<Object[]> objRDD = rowRDD.map(new Function<Row, Object[]>() {
            @Override
            public Object[] call(Row row) throws Exception {
                Object[] objects = new Object[row.length()];
                for (int i = 0; i < row.length(); i++) {
                    objects[i] = row.get(i);
                }
                return objects;
            }
        });

        logger.info("partitions: {}", objRDD.getNumPartitions());

        List<Object[]> result = objRDD.collect();
        return result.iterator();
    }

    private Column[] getAggColumns(ImmutableBitSet metrics, CuboidToGridTableMapping mapping) {
        Column[] columns = new Column[metrics.trueBitCount()];
        Map<MeasureDesc, Integer> met2gt = mapping.getMet2gt();

        for (int i = 0; i < metrics.trueBitCount(); i++) {
            int c = metrics.trueBitAt(i);
            for (Map.Entry<MeasureDesc, Integer> entry : met2gt.entrySet()) {
                if (entry.getValue() == c) {
                    MeasureDesc measureDesc = entry.getKey();
                    String func = measureDesc.getFunction().getExpression();
                    columns[i] = getAggColumn(measureDesc.getName(), func, measureDesc.getFunction().getReturnDataType());
                    break;
                }
            }
        }

        return columns;
    }

    private Column getAggColumn(String metName, String func, DataType dataType) {
        Column column;
        switch (func) {
            case "SUM":
                column = sum(metName);
                break;
            case "MIN":
                column = min(metName);
                break;
            case "MAX":
                column = max(metName);
                break;
            case "COUNT":
                column = sum(metName);
                break;
            case "TOP_N":
            case "COUNT_DISTINCT":
            case "EXTENDED_COLUMN":
            case "PERCENTILE_APPROX":
            case "RAW":
                String udf = UdfManager.register(dataType, func);
                column = callUDF(udf, col(metName));
                break;
            default:
                throw new IllegalArgumentException("Function " + func + " is not supported");

        }
        return column.alias(metName);
    }

    private void getDimColumn(ImmutableBitSet dimensions, Column[] columns, int from, CuboidToGridTableMapping mapping) {
        Map<TblColRef, Integer> dim2gt = mapping.getDim2gt();

        for (int i = 0; i < dimensions.trueBitCount(); i++) {
            int c = dimensions.trueBitAt(i);
            for (Map.Entry<TblColRef, Integer> entry : dim2gt.entrySet()) {
                if (entry.getValue() == c) {
                    columns[i + from] = col(entry.getKey().getTableAlias() + "_" + entry.getKey().getName());
                    break;
                }
            }
        }
    }

    private void getMetColumn(ImmutableBitSet metrics, Column[] columns, int from, CuboidToGridTableMapping mapping) {
        Map<MeasureDesc, Integer> met2gt = mapping.getMet2gt();

        for (int i = 0; i < metrics.trueBitCount(); i++) {
            int m = metrics.trueBitAt(i);
            for (Map.Entry<MeasureDesc, Integer> entry : met2gt.entrySet()) {
                if (entry.getValue() == m) {
                    columns[i + from] = col(entry.getKey().getName());
                    break;
                }
            }
        }
    }

    private Column[] getSelectColumn(ImmutableBitSet dimensions, ImmutableBitSet metrics, CuboidToGridTableMapping mapping) {
        Column[] columns = new Column[dimensions.trueBitCount() + metrics.trueBitCount()];

        getDimColumn(dimensions, columns, 0, mapping);

        getMetColumn(metrics, columns, dimensions.trueBitCount(), mapping);

        return columns;
    }

    private Column[] getGroupByColumn(ImmutableBitSet dimensions, CuboidToGridTableMapping mapping) {
        Column[] columns = new Column[dimensions.trueBitCount()];

        getDimColumn(dimensions, columns, 0, mapping);

        return columns;
    }

    private Column[] getSortColumn(ImmutableBitSet dimensions, CuboidToGridTableMapping mapping) {
        Column[] columns = new Column[dimensions.trueBitCount()];
        Map<TblColRef, Integer> dim2gt = mapping.getDim2gt();

        for (int i = 0; i < dimensions.trueBitCount(); i++) {
            int c = dimensions.trueBitAt(i);
            for (Map.Entry<TblColRef, Integer> entry : dim2gt.entrySet()) {
                if (entry.getValue() == c) {
                    columns[i] = asc(entry.getKey().getTableAlias() + "_" + entry.getKey().getName());
                    break;
                }
            }
        }

        return columns;
    }
}
