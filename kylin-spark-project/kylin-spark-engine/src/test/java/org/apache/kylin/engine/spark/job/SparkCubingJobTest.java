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

package org.apache.kylin.engine.spark.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.metadata.FunctionDesc;
import org.apache.kylin.engine.spark.metadata.MetadataConverter;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.common.SparkQueryTest;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.udaf.PreciseCountDistinct;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SparkCubingJobTest extends LocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(SparkCubingJobTest.class);
    private static StructType OUT_SCHEMA = null;
    private KylinConfig kylinConfig;
    protected CubeManager cubeMgr;
    protected ExecutableManager execMgr;

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        ss.sparkContext().setLogLevel("WARN");
        kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.source.provider.0", "org.apache.kylin.engine.spark.source.HiveSource");
        cubeMgr = CubeManager.getInstance(kylinConfig);
        execMgr = ExecutableManager.getInstance(kylinConfig);
    }

    @Override
    public void after() {
        super.after();
    }

    @Test
    public void testBuildJob() throws Exception {
        String cubeName = "ci_inner_join_cube";

        cleanupSegments(cubeName);
        CubeInstance cubeInstance = cubeMgr.getCube(cubeName);
        ExecutableManager jobService = ExecutableManager.getInstance(kylinConfig);

        long date1 = 0;
        long date2 = dateToLong("2012-06-01");
        long date3 = dateToLong("2013-07-01");

        CubeSegment segment = cubeMgr.appendSegment(cubeInstance, new SegmentRange.TSRange(date1, date2));
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), "ADMIN");
        jobService.addJob(job);
        // wait job done
        ExecutableState state = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, state);

        // Result cmp: Parquet vs Spark SQL
        queryTest(segment);
        snapshotTest(segment);

        // Test build 2nd segment
        CubeSegment segment2 = cubeMgr.appendSegment(cubeInstance, new SegmentRange.TSRange(date2, date3));
        NSparkCubingJob job2 = NSparkCubingJob.create(Sets.newHashSet(segment2), "ADMIN");
        jobService.addJob(job2);
        // wait job done
        ExecutableState state2 = wait(job2);
        Assert.assertEquals(ExecutableState.SUCCEED, state2);

        cubeInstance = cubeMgr.reloadCube(cubeName);
        Assert.assertEquals(2, cubeInstance.getSegments().size());
    }

    @Test
    public void testBuildTwoSegmentsAndMerge() throws Exception {
        String cubeName = "ci_inner_join_cube";
        cleanupSegments(cubeName);
        CubeInstance cubeInstance = cubeMgr.getCube(cubeName);
        ExecutableManager jobService = ExecutableManager.getInstance(kylinConfig);

        /**
         * Round1. Build 2 segment
         */
        long date1 = dateToLong("2010-01-01");
        long date2 = dateToLong("2012-01-01");
        long date3 = dateToLong("2014-01-01");

        CubeSegment segment = cubeMgr.appendSegment(cubeInstance, new SegmentRange.TSRange(date1, date2));
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), "ADMIN");
        jobService.addJob(job);
        // wait job done
        ExecutableState state = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, state);

        CubeSegment segment2 = cubeMgr.appendSegment(cubeInstance, new SegmentRange.TSRange(date2, date3));
        NSparkCubingJob job2 = NSparkCubingJob.create(Sets.newHashSet(segment2), "ADMIN");
        jobService.addJob(job2);
        // wait job done
        ExecutableState state2 = wait(job2);
        Assert.assertEquals(ExecutableState.SUCCEED, state2);

        cubeInstance = cubeMgr.reloadCube(cubeName);

        /**
         * Round2. Merge two segments
         */
        CubeSegment firstMergeSeg = cubeMgr.mergeSegments(cubeInstance, new SegmentRange.TSRange(date1, date3),
                null, true);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, "ADMIN");
        jobService.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));

        // Parquet cuboids vs Spark SQL
        cubeInstance = cubeMgr.reloadCube(cubeName);
        for (CubeSegment seg : cubeInstance.getSegments()) {
            queryTest(seg);
        }
    }

    public void snapshotTest(CubeSegment segment) {
        String cubeName = segment.getCubeInstance().getName();
        CubeInstance cubeInstance = cubeMgr.reloadCube(cubeName);
        segment = cubeInstance.getSegmentById(segment.getUuid());
        Assert.assertEquals(5, segment.getSnapshots().size());
    }

    private void queryTest(CubeSegment segment) {
        // Result cmp: Parquet vs Spark SQL
        for (LayoutEntity entity : MetadataConverter.extractEntityList2JavaList(segment.getCubeInstance())) {
            // Parquet result
            Dataset<Row> layoutDataset = StorageFactory.createEngineAdapter(new IStorageAware() { // Hardcode
                @Override
                public int getStorageType() {
                    return 4;
                }
            }, NSparkCubingEngine.NSparkCubingStorage.class)
                    .getFrom(PathManager.getParquetStoragePath(segment.getConfig(), segment.getCubeInstance().getName(),
                            segment.getName(), segment.getStorageLocationIdentifier(), String.valueOf(entity.getId())), ss);

            Set<Integer> measures = new HashSet<Integer>();
            Set<Integer> rowKeys = entity.getOrderedDimensions().keySet();
            for (Map.Entry<Integer, FunctionDesc> entry : entity.getOrderedMeasures().entrySet()) {
                String type = entry.getValue().returnType().dataType();
                if (type.equals("hllc") || type.equals("topn") || type.equals("percentile")) {
                    continue;
                }
                measures.add(entry.getKey());
            }
            layoutDataset = layoutDataset.select(NSparkCubingUtil.getColumns(rowKeys, measures))
                    .sort(NSparkCubingUtil.getColumns(rowKeys));
            System.out.println("Query cuboid ------------ " + entity.getId());
            layoutDataset = dsConvertToOriginal(layoutDataset, entity);
            layoutDataset.show(10);

            // Spark sql
            Dataset<Row> ds = initFlatTable(segment);
            if (!entity.isTableIndex()) {
                ds = CuboidAggregator.agg(ss, ds, entity.getOrderedDimensions().keySet(), entity.getOrderedMeasures(),
                        null, true);
            }
            Dataset<Row> exceptDs = ds.select(NSparkCubingUtil.getColumns(rowKeys, measures))
                    .sort(NSparkCubingUtil.getColumns(rowKeys));
            System.out.println("Spark sql ------------ ");
            exceptDs.show(10);
            long layoutCount = layoutDataset.count();
            long expectCount = exceptDs.count();
            Assert.assertEquals(layoutCount, expectCount);

            String msg = SparkQueryTest.checkAnswer(layoutDataset, exceptDs, false);
            Assert.assertNull(msg);
        }
    }

    private Dataset<Row> dsConvertToOriginal(Dataset<Row> layoutDs, LayoutEntity entity) {
        Map<Integer, FunctionDesc> orderedMeasures = entity.getOrderedMeasures();

        for (final Map.Entry<Integer, FunctionDesc> entry : orderedMeasures.entrySet()) {
            FunctionDesc functionDesc = entry.getValue();
            if (functionDesc != null) {
                final String[] columns = layoutDs.columns();
                String functionName = functionDesc.returnType().dataType();

                if ("bitmap".equals(functionName)) {
                    final int finalIndex = convertOutSchema(layoutDs, entry.getKey().toString(), DataTypes.LongType);
                    PreciseCountDistinct preciseCountDistinct = new PreciseCountDistinct(null);
                    layoutDs = layoutDs.map((MapFunction<Row, Row>) value -> {
                        Object[] ret = new Object[value.size()];
                        for (int i = 0; i < columns.length; i++) {
                            if (i == finalIndex) {
                                byte[] bytes = (byte[]) value.get(i);
                                Roaring64NavigableMap bitmapCounter = preciseCountDistinct.deserialize(bytes);
                                ret[i] = bitmapCounter.getLongCardinality();
                            } else {
                                ret[i] = value.get(i);
                            }
                        }
                        return RowFactory.create(ret);
                    }, RowEncoder.apply(OUT_SCHEMA));
                }
            }
        }
        return layoutDs;
    }

    private Integer convertOutSchema(Dataset<Row> layoutDs, String fieldName,
            org.apache.spark.sql.types.DataType dataType) {
        StructField[] structFieldList = layoutDs.schema().fields();
        String[] columns = layoutDs.columns();

        int index = 0;
        StructField[] outStructFieldList = new StructField[structFieldList.length];
        for (int i = 0; i < structFieldList.length; i++) {
            if (columns[i].equalsIgnoreCase(fieldName)) {
                index = i;
                StructField structField = structFieldList[i];
                outStructFieldList[i] = new StructField(structField.name(), dataType, false, structField.metadata());
            } else {
                outStructFieldList[i] = structFieldList[i];
            }
        }

        OUT_SCHEMA = new StructType(outStructFieldList);

        return index;
    }
}
