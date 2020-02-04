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

import io.kyligence.kap.engine.spark.NSparkCubingEngine;
import io.kyligence.kap.engine.spark.builder.CreateFlatTable;
import io.kyligence.kap.engine.spark.job.CuboidAggregator;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.engine.spark.job.NSparkMergingJob;
import io.kyligence.kap.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.metadata.FunctionDesc;
import org.apache.kylin.engine.spark.metadata.MetadataConverter;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.engine.spark.metadata.cube.model.LayoutEntity;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
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
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Sets;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

public class SparkCubingJobTest extends LocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(SparkCubingJobTest.class);
    private static StructType OUT_SCHEMA = null;

    private CubeManager cubeManager;
    private DefaultScheduler scheduler;
    private ExecutableManager jobService;

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kap.engine.persist-flattable-threshold", "0");
        System.setProperty("kylin.metadata.distributed-lock-impl", "io.kyligence.kap.engine.spark.utils.MockedDistributedLock$MockedFactory");
        System.setProperty(KylinConfig.KYLIN_CONF, LocalFileMetadataTestCase.LOCALMETA_TEST_DATA);
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.source.provider.0", "io.kyligence.kap.engine.spark.source.HiveSource");
        cubeManager = CubeManager.getInstance(kylinConfig);
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.createInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        for (String jobId : jobService.getAllJobIds()) {
            AbstractExecutable executable = jobService.getJob(jobId);
            if (executable instanceof CheckpointExecutable) {
                jobService.deleteJob(jobId);
            }
        }
    }

    @Test
    public void testBuildJob() throws Exception {
        String cubeName = "ci_inner_join_cube";

        clearSegment(cubeName);
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = 0;
        long date2 = f.parse("2012-06-01").getTime();
        long date3 = f.parse("2013-07-01").getTime();

        CubeSegment segment = cubeManager.appendSegment(cubeInstance, new SegmentRange.TSRange(date1, date2));
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), "ADMIN");
        jobService.addJob(job);
        // wait job done
        ExecutableState state = waitForJob(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, state);

        CubeSegment segment2 = cubeManager.appendSegment(cubeInstance, new SegmentRange.TSRange(date2, date3));
        NSparkCubingJob job2 = NSparkCubingJob.create(Sets.newHashSet(segment2), "ADMIN");
        jobService.addJob(job2);
        // wait job done
        ExecutableState state2 = waitForJob(job2.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, state2);

        // Result cmp: Parquet vs Spark SQL
        queryTest(segment);
    }

    @Test
    public void testBuildTwoSegmentsAndMerge() throws Exception {
        String cubeName = "ci_inner_join_cube";
        clearSegment(cubeName);
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);

        /**
         * Round1. Build 2 segment
         */
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));
        long date1 = f.parse("2010-01-01").getTime();
        long date2 = f.parse("2013-01-01").getTime();
        long date3 = f.parse("2014-01-01").getTime();

        CubeSegment segment = cubeManager.appendSegment(cubeInstance, new SegmentRange.TSRange(date1, date2));
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), "ADMIN");
        jobService.addJob(job);
        // wait job done
        ExecutableState state = waitForJob(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, state);

        CubeSegment segment2 = cubeManager.appendSegment(cubeInstance, new SegmentRange.TSRange(date2, date3));
        NSparkCubingJob job2 = NSparkCubingJob.create(Sets.newHashSet(segment2), "ADMIN");
        jobService.addJob(job2);
        // wait job done
        ExecutableState state2 = waitForJob(job2.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, state2);

        cubeInstance = cubeManager.reloadCube(cubeName);

        /**
         * Round2. Merge two segments
         */
        CubeSegment firstMergeSeg = cubeManager.mergeSegments(cubeInstance,
                new SegmentRange.TSRange(date1, date3), null, false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, "ADMIN",
                UUID.randomUUID().toString());
        jobService.addJob(firstMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));

        AfterMergeOrRefreshResourceMerger merger = new AfterMergeOrRefreshResourceMerger(config());
        merger.merge(firstMergeJob.getSparkMergingStep());
    }

    @Test
    public void testMergeResult() {
        String cubeName = "ci_inner_join_cube";

        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        for (CubeSegment segment : cubeInstance.getSegments()) {
            queryTest(segment);
        }
    }

    @Test
    public void testParquetQuery() {
        String cubeName = "ci_inner_join_cube";
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        Segments<CubeSegment> segments = cubeInstance.getSegments();
        for (CubeSegment segment : segments) {
            queryTest(segment);
        }
    }

    private void queryTest(CubeSegment segment) {
        // Result cmp: Parquet vs Spark SQL
        for (LayoutEntity entity : MetadataConverter.extractEntityList2JavaList(segment.getCubeInstance())) {
            // Parquet result
            Dataset<Row> layoutDataset = StorageFactory
                    .createEngineAdapter(new IStorageAware() { // Hardcode
                        @Override
                        public int getStorageType() {
                            return 4;
                        }
                    }, NSparkCubingEngine.NSparkCubingStorage.class)
                    .getFrom(PathManager.getParquetStoragePath(segment.getConfig(),
                            segment.getCubeInstance().getId(),
                            segment.getUuid(), String.valueOf(entity.getId())),
                            ss);

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
                ds = CuboidAggregator.agg(ss, ds, entity.getOrderedDimensions().keySet(), entity.getOrderedMeasures(), null, true);
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

    private Dataset<Row> initFlatTable(CubeSegment segment) {
        System.out.println(getTestConfig().getMetadataUrl());

        CreateFlatTable flatTable = new CreateFlatTable(
                MetadataConverter.getSegmentInfo(segment.getCubeInstance(),
                        segment.getUuid()),
                null,
                ss,
                null);
        Dataset<Row> ds = flatTable.generateDataset(false, true);
        return ds;
    }

    private void clearSegment(String cubeName) throws Exception {
        CubeInstance cube = cubeManager.getCube(cubeName);
        cubeManager.updateCubeDropSegments(cube, cube.getSegments());
    }

    protected ExecutableState waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
                return job.getStatus();
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void deployMetadata(String localMetaData) throws IOException {
        // install metadata to hbase
        new ResourceTool().reset(config());
        new ResourceTool().copy(KylinConfig.createInstanceFromUri(localMetaData), config());

        // update cube desc signature.
        for (CubeInstance cube : CubeManager.getInstance(config()).listAllCubes()) {
            CubeDescManager.getInstance(config()).updateCubeDesc(cube.getDescriptor());//enforce signature updating
        }
    }

    private static KylinConfig config() {
        return KylinConfig.getInstanceFromEnv();
    }

}
