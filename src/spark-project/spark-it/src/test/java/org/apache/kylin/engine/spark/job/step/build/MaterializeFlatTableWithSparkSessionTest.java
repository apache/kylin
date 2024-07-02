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

package org.apache.kylin.engine.spark.job.step.build;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.job.NSparkCubingUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.datasource.storage.StorageStore;
import org.apache.spark.sql.datasource.storage.StorageStoreFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import lombok.AllArgsConstructor;
import lombok.Getter;

@SuppressWarnings("unchecked")
public class MaterializeFlatTableWithSparkSessionTest extends NLocalWithSparkSessionTest {

    @Getter
    @AllArgsConstructor
    static class ColumnStruct {
        private Long rowId;
        private String rowName;
        private DataType type;
    }

    private static final LinkedHashMap<Long, ColumnStruct> DIM_SCHEMAS = new LinkedHashMap<>();
    private static final LinkedHashMap<Long, ColumnStruct> MEASURE_SCHEMAS = new LinkedHashMap<>();
    static {
        DIM_SCHEMAS.put(0L, new ColumnStruct(0L, "LO_ORDERKEY", DataTypes.LongType));
        DIM_SCHEMAS.put(13L, new ColumnStruct(13L, "LO_QUANTITY", DataTypes.LongType));
        DIM_SCHEMAS.put(16L, new ColumnStruct(16L, "LO_CUSTKEY", DataTypes.LongType));
        DIM_SCHEMAS.put(22L, new ColumnStruct(22L, "C_NAME", DataTypes.StringType));
        DIM_SCHEMAS.put(24L, new ColumnStruct(24L, "C_CUSTKEY", DataTypes.LongType));

        MEASURE_SCHEMAS.put(100000L, new ColumnStruct(100000L, "COUNT_ALL", DataTypes.LongType));
        MEASURE_SCHEMAS.put(100001L, new ColumnStruct(100001L, "SUM_LINEORDER_LO_QUANTITY", DataTypes.LongType));
    }

    private static final String MODEL_ID = "3ec47efc-573a-9304-4405-8e05ae184322";
    private static final String SEGMENT_ID = "b2f206e1-7a15-c94a-20f5-f608d550ead6";
    private static final long LAYOUT_ID_TO_BUILD = 20001L;

    private KylinConfig config;
    private NIndexPlanManager indexPlanManager;
    private NDataflowManager dataflowManager;

    @Override
    public String getProject() {
        return "index_build_test";
    }

    @BeforeAll
    public static void beforeClass() {
        NLocalWithSparkSessionTest.beforeClass();
    }

    @AfterAll
    public static void afterClass() {
        NLocalWithSparkSessionTest.afterClass();
    }

    @BeforeEach
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();

        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        JobContextUtil.getJobContext(getTestConfig());

        config = getTestConfig();
        indexPlanManager = NIndexPlanManager.getInstance(config, getProject());
        dataflowManager = NDataflowManager.getInstance(config, getProject());
    }

    @AfterEach
    public void tearDown() throws Exception {
        super.tearDown();
        JobContextUtil.cleanUp();
    }

    private Object[] initDataCountCheckTest() {
        // Enable data count check
        overwriteSystemProp("kylin.build.data-count-check-enabled", "true");
        // Index Plan
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);
        // Segment
        NDataflow dataflow = dataflowManager.getDataflow(MODEL_ID);
        NDataSegment segment = dataflow.getSegment(SEGMENT_ID);
        // Layouts to build
        Set<Long> layoutIDs = Sets.newHashSet(LAYOUT_ID_TO_BUILD);
        Set<LayoutEntity> readOnlyLayouts = Collections
                .unmodifiableSet(NSparkCubingUtil.toLayouts(indexPlan, layoutIDs));

        return new Object[] { indexPlan, dataflow, segment, readOnlyLayouts };
    }

    @Test
    void testCheckDataCount_Good() throws Exception {
        Object[] objs = initDataCountCheckTest();
        IndexPlan indexPlan = (IndexPlan) objs[0];
        NDataflow dataflow = (NDataflow) objs[1];
        NDataSegment segment = (NDataSegment) objs[2];
        Set<LayoutEntity> readOnlyLayouts = (Set<LayoutEntity>) objs[3];

        // Prepare flat table parquet
        prepareFlatTableParquet(dataflow, segment, false);
        // Prepare already existing layouts' parquet
        prepareLayoutsParquet(indexPlan, segment, false, false, false);
        // Execute job
        ExecutableState state = executeJob(segment, readOnlyLayouts);

        // Verify
        assertEquals(ExecutableState.SUCCEED, state);
        NDataflow dataflowForVerify = dataflowManager.getDataflow(MODEL_ID);
        NDataSegment segmentForVerify = dataflowForVerify.getSegment(SEGMENT_ID);
        NDataLayout layoutForVerify = segmentForVerify.getLayout(LAYOUT_ID_TO_BUILD, true);
        assertNotNull(layoutForVerify);
        assertNull(layoutForVerify.getAbnormalType());
    }

    @ParameterizedTest
    @ValueSource(strings = { "1,10001", "20000000001,20000010001", "1,10001,20000000001,20000010001" })
    void testCheckDataCount_Good_WithNonExistingLayouts(String nonExistingLayouts) throws Exception {
        // Enable data count check
        overwriteSystemProp("kylin.build.data-count-check-enabled", "true");

        Set<Long> nonExistingLayoutsSet = Arrays.stream(nonExistingLayouts.split(",")).map(Long::parseLong)
                .collect(Collectors.toSet());

        IndexPlan indexPlan = indexPlanManager.getIndexPlan(MODEL_ID);
        // Layouts to build
        Set<Long> layoutIDs = Sets.newHashSet(LAYOUT_ID_TO_BUILD);
        Set<LayoutEntity> readOnlyLayouts = Collections
                .unmodifiableSet(NSparkCubingUtil.toLayouts(indexPlan, layoutIDs));
        // Remove all layouts for test
        indexPlanManager.updateIndexPlan(MODEL_ID, copyForWrite -> {
            copyForWrite.removeLayouts(nonExistingLayoutsSet, true, true);
        });
        dataflowManager.reloadAll();
        NDataflow dataflow = dataflowManager.getDataflow(MODEL_ID);
        NDataSegment segment = dataflow.getSegment(SEGMENT_ID);

        // Prepare flat table parquet
        prepareFlatTableParquet(dataflow, segment, false);
        // Prepare already existing layouts' parquet
        prepareLayoutsParquet(indexPlan, segment, false, false, false, nonExistingLayoutsSet);
        // Execute job
        ExecutableState state = executeJob(segment, readOnlyLayouts);

        // Verify
        assertEquals(ExecutableState.SUCCEED, state);
        NDataflow dataflowForVerify = dataflowManager.getDataflow(MODEL_ID);
        NDataSegment segmentForVerify = dataflowForVerify.getSegment(SEGMENT_ID);
        NDataLayout layoutForVerify = segmentForVerify.getLayout(LAYOUT_ID_TO_BUILD, true);
        assertNotNull(layoutForVerify);
        assertNull(layoutForVerify.getAbnormalType());
    }

    @Test
    void testCheckDataCount_Good_WithNonStrictCountCheckEnabled() throws Exception {
        Object[] objs = initDataCountCheckTest();
        IndexPlan indexPlan = (IndexPlan) objs[0];
        NDataflow dataflow = (NDataflow) objs[1];
        NDataSegment segment = (NDataSegment) objs[2];
        Set<LayoutEntity> readOnlyLayouts = (Set<LayoutEntity>) objs[3];

        overwriteSystemProp("kylin.build.allow-non-strict-count-check", "true");

        // Prepare flat table parquet
        prepareFlatTableParquet(dataflow, segment, false);
        // Prepare already existing layouts' parquet
        prepareLayoutsParquet(indexPlan, segment, false, false, true);
        // Execute job
        ExecutableState state = executeJob(segment, readOnlyLayouts);

        // Verify
        assertEquals(ExecutableState.SUCCEED, state);
        NDataflow dataflowForVerify = dataflowManager.getDataflow(MODEL_ID);
        NDataSegment segmentForVerify = dataflowForVerify.getSegment(SEGMENT_ID);
        NDataLayout layoutForVerify = segmentForVerify.getLayout(LAYOUT_ID_TO_BUILD, true);
        assertNotNull(layoutForVerify);
        assertNull(layoutForVerify.getAbnormalType());
    }

    @ParameterizedTest
    @CsvSource({ "true, false, false", "false, true, false", "false, false, true" })
    void testCheckDataCount_Failed(boolean loseOneAggLayoutRecord, boolean loseOneTableLayoutRecord,
            boolean loseTowTableLayoutRecords) throws Exception {
        Object[] objs = initDataCountCheckTest();
        IndexPlan indexPlan = (IndexPlan) objs[0];
        NDataflow dataflow = (NDataflow) objs[1];
        NDataSegment segment = (NDataSegment) objs[2];
        Set<LayoutEntity> readOnlyLayouts = (Set<LayoutEntity>) objs[3];

        // Prepare flat table parquet
        prepareFlatTableParquet(dataflow, segment, false);
        // Prepare already existing layouts' parquet
        prepareLayoutsParquet(indexPlan, segment, loseOneAggLayoutRecord, loseOneTableLayoutRecord,
                loseTowTableLayoutRecords);
        // Execute job
        ExecutableState state = executeJob(segment, readOnlyLayouts);

        // Verify
        assertEquals(ExecutableState.SUCCEED, state);
        NDataflow dataflowForVerify = dataflowManager.getDataflow(MODEL_ID);
        NDataSegment segmentForVerify = dataflowForVerify.getSegment(SEGMENT_ID);
        NDataLayout layoutForVerify = segmentForVerify.getLayout(LAYOUT_ID_TO_BUILD, true);
        assertNotNull(layoutForVerify);
        assertEquals(NDataLayout.AbnormalType.DATA_INCONSISTENT, layoutForVerify.getAbnormalType());
    }

    private void prepareFlatTableParquet(NDataflow dataflow, NDataSegment segment, boolean loseOneRecordForTest) {
        List<StructField> fields = new ArrayList<>();
        for (ColumnStruct cs : DIM_SCHEMAS.values()) {
            StructField field = DataTypes.createStructField(String.valueOf(cs.getRowId()), cs.getType(), true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1L, 5L, 1L, "Customer#000000001", 1L));
        rows.add(RowFactory.create(2L, 15L, 1L, "Customer#000000001", 1L));
        rows.add(RowFactory.create(3L, 5L, 2L, "Customer#000000002", 2L));
        rows.add(RowFactory.create(4L, 15L, 3L, "Customer#000000003", 3L));
        rows.add(RowFactory.create(5L, 5L, 5L, "Customer#000000005", 5L));
        if (!loseOneRecordForTest) {
            rows.add(RowFactory.create(6L, 15L, 5L, "Customer#000000005", 5L));
        }
        Dataset<Row> flatTableDS = ss.createDataFrame(rows, schema);

        ss.sessionState().conf().setLocalProperty("spark.sql.sources.repartitionWritingDataSource", "true");
        flatTableDS.write().mode(SaveMode.Overwrite)
                .parquet(config.getFlatTableDir(getProject(), dataflow.getId(), segment.getId()).toString());
    }

    private void prepareLayoutsParquet(IndexPlan indexPlan, NDataSegment segment, boolean loseOneAggLayoutRecordForTest,
            boolean loseOneTableLayoutRecordForTest, boolean loseTwoTableLayoutRecordsForTest) {
        prepareLayoutsParquet(indexPlan, segment, loseOneAggLayoutRecordForTest, loseOneTableLayoutRecordForTest,
                loseTwoTableLayoutRecordsForTest, null);
    }

    private void prepareLayoutsParquet(IndexPlan indexPlan, NDataSegment segment, boolean loseOneAggLayoutRecordForTest,
            boolean loseOneTableLayoutRecordForTest, boolean loseTwoTableLayoutRecordsForTest,
            Set<Long> nonExistingLayouts) {
        StorageStore store = StorageStoreFactory.create(NDataModel.DataStorageType.V1);

        // Prepare layout 1
        {
            List<StructField> fields = new ArrayList<>();
            for (ColumnStruct cs : toColumnStructs("0 13 16 24 22 100000")) {
                StructField field = DataTypes.createStructField(String.valueOf(cs.getRowId()), cs.getType(), true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            List<Row> rows = new ArrayList<>();
            rows.add(RowFactory.create(1L, 5L, 1L, 1L, "Customer#000000001", 1L));
            rows.add(RowFactory.create(2L, 15L, 1L, 1L, "Customer#000000001", 1L));
            rows.add(RowFactory.create(3L, 5L, 2L, 2L, "Customer#000000002", 1L));
            rows.add(RowFactory.create(4L, 15L, 3L, 3L, "Customer#000000003", 1L));
            rows.add(RowFactory.create(5L, 5L, 5L, 5L, "Customer#000000005", 1L));
            if (!loseOneAggLayoutRecordForTest) {
                rows.add(RowFactory.create(6L, 15L, 5L, 5L, "Customer#000000005", 1L));
            }

            Dataset<Row> layoutDS = ss.createDataFrame(rows, schema);

            final long layoutId = 1L;
            LayoutEntity layoutEntity = indexPlan.getLayoutEntity(layoutId);
            if (CollectionUtils.isEmpty(nonExistingLayouts) || !nonExistingLayouts.contains(layoutId)) {
                store.saveSegmentLayout(layoutEntity, segment, KapConfig.wrap(config), layoutDS);
            }
        }
        // Prepare layout 10001
        {
            List<StructField> fields = new ArrayList<>();
            for (ColumnStruct cs : toColumnStructs("24 22 100000 100001")) {
                StructField field = DataTypes.createStructField(String.valueOf(cs.getRowId()), cs.getType(), true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            List<Row> rows = new ArrayList<>();
            rows.add(RowFactory.create(1L, "Customer#000000001", 2L, 20L));
            rows.add(RowFactory.create(2L, "Customer#000000002", 1L, 5L));
            rows.add(RowFactory.create(3L, "Customer#000000003", 1L, 15L));
            rows.add(RowFactory.create(5L, "Customer#000000005", 2L, 20L));
            Dataset<Row> layoutDS = ss.createDataFrame(rows, schema);

            final long layoutId = 10001L;
            LayoutEntity layoutEntity = indexPlan.getLayoutEntity(layoutId);
            if (CollectionUtils.isEmpty(nonExistingLayouts) || !nonExistingLayouts.contains(layoutId)) {
                store.saveSegmentLayout(layoutEntity, segment, KapConfig.wrap(config), layoutDS);
            }
        }
        // Prepare layout 20000000001
        {
            List<StructField> fields = new ArrayList<>();
            for (ColumnStruct cs : toColumnStructs("0 13 16 24 22")) {
                StructField field = DataTypes.createStructField(String.valueOf(cs.getRowId()), cs.getType(), true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            List<Row> rows = new ArrayList<>();
            rows.add(RowFactory.create(1L, 5L, 1L, 1L, "Customer#000000001"));
            rows.add(RowFactory.create(2L, 15L, 1L, 1L, "Customer#000000001"));
            rows.add(RowFactory.create(3L, 5L, 2L, 2L, "Customer#000000002"));
            rows.add(RowFactory.create(4L, 15L, 3L, 3L, "Customer#000000003"));
            rows.add(RowFactory.create(5L, 5L, 5L, 5L, "Customer#000000005"));
            if (!loseOneTableLayoutRecordForTest && !loseTwoTableLayoutRecordsForTest) {
                rows.add(RowFactory.create(6L, 15L, 5L, 5L, "Customer#000000005"));
            }
            Dataset<Row> layoutDS = ss.createDataFrame(rows, schema);

            final long layoutId = 20_000_000_001L;
            LayoutEntity layoutEntity = indexPlan.getLayoutEntity(layoutId);
            if (CollectionUtils.isEmpty(nonExistingLayouts) || !nonExistingLayouts.contains(layoutId)) {
                store.saveSegmentLayout(layoutEntity, segment, KapConfig.wrap(config), layoutDS);
            }
        }
        // Prepare layout 20000010001
        {
            List<StructField> fields = new ArrayList<>();
            for (ColumnStruct cs : toColumnStructs("0 13")) {
                StructField field = DataTypes.createStructField(String.valueOf(cs.getRowId()), cs.getType(), true);
                fields.add(field);
            }
            StructType schema = DataTypes.createStructType(fields);

            List<Row> rows = new ArrayList<>();
            rows.add(RowFactory.create(1L, 5L));
            rows.add(RowFactory.create(2L, 15L));
            rows.add(RowFactory.create(3L, 5L));
            rows.add(RowFactory.create(4L, 15L));
            rows.add(RowFactory.create(5L, 5L));
            if (!loseTwoTableLayoutRecordsForTest) {
                rows.add(RowFactory.create(6L, 15L));
            }
            Dataset<Row> layoutDS = ss.createDataFrame(rows, schema);

            final long layoutId = 20_000_010_001L;
            LayoutEntity layoutEntity = indexPlan.getLayoutEntity(layoutId);
            if (CollectionUtils.isEmpty(nonExistingLayouts) || !nonExistingLayouts.contains(layoutId)) {
                store.saveSegmentLayout(layoutEntity, segment, KapConfig.wrap(config), layoutDS);
            }
        }
    }

    private List<ColumnStruct> toColumnStructs(String columnIds) {
        return Arrays.stream(columnIds.split(" ")).map(id -> {
            ColumnStruct cs = DIM_SCHEMAS.get(Long.valueOf(id));
            if (cs == null) {
                cs = MEASURE_SCHEMAS.get(Long.valueOf(id));
            }
            return cs;
        }).collect(Collectors.toList());
    }

    private ExecutableState executeJob(NDataSegment segment, Set<LayoutEntity> readOnlyLayouts)
            throws InterruptedException {
        ExecutableManager executableManager = ExecutableManager.getInstance(config, getProject());

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), readOnlyLayouts, "ADMIN", null);
        executableManager.addJob(job);
        return IndexDataConstructor.wait(job);
    }
}
