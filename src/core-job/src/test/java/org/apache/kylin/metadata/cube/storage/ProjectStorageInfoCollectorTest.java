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

package org.apache.kylin.metadata.cube.storage;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.cube.optimization.IncludedLayoutOptStrategy;
import org.apache.kylin.metadata.cube.optimization.IndexOptimizer;
import org.apache.kylin.metadata.cube.optimization.LowFreqLayoutOptStrategy;
import org.apache.kylin.metadata.cube.optimization.SimilarLayoutOptStrategy;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metrics.HdfsCapacityMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectStorageInfoCollectorTest extends NLocalFileMetadataTestCase {

    private static final String GC_PROJECT = "gc_test";
    private static final String GC_MODEL_ID = "e0e90065-e7c3-49a0-a801-20465ca64799";
    private static final String DEFAULT_PROJECT = "default";
    private static final String DEFAULT_MODEL_BASIC_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
    private JdbcRawRecStore jdbcRawRecStore;

    private static final long DAY_IN_MILLIS = 24 * 60 * 60 * 1000L;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        overwriteSystemProp("kylin.cube.low-frequency-threshold", "5");
        try {
            jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            log.error("initialize rec store failed.");
        }
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetStorageVolumeInfo() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        initTestData();

        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE, StorageInfoEnum.STORAGE_QUOTA,
                StorageInfoEnum.TOTAL_STORAGE);
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val volumeInfo = collector.getStorageVolumeInfo(getTestConfig(), DEFAULT_PROJECT);

        Assert.assertEquals(10240L * 1024 * 1024 * 1024, volumeInfo.getStorageQuotaSize());
        Assert.assertEquals(3417187L, volumeInfo.getGarbageStorageSize());
        Assert.assertEquals(4, volumeInfo.getGarbageModelIndexMap().size());
        Assert.assertEquals(5, volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).size());

        //  layout 1L with low frequency, other without layoutHitCount => garbage
        Assert.assertTrue(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(1L));
        Assert.assertTrue(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(20001L));
        Assert.assertTrue(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(30001L));
        Assert.assertTrue(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(1000001L));
        Assert.assertTrue(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(20_000_010_001L));

        // layout 10_001L, 10002L, 40_001L, 40_002L and 20_000_010_001L were not considered as garbage
        Assert.assertFalse(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(10001L));
        Assert.assertFalse(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(10002L));
        Assert.assertFalse(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(40001L));
        Assert.assertFalse(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(40002L));
        Assert.assertFalse(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(20_000_000_001L));

        // layout 20_000_040_001L is both auto and manual, consider as manual
        Assert.assertFalse(volumeInfo.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID).contains(20_000_040_001L));
    }

    @Test
    public void testNullFrequencyMap() {
        overwriteSystemProp("kylin.cube.low-frequency-threshold", "0");
        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dfManager.getDataflow(DEFAULT_MODEL_BASIC_ID);
        val garbageLayouts = IndexOptimizer.findGarbageLayouts(df, new LowFreqLayoutOptStrategy());

        Assert.assertTrue(garbageLayouts.isEmpty());
    }

    @Test
    public void testLowFreqLayoutStrategy() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        initTestData();
        NDataflowManager instance = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow dataflow = instance.getDataflow(DEFAULT_MODEL_BASIC_ID);

        //consider TableIndex
        getTestConfig().setProperty("kylin.index.frequency-strategy.consider-table-index", "true");
        Set<Long> garbageLayouts = IndexOptimizer.findGarbageLayouts(dataflow, new LowFreqLayoutOptStrategy());

        //  layout 1L and 20_000_040_001L with low frequency => garbage
        Assert.assertTrue(garbageLayouts.containsAll(Sets.newHashSet(1L, 20_000_040_001L)));

        // without frequency hit layout => garbage
        Assert.assertTrue(garbageLayouts.containsAll(Sets.newHashSet(20001L, 30001L, 1000001L, 20_000_020_001L)));

        // layout 10_001L, 10002L, 40_001L, 40_002L, 20_000_000_001L and 20_000_010_001L were not considered as garbage
        Assert.assertFalse(garbageLayouts.contains(10001L));
        Assert.assertFalse(garbageLayouts.contains(10002L));
        Assert.assertFalse(garbageLayouts.contains(40001L));
        Assert.assertFalse(garbageLayouts.contains(40002L));
        Assert.assertFalse(garbageLayouts.contains(20_000_000_001L));
        Assert.assertFalse(garbageLayouts.contains(20_000_010_001L));

        getTestConfig().setProperty("kylin.index.frequency-strategy.consider-table-index", "false");
        Set<Long> garbageLayouts2 = IndexOptimizer.findGarbageLayouts(dataflow, new LowFreqLayoutOptStrategy());
        Assert.assertEquals(Sets.newHashSet(1L, 20001L, 30001L, 1000001L), garbageLayouts2);
    }

    @Test
    public void testLowFreqStrategyOfFreqTimeWindow() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        initTestData();
        val collector = new ProjectStorageInfoCollector(Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE));
        getTestConfig().setProperty("kylin.cube.frequency-time-window", "90");
        val volumeInfo2 = collector.getStorageVolumeInfo(getTestConfig(), DEFAULT_PROJECT);
        final Set<Long> garbageSet2 = volumeInfo2.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID);
        Assert.assertEquals(Sets.newHashSet(20001L, 30001L, 20000010001L, 1000001L), garbageSet2);
    }

    @Test
    public void testLowFreqStrategyOfLowFreqStrategyThreshold() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        initTestData();
        val collector = new ProjectStorageInfoCollector(Lists.newArrayList(StorageInfoEnum.GARBAGE_STORAGE));
        getTestConfig().setProperty("kylin.cube.low-frequency-threshold", "2");
        val volumeInfo3 = collector.getStorageVolumeInfo(getTestConfig(), DEFAULT_PROJECT);
        final Set<Long> garbageSet3 = volumeInfo3.getGarbageModelIndexMap().get(DEFAULT_MODEL_BASIC_ID);
        Assert.assertEquals(Sets.newHashSet(1L, 20001L, 30001L, 20000010001L, 1000001L), garbageSet3);
    }

    @Test
    public void testIncludedLayoutGcStrategy() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(DEFAULT_MODEL_BASIC_ID);
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            LayoutEntity layout1 = new LayoutEntity();
            layout1.setId(20_000_040_001L);
            layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8));
            layout1.setAuto(true);
            IndexEntity index1 = new IndexEntity();
            index1.setId(20_000_040_000L);
            index1.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8));
            index1.setLayouts(Lists.newArrayList(layout1));

            LayoutEntity layout2 = new LayoutEntity();
            layout2.setId(20_000_050_001L);
            layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            layout2.setAuto(true);
            IndexEntity index2 = new IndexEntity();
            index2.setId(20_000_050_000L);
            index2.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            index2.setLayouts(Lists.newArrayList(layout2));

            copyForWrite.setIndexes(Lists.newArrayList(index1, index2));
        });

        IndexPlan indexPlanAfter = indexPlanManager.getIndexPlan(DEFAULT_MODEL_BASIC_ID);
        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);
        updateLayoutHitCount(dataflowManager, indexPlanAfter, currentDate, DEFAULT_MODEL_BASIC_ID, 100);

        // change all layouts' status to ready.
        NDataflow dataflow = dataflowManager.getDataflow(DEFAULT_MODEL_BASIC_ID);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getUuid());
        NDataSegment latestReadySegment = dataflow.getLatestReadySegment();
        Set<Long> ids = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        update.setToAddOrUpdateLayouts(genCuboids(dataflow, latestReadySegment.getId(), ids));
        dataflowManager.updateDataflow(update);

        dataflow = dataflowManager.getDataflow(DEFAULT_MODEL_BASIC_ID);
        getTestConfig().setProperty("kylin.index.include-strategy.consider-table-index", "false");
        Set<Long> garbageLayouts = IndexOptimizer.findGarbageLayouts(dataflow, new IncludedLayoutOptStrategy());
        Assert.assertTrue(garbageLayouts.isEmpty());

        getTestConfig().setProperty("kylin.index.include-strategy.consider-table-index", "true");
        Set<Long> garbageLayouts2 = IndexOptimizer.findGarbageLayouts(dataflow, new IncludedLayoutOptStrategy());
        Map<Long, FrequencyMap> layoutHitCount = dataflowManager.getDataflow(DEFAULT_MODEL_BASIC_ID)
                .getLayoutHitCount();
        Assert.assertEquals(1, garbageLayouts2.size());
        Assert.assertTrue(garbageLayouts2.contains(20_000_050_001L));
        NavigableMap<Long, Integer> dateFrequency = layoutHitCount.get(20_000_040_001L).getDateFrequency();
        Assert.assertEquals(new Integer(200), dateFrequency.get(currentDate - 3 * DAY_IN_MILLIS));
        Assert.assertEquals(new Integer(200), dateFrequency.get(currentDate - 8 * DAY_IN_MILLIS));
        Assert.assertEquals(new Integer(200), dateFrequency.get(currentDate - DAY_IN_MILLIS));
    }

    @Test
    public void testSimilarLayoutGcStrategy() {
        /*
         * -- without * is rulebased, 60001 --> 40001           (similar)
         * --                                   40001 --> 30001 (similar)
         * --                         60001 ------------> 30001 (similar)
         * --                         60001 --> 20001           (not similar)
         * --                                   20001 --> 1     (not similar)
         * --                         60001 ------------> 1     (not similar)
         * --                                   20001 --> 10001 (not similar)
         * --                         60001 ------------> 10001 (not similar)
         * --                         60001 --> 50001           (similar)
         * --                                   50001 --> 70001 (similar)
         * --                                   50001 --> 1     (not similar)
         * --                                   40001 --> 1     (not similar)
         * --                         60001 ------------> 70001 (similar)
         * --
         * --                                             --------------------------
         * --                                              layout_id |     rows
         * --                                             --------------------------
         * --                     60001                    60001     |  115_000_000
         * --                   /   |   \                  20001     |   81_000_000
         * --                  /    |    \                 50001     |  114_050_000
         * --              20001  50001  40001             40001     |  113_000_000
         * --             /  |     /| \ /  |               30001     |  112_000_000
         * --            /   |    / | /\   |               10001     |   68_000_000
         * --           /    |   /  |/  \  |                   1     |    4_000_000
         * --          /     |  /  /|    \ |               70001     |  112_000_000
         * --        10001   | / /  |     \|              --------------------------
         * --                |//    |     30001
         * --                |/  70001(*)
         * --                1
         */

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), GC_PROJECT);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), GC_PROJECT);

        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);

        IndexPlan oriIndexPlan = indexPlanManager.getIndexPlan(GC_MODEL_ID);
        updateLayoutHitCount(dataflowManager, oriIndexPlan, currentDate, GC_MODEL_ID, 3);
        NDataflow dataflow = dataflowManager.getDataflow(GC_MODEL_ID);
        Set<Long> garbageLayouts = IndexOptimizer.findGarbageLayouts(dataflow, new SimilarLayoutOptStrategy());
        Assert.assertEquals(Sets.newHashSet(30001L, 40001L, 50001L, 70001L), garbageLayouts);
        NavigableMap<Long, Integer> dateFrequency = dataflow.getLayoutHitCount().get(60001L).getDateFrequency();
        Assert.assertEquals(15, dateFrequency.get(currentDate - 3 * DAY_IN_MILLIS).intValue());

        // set kylin.garbage.reject-similarity-threshold to 1_000_000
        getTestConfig().setProperty("kylin.index.beyond-similarity-bias-threshold", "1000000");
        oriIndexPlan = indexPlanManager.getIndexPlan(GC_MODEL_ID);
        updateLayoutHitCount(dataflowManager, oriIndexPlan, currentDate, GC_MODEL_ID, 3);
        dataflow = dataflowManager.getDataflow(GC_MODEL_ID);
        Set<Long> garbageLayouts2 = IndexOptimizer.findGarbageLayouts(dataflow, new SimilarLayoutOptStrategy());
        val dateFrequency60001 = dataflow.getLayoutHitCount().get(60001L).getDateFrequency();
        val dateFrequency40001 = dataflow.getLayoutHitCount().get(40001L).getDateFrequency();
        Assert.assertEquals(Sets.newHashSet(30001L, 50001L), garbageLayouts2);
        Assert.assertEquals(6, dateFrequency60001.get(currentDate - 3 * DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency60001.get(currentDate - 8 * DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency60001.get(currentDate - DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency40001.get(currentDate - 3 * DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency40001.get(currentDate - 8 * DAY_IN_MILLIS).intValue());
        Assert.assertEquals(6, dateFrequency40001.get(currentDate - DAY_IN_MILLIS).intValue());

        // test TableIndex
        Set<Long> garbageLayouts3 = IndexOptimizer.findGarbageLayouts(dataflow, new SimilarLayoutOptStrategy());
        Assert.assertEquals(Sets.newHashSet(30001L, 50001L), garbageLayouts3);
    }

    private void updateLayoutHitCount(NDataflowManager dataflowManager, IndexPlan indexPlan, long currentDate,
            String gcModelId, int i) {
        dataflowManager.updateDataflow(gcModelId, copyForWrite -> {
            Map<Long, FrequencyMap> frequencyMap = Maps.newHashMap();
            indexPlan.getAllLayouts().forEach(layout -> {
                TreeMap<Long, Integer> hit = Maps.newTreeMap();
                hit.put(currentDate - 3 * DAY_IN_MILLIS, i);
                hit.put(currentDate - 8 * DAY_IN_MILLIS, i);
                hit.put(currentDate - DAY_IN_MILLIS, i);
                frequencyMap.putIfAbsent(layout.getId(), new FrequencyMap(hit));
            });
            copyForWrite.setLayoutHitCount(frequencyMap);
        });
    }

    private void initTestData() {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val indexPlan = indexPlanManager.getIndexPlan(DEFAULT_MODEL_BASIC_ID);

        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);

        dataflowManager.updateDataflow(DEFAULT_MODEL_BASIC_ID,
                copyForWrite -> copyForWrite.setLayoutHitCount(new HashMap<Long, FrequencyMap>() {
                    {
                        put(1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 1);
                                put(currentDate - 31 * DAY_IN_MILLIS, 100);
                            }
                        }));
                        put(40001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 1);
                                put(currentDate, 2);
                            }
                        }));
                        put(40002L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 1);
                                put(currentDate, 2);
                            }
                        }));
                        put(10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 30 * DAY_IN_MILLIS, 10);
                            }
                        }));
                        put(10002L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 30 * DAY_IN_MILLIS, 10);
                            }
                        }));
                        put(IndexEntity.TABLE_INDEX_START_ID + 10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 100);
                            }
                        }));
                        put(IndexEntity.TABLE_INDEX_START_ID + 1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                            {
                                put(currentDate - 7 * DAY_IN_MILLIS, 100);
                            }
                        }));
                    }
                }));

        // add some new layouts for cube
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            val newDesc = new IndexEntity();
            newDesc.setId(40000);
            newDesc.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            newDesc.setMeasures(Lists.newArrayList(100000, 100001, 100005));
            val layout = new LayoutEntity();
            layout.setId(40001);
            layout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100001, 100005));
            layout.setAuto(true);
            layout.setUpdateTime(currentTime - 8 * DAY_IN_MILLIS);
            val layout3 = new LayoutEntity();
            layout3.setId(40002);
            layout3.setColOrder(Lists.newArrayList(3, 2, 1, 4, 100000, 100001, 100005));
            layout3.setAuto(true);
            layout3.setUpdateTime(currentTime - 8 * DAY_IN_MILLIS);
            newDesc.setLayouts(Lists.newArrayList(layout, layout3));

            val newDesc2 = new IndexEntity();
            newDesc2.setId(IndexEntity.TABLE_INDEX_START_ID + 40000);
            newDesc2.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            val layout2 = new LayoutEntity();
            layout2.setId(IndexEntity.TABLE_INDEX_START_ID + 40001);
            layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7));
            layout2.setAuto(true);
            layout2.setManual(true);
            newDesc2.setLayouts(Lists.newArrayList(layout2));

            copyForWrite.getIndexes().add(newDesc);
            copyForWrite.getIndexes().add(newDesc2);
        });

        // change all layouts' status to ready.
        NDataflow df = dataflowManager.getDataflow(DEFAULT_MODEL_BASIC_ID);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        NDataSegment latestReadySegment = df.getLatestReadySegment();
        Set<Long> ids = Sets.newHashSet(2_000_020_001L, 2_000_030_001L, 2_000_040_001L, 40_001L, 40_002L);
        update.setToAddOrUpdateLayouts(genCuboids(df, latestReadySegment.getId(), ids));
        dataflowManager.updateDataflow(update);
    }

    private NDataLayout[] genCuboids(NDataflow df, String segId, Set<Long> layoutIds) {
        List<NDataLayout> realLayouts = Lists.newArrayList();
        layoutIds.forEach(id -> realLayouts.add(NDataLayout.newDataLayout(df, segId, id)));
        return realLayouts.toArray(new NDataLayout[0]);
    }

    @Test
    public void testGetStorageVolumeInfoEmpty() {
        List<StorageInfoEnum> storageInfoEnumList = Lists.newArrayList();
        val collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        val storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), DEFAULT_PROJECT);

        Assert.assertEquals(-1L, storageVolumeInfo.getStorageQuotaSize());
        Assert.assertEquals(-1L, storageVolumeInfo.getTotalStorageSize());
        Assert.assertEquals(-1L, storageVolumeInfo.getGarbageStorageSize());
        Assert.assertEquals(0, storageVolumeInfo.getGarbageModelIndexMap().size());
    }

    @Test
    public void testGetStorageVolumeException() throws NoSuchFieldException, IllegalAccessException, IOException {
        List<StorageInfoEnum> storageInfoEnumList = Lists.newArrayList();
        TotalStorageCollector totalStorageCollector = Mockito.spy(TotalStorageCollector.class);
        ProjectStorageInfoCollector collector = new ProjectStorageInfoCollector(storageInfoEnumList);
        Field field = collector.getClass().getDeclaredField("collectors");
        Unsafe.changeAccessibleObject(field, true);
        List<StorageInfoCollector> collectors = (List<StorageInfoCollector>) field.get(collector);
        collectors.add(totalStorageCollector);
        Mockito.doThrow(new RuntimeException("catch me")).when(totalStorageCollector).collect(Mockito.any(),
                Mockito.anyString(), Mockito.any(StorageVolumeInfo.class));

        StorageVolumeInfo storageVolumeInfo = collector.getStorageVolumeInfo(getTestConfig(), DEFAULT_PROJECT);

        Assert.assertEquals(-1L, storageVolumeInfo.getTotalStorageSize());
        Assert.assertEquals(1, storageVolumeInfo.getThrowableMap().size());
        Assert.assertEquals(RuntimeException.class,
                storageVolumeInfo.getThrowableMap().values().iterator().next().getClass());
        Assert.assertEquals("catch me", storageVolumeInfo.getThrowableMap().values().iterator().next().getMessage());

    }

    @Test
    public void testGetStorageVolumeWithOutHdfsCapacityMetrics() throws IOException {
        KylinConfig testConfig = getTestConfig();
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "false");
        StorageVolumeInfo storageVolumeInfo = Mockito.spy(StorageVolumeInfo.class);
        TotalStorageCollector totalStorageCollector = new TotalStorageCollector();
        totalStorageCollector.collect(testConfig, DEFAULT_PROJECT, storageVolumeInfo);
        Assert.assertEquals(0, storageVolumeInfo.getTotalStorageSize());
    }

    @Test
    public void testGetStorageVolumeWithHdfsCapacityMetrics() throws IOException {
        KylinConfig testConfig = getTestConfig();
        overwriteSystemProp("kylin.metrics.hdfs-periodic-calculation-enabled", "true");
        HdfsCapacityMetrics.registerHdfsMetrics();
        StorageVolumeInfo storageVolumeInfo = Mockito.spy(StorageVolumeInfo.class);
        TotalStorageCollector totalStorageCollector = new TotalStorageCollector();
        totalStorageCollector.collect(testConfig, DEFAULT_PROJECT, storageVolumeInfo);
        Assert.assertEquals(0, storageVolumeInfo.getTotalStorageSize());
    }

}
