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

package org.apache.kylin.metadata.cube.utils;

import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class IndexPlanReduceUtilTest {

    private IndexEntity aggIndex1, aggIndex2, aggIndex3, aggIndex4, aggIndex5, aggIndex6;
    private IndexEntity tableIndex1, tableIndex2, tableIndex3, tableIndex4;

    @Test
    public void testCollectIncludedAggIndexLayoutsForGarbageCleaning() {
        initAggIndex();
        // case 1: index1.dimensions == index2.dimensions && index2.measures contains index1.measures
        Map<LayoutEntity, LayoutEntity> map1 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(aggIndex1, aggIndex2), true);
        Assert.assertEquals(1, map1.size());
        LayoutEntity redundant1 = map1.keySet().iterator().next();
        LayoutEntity reserved1 = map1.get(redundant1);
        Assert.assertEquals("[1, 2, 3, 4, 100000, 100003]", redundant1.getColOrder().toString());
        Assert.assertEquals("[1, 2, 3, 4, 100000, 100002, 100003]", reserved1.getColOrder().toString());

        // case 2: index1.dimensions == index3.dimensions && index1.measures != index3.measures
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(aggIndex1, aggIndex3), true);
        Assert.assertTrue(map2.isEmpty());

        // case 3: index3.dimensions != index4.dimensions && index3.measures == index4.measures
        Map<LayoutEntity, LayoutEntity> map3 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(aggIndex3, aggIndex4), true);
        Assert.assertTrue(map3.isEmpty());

        // case 4: index5.dimensions == index6.dimensions == 0 && index5.measures contains index6.measures
        Map<LayoutEntity, LayoutEntity> map4 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(aggIndex5, aggIndex6), true);
        Assert.assertEquals(1, map4.size());
        LayoutEntity redundant4 = map4.keySet().iterator().next();
        LayoutEntity reserved4 = map4.get(redundant4);
        Assert.assertEquals("[100000]", redundant4.getColOrder().toString());
        Assert.assertEquals("[100000, 100001]", reserved4.getColOrder().toString());

        // a case may not happen at present
        LayoutEntity layout1 = new LayoutEntity();
        layout1.setId(1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout1.setShardByColumns(Lists.newArrayList(1));
        LayoutEntity layout2 = new LayoutEntity();
        layout2.setId(2);
        layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout2.setShardByColumns(Lists.newArrayList(2));
        IndexEntity entity = new IndexEntity();
        entity.setLayouts(Lists.newArrayList(layout1, layout2));
        entity.setMeasures(Lists.newArrayList(100000, 100001));
        entity.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        layout1.setIndex(entity);
        layout2.setIndex(entity);
        entity.setId(calIndexId(entity));
        Map<LayoutEntity, LayoutEntity> map5 = IndexPlanReduceUtil.collectIncludedLayouts(collectLayouts(entity), true);
        Assert.assertTrue(map5.isEmpty());
    }

    @Test
    public void testCollectIncludedAggIndexLayoutsWithProposingLargerIndex() {
        LayoutEntity layout1 = new LayoutEntity(); // a proposed layout
        layout1.setId(1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout1.setInProposing(false);
        LayoutEntity layout2 = new LayoutEntity(); // a proposing layout
        layout2.setId(10001);
        layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001, 100002));
        layout2.setInProposing(true);
        LayoutEntity layout3 = new LayoutEntity(); // a proposing layout
        layout3.setId(10002);
        layout3.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100001, 100002));
        layout3.setInProposing(true);
        LayoutEntity layout4 = new LayoutEntity(); // a proposing layout
        layout4.setId(20001);
        layout4.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100002));
        layout4.setInProposing(true);
        IndexEntity entity1 = new IndexEntity();
        entity1.setLayouts(Lists.newArrayList(layout1));
        layout1.setIndex(entity1);
        entity1.setMeasures(Lists.newArrayList(100000, 100001));
        entity1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity1.setId(calIndexId(entity1));
        IndexEntity entity2 = new IndexEntity();
        entity2.setLayouts(Lists.newArrayList(layout2, layout3));
        layout2.setIndex(entity2);
        layout3.setIndex(entity2);
        entity2.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity2.setMeasures(Lists.newArrayList(100000, 100001, 100002));
        entity2.setId(calIndexId(entity2));
        IndexEntity entity3 = new IndexEntity();
        entity3.setLayouts(Lists.newArrayList(layout4));
        layout4.setIndex(entity3);
        entity3.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity3.setMeasures(Lists.newArrayList(100000, 100002));
        entity3.setId(calIndexId(entity3));

        // for auto-modeling proposition
        Map<LayoutEntity, LayoutEntity> map = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(entity1, entity2, entity3), false);
        Assert.assertEquals(1, map.size());
        Assert.assertTrue(map.containsKey(layout4));
        Assert.assertEquals("[2, 1, 3, 4, 100000, 100001, 100002]", map.get(layout4).getColOrder().toString());

        // for garbage-cleaning
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(entity1, entity2, entity3), true);
        Assert.assertEquals(2, map2.size());
        Assert.assertTrue(map2.containsKey(layout1));
        Assert.assertEquals("[1, 2, 3, 4, 100000, 100001, 100002]", map2.get(layout1).getColOrder().toString());
        Assert.assertTrue(map2.containsKey(layout4));
        Assert.assertEquals("[2, 1, 3, 4, 100000, 100001, 100002]", map2.get(layout4).getColOrder().toString());

    }

    @Test
    public void testCollectIncludedAggIndexLayoutsWithProposingSmallerIndex() {
        LayoutEntity layout1 = new LayoutEntity(); // a proposed layout
        layout1.setId(1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout1.setInProposing(false);
        LayoutEntity layout2 = new LayoutEntity(); // a proposing layout
        layout2.setId(30001);
        layout2.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000));
        layout2.setInProposing(true);
        IndexEntity entity1 = new IndexEntity();
        entity1.setLayouts(Lists.newArrayList(layout1));
        layout1.setIndex(entity1);
        entity1.setMeasures(Lists.newArrayList(100000, 100001));
        entity1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity1.setId(calIndexId(entity1));
        IndexEntity entity2 = new IndexEntity();
        entity2.setLayouts(Lists.newArrayList(layout2));
        layout2.setIndex(entity2);
        entity2.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity2.setMeasures(Lists.newArrayList(100000));
        entity2.setId(calIndexId(entity2));

        // for auto-modeling proposition, no need to propose smaller agg layout
        Map<LayoutEntity, LayoutEntity> map = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(entity1, entity2), false);
        Assert.assertEquals(1, map.size());
        Assert.assertTrue(map.containsKey(layout2));
        Assert.assertEquals("[1, 2, 3, 4, 100000, 100001]", map.get(layout2).getColOrder().toString());

        // for garbage-cleaning
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(entity1, entity2), true);
        Assert.assertEquals(1, map2.size());
        Assert.assertTrue(map2.containsKey(layout2));
        Assert.assertEquals("[1, 2, 3, 4, 100000, 100001]", map2.get(layout2).getColOrder().toString());
    }

    @Test
    public void testCollectIncludedTableIndexLayoutsForGarbageCleaning() {
        initTableIndex();

        // case 1: index1.dimensions contains index2.dimensions,
        //         1) different colOrders with same dimensions will not be reduced;
        //         2) if these layouts have same shardByColumns, they will be reduced;
        //         3) if these layouts don't have same shardByColumns, they will not be reduced
        Map<LayoutEntity, LayoutEntity> map1 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(tableIndex1, tableIndex2), true);
        Assert.assertEquals(1, map1.size());
        LayoutEntity redundant = map1.keySet().iterator().next();
        Assert.assertEquals("[1, 2, 3]", redundant.getColOrder().toString());
        Assert.assertTrue(redundant.getShardByColumns().isEmpty());
        Assert.assertEquals("[1, 2, 3, 4]", map1.get(redundant).getColOrder().toString());

        // case 2: layouts in index4 only shardByColumns are not the same, they will not be reduced;
        //         layouts in index3 don't contain shardByColumns, they will not be reduced.
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(tableIndex3, tableIndex4), true);
        Assert.assertTrue(map2.isEmpty());
    }

    @Test
    public void testCollectIncludedTableIndexLayouts() {
        LayoutEntity[] layoutArray = new LayoutEntity[6];
        layoutArray[0] = new LayoutEntity();
        layoutArray[0].setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        layoutArray[0].setColOrder(Lists.newArrayList(1, 2, 3, 4));
        layoutArray[0].setAuto(true);
        layoutArray[1] = new LayoutEntity();
        layoutArray[1].setId(IndexEntity.TABLE_INDEX_START_ID + 2);
        layoutArray[1].setColOrder(Lists.newArrayList(1, 3, 2, 4));
        layoutArray[1].setAuto(false);
        layoutArray[1].setInProposing(false);
        layoutArray[2] = new LayoutEntity();
        layoutArray[2].setId(IndexEntity.TABLE_INDEX_START_ID + IndexEntity.INDEX_ID_STEP + 1);
        layoutArray[2].setColOrder(Lists.newArrayList(1, 2, 3, 4, 5));
        layoutArray[2].setAuto(true);
        layoutArray[2].setInProposing(true);
        layoutArray[3] = new LayoutEntity();
        layoutArray[3].setId(IndexEntity.TABLE_INDEX_START_ID + 2 * IndexEntity.INDEX_ID_STEP + 1);
        layoutArray[3].setColOrder(Lists.newArrayList(1, 2, 3));
        layoutArray[3].setAuto(true);
        layoutArray[3].setInProposing(true);
        layoutArray[4] = new LayoutEntity();
        layoutArray[4].setId(IndexEntity.TABLE_INDEX_START_ID + 2 * IndexEntity.INDEX_ID_STEP + 2);
        layoutArray[4].setColOrder(Lists.newArrayList(1, 3, 2));
        layoutArray[4].setAuto(true);
        layoutArray[4].setInProposing(false);
        layoutArray[5] = new LayoutEntity();
        layoutArray[5].setId(IndexEntity.TABLE_INDEX_START_ID + 3 * IndexEntity.INDEX_ID_STEP + 1);
        layoutArray[5].setColOrder(Lists.newArrayList(1, 2, 3, 4, 6));
        layoutArray[5].setAuto(true);
        layoutArray[5].setInProposing(true);

        IndexEntity entity1 = new IndexEntity();
        entity1.setLayouts(Lists.newArrayList(layoutArray[0], layoutArray[1]));
        entity1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity1.setId(calIndexId(entity1));
        IndexEntity entity2 = new IndexEntity();
        entity2.setLayouts(Lists.newArrayList(layoutArray[2]));
        entity2.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5));
        entity2.setId(calIndexId(entity2));
        IndexEntity entity3 = new IndexEntity();
        entity3.setLayouts(Lists.newArrayList(layoutArray[3], layoutArray[4]));
        entity3.setDimensions(Lists.newArrayList(1, 2, 3));
        entity3.setId(calIndexId(entity3));
        IndexEntity entity4 = new IndexEntity();
        entity4.setLayouts(Lists.newArrayList(layoutArray[5]));
        entity4.setDimensions(Lists.newArrayList(1, 2, 3, 4, 6));
        entity4.setId(calIndexId(entity4));

        // for auto-modeling proposition
        Map<LayoutEntity, LayoutEntity> map1 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(entity1, entity2, entity3, entity4), false);
        Assert.assertEquals(1, map1.size());
        Assert.assertTrue(map1.containsKey(layoutArray[3]));
        Assert.assertEquals("[1, 2, 3, 4, 5]", map1.get(layoutArray[3]).getColOrder().toString());

        // for garbage-cleaning
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(entity1, entity2, entity3, entity4), true);
        Assert.assertEquals(3, map2.size());
        Assert.assertTrue(map2.containsKey(layoutArray[3]));
        Assert.assertEquals("[1, 2, 3, 4, 5]", map2.get(layoutArray[3]).getColOrder().toString());
        Assert.assertTrue(map2.containsKey(layoutArray[0]));
        Assert.assertEquals("[1, 2, 3, 4, 5]", map2.get(layoutArray[0]).getColOrder().toString());
        Assert.assertTrue(map2.containsKey(layoutArray[4]));
        Assert.assertEquals("[1, 3, 2, 4]", map2.get(layoutArray[4]).getColOrder().toString());
    }

    @Test
    public void testCollectIncludedTableIndexLayoutsWithProposingSmallerIndex() {
        LayoutEntity[] layoutArray = new LayoutEntity[2];
        layoutArray[0] = new LayoutEntity();
        layoutArray[0].setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        layoutArray[0].setColOrder(Lists.newArrayList(1, 2, 3, 4));
        layoutArray[0].setAuto(true);
        layoutArray[0].setInProposing(false);
        layoutArray[1] = new LayoutEntity();
        layoutArray[1].setId(IndexEntity.TABLE_INDEX_START_ID + 2 * IndexEntity.INDEX_ID_STEP + 1);
        layoutArray[1].setColOrder(Lists.newArrayList(1, 2, 3));
        layoutArray[1].setAuto(true);
        layoutArray[1].setInProposing(true);

        IndexEntity entity1 = new IndexEntity();
        entity1.setLayouts(Lists.newArrayList(layoutArray[0]));
        entity1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        entity1.setId(calIndexId(entity1));
        IndexEntity entity2 = new IndexEntity();
        entity2.setLayouts(Lists.newArrayList(layoutArray[1]));
        entity2.setDimensions(Lists.newArrayList(1, 2, 3));
        entity2.setId(calIndexId(entity2));

        // for auto-modeling proposition

        List<LayoutEntity> inputLayouts = collectLayouts(entity1, entity2);
        inputLayouts.removeIf(layout -> IndexEntity.isTableIndex(layout.getId())); // consist with NCuboidReducer
        Map<LayoutEntity, LayoutEntity> map1 = IndexPlanReduceUtil.collectIncludedLayouts(inputLayouts, false);
        Assert.assertEquals(0, map1.size());

        // for garbage-cleaning
        Map<LayoutEntity, LayoutEntity> map2 = IndexPlanReduceUtil
                .collectIncludedLayouts(collectLayouts(entity1, entity2), true);
        Assert.assertEquals(1, map2.size());
        Assert.assertTrue(map2.containsKey(layoutArray[1]));
        Assert.assertEquals("[1, 2, 3, 4]", map2.get(layoutArray[1]).getColOrder().toString());
    }

    @Test
    public void testCollectIncludedLayoutsWhenAggIndexWithEmptyDim() {
        // table index [dim:5]
        LayoutEntity layout1 = new LayoutEntity();
        layout1.setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        layout1.setColOrder(Lists.newArrayList(5));
        layout1.setAuto(true);
        layout1.setInProposing(true);
        IndexEntity index1 = new IndexEntity();
        index1.setLayouts(Lists.newArrayList(layout1));
        index1.setDimensions(Lists.newArrayList(5));
        index1.setId(calIndexId(index1));
        layout1.setIndex(index1);

        // agg index [dim: measure:100000,100001]
        LayoutEntity layout2 = new LayoutEntity();
        layout2.setId(1);
        layout2.setColOrder(Lists.newArrayList(100000, 100001));
        layout2.setAuto(true);
        layout2.setInProposing(true);
        IndexEntity index2 = new IndexEntity();
        index2.setLayouts(Lists.newArrayList(layout2));
        index2.setId(calIndexId(index2));
        index2.setMeasures(Lists.newArrayList(100000, 100001));
        layout2.setIndex(index2);

        List<LayoutEntity> inputLayouts = collectLayouts(index1, index2);
        Map<LayoutEntity, LayoutEntity> map = IndexPlanReduceUtil.collectIncludedLayouts(inputLayouts, false);
        Assert.assertTrue(map.isEmpty());
    }

    private void initTableIndex() {
        /*
         * - Table index: index1(dims[1, 2, 3, 4]
         * -                |----layout([1, 2, 3, 4])
         * -                |----layout([1, 3, 2, 4])
         * -              index2(dims[1, 2, 3])
         * -                |----layout([1, 2, 3])
         * -                |----layout([1, 2, 3], shardBy=2)
         * -              index3(dims[4])
         * -                |----layout([4])
         * -              index4(dims([4, 5])
         * -                |----layout([4, 5], shardBy=4)
         * -                |----layout([4, 5], shardBy=5)
         */
        LayoutEntity layout1, layout2, layout3, layout4, layout5, layout6, layout7;
        layout1 = new LayoutEntity();
        layout1.setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4));
        layout1.setAuto(true);
        layout2 = new LayoutEntity();
        layout2.setId(IndexEntity.TABLE_INDEX_START_ID + 2);
        layout2.setColOrder(Lists.newArrayList(1, 3, 2, 4));
        layout2.setAuto(true);

        layout3 = new LayoutEntity();
        layout3.setId(IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 1);
        layout3.setColOrder(Lists.newArrayList(1, 2, 3));
        layout3.setAuto(true);
        layout4 = new LayoutEntity();
        layout4.setId(IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 2);
        layout4.setColOrder(Lists.newArrayList(1, 2, 3));
        layout4.setShardByColumns(Lists.newArrayList(2));
        layout4.setAuto(true);

        layout5 = new LayoutEntity();
        layout5.setId(2 * IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 1);
        layout5.setColOrder(Lists.newArrayList(4));
        layout5.setAuto(true);

        layout6 = new LayoutEntity();
        layout6.setId(3 * IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 1);
        layout6.setColOrder(Lists.newArrayList(4, 5));
        layout6.setShardByColumns(Lists.newArrayList(4));
        layout6.setAuto(true);
        layout7 = new LayoutEntity();
        layout7.setId(3 * IndexEntity.INDEX_ID_STEP + IndexEntity.TABLE_INDEX_START_ID + 2);
        layout7.setColOrder(Lists.newArrayList(4, 5));
        layout7.setShardByColumns(Lists.newArrayList(5));
        layout7.setAuto(true);

        tableIndex1 = new IndexEntity();
        tableIndex1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        tableIndex1.setLayouts(Lists.newArrayList(layout1, layout2));
        layout1.setIndex(tableIndex1);
        layout2.setIndex(tableIndex1);
        tableIndex1.setId(calIndexId(tableIndex1));

        tableIndex2 = new IndexEntity();
        tableIndex2.setDimensions(Lists.newArrayList(1, 2, 3));
        tableIndex2.setLayouts(Lists.newArrayList(layout3, layout4));
        layout3.setIndex(tableIndex2);
        layout4.setIndex(tableIndex2);
        tableIndex2.setId(calIndexId(tableIndex2));

        tableIndex3 = new IndexEntity();
        tableIndex3.setDimensions(Lists.newArrayList(4));
        tableIndex3.setLayouts(Lists.newArrayList(layout5));
        layout5.setIndex(tableIndex3);
        tableIndex3.setId(calIndexId(tableIndex3));

        tableIndex4 = new IndexEntity();
        tableIndex4.setDimensions(Lists.newArrayList(4, 5));
        tableIndex4.setLayouts(Lists.newArrayList(layout6, layout7));
        layout6.setIndex(tableIndex4);
        layout7.setIndex(tableIndex4);
        tableIndex4.setId(calIndexId(tableIndex4));
    }

    private void initAggIndex() {
        /*
         * - Agg index: index1(dims[1, 2, 3, 4], measures[100000, 100003])
         * -              |----layout([1, 2, 3, 4, 100000, 100003])
         * -              |----layout([2, 1, 3, 4, 100000, 100003])
         * -            index2
         * -              |----layout([1, 2, 3, 4, 100000, 100002, 100003])
         * -            index3
         * -              |----layout([1, 2, 3, 4, 100000, 100001])
         * -            index4
         * -              |----layout([1, 2, 3, 4, 5, 100000, 100001])
         * -            index5
         * -              |----layout([100000, 100001])
         * -            index6
         * -              |----layout([100000])
         */
        LayoutEntity layout1, layout2, layout3, layout4, layout5, layout6, layout7;
        layout1 = new LayoutEntity();
        layout1.setId(1);
        layout1.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100003));
        layout2 = new LayoutEntity();
        layout2.setId(2);
        layout2.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000, 100003));
        layout3 = new LayoutEntity();
        layout3.setId(10001);
        layout3.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100001));
        layout4 = new LayoutEntity();
        layout4.setId(20001);
        layout4.setColOrder(Lists.newArrayList(1, 2, 3, 4, 100000, 100002, 100003));
        layout5 = new LayoutEntity();
        layout5.setId(30001);
        layout5.setColOrder(Lists.newArrayList(1, 2, 3, 4, 5, 100000, 100001));
        layout6 = new LayoutEntity();
        layout6.setId(40001);
        layout6.setColOrder(Lists.newArrayList(100000, 100001));
        layout7 = new LayoutEntity();
        layout7.setId(50001);
        layout7.setColOrder(Lists.newArrayList(100000));

        aggIndex1 = new IndexEntity();
        aggIndex1.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        aggIndex1.setMeasures(Lists.newArrayList(100000, 100003));
        aggIndex1.setLayouts(Lists.newArrayList(layout1, layout2));
        layout1.setIndex(aggIndex1);
        layout2.setIndex(aggIndex1);
        aggIndex1.setId(calIndexId(aggIndex1));

        aggIndex2 = new IndexEntity();
        aggIndex2.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        aggIndex2.setMeasures(Lists.newArrayList(100000, 100002, 100003));
        aggIndex2.setLayouts(Lists.newArrayList(layout4));
        layout4.setIndex(aggIndex2);
        aggIndex2.setId(calIndexId(aggIndex2));

        aggIndex3 = new IndexEntity();
        aggIndex3.setDimensions(Lists.newArrayList(1, 2, 3, 4));
        aggIndex3.setMeasures(Lists.newArrayList(100000, 100001));
        aggIndex3.setLayouts(Lists.newArrayList(layout3));
        layout3.setIndex(aggIndex3);
        aggIndex3.setId(calIndexId(aggIndex3));

        aggIndex4 = new IndexEntity();
        aggIndex4.setDimensions(Lists.newArrayList(1, 2, 3, 4, 5));
        aggIndex4.setMeasures(Lists.newArrayList(100000, 100001));
        aggIndex4.setLayouts(Lists.newArrayList(layout5));
        layout5.setIndex(aggIndex4);
        aggIndex4.setId(calIndexId(aggIndex4));

        aggIndex5 = new IndexEntity();
        aggIndex5.setDimensions(Lists.newArrayList());
        aggIndex5.setMeasures(Lists.newArrayList(100000, 100001));
        aggIndex5.setLayouts(Lists.newArrayList(layout6));
        layout6.setIndex(aggIndex5);
        aggIndex5.setId(calIndexId(aggIndex5));

        aggIndex6 = new IndexEntity();
        aggIndex6.setDimensions(Lists.newArrayList());
        aggIndex6.setMeasures(Lists.newArrayList(100000, 100001));
        aggIndex6.setLayouts(Lists.newArrayList(layout7));
        layout7.setIndex(aggIndex6);
        aggIndex6.setId(calIndexId(aggIndex6));
    }

    private long calIndexId(IndexEntity entity) {
        if (entity.getLayouts().isEmpty()) {
            throw new IllegalStateException("Index without layouts!");
        }
        return entity.getLayouts().get(0).getId() - entity.getLayouts().get(0).getId() % IndexEntity.INDEX_ID_STEP;
    }

    private List<LayoutEntity> collectLayouts(IndexEntity... entities) {
        List<LayoutEntity> layouts = Lists.newArrayList();
        for (IndexEntity entity : entities) {
            layouts.addAll(entity.getLayouts());
        }
        return layouts;
    }
}
