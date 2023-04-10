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

package org.apache.kylin.query.routing;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.util.MetadataTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MetadataInfo
class LayoutCandidateSortTest {

    @Test
    void testPreferAggComparator() {
        MockEntity mock1 = new MockEntity(IndexEntity.TABLE_INDEX_START_ID + 1, ImmutableList.of(1), 2);
        MockEntity mock2 = new MockEntity(1L, ImmutableList.of(1), 3);
        assertSortedResult(1L, QueryLayoutChooser.preferAggComparator(), mock1, mock2);
    }

    @Test
    void testSegmentRangeComparator() {
        MockEntity mock1 = new MockEntity(IndexEntity.TABLE_INDEX_START_ID + 1, ImmutableList.of(1, 2), 5000, 2000);
        MockEntity mock2 = new MockEntity(IndexEntity.TABLE_INDEX_START_ID + IndexEntity.INDEX_ID_STEP + 1,
                ImmutableList.of(1, 3), 2000, 2000);
        assertSortedResult(IndexEntity.TABLE_INDEX_START_ID + 1, QueryLayoutChooser.segmentRangeComparator(), mock1,
                mock2);
    }

    @Test
    void testSegmentEffectivenessComparator() {
        MockEntity mock1 = new MockEntity(IndexEntity.TABLE_INDEX_START_ID + 1, ImmutableList.of(1, 2), 1000, 3000);
        MockEntity mock2 = new MockEntity(IndexEntity.TABLE_INDEX_START_ID + IndexEntity.INDEX_ID_STEP + 1,
                ImmutableList.of(1, 3), 1000, 2000);
        assertSortedResult(IndexEntity.TABLE_INDEX_START_ID + 1, QueryLayoutChooser.segmentEffectivenessComparator(),
                mock1, mock2);
    }

    @Test
    void testRowSizeComparator() {
        MockEntity mock1 = new MockEntity(1L, ImmutableList.of(1, 2), 90);
        MockEntity mock2 = new MockEntity(IndexEntity.INDEX_ID_STEP + 1L, ImmutableList.of(1, 4), 30);
        MockEntity mock3 = new MockEntity(2 * IndexEntity.INDEX_ID_STEP + 1L, ImmutableList.of(1, 5), 10);
        assertSortedResult(2 * IndexEntity.INDEX_ID_STEP + 1L, QueryLayoutChooser.rowSizeComparator(), mock1, mock2,
                mock3);
    }

    @Test
    void testDerivedLayoutComparator() {
        DeriveInfo mockDeriveInfo = Mockito.mock(DeriveInfo.class);
        MockEntity mock1 = new MockEntity(1L, ImmutableList.of(1, 2), ImmutableMap.of(5, mockDeriveInfo));
        MockEntity mock2 = new MockEntity(2 * IndexEntity.INDEX_ID_STEP + 1L, ImmutableList.of(1, 4),
                ImmutableMap.of(3, mockDeriveInfo));
        MockEntity mock3 = new MockEntity(IndexEntity.INDEX_ID_STEP + 1L, ImmutableList.of(1, 3), ImmutableMap.of());

        Comparator<NLayoutCandidate> comparator = QueryLayoutChooser.derivedLayoutComparator();

        // both not empty, choose the first one
        assertSortedResult(1L, comparator, mock1, mock2);

        // choose the layout no need derivedInfo
        assertSortedResult(IndexEntity.INDEX_ID_STEP + 1, comparator, mock1, mock3);

        // turn on table exclusion, but not prefer snapshot
        MetadataTestUtils.updateProjectConfig("default", "kylin.metadata.table-exclusion-enabled", "true");
        MetadataTestUtils.updateProjectConfig("default", "kylin.query.snapshot-preferred-for-table-exclusion", "false");
        assertSortedResult(IndexEntity.INDEX_ID_STEP + 1, comparator, mock1, mock3);

        // turn on table exclusion and prefer snapshot
        MetadataTestUtils.updateProjectConfig("default", "kylin.metadata.table-exclusion-enabled", "true");
        MetadataTestUtils.updateProjectConfig("default", "kylin.query.snapshot-preferred-for-table-exclusion", "true");
        assertSortedResult(1L, comparator, mock1, mock3);
    }

    @Test
    void testShardByComparator() {

        {
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(0, 1, 2), ImmutableList.of(), ImmutableList.of(0));
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(1, 0, 2), ImmutableList.of(), ImmutableList.of(1));

            // all layout candidates have shardBy column
            List<Integer> sortedFilters = Lists.newArrayList(2, 1, 0);
            assertSortedResult(2L, QueryLayoutChooser.shardByComparator(sortedFilters), mock1, mock2);

            sortedFilters = Lists.newArrayList(2, 0, 1);
            assertSortedResult(1L, QueryLayoutChooser.shardByComparator(sortedFilters), mock1, mock2);
        }

        {
            // the layout(id=2) has shardBy column(colId=1)
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(0, 1, 2), ImmutableList.of(), ImmutableList.of());
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(1, 0, 2), ImmutableList.of(), ImmutableList.of(1));

            List<Integer> sortedFilters = Lists.newArrayList(2, 1, 0);
            assertSortedResult(2L, QueryLayoutChooser.shardByComparator(sortedFilters), mock1, mock2);
        }

        {
            // the layout(id=1) has shardBy column(colId=0)
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(0, 1, 2), ImmutableList.of(), ImmutableList.of(0));
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(1, 0, 2), ImmutableList.of(), ImmutableList.of());

            List<Integer> sortedFilters = Lists.newArrayList(2, 1, 0);
            assertSortedResult(1L, QueryLayoutChooser.shardByComparator(sortedFilters), mock1, mock2);
        }
    }

    @Test
    void testFilterColumnComparator() {
        {
            // both with shardBy, subject to the shardBy order
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(1, 2, 3), ImmutableList.of(), ImmutableList.of(1));
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(2, 1, 3), ImmutableList.of(), ImmutableList.of(2));
            List<Integer> sortedFilters = Lists.newArrayList(1, 2, 3);
            assertSortedResult(1L, QueryLayoutChooser.filterColumnComparator(sortedFilters), mock1, mock2);

            sortedFilters = Lists.newArrayList(3, 2, 1);
            assertSortedResult(2L, QueryLayoutChooser.filterColumnComparator(sortedFilters), mock1, mock2);
        }

        {
            // one with shardBy
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(2, 1, 3), ImmutableList.of(), ImmutableList.of());
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(1, 2, 3), ImmutableList.of(), ImmutableList.of(1));
            List<Integer> sortedFilters = Lists.newArrayList(1, 2, 3);
            assertSortedResult(2L, QueryLayoutChooser.filterColumnComparator(sortedFilters), mock1, mock2);
        }

        {
            // one with shardBy
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(2, 1, 3), ImmutableList.of(), ImmutableList.of(2));
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(1, 2, 3), ImmutableList.of(), ImmutableList.of());
            List<Integer> sortedFilters = Lists.newArrayList(1, 2, 3);
            assertSortedResult(1L, QueryLayoutChooser.filterColumnComparator(sortedFilters), mock1, mock2);
        }

        {
            // both without shardBy
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(2, 1, 3), ImmutableMap.of());
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(1, 2, 3), ImmutableMap.of());
            List<Integer> sortedFilters = Lists.newArrayList(1, 2, 3);
            assertSortedResult(2L, QueryLayoutChooser.filterColumnComparator(sortedFilters), mock1, mock2);
        }
    }

    @Test
    void testNonFilterColumnComparator() {
        {
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(2, 1, 3), ImmutableMap.of());
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(1, 2, 3), ImmutableMap.of());
            List<Integer> sortedFilters = Lists.newArrayList(1, 2, 3);
            assertSortedResult(2L, QueryLayoutChooser.nonFilterColumnComparator(sortedFilters), mock1, mock2);
        }

        {
            MockEntity mock1 = new MockEntity(1L, ImmutableList.of(2, 1, 3), ImmutableMap.of());
            MockEntity mock2 = new MockEntity(2L, ImmutableList.of(1, 2, 3), ImmutableMap.of());
            List<Integer> sortedFilters = Lists.newArrayList(2, 1, 3);
            assertSortedResult(1L, QueryLayoutChooser.nonFilterColumnComparator(sortedFilters), mock1, mock2);
        }
    }

    @Test
    void testMeasureSizeComparator() {
        MockEntity mock1 = new MockEntity(1L, ImmutableList.of(0), ImmutableList.of(100_000, 100_001));
        MockEntity mock2 = new MockEntity(10_001, ImmutableList.of(0), ImmutableList.of(100_000));
        List<NLayoutCandidate> layoutCandidates = mockLayouts(mock1, mock2);
        layoutCandidates.sort(QueryLayoutChooser.measureSizeComparator());
        Assertions.assertEquals(10_001L, layoutCandidates.get(0).getLayoutEntity().getId());
    }

    @Test
    void testDimensionSizeComparator() {
        MockEntity mock1 = new MockEntity(1L, ImmutableList.of(0, 1, 2), ImmutableList.of());
        MockEntity mock2 = new MockEntity(10_001L, ImmutableList.of(0, 1, 2, 3), ImmutableList.of());
        List<NLayoutCandidate> layoutCandidates = mockLayouts(mock1, mock2);
        layoutCandidates.sort(QueryLayoutChooser.dimensionSizeComparator());
        Assertions.assertEquals(1L, layoutCandidates.get(0).getLayoutEntity().getId());
    }

    private void assertSortedResult(long expectedId, Comparator<NLayoutCandidate> comparator,
            MockEntity... mockEntities) {
        List<NLayoutCandidate> layoutCandidates = mockLayouts(mockEntities);
        layoutCandidates.sort(comparator);
        Assertions.assertEquals(expectedId, layoutCandidates.get(0).getLayoutEntity().getId());
    }

    static class MockEntity {
        long id;
        List<Integer> dimensions;
        List<Integer> measures;
        List<Integer> shardByCols;
        Map<Integer, DeriveInfo> deriveInfoMap;
        long segRange;
        long maxSegEnd;
        double rowCost;

        MockEntity(long id, List<Integer> dimensions, Map<Integer, DeriveInfo> deriveInfoMap) {
            this(id, dimensions, Lists.newArrayList(100_000), Lists.newArrayList());
            this.deriveInfoMap = deriveInfoMap;
        }

        MockEntity(long id, List<Integer> dimensions, double rowCost) {
            this(id, dimensions, Lists.newArrayList(100_000), Lists.newArrayList());
            this.rowCost = rowCost;
        }

        MockEntity(long id, List<Integer> dimensions, long segRange, long maxSegEnd) {
            this(id, dimensions, Lists.newArrayList(100_000), Lists.newArrayList());
            this.segRange = segRange;
            this.maxSegEnd = maxSegEnd;
        }

        MockEntity(long id, List<Integer> dimensions, List<Integer> measures) {
            this(id, dimensions, measures, Lists.newArrayList());
        }

        MockEntity(long id, List<Integer> dimensions, List<Integer> measures, List<Integer> shardByCols) {
            this.shardByCols = shardByCols;
            this.id = id;
            this.dimensions = dimensions;
            this.measures = id < IndexEntity.TABLE_INDEX_START_ID ? measures : Lists.newArrayList();
        }
    }

    private List<NLayoutCandidate> mockLayouts(MockEntity... entities) {
        String project = "default";
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String modelId = "82fa7671-a935-45f5-8779-85703601f49a";
        NDataModelManager modelMgr = NDataModelManager.getInstance(kylinConfig, project);
        modelMgr.updateDataModel(modelId, copyForWrite -> {
            List<NDataModel.NamedColumn> allNamedColumns = copyForWrite.getAllNamedColumns();
            for (NDataModel.NamedColumn column : allNamedColumns) {
                if (column.isExist()) {
                    column.setStatus(NDataModel.ColumnStatus.DIMENSION);
                }
            }
        });
        Map<Long, MockEntity> idToMockEntityMap = Maps.newHashMap();
        NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(kylinConfig, project);
        indexMgr.updateIndexPlan(modelId, copyForWrite -> {
            // clear ruleBasedIndex and auto indexes
            copyForWrite.setRuleBasedIndex(new RuleBasedIndex());
            copyForWrite.getIndexes().clear();
            Map<Long, IndexEntity> indexMap = Maps.newHashMap();
            for (MockEntity entity : entities) {
                idToMockEntityMap.put(entity.id, entity);
                long indexId = entity.id - entity.id % IndexEntity.INDEX_ID_STEP;
                IndexEntity index = indexMap.get(indexId);
                if (index == null) {
                    index = new IndexEntity();
                    index.setId(indexId);
                    index.setDimensions(entity.dimensions);
                    index.setMeasures(entity.measures);
                }
                // add layout
                LayoutEntity layout = new LayoutEntity();
                List<Integer> colOrder = Lists.newArrayList();
                colOrder.addAll(entity.dimensions);
                colOrder.addAll(entity.measures);
                layout.setIndex(index);
                layout.setAuto(true);
                layout.setColOrder(colOrder);
                layout.setShardByColumns(entity.shardByCols);
                layout.setId(entity.id);
                index.addLayout(layout);
                indexMap.put(index.getId(), index);
            }
            copyForWrite.updateNextId();
            copyForWrite.getIndexes().addAll(indexMap.values());
        });
        List<LayoutEntity> allLayouts = indexMgr.getIndexPlan(modelId).getAllLayouts();
        return allLayouts.stream().map(layout -> {
            NLayoutCandidate layoutCandidate = new NLayoutCandidate(layout, 0, new CapabilityResult());
            MockEntity mockEntity = idToMockEntityMap.get(layout.getId());
            layoutCandidate.setRange(mockEntity.segRange);
            layoutCandidate.setMaxSegEnd(mockEntity.maxSegEnd);
            layoutCandidate.setCost(mockEntity.rowCost);
            layoutCandidate.setDerivedToHostMap(mockEntity.deriveInfoMap);
            return layoutCandidate;
        }).collect(Collectors.toList());
    }
}
