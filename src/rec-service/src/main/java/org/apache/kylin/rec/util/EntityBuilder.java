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

package org.apache.kylin.rec.util;

import java.util.Collection;
import java.util.List;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;

import lombok.Setter;

public class EntityBuilder {

    private EntityBuilder() {
    }

    public static class LayoutEntityBuilder {
        private final long layoutId;
        private List<Integer> colOrderIds = Lists.newArrayList();
        private List<Integer> partitionCols = Lists.newArrayList();
        private List<Integer> shardByCols = Lists.newArrayList();
        private final IndexEntity index;
        private boolean isAuto;

        public LayoutEntityBuilder(long layoutId, IndexEntity index) {
            this.layoutId = layoutId;
            this.index = index;
        }

        public LayoutEntityBuilder colOrderIds(List<Integer> colOrderIds) {
            this.colOrderIds = colOrderIds;
            return this;
        }

        public LayoutEntityBuilder partitionCol(List<Integer> partitionCols) {
            this.partitionCols = partitionCols;
            return this;
        }

        public LayoutEntityBuilder shardByCol(List<Integer> shardByCols) {
            this.shardByCols = shardByCols;
            return this;
        }

        public LayoutEntityBuilder isAuto(boolean isAuto) {
            this.isAuto = isAuto;
            return this;
        }

        public LayoutEntity build() {
            LayoutEntity layout = new LayoutEntity();
            layout.setColOrder(colOrderIds);
            layout.setId(layoutId);
            layout.setIndex(index);
            layout.setAuto(isAuto);
            layout.setUpdateTime(System.currentTimeMillis());
            layout.setPartitionByColumns(partitionCols);
            layout.setShardByColumns(shardByCols);
            return layout;
        }

    }

    public static void checkDimensionsAndMeasures(List<Integer> dimIds, List<Integer> measureIds) {
        Preconditions.checkState(!dimIds.isEmpty() || !measureIds.isEmpty(),
                "Neither dimension nor measure could be proposed for indexEntity");
    }

    public static class IndexEntityBuilder {

        public static long findAvailableIndexEntityId(IndexPlan indexPlan, Collection<IndexEntity> existedIndex,
                boolean isTableIndex) {
            long result = isTableIndex ? IndexEntity.TABLE_INDEX_START_ID : 0;
            List<Long> cuboidIds = Lists.newArrayList();
            for (IndexEntity indexEntity : existedIndex) {
                long indexEntityId = indexEntity.getId();
                if ((isTableIndex && IndexEntity.isTableIndex(indexEntityId))
                        || (!isTableIndex && indexEntityId < IndexEntity.TABLE_INDEX_START_ID)) {
                    cuboidIds.add(indexEntityId);
                }
            }

            if (!cuboidIds.isEmpty()) {
                // use the largest cuboid id + step
                cuboidIds.sort(Long::compareTo);
                result = cuboidIds.get(cuboidIds.size() - 1) + IndexEntity.INDEX_ID_STEP;
            }
            return Math.max(result,
                    isTableIndex ? indexPlan.getNextTableIndexId() : indexPlan.getNextAggregationIndexId());
        }

        private final long id;
        private final IndexPlan indexPlan;
        @Setter
        private List<Integer> dimIds = Lists.newArrayList();
        @Setter
        private List<Integer> measureIds = Lists.newArrayList();
        private final List<LayoutEntity> layouts = Lists.newArrayList();

        public IndexEntityBuilder(long id, IndexPlan indexPlan) {
            this.id = id;
            this.indexPlan = indexPlan;
        }

        public IndexEntityBuilder dimIds(List<Integer> dimIds) {
            this.dimIds = dimIds;
            return this;
        }

        public IndexEntityBuilder measure(List<Integer> measureIds) {
            this.measureIds = measureIds;
            return this;
        }

        public IndexEntityBuilder addLayout(LayoutEntity layout) {
            this.layouts.add(layout);
            return this;
        }

        public IndexEntityBuilder addLayouts(List<LayoutEntity> layouts) {
            this.layouts.addAll(layouts);
            return this;
        }

        public IndexEntityBuilder setLayout(List<LayoutEntity> layouts) {
            this.layouts.clear();
            this.layouts.addAll(layouts);
            return this;
        }

        public IndexEntity build() {
            EntityBuilder.checkDimensionsAndMeasures(dimIds, measureIds);
            IndexEntity indexEntity = new IndexEntity();
            indexEntity.setId(id);
            indexEntity.setDimensions(dimIds);
            indexEntity.setMeasures(Lists.newArrayList(measureIds));
            indexEntity.setIndexPlan(indexPlan);
            indexEntity.setLayouts(layouts);
            return indexEntity;
        }
    }

}
