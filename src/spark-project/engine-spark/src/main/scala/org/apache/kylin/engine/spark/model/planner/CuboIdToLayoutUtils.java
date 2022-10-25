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

package org.apache.kylin.engine.spark.model.planner;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;

public class CuboIdToLayoutUtils {
    /**
     * convert the cuboids to layout entity
     * @param cuboids
     * @param flatTableDesc
     * @return
     */
    public static Set<LayoutEntity> convertCuboIdsToLayoutEntity(Map<Long, Long> cuboids, SegmentFlatTableDesc flatTableDesc) {
        IndexPlan indexPlan = flatTableDesc.getIndexPlan();
        Set<Integer> measuresIds = indexPlan.getEffectiveMeasures().keySet();
        Set<LayoutEntity> result = new HashSet<>();
        Set<List<Integer>> colOrders = convertCuboIdsToColOrders(cuboids, indexPlan.getEffectiveDimCols().size(), measuresIds);
        for (List<Integer> colOrder : colOrders) {
            result.add(indexPlan.createRecommendAggIndexLayout(colOrder));
        }
        return result;
    }

    /**
     * convert the cuboids to column order set which contain dimension ids and measure ids
     * @param cuboids
     * @param dimensionCount
     * @param measuresIds
     * @return
     */
    protected static Set<List<Integer>> convertCuboIdsToColOrders(Map<Long, Long> cuboids, int dimensionCount, Set<Integer> measuresIds) {
        Set<List<Integer>> result = new HashSet<>();
        // TODO: make sure that maxDimensionCount <= 64
        dimensionCount = Math.max(dimensionCount, 0);
        for (Long cuboid : cuboids.keySet()) {
            List<Integer> colOrder = convertLongToDimensionColOrder(cuboid, dimensionCount);
            if (colOrder.isEmpty()) {
                continue;
            }
            // In the current cube planner, each layout should contains all of the measures in the model
            colOrder.addAll(measuresIds);
            result.add(colOrder);
        }
        return result;
    }

    /**
     * convert the cuboid to the column order which just contains the dimension ids
     * @param cuboid
     * @param maxDimensionCount
     * @return
     */
    protected static List<Integer> convertLongToDimensionColOrder(long cuboid, int maxDimensionCount) {
        // If cuboid is 00000000,00000000,00000000,10001001, and the max dimension count is 12
        // It will be converted to [4,8,11]
        List<Integer> colOrder = new ArrayList<>();
        for (int i = 0; i < maxDimensionCount; i++) {
            int rightShift = maxDimensionCount - i - 1;
            boolean exist = ((cuboid >>> rightShift) & 1) != 0;
            if (exist) {
                colOrder.add(i);
            }
        }
        return colOrder;
    }
}
