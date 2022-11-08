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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;

public class CuboIdToLayoutUtils {
    /**
     * convert the cuboids to layout entity
     * @param cuboids
     * @param indexPlan
     * @return
     */
    public static Set<LayoutEntity> convertCuboIdsToLayoutEntity(Map<Long, Long> cuboids, IndexPlan indexPlan) {
        Set<Integer> measuresIds = indexPlan.getEffectiveMeasures().keySet();
        Set<LayoutEntity> result = new HashSet<>();
        // this is the base order for all the dimensions
        List<Integer> sortOfDims = indexPlan.getRuleBasedIndex().getDimensions();
        Set<List<Integer>> colOrders = convertCuboIdsToColOrders(cuboids, indexPlan.getEffectiveDimCols().size(),
                measuresIds, indexPlan.getRowKeyIdToColumnId(), sortOfDims);
        for (List<Integer> colOrder : colOrders) {
            result.add(indexPlan.createRecommendAggIndexLayout(colOrder));
        }
        return result;
    }

    public static Set<LayoutEntity> tryRemoveExistLayouts(Set<LayoutEntity> input, IndexPlan indexPlan) {
        for (LayoutEntity existLayout : indexPlan.getAllLayouts()) {
            if (input.contains(existLayout)) {
                input.remove(existLayout);
            }
        }
        return input;
    }

    /**
     * convert the cuboids to column order set which contain dimension ids and measure ids
     * @param cuboids
     * @param dimensionCount
     * @param measuresIds
     * @return
     */
    protected static Set<List<Integer>> convertCuboIdsToColOrders(Map<Long, Long> cuboids, int dimensionCount,
            Set<Integer> measuresIds, Map<Integer, Integer> rowkeyIdToColumnId, List<Integer> sortOfDims) {
        Set<List<Integer>> result = new HashSet<>();
        for (Long cuboid : cuboids.keySet()) {
            List<Integer> colOrder = convertLongToDimensionColOrder(cuboid, dimensionCount, rowkeyIdToColumnId,
                    sortOfDims);
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
     *
     * @param cuboid
     * @param maxDimensionCount
     * @return
     */
    public static List<Integer> convertLongToDimensionColOrder(long cuboid, int maxDimensionCount,
            Map<Integer, Integer> rowkeyIdToColumnId, List<Integer> sortOfDims) {
        // If cuboid is 00000000,00000000,00000000,10001001, and the max dimension count is 12
        // It will be converted to [4,8,11]
        List<Integer> colOrder = new ArrayList<>();
        for (int rowkeyId = 0; rowkeyId < maxDimensionCount; rowkeyId++) {
            int rightShift = maxDimensionCount - rowkeyId - 1;
            boolean exist = ((cuboid >> rightShift) & 1L) != 0;
            if (exist) {
                if (!rowkeyIdToColumnId.containsKey(rowkeyId)) {
                    throw new RuntimeException("Can't find the column id from the rowkey id");
                }
                int columnId = rowkeyIdToColumnId.get(rowkeyId);
                if (sortOfDims.indexOf(columnId) < 0) {
                    // can't find the dimension in the sorted dimension
                    // this cuboid is not recommended from the rule base index
                    // just skip and ignore it
                    colOrder.clear();
                    break;
                }
                colOrder.add(columnId);
            }
        }
        if (!colOrder.isEmpty()) {
            // sort the dimension by the sortOfDims
            Collections.sort(colOrder, new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    int index1 = sortOfDims.indexOf(o1);
                    int index2 = sortOfDims.indexOf(o2);
                    return index1 - index2;
                }
            });
        }
        return colOrder;
    }
}
