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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;

import com.google.common.collect.Lists;

public class CuboIdToLayoutUtils {
    /**
     * convert the cuboids to layout entity
     * @param cuboids
     * @param ruleBasedIndex
     * @return
     */
    public static Set<LayoutEntity> convertCuboIdsToLayoutEntity(Map<BigInteger, Long> cuboids,
            RuleBasedIndex ruleBasedIndex) {
        // convert the cuboid to each agg group
        Set<LayoutEntity> result = new HashSet<>();
        List<NAggregationGroup> aggregationGroups = ruleBasedIndex != null ? ruleBasedIndex.getAggregationGroups()
                : Lists.newArrayList();
        for (NAggregationGroup group : aggregationGroups) {
            // dimension order in this agg group
            List<Integer> dimensionOrder = Lists.newArrayList(group.getIncludes());
            // measure order in this agg group
            List<Integer> measuresIds = Lists.newArrayList(group.getMeasures());
            Set<List<Integer>> colOrders = convertCuboIdsToColOrders(cuboids, ruleBasedIndex.countOfIncludeDimension(),
                    measuresIds, ruleBasedIndex.getRowKeyIdToColumnId(), dimensionOrder);
            for (List<Integer> colOrder : colOrders) {
                result.add(createRecommendAggIndexLayout(colOrder));
            }
        }

        // base agg layout for each agg group
        for (NAggregationGroup group : aggregationGroups) {
            List<Integer> colOrders = Lists.newArrayList();
            // all dimension in the agg
            colOrders.addAll(Lists.newArrayList(group.getIncludes()));
            // all measure in this agg
            colOrders.addAll(Lists.newArrayList(group.getMeasures()));
            result.add(createRecommendAggIndexLayout(colOrders));
        }
        return result;
    }

    /**
     * create recommend agg layout base on the colOrder.
     * @param colOrder
     * @return LayoutEntity or null
     *  null: if the measures in the colOrder can't match the measures in this Index Plan
     */
    private static LayoutEntity createRecommendAggIndexLayout(List<Integer> colOrder) {
        LayoutEntity newAddIndexLayout = new LayoutEntity();
        // The layout is not the manual
        newAddIndexLayout.setManual(false);
        newAddIndexLayout.setColOrder(colOrder);
        newAddIndexLayout.setUpdateTime(System.currentTimeMillis());
        newAddIndexLayout.setBase(false);
        newAddIndexLayout.initalId(true);
        return newAddIndexLayout;
    }

    /**
     * convert the cuboids to column order set which contain dimension ids and measure ids
     * @param cuboids
     * @param dimensionCount
     * @param measuresIds
     * @return
     */
    protected static Set<List<Integer>> convertCuboIdsToColOrders(Map<BigInteger, Long> cuboids, int dimensionCount,
            List<Integer> measuresIds, Map<Integer, Integer> rowkeyIdToColumnId, List<Integer> sortOfDims) {
        Set<List<Integer>> result = new HashSet<>();
        for (BigInteger cuboid : cuboids.keySet()) {
            // 1. get the dimension with order
            // convert the cuboid to the order of dimension which is sorted by the order of `sortOfDims`
            List<Integer> colOrder = convertLongToDimensionColOrder(cuboid, dimensionCount, rowkeyIdToColumnId,
                    sortOfDims);
            if (colOrder.isEmpty()) {
                // If the cuboid can't match the `sortOfDims`, and will not get the column order for this layout
                continue;
            }
            // 2. get the measure with order
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
    public static List<Integer> convertLongToDimensionColOrder(BigInteger cuboid, int maxDimensionCount,
            Map<Integer, Integer> rowkeyIdToColumnId, List<Integer> sortOfDims) {
        // If cuboid is 00000000,00000000,00000000,10001001, and the max dimension count is 12
        // It will be converted to [4,8,11]
        List<Integer> colOrder = new ArrayList<>();
        for (int rowkeyId = 0; rowkeyId < maxDimensionCount; rowkeyId++) {
            int rightShift = maxDimensionCount - rowkeyId - 1;
            boolean exist = cuboid.testBit(rightShift);
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
