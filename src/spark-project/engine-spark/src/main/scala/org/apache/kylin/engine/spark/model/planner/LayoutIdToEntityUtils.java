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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;

import com.google.common.collect.Lists;

public class LayoutIdToEntityUtils {
    /**
     * convert the layouts to column orders
     * @param layoutMap
     * @param ruleBasedIndex
     * @return
     */
    public static Set<List<Integer>> convertLayoutIdsToColOrders(Map<BigInteger, Long> layoutMap,
            RuleBasedIndex ruleBasedIndex) {
        // convert the layout to each agg group
        Set<List<Integer>> result = new HashSet<>();
        List<NAggregationGroup> aggregationGroups = ruleBasedIndex != null ? ruleBasedIndex.getAggregationGroups()
                : Lists.newArrayList();
        for (NAggregationGroup group : aggregationGroups) {
            // dimension order in this agg group
            List<Integer> dimensionOrder = Lists.newArrayList(group.getIncludes());
            // measure order in this agg group
            List<Integer> measuresIds = Lists.newArrayList(group.getMeasures());
            Set<List<Integer>> colOrders = convertLayoutIdsToColOrders(layoutMap,
                    ruleBasedIndex.countOfIncludeDimension(), measuresIds, ruleBasedIndex.getRowKeyIdToColumnId(),
                    dimensionOrder);
            result.addAll(colOrders);
        }

        // base agg layout for each agg group
        for (NAggregationGroup group : aggregationGroups) {
            List<Integer> colOrders = Lists.newArrayList();
            // all dimension in the agg
            colOrders.addAll(Lists.newArrayList(group.getIncludes()));
            // all measure in this agg
            colOrders.addAll(Lists.newArrayList(group.getMeasures()));
            result.add(colOrders);
        }
        return result;
    }

    /**
     * convert the layouts to column order set which contain dimension ids and measure ids
     * @param layoutsMap
     * @param dimensionCount
     * @param measuresIds
     * @return
     */
    protected static Set<List<Integer>> convertLayoutIdsToColOrders(Map<BigInteger, Long> layoutsMap,
            int dimensionCount, List<Integer> measuresIds, Map<Integer, Integer> rowkeyIdToColumnId,
            List<Integer> sortOfDims) {
        Set<List<Integer>> result = new HashSet<>();
        for (BigInteger layout : layoutsMap.keySet()) {
            // 1. get the dimension with order
            // convert the layout to the order of dimension which is sorted by the order of `sortOfDims`
            List<Integer> colOrder = converLayoutToDimensionColOrder(layout, dimensionCount, rowkeyIdToColumnId,
                    sortOfDims);
            if (colOrder.isEmpty()) {
                // If the layout can't match the `sortOfDims`, and will not get the column order for this layout
                continue;
            }
            // 2. get the measure with order
            // In the current cube planner, each layout should contain all the measures in the model
            colOrder.addAll(measuresIds);
            result.add(colOrder);
        }
        return result;
    }

    /**
     * convert the layout to the column order which just contains the dimension ids
     *
     * @param layout
     * @param maxDimensionCount
     * @return
     */
    public static List<Integer> converLayoutToDimensionColOrder(BigInteger layout, int maxDimensionCount,
            Map<Integer, Integer> rowkeyIdToColumnId, List<Integer> sortOfDims) {
        // If layout is 00000000,00000000,00000000,10001001, and the max dimension count is 12
        // It will be converted to [4,8,11]
        List<Integer> colOrder = new ArrayList<>();
        for (int rowkeyId = 0; rowkeyId < maxDimensionCount; rowkeyId++) {
            int rightShift = maxDimensionCount - rowkeyId - 1;
            boolean exist = layout.testBit(rightShift);
            if (exist) {
                if (!rowkeyIdToColumnId.containsKey(rowkeyId)) {
                    throw new RuntimeException("Can't find the column id from the rowkey id");
                }
                int columnId = rowkeyIdToColumnId.get(rowkeyId);
                if (!sortOfDims.contains(columnId)) {
                    // can't find the dimension in the sorted dimension
                    // this layout is not recommended from the rule base index
                    // just skip and ignore it
                    colOrder.clear();
                    break;
                }
                colOrder.add(columnId);
            }
        }
        if (!colOrder.isEmpty()) {
            // sort the dimension by the sortOfDims
            colOrder.sort((o1, o2) -> {
                int index1 = sortOfDims.indexOf(o1);
                int index2 = sortOfDims.indexOf(o2);
                return index1 - index2;
            });
        }
        return colOrder;
    }
}
