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

package org.apache.kylin.engine.spark.planner;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HLLCountEstimator {

    private final Map<BitSet, Long> countMap;

    public HLLCountEstimator(Dataset<Row> flatTable, Map<BitSet, List<Integer>> indexMap) throws IOException {
        // heavy action
        this.countMap = calculate(flatTable, indexMap);
    }

    public Map<BitSet, Long> countMap() {
        return countMap;
    }

    private Map<BitSet, Long> calculate(Dataset<Row> flatTable, Map<BitSet, List<Integer>> indexMap)
            throws IOException {
        // wip: precision should be configurable
        final int precision = HLLCountCounter.DEFAULT_PRECISION;
        // traverse flat table once counting cuboid tuple
        Map<BitSet, byte[]> bytesMap = flatTable.javaRDD()
                // mapper: convert to count
                .mapPartitionsToPair(new HLLCountFlatMapFunc(precision, indexMap))
                // reducer: merge count by cuboid
                .reduceByKey(new HLLCountReduceFunc2(precision))
                // action trigger
                .collectAsMap();

        ImmutableMap.Builder<BitSet, Long> builder = ImmutableMap.builder();
        for (Map.Entry<BitSet, byte[]> entry : bytesMap.entrySet()) {
            builder.put(entry.getKey(), HLLCountCounter.readCount(entry.getValue(), precision));
        }
        return builder.build();
    }

}
