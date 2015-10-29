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
package org.apache.kylin.cube.inmemcubing;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 */
public final class InMemCubeBuilderUtils {
    
    public static final HashMap<Integer, Dictionary<String>> createTopNDisplayColDictionaryMap(CubeDesc cubeDesc, CubeJoinedFlatTableDesc intermediateTableDesc, Map<TblColRef, Dictionary<?>> dictionaryMap) {
        HashMap<Integer, Dictionary<String>> result = Maps.newHashMap();
        for (int measureIdx = 0; measureIdx < cubeDesc.getMeasures().size(); measureIdx++) {
            MeasureDesc measureDesc = cubeDesc.getMeasures().get(measureIdx);
            FunctionDesc func = measureDesc.getFunction();
            if (func.isTopN()) {
                int[] flatTableIdx = intermediateTableDesc.getMeasureColumnIndexes()[measureIdx];
                int displayColIdx = flatTableIdx[flatTableIdx.length - 1];
                TblColRef displayCol = func.getParameter().getColRefs().get(flatTableIdx.length - 1);
                @SuppressWarnings("unchecked")
                Dictionary<String> dictionary = (Dictionary<String>) dictionaryMap.get(displayCol);
                result.put(displayColIdx, Preconditions.checkNotNull(dictionary));
            }
        }
        return result;
    }

    public static final Pair<ImmutableBitSet, ImmutableBitSet> getDimensionAndMetricColumnBitSet(final long cuboidId, final int measureCount) {
        BitSet bitSet = BitSet.valueOf(new long[] { cuboidId });
        BitSet dimension = new BitSet();
        dimension.set(0, bitSet.cardinality());
        BitSet metrics = new BitSet();
        metrics.set(bitSet.cardinality(), bitSet.cardinality() + measureCount);
        return Pair.newPair(new ImmutableBitSet(dimension), new ImmutableBitSet(metrics));
    }
    
    public static final Pair<ImmutableBitSet, ImmutableBitSet> getDimensionAndMetricColumnBitSet(final long baseCuboidId, final long childCuboidId, final int measureCount) {
        final Pair<ImmutableBitSet, ImmutableBitSet> parentDimensionAndMetricColumnBitSet = getDimensionAndMetricColumnBitSet(baseCuboidId, measureCount);
        ImmutableBitSet parentDimensions = parentDimensionAndMetricColumnBitSet.getFirst();
        ImmutableBitSet measureColumns = parentDimensionAndMetricColumnBitSet.getSecond();
        ImmutableBitSet childDimensions = parentDimensions;
        long mask = Long.highestOneBit(baseCuboidId);
        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
        int index = 0;
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & baseCuboidId) > 0) {
                if ((mask & childCuboidId) == 0) {
                    // this dim will be aggregated
                    childDimensions = childDimensions.set(index, false);
                }
                index++;
            }
            mask = mask >> 1;
        }
        return Pair.newPair(childDimensions, measureColumns);
    }
}
