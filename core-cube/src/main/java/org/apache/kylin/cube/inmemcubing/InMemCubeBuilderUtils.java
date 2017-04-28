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

import java.util.BitSet;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;

/**
 */
public final class InMemCubeBuilderUtils {

    public static final Pair<ImmutableBitSet, ImmutableBitSet> getDimensionAndMetricColumnBitSet(final long cuboidId, final int measureCount) {
        int cardinality = Long.bitCount(cuboidId);
        BitSet dimension = new BitSet();
        dimension.set(0, cardinality);
        BitSet metrics = new BitSet();
        metrics.set(cardinality, cardinality + measureCount);
        return Pair.newPair(new ImmutableBitSet(dimension), new ImmutableBitSet(metrics));
    }

    public static final Pair<ImmutableBitSet, ImmutableBitSet> getDimensionAndMetricColumnBitSet(final long baseCuboidId, final long childCuboidId, final int measureCount) {
        final Pair<ImmutableBitSet, ImmutableBitSet> parentDimensionAndMetricColumnBitSet = getDimensionAndMetricColumnBitSet(baseCuboidId, measureCount);
        ImmutableBitSet parentDimensions = parentDimensionAndMetricColumnBitSet.getFirst();
        ImmutableBitSet measureColumns = parentDimensionAndMetricColumnBitSet.getSecond();
        ImmutableBitSet childDimensions = parentDimensions;
        long mask = Long.highestOneBit(baseCuboidId);
        long parentCuboidIdActualLength = (long)Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
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
