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

package org.apache.kylin.cube.cuboid.algorithm.generic;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.cube.cuboid.algorithm.CuboidStats;

import com.google.common.collect.Lists;

public class CuboidEncoder {
    private List<Long> selectionCuboids;

    public CuboidEncoder(CuboidStats cuboidStats) {
        selectionCuboids = Lists.newArrayList(cuboidStats.getAllCuboidsForSelection());
        Collections.sort(selectionCuboids, Collections.reverseOrder());
    }

    public List<Long> toCuboidList(BitSet bits) {
        List<Long> cuboids = Lists.newArrayListWithCapacity(bits.cardinality());
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
            cuboids.add(selectionCuboids.get(i));
        }
        return cuboids;
    }
}
