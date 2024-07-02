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

import org.apache.kylin.measure.hllc.HLLCounter;

public class TupleCountCounter {
    private final BitSet cuboid;
    private final List<Integer> indexList;
    private final HLLCounter hll;

    private long plainCount;

    public TupleCountCounter(BitSet cuboid, List<Integer> indexList, HLLCounter hll) {
        this.cuboid = cuboid;
        this.indexList = indexList;
        this.hll = hll;
    }

    public BitSet getCuboid() {
        return cuboid;
    }

    public List<Integer> indexList() {
        return indexList;
    }

    public void accumulate(long hash) {
        hll.addHashDirectly(hash);
        plainCount++;
    }

    public byte[] countBytes() throws IOException {
        if (indexList == null || indexList.isEmpty() || cuboid.nextSetBit(0) < 0) {
            return HLLCountCounter.plainBytes(plainCount);
        }
        return HLLCountCounter.hllBytes(hll);
    }

}
