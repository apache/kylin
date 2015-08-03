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

package org.apache.kylin.cube.cuboid;

/** 
 * @author George Song (ysong1)
 * 
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.RowKeyDesc.AggrGroupMask;

public class CuboidScheduler {

    private final CubeDesc cubeDef;
    private final int size;
    private final long max;
    private final Map<Long, Collection<Long>> cache;

    public CuboidScheduler(CubeDesc cube) {
        this.cubeDef = cube;
        this.size = cube.getRowkey().getRowKeyColumns().length;
        this.max = (long) Math.pow(2, size) - 1;
        this.cache = new ConcurrentHashMap<Long, Collection<Long>>();
    }

    public Collection<Long> getSpanningCuboid(long cuboid) {
        if (cuboid > max || cuboid < 0) {
            throw new IllegalArgumentException("Cuboid " + cuboid + " is out of scope 0-" + max);
        }

        Collection<Long> result = cache.get(cuboid);
        if (result != null) {
            return result;
        }

        // smaller sibling's children
        Collection<Long> allPrevOffspring = new HashSet<Long>();
        for (Long sibling : findSmallerSibling(cuboid)) {
            Collection<Long> prevOffsprings = generateChildren(sibling);
            allPrevOffspring.addAll(prevOffsprings);
        }

        // my children is my generation excluding smaller sibling's generation
        result = new ArrayList<Long>();
        for (Long offspring : generateChildren(cuboid)) {
            if (!allPrevOffspring.contains(offspring)) {
                result.add(offspring);
            }
        }

        cache.put(cuboid, result);
        return result;
    }

    private Collection<Long> generateChildren(long cuboid) {
        Collection<Long> result = new HashSet<Long>();

        // generate zero tail cuboid -- the one with all 1 in the first
        // aggregation group and all 0 for the rest bits
        generateZeroTailBase(cuboid, result);

        RowKeyDesc rowkey = cubeDef.getRowkey();
        long cuboidWithoutMandatory = cuboid & ~rowkey.getMandatoryColumnMask();
        for (AggrGroupMask mask : rowkey.getAggrGroupMasks()) {
            if (belongTo(cuboidWithoutMandatory, mask) == false)
                continue;

            long[] groupOneBitMasks = mask.groupOneBitMasks;
            for (int i = 0; i < groupOneBitMasks.length; i++) {
                long oneBit = groupOneBitMasks[i];
                if ((cuboid & oneBit) == 0)
                    continue;

                long child = cuboid ^ oneBit;
                if (Cuboid.isValid(cubeDef, child)) {
                    result.add(child);
                }
            }

            if ((cuboidWithoutMandatory & mask.uniqueMask) > 0)
                break;
        }

        return result;
    }

    private void generateZeroTailBase(long cuboid, Collection<Long> result) {
        RowKeyDesc rowkey = cubeDef.getRowkey();

        long cuboidWithoutMandatory = cuboid & ~rowkey.getMandatoryColumnMask();

        for (AggrGroupMask mask : rowkey.getAggrGroupMasks()) {
            if ((cuboidWithoutMandatory & mask.groupMask) == mask.groupMask && (cuboidWithoutMandatory & mask.leftoverMask) == mask.leftoverMask) {
                long zeroTail = rowkey.getMandatoryColumnMask() | mask.groupMask;
                if (zeroTail > 0 && zeroTail != cuboid) {
                    result.add(zeroTail);
                }
            }
            if ((cuboidWithoutMandatory & mask.uniqueMask) > 0)
                break;
        }
    }

    public Collection<Long> findSmallerSibling(long cuboid) {
        if (!Cuboid.isValid(cubeDef, cuboid)) {
            return Collections.emptyList();
        }

        RowKeyDesc rowkey = cubeDef.getRowkey();

        // do combination in all related groups
        long groupAllBitMask = 0;
        for (AggrGroupMask mask : rowkey.getAggrGroupMasks()) {
            if ((mask.groupMask & cuboid) > 0) {
                groupAllBitMask |= mask.groupMask;
            }
        }

        long groupBitValue = cuboid & groupAllBitMask;
        long leftBitValue = cuboid & ~groupAllBitMask;
        long[] groupOneBits = bits(groupAllBitMask);

        Collection<Long> siblings = new HashSet<Long>();
        combination(cuboid, siblings, groupOneBits, 0, leftBitValue, Long.bitCount(groupBitValue));
        return siblings;
    }

    private long[] bits(long groupAllBitMask) {
        int size = Long.bitCount(groupAllBitMask);
        long[] r = new long[size];
        long l = groupAllBitMask;
        int i = 0;
        while (l != 0) {
            long bit = Long.highestOneBit(l);
            r[i++] = bit;
            l ^= bit;
        }
        return r;
    }

    private void combination(long cuboid, Collection<Long> siblings, long[] bitMasks, int offset, long bitValue, int k) {
        if (k == 0) {
            if (Cuboid.isValid(cubeDef, bitValue)) {
                siblings.add(bitValue);
            }
        } else {
            for (int i = offset; i < bitMasks.length; i++) {
                long newBitValue = bitValue | bitMasks[i];
                if (newBitValue < cuboid) {
                    combination(cuboid, siblings, bitMasks, i + 1, newBitValue, k - 1);
                }
            }
        }
    }

    private boolean belongTo(long cuboidWithoutMandatory, AggrGroupMask mask) {
        long groupBits = cuboidWithoutMandatory & mask.groupMask;
        long leftoverBits = cuboidWithoutMandatory & mask.leftoverMask;
        return groupBits > 0 && (leftoverBits == 0 || leftoverBits == mask.leftoverMask);
    }

    public int getCardinality(long cuboid) {
        if (cuboid > max || cuboid < 0) {
            throw new IllegalArgumentException("Cubiod " + cuboid + " is out of scope 0-" + max);
        }

        return Long.bitCount(cuboid);
    }
}
