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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.RowKeyDesc.AggrGroupMask;
import org.apache.kylin.cube.model.RowKeyDesc.HierarchyMask;

/**
 * @author yangli9
 * 
 */
public class CuboidCLI {

    public static void main(String[] args) throws IOException {
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(KylinConfig.getInstanceFromEnv());

        if ("test".equals(args[0])) {
            CubeDesc cubeDesc = cubeDescMgr.getCubeDesc(args[1]);
            simulateCuboidGeneration(cubeDesc);
        }
    }

    public static int simulateCuboidGeneration(CubeDesc cube) {
        CuboidScheduler scheduler = new CuboidScheduler(cube);

        long baseCuboid = Cuboid.getBaseCuboidId(cube);
        Collection<Long> cuboidSet = new TreeSet<Long>();
        cuboidSet.add(baseCuboid);
        LinkedList<Long> cuboidQueue = new LinkedList<Long>();
        cuboidQueue.push(baseCuboid);
        while (!cuboidQueue.isEmpty()) {
            long cuboid = cuboidQueue.pop();
            Collection<Long> spnanningCuboids = scheduler.getSpanningCuboid(cuboid);
            for (Long sc : spnanningCuboids) {
                boolean notfound = cuboidSet.add(sc);
                if (!notfound) {
                    throw new IllegalStateException("Find duplicate spanning cuboid " + sc + " from cuboid " + cuboid);
                }
                cuboidQueue.push(sc);
            }
        }
        
        /** disable this due to poor performance when dimension number is big
        TreeSet<Long> enumCuboids = enumCalcCuboidCount(cube);
        if (enumCuboids.equals(cuboidSet) == false) {
            throw new IllegalStateException("Expected cuboid set " + enumCuboids + "; but actual cuboid set " + cuboidSet);
        }
         */

        int mathCount = mathCalcCuboidCount(cube);
        if (mathCount != cuboidSet.size()) {
            throw new IllegalStateException("Math cuboid count " + mathCount + ", but actual cuboid count " + cuboidSet.size());
        }

        return mathCount;

    }

    public static TreeSet<Long> enumCalcCuboidCount(CubeDesc cube) {
        long baseCuboid = Cuboid.getBaseCuboidId(cube);
        TreeSet<Long> expectedCuboids = new TreeSet<Long>();
        for (long cuboid = 0; cuboid <= baseCuboid; cuboid++) {
            if (Cuboid.isValid(cube, cuboid)) {
                expectedCuboids.add(cuboid);
            }
        }
        return expectedCuboids;
    }

    public static int[] calculateAllLevelCount(CubeDesc cube) {
        int levels = cube.getRowkey().getNCuboidBuildLevels();
        int[] allLevelCounts = new int[levels + 1];

        CuboidScheduler scheduler = new CuboidScheduler(cube);
        LinkedList<Long> nextQueue = new LinkedList<Long>();
        LinkedList<Long> currentQueue = new LinkedList<Long>();
        long baseCuboid = Cuboid.getBaseCuboidId(cube);
        currentQueue.push(baseCuboid);

        for (int i = 0; i <= levels; i++) {
            allLevelCounts[i] = currentQueue.size();
            while (!currentQueue.isEmpty()) {
                long cuboid = currentQueue.pop();
                Collection<Long> spnanningCuboids = scheduler.getSpanningCuboid(cuboid);
                nextQueue.addAll(spnanningCuboids);
            }
            currentQueue = nextQueue;
            nextQueue = new LinkedList<Long>();
        }

        return allLevelCounts;
    }

    public static int mathCalcCuboidCount(CubeDesc cube) {
        int result = 1; // 1 for base cuboid

        RowKeyDesc rowkey = cube.getRowkey();
        AggrGroupMask[] aggrGroupMasks = rowkey.getAggrGroupMasks();
        for (int i = 0; i < aggrGroupMasks.length; i++) {
            boolean hasTail = i < aggrGroupMasks.length - 1 || rowkey.getTailMask() > 0;
            result += mathCalcCuboidCount_aggrGroup(rowkey, aggrGroupMasks[i], hasTail);
        }

        return result;
    }

    private static int mathCalcCuboidCount_aggrGroup(RowKeyDesc rowkey, AggrGroupMask aggrGroupMask, boolean hasTail) {
        long groupMask = aggrGroupMask.groupMask;
        int n = mathCalcCuboidCount_combination(rowkey, groupMask);
        n -= 2; // exclude group all 1 and all 0

        long nonUniqueMask = groupMask & (~aggrGroupMask.uniqueMask);
        if (nonUniqueMask > 0) {
            // exclude duplicates caused by non-unique columns
            // FIXME this assumes non-unique masks consolidates in ONE following group which maybe not be true
            n -= mathCalcCuboidCount_combination(rowkey, nonUniqueMask) - 1; // exclude all 0
        }

        if (hasTail) {
            n *= 2; // tail being 1 and 0
            n += 2; // +1 for group all 1 and tail 0; +1 for group all 0 and tail 1
        }

        return n;
    }

    private static int mathCalcCuboidCount_combination(RowKeyDesc rowkey, long colMask) {
        if (colMask == 0) // no column selected
            return 0;

        int count = 1;

        for (HierarchyMask hierMask : rowkey.getHierarchyMasks()) {
            long hierBits = colMask & hierMask.fullMask;
            if (hierBits != 0) {
                count *= Long.bitCount(hierBits) + 1; // +1 is for all-zero case
                colMask &= ~hierBits;
            }
        }

        count *= Math.pow(2, Long.bitCount(colMask));

        return count;
    }
}
