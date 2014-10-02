/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.cube.cuboid;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.RowKeyDesc.AggrGroupMask;
import com.kylinolap.metadata.model.cube.RowKeyDesc.HierarchyMask;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;

/**
 * @author yangli9
 */
public class CuboidCLI {

    public static void main(String[] args) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());

        if ("test".equals(args[0])) {
            CubeDesc cubeDesc = metaMgr.getCubeDesc(args[1]);
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
                    throw new IllegalStateException("Find duplicate spanning cuboid " + sc + " from cuboid "
                            + cuboid);
                }
                cuboidQueue.push(sc);
            }
        }

        TreeSet<Long> enumCuboids = enumCalcCuboidCount(cube);
        if (enumCuboids.equals(cuboidSet) == false) {
            throw new IllegalStateException("Expected cuboid set " + enumCuboids + "; but actual cuboid set "
                    + cuboidSet);
        }

        int mathCount = mathCalcCuboidCount(cube);
        if (mathCount != enumCuboids.size()) {
            throw new IllegalStateException("Math cuboid count " + mathCount + ", but actual cuboid count "
                    + enumCuboids.size());
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
        int result = 0;
        int lastAggrGroupCount = 0;
        for (AggrGroupMask aggrGroupMask : cube.getRowkey().getAggrGroupMasks()) {
            lastAggrGroupCount = mathCalcCuboidCount_aggrGroup(cube, aggrGroupMask);
            result += lastAggrGroupCount;
        }
        result *= 2; // for zero-tail
        if (cube.getRowkey().getTailMask() == 0) { // last group may not have zero-tail cuboid
            result -= lastAggrGroupCount;
        }
        result--; // minus the all-zero cuboid
        return result;
    }

    private static int mathCalcCuboidCount_aggrGroup(CubeDesc cube, AggrGroupMask aggrGroupMask) {
        long groupMask = aggrGroupMask.groupMask;
        long nonUniqueMask = groupMask & (~aggrGroupMask.uniqueMask);
        return mathCalcCuboidCount_combination(cube, groupMask)
                - mathCalcCuboidCount_combination(cube, nonUniqueMask);
    }

    private static int mathCalcCuboidCount_combination(CubeDesc cube, long colMask) {
        if (colMask == 0) // no column selected
            return 0;

        int count = 1;

        for (HierarchyMask hierMask : cube.getRowkey().getHierarchyMasks()) {
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
