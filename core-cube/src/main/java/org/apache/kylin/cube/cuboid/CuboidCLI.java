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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.TreeSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;

/**
 * @author yangli9
 */
public class CuboidCLI {

    public static void main(String[] args) throws IOException {
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(KylinConfig.getInstanceFromEnv());

        if ("test".equals(args[0])) {
            CubeDesc cubeDesc = cubeDescMgr.getCubeDesc(args[1]);
            simulateCuboidGeneration(cubeDesc, true);
        }
    }

    public static int simulateCuboidGeneration(CubeDesc cubeDesc, boolean validate) {
        CuboidScheduler scheduler = cubeDesc.getInitialCuboidScheduler();
        long baseCuboid = Cuboid.getBaseCuboidId(cubeDesc);
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

        boolean enableDimCap = false;
        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            if (agg.getDimCap() > 0) {
                enableDimCap = true;
                break;
            }
        }

        if (validate) {
            if (enableDimCap) {
                if (cubeDesc.getAllCuboids().size() != cuboidSet.size()) {
                    throw new IllegalStateException("Expected cuboid set " + cubeDesc.getAllCuboids() + "; but actual cuboid set " + cuboidSet);
                }
            } else {
                //only run this for test purpose, performance is bad when # of dims is large
                TreeSet<Long> enumCuboids = enumCalcCuboidCount(cubeDesc);
                System.out.println(Arrays.toString(enumCuboids.toArray(new Long[enumCuboids.size()])));
                if (enumCuboids.equals(cuboidSet) == false) {
                    throw new IllegalStateException("Expected cuboid set " + enumCuboids + "; but actual cuboid set " + cuboidSet);
                }

                //check all valid and invalid
                for (long i = 0; i < baseCuboid; ++i) {
                    if (cuboidSet.contains(i)) {
                        if (!scheduler.isValid(i)) {
                            throw new RuntimeException();
                        }

                        if (scheduler.findBestMatchCuboid(i) != i) {
                            throw new RuntimeException();
                        }
                    } else {
                        if (scheduler.isValid(i)) {
                            throw new RuntimeException();
                        }

                        long corrected = scheduler.findBestMatchCuboid(i);
                        if (corrected == i) {
                            throw new RuntimeException();
                        }

                        if (!scheduler.isValid(corrected)) {
                            throw new RuntimeException();
                        }

                        if (scheduler.findBestMatchCuboid(corrected) != corrected) {
                            throw new RuntimeException();
                        }
                    }
                }
            }
        }

        return cuboidSet.size();

    }

    public static TreeSet<Long> enumCalcCuboidCount(CubeDesc cube) {
        long baseCuboid = Cuboid.getBaseCuboidId(cube);
        TreeSet<Long> expectedCuboids = new TreeSet<Long>();
        for (long cuboid = 0; cuboid <= baseCuboid; cuboid++) {
            if (cube.getInitialCuboidScheduler().isValid(cuboid)) {
                expectedCuboids.add(cuboid);
            }
        }
        return expectedCuboids;
    }

    public static int[] calculateAllLevelCount(CubeDesc cube) {
        int levels = cube.getInitialCuboidScheduler().getBuildLevel();
        int[] allLevelCounts = new int[levels + 1];

        CuboidScheduler scheduler = cube.getInitialCuboidScheduler();
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

            if (i == levels) {
                if (!currentQueue.isEmpty()) {
                    throw new IllegalStateException();
                }
            }
        }

        return allLevelCounts;
    }
}
