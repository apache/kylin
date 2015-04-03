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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.RowKeyDesc.AggrGroupMask;
import org.apache.kylin.cube.model.RowKeyDesc.HierarchyMask;
import org.apache.kylin.metadata.model.TblColRef;

/**
 * @author George Song (ysong1)
 */
public class Cuboid implements Comparable<Cuboid> {

    private final static Map<String, Map<Long, Cuboid>> CUBOID_CACHE = new ConcurrentHashMap<String, Map<Long, Cuboid>>();

    public static Cuboid findById(CubeDesc cube, byte[] cuboidID) {
        return findById(cube, Bytes.toLong(cuboidID));
    }

    public static Cuboid findById(CubeDesc cube, long cuboidID) {
        Map<Long, Cuboid> cubeCache = CUBOID_CACHE.get(cube.getName());
        if (cubeCache == null) {
            cubeCache = new ConcurrentHashMap<Long, Cuboid>();
            CUBOID_CACHE.put(cube.getName(), cubeCache);
        }
        Cuboid cuboid = cubeCache.get(cuboidID);
        if (cuboid == null) {
            long validCuboidID = translateToValidCuboid(cube, cuboidID);
            cuboid = new Cuboid(cube, cuboidID, validCuboidID);
            cubeCache.put(cuboidID, cuboid);
        }
        return cuboid;

    }

    public static boolean isValid(CubeDesc cube, long cuboidID) {
        RowKeyDesc rowkey = cube.getRowkey();

        if (cuboidID < 0) {
            throw new IllegalArgumentException("Cuboid " + cuboidID + " should be greater than 0");
        }

        if (checkBaseCuboid(rowkey, cuboidID)) {
            return true;
        }

        if (checkMandatoryColumns(rowkey, cuboidID) == false) {
            return false;
        }

        if (checkAggregationGroup(rowkey, cuboidID) == false) {
            return false;
        }

        if (checkHierarchy(rowkey, cuboidID) == false) {
            return false;
        }

        return true;
    }

    public static long getBaseCuboidId(CubeDesc cube) {
        return cube.getRowkey().getFullMask();
    }

    // Breadth-First-Search
    private static long translateToValidCuboid(CubeDesc cube, long cuboidID) {
        if (Cuboid.isValid(cube, cuboidID)) {
            return cuboidID;
        }

        HashSet<Long> dedupped = new HashSet<Long>();
        Queue<Long> queue = new LinkedList<Long>();
        List<Long> parents = Cuboid.getAllPossibleParents(cube, cuboidID);

        // check each parent
        addToQueue(queue, parents, dedupped);
        while (queue.size() > 0) {
            long parent = pollFromQueue(queue, dedupped);
            if (Cuboid.isValid(cube, parent)) {
                return parent;
            } else {
                addToQueue(queue, Cuboid.getAllPossibleParents(cube, parent), dedupped);
            }
        }
        return -1;
    }

    private static List<Long> getAllPossibleParents(CubeDesc cube, long cuboidID) {
        List<Long> allPossibleParents = new ArrayList<Long>();

        for (int i = 0; i < cube.getRowkey().getRowKeyColumns().length; i++) {
            long mask = 1L << i;
            long parentId = cuboidID | mask;
            if (parentId != cuboidID) {
                allPossibleParents.add(parentId);
            }
        }

        return allPossibleParents;
    }

    private static void addToQueue(Queue<Long> queue, List<Long> parents, HashSet<Long> dedupped) {
        Collections.sort(parents);
        for (Long p : parents) {
            if (!dedupped.contains(p)) {
                dedupped.add(p);
                queue.offer(p);
            }
        }
    }

    private static long pollFromQueue(Queue<Long> queue, HashSet<Long> dedupped) {
        long element = queue.poll();
        dedupped.remove(element);
        return element;
    }

    private static boolean checkBaseCuboid(RowKeyDesc rowkey, long cuboidID) {
        long baseCuboidId = rowkey.getFullMask();
        if (cuboidID > baseCuboidId) {
            throw new IllegalArgumentException("Cubiod " + cuboidID + " is out of scope 0-" + baseCuboidId);
        }
        return baseCuboidId == cuboidID;
    }

    private static boolean checkMandatoryColumns(RowKeyDesc rowkey, long cuboidID) {
        long mandatoryColumnMask = rowkey.getMandatoryColumnMask();

        // note the all-zero cuboid (except for mandatory) is not valid
        if (cuboidID <= mandatoryColumnMask)
            return false;

        return (cuboidID & mandatoryColumnMask) == mandatoryColumnMask;
    }

    private static boolean checkHierarchy(RowKeyDesc rowkey, long cuboidID) {
        List<HierarchyMask> hierarchyMaskList = rowkey.getHierarchyMasks();
        // if no hierarchy defined in metadata
        if (hierarchyMaskList == null || hierarchyMaskList.size() == 0) {
            return true;
        }

        hier: for (HierarchyMask hierarchyMasks : hierarchyMaskList) {
            long result = cuboidID & hierarchyMasks.fullMask;
            if (result > 0) {
                // if match one of the hierarchy constrains, return true;
                for (long mask : hierarchyMasks.allMasks) {
                    if (result == mask) {
                        continue hier;
                    }
                }
                return false;
            }
        }
        return true;
    }

    private static boolean checkAggregationGroup(RowKeyDesc rowkey, long cuboidID) {
        long cuboidWithoutMandatory = cuboidID & ~rowkey.getMandatoryColumnMask();
        long leftover;
        for (AggrGroupMask mask : rowkey.getAggrGroupMasks()) {
            if ((cuboidWithoutMandatory & mask.uniqueMask) != 0) {
                leftover = cuboidWithoutMandatory & ~mask.groupMask;
                return leftover == 0 || leftover == mask.leftoverMask;
            }
        }

        leftover = cuboidWithoutMandatory & rowkey.getTailMask();
        return leftover == 0 || leftover == rowkey.getTailMask();
    }
    
    // ============================================================================

    private CubeDesc cube;
    private final long inputID;
    private final long id;
    private final byte[] idBytes;
    private final boolean requirePostAggregation;
    private List<TblColRef> dimensionColumns;

    // will translate the cuboidID if it is not valid
    private Cuboid(CubeDesc cube, long originalID, long validID) {
        this.cube = cube;
        this.inputID = originalID;
        this.id = validID;
        this.idBytes = Bytes.toBytes(id);
        this.dimensionColumns = translateIdToColumns(this.id);
        this.requirePostAggregation = calcExtraAggregation(this.inputID, this.id) != 0;
    }

    private List<TblColRef> translateIdToColumns(long cuboidID) {
        List<TblColRef> dimesnions = new ArrayList<TblColRef>();
        RowKeyColDesc[] allColumns = cube.getRowkey().getRowKeyColumns();
        for (int i = 0; i < allColumns.length; i++) {
            // NOTE: the order of column in list!!!
            long bitmask = 1L << allColumns[i].getBitIndex();
            if ((cuboidID & bitmask) != 0) {
                TblColRef colRef = allColumns[i].getColRef();
                dimesnions.add(colRef);
            }
        }
        return dimesnions;
    }

    private long calcExtraAggregation(long inputID, long id) {
        long diff = id ^ inputID;
        return eliminateHierarchyAggregation(diff);
    }

    // higher level in hierarchy can be ignored when counting aggregation columns
    private long eliminateHierarchyAggregation(long id) {
        List<HierarchyMask> hierarchyMaskList = cube.getRowkey().getHierarchyMasks();
        if (hierarchyMaskList != null && hierarchyMaskList.size() > 0) {
            for (HierarchyMask hierMask : hierarchyMaskList) {
                long[] allMasks = hierMask.allMasks;
                for (int i = allMasks.length - 1; i > 0; i--) {
                    long bit = allMasks[i] ^ allMasks[i - 1];
                    if ((inputID & bit) != 0) {
                        id &= ~allMasks[i - 1];
                    }
                }
            }
        }
        return id;
    }

    public CubeDesc getCube() {
        return cube;
    }

    public List<TblColRef> getColumns() {
        return dimensionColumns;
    }

    public List<TblColRef> getAggregationColumns() {
        long aggrColsID = eliminateHierarchyAggregation(id);
        return translateIdToColumns(aggrColsID);
    }

    public long getId() {
        return id;
    }

    public byte[] getBytes() {
        return idBytes;
    }

    public long getInputID() {
        return inputID;
    }

    public boolean useAncestor() {
        return inputID != id;
    }

    public boolean requirePostAggregation() {
        return requirePostAggregation;
    }

    @Override
    public String toString() {
        return "Cuboid [id=" + id + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Cuboid other = (Cuboid) obj;
        if (id != other.id)
            return false;
        return true;
    }

    @Override
    public int compareTo(Cuboid o) {
        if (this.id < o.id) {
            return -1;
        } else if (this.id > o.id) {
            return 1;
        } else {
            return 0;
        }
    }

}
