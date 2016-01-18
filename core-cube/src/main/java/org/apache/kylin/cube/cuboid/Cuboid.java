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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.HierarchyDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.RowKeyDesc.AggrGroupMask;
import org.apache.kylin.cube.model.RowKeyDesc.HierarchyMask;
import org.apache.kylin.metadata.model.TblColRef;

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

    public static Cuboid getBaseCuboid(CubeDesc cube) {
        return findById(cube, getBaseCuboidId(cube));
    }

    private static long translateToValidCuboid(CubeDesc cubeDesc, long cuboidID) {
        // add mandantory
        RowKeyDesc rowkey = cubeDesc.getRowkey();
        long mandatoryColumnMask = rowkey.getMandatoryColumnMask();
        if (cuboidID < mandatoryColumnMask) {
            cuboidID = cuboidID | mandatoryColumnMask;
        }

        // add hierarchy
        for (DimensionDesc dimension : cubeDesc.getDimensions()) {
            HierarchyDesc[] hierarchies = dimension.getHierarchy();
            boolean found = false;
            long result = 0;
            if (hierarchies != null && hierarchies.length > 0) {
                for (int i = hierarchies.length - 1; i >= 0; i--) {
                    TblColRef hColumn = hierarchies[i].getColumnRef();
                    Integer index = rowkey.getColumnBitIndex(hColumn);
                    long bit = 1L << index;

                    if ((rowkey.getTailMask() & bit) > 0)
                        continue; // ignore levels in tail, they don't participate

                    if ((bit & cuboidID) > 0) {
                        found = true;
                    }

                    if (found == true) {
                        result = result | bit;
                    }
                }
                cuboidID = cuboidID | result;
            }
        }

        // find the left-most aggregation group
        long cuboidWithoutMandatory = cuboidID & ~rowkey.getMandatoryColumnMask();
        long leftover;
        for (AggrGroupMask mask : rowkey.getAggrGroupMasks()) {
            if ((cuboidWithoutMandatory & mask.uniqueMask) > 0) {
                leftover = cuboidWithoutMandatory & ~mask.groupMask;

                if (leftover == 0) {
                    return cuboidID;
                }

                if (leftover != 0) {
                    cuboidID = cuboidID | mask.leftoverMask;
                    return cuboidID;
                }
            }
        }

        // doesn't have column in aggregation groups
        leftover = cuboidWithoutMandatory & rowkey.getTailMask();
        if (leftover == 0) {
            // doesn't have column in tail group
            if (cuboidWithoutMandatory != 0) {
                return cuboidID;
            } else {
                // no column except mandatory, add one column
                cuboidID = cuboidID | Long.lowestOneBit(rowkey.getAggrGroupFullMask());
                return translateToValidCuboid(cubeDesc, cuboidID);
            }
        }

        // has column in tail group
        cuboidID = cuboidID | rowkey.getTailMask();
        return cuboidID;

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

    private volatile CuboidToGridTableMapping cuboidToGridTableMapping = null;

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

    public static void clearCache() {
        CUBOID_CACHE.clear();
    }

    public static void reloadCache(String cubeDescName) {
        CUBOID_CACHE.remove(cubeDescName);
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

    public CuboidToGridTableMapping getCuboidToGridTableMapping() {
        if (cuboidToGridTableMapping == null) {
            cuboidToGridTableMapping = new CuboidToGridTableMapping(this);
        }
        return cuboidToGridTableMapping;
    }
}
