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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.AggregationGroup.HierarchyMask;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

@SuppressWarnings("serial")
public class Cuboid implements Comparable<Cuboid>, Serializable {

    private final static Map<String, Map<Long, Cuboid>> CUBOID_CACHE = new ConcurrentHashMap<String, Map<Long, Cuboid>>();

    // smaller is better
    public final static Comparator<Long> cuboidSelectComparator = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return ComparisonChain.start().compare(Long.bitCount(o1), Long.bitCount(o2)).compare(o1, o2).result();
        }
    };

    // this is the only entry point for query to find the right cuboid
    public static Cuboid identifyCuboid(CubeDesc cubeDesc, Set<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        long cuboidID = identifyCuboidId(cubeDesc, dimensions, metrics);
        return Cuboid.findById(cubeDesc, cuboidID);
    }

    public static long identifyCuboidId(CubeDesc cubeDesc, Set<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (FunctionDesc metric : metrics) {
            if (metric.getMeasureType().onlyAggrInBaseCuboid())
                return Cuboid.getBaseCuboidId(cubeDesc);
        }

        long cuboidID = 0;
        for (TblColRef column : dimensions) {
            int index = cubeDesc.getRowkey().getColumnBitIndex(column);
            cuboidID |= 1L << index;
        }
        return cuboidID;
    }

    // for full cube, no need to translate cuboid
    public static Cuboid findForFullCube(CubeDesc cube, long cuboidID) {
        return new Cuboid(cube, cuboidID, cuboidID);
    }

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
        return cube.getAllCuboids().contains(cuboidID);
    }

    public static long getBaseCuboidId(CubeDesc cube) {
        return cube.getRowkey().getFullMask();
    }

    public static Cuboid getBaseCuboid(CubeDesc cube) {
        return findById(cube, getBaseCuboidId(cube));
    }

    static long translateToValidCuboid(CubeDesc cubeDesc, long cuboidID) {
        long baseCuboidId = getBaseCuboidId(cubeDesc);
        if (cubeDesc.getAllCuboids().contains(cuboidID)) {
            return cuboidID;
        }
        List<Long> onTreeCandidates = Lists.newArrayList();
        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            Long candidate = translateToOnTreeCuboid(agg, cuboidID);
            if (candidate != null) {
                onTreeCandidates.add(candidate);
            }
        }

        if (onTreeCandidates.size() == 0) {
            return baseCuboidId;
        }

        long onTreeCandi = Collections.min(onTreeCandidates, cuboidSelectComparator);
        if (isValid(cubeDesc, onTreeCandi)) {
            return onTreeCandi;
        }

        return cubeDesc.getCuboidScheduler().findBestMatchCuboid(onTreeCandi);
    }

    private static Long translateToOnTreeCuboid(AggregationGroup agg, long cuboidID) {
        if ((cuboidID & ~agg.getPartialCubeFullMask()) > 0) {
            //the partial cube might not contain all required dims
            return null;
        }

        // add mandantory
        cuboidID = cuboidID | agg.getMandatoryColumnMask();

        // add hierarchy
        for (HierarchyMask hierarchyMask : agg.getHierarchyMasks()) {
            long fullMask = hierarchyMask.fullMask;
            long intersect = cuboidID & fullMask;
            if (intersect != 0 && intersect != fullMask) {

                boolean startToFill = false;
                for (int i = hierarchyMask.dims.length - 1; i >= 0; i--) {
                    if (startToFill) {
                        cuboidID |= hierarchyMask.dims[i];
                    } else {
                        if ((cuboidID & hierarchyMask.dims[i]) != 0) {
                            startToFill = true;
                            cuboidID |= hierarchyMask.dims[i];
                        }
                    }
                }
            }
        }

        // add joint dims
        for (Long joint : agg.getJoints()) {
            if (((cuboidID | joint) != cuboidID) && ((cuboidID & ~joint) != cuboidID)) {
                cuboidID = cuboidID | joint;
            }
        }

        if (!agg.isOnTree(cuboidID)) {
            // no column, add one column
            long nonJointDims = removeBits((agg.getPartialCubeFullMask() ^ agg.getMandatoryColumnMask()), agg.getJoints());
            if (nonJointDims != 0) {
                long nonJointNonHierarchy = removeBits(nonJointDims, Collections2.transform(agg.getHierarchyMasks(), new Function<HierarchyMask, Long>() {
                    @Override
                    public Long apply(HierarchyMask input) {
                        return input.fullMask;
                    }
                }));
                if (nonJointNonHierarchy != 0) {
                    //there exists dim that does not belong to any joint or any hierarchy, that's perfect
                    return cuboidID | Long.lowestOneBit(nonJointNonHierarchy);
                } else {
                    //choose from a hierarchy that does not intersect with any joint dim, only check level 1 
                    long allJointDims = agg.getJointDimsMask();
                    for (HierarchyMask hierarchyMask : agg.getHierarchyMasks()) {
                        long dim = hierarchyMask.allMasks[0];
                        if ((dim & allJointDims) == 0) {
                            return cuboidID | dim;
                        }
                    }
                }
            }

            cuboidID = cuboidID | Collections.min(agg.getJoints(), cuboidSelectComparator);
            Preconditions.checkState(agg.isOnTree(cuboidID));
        }
        return cuboidID;
    }

    private static long removeBits(long original, Collection<Long> toRemove) {
        long ret = original;
        for (Long joint : toRemove) {
            ret = ret & ~joint;
        }
        return ret;
    }

    // ============================================================================

    private CubeDesc cubeDesc;
    private final long inputID;
    private final long id;
    private final byte[] idBytes;
    private final boolean requirePostAggregation;
    private List<TblColRef> dimensionColumns;

    private volatile CuboidToGridTableMapping cuboidToGridTableMapping = null;

    // will translate the cuboidID if it is not valid
    private Cuboid(CubeDesc cubeDesc, long originalID, long validID) {
        this.cubeDesc = cubeDesc;
        this.inputID = originalID;
        this.id = validID;
        this.idBytes = Bytes.toBytes(id);
        this.dimensionColumns = translateIdToColumns(this.id);
        this.requirePostAggregation = calcExtraAggregation(this.inputID, this.id) != 0;
    }

    private List<TblColRef> translateIdToColumns(long cuboidID) {
        List<TblColRef> dimesnions = new ArrayList<TblColRef>();
        RowKeyColDesc[] allColumns = cubeDesc.getRowkey().getRowKeyColumns();
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
        long finalId = id;

        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            long temp = id;
            List<HierarchyMask> hierarchyMaskList = agg.getHierarchyMasks();
            if (hierarchyMaskList != null && hierarchyMaskList.size() > 0) {
                for (HierarchyMask hierMask : hierarchyMaskList) {
                    long[] allMasks = hierMask.allMasks;
                    for (int i = allMasks.length - 1; i > 0; i--) {
                        long bit = allMasks[i] ^ allMasks[i - 1];
                        if ((inputID & bit) != 0) {
                            temp &= ~allMasks[i - 1];
                            if (temp < finalId)
                                finalId = temp;
                        }
                    }
                }
            }
        }
        return finalId;
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
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

    public static String getDisplayName(long cuboidID, int dimensionCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dimensionCount; ++i) {
            if ((cuboidID & (1L << i)) == 0) {
                sb.append('0');
            } else {
                sb.append('1');
            }
        }
        return StringUtils.reverse(sb.toString());
    }
}
