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
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.AggregationGroup.HierarchyMask;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.collect.ComparisonChain;

@SuppressWarnings("serial")
public class Cuboid implements Comparable<Cuboid>, Serializable {

    // smaller is better
    public final static Comparator<Long> cuboidSelectComparator = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return ComparisonChain.start().compare(Long.bitCount(o1), Long.bitCount(o2)).compare(o1, o2).result();
        }
    };

    // for mandatory cuboid, no need to translate cuboid
    public static Cuboid findForMandatory(CubeDesc cube, long cuboidID) {
        return CuboidManager.getInstance(cube.getConfig()).findMandatoryId(cube, cuboidID);
    }
    
    public static Cuboid findCuboid(CuboidScheduler cuboidScheduler, Set<TblColRef> dimensions,
            Collection<FunctionDesc> metrics) {
        long cuboidID = toCuboidId(cuboidScheduler.getCubeDesc(), dimensions, metrics);
        return Cuboid.findById(cuboidScheduler, cuboidID);
    }

    public static Cuboid findById(CuboidScheduler cuboidScheduler, byte[] cuboidID) {
        return findById(cuboidScheduler, Bytes.toLong(cuboidID));
    }

    @Deprecated
    public static Cuboid findById(CubeSegment cubeSegment, long cuboidID) {
        return findById(cubeSegment.getCuboidScheduler(), cuboidID);
    }

    @VisibleForTesting
    static Cuboid findById(CubeDesc cubeDesc, long cuboidID) {
        return findById(cubeDesc.getInitialCuboidScheduler(), cuboidID);
    }

    public static Cuboid findById(CuboidScheduler cuboidScheduler, long cuboidID) {
        KylinConfig config = cuboidScheduler.getCubeDesc().getConfig();
        return CuboidManager.getInstance(config).findById(cuboidScheduler, cuboidID);
    }

    public static void clearCache(CubeInstance cubeInstance) {
        KylinConfig config = cubeInstance.getConfig();
        CuboidManager.getInstance(config).clearCache(cubeInstance);
    }

    public static long toCuboidId(CubeDesc cubeDesc, Set<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
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

    public static long getBaseCuboidId(CubeDesc cube) {
        return cube.getRowkey().getFullMask();
    }

    public static Cuboid getBaseCuboid(CubeDesc cube) {
        return findById(cube.getInitialCuboidScheduler(), getBaseCuboidId(cube));
    }

    // ============================================================================

    private CubeDesc cubeDesc;
    private final long inputID;
    private final long id;
    private final byte[] idBytes;
    private final boolean requirePostAggregation;
    private List<TblColRef> dimensionColumns;

    private volatile CuboidToGridTableMapping cuboidToGridTableMapping = null;

    /** Should be more private. For test only. */
    public Cuboid(CubeDesc cubeDesc, long originalID, long validID) {
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
