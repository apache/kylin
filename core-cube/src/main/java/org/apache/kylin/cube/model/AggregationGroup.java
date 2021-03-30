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

package org.apache.kylin.cube.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Collections2;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.math.LongMath;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class AggregationGroup implements Serializable {
    public static class HierarchyMask implements Serializable {
        public long fullMask; // 00000111
        public long[] allMasks; // 00000100,00000110,00000111
        public long[] dims; // 00000100,00000010,00000001
    }

    @JsonProperty("includes")
    private String[] includes;
    @JsonProperty("select_rule")
    private SelectRule selectRule;

    //computed
    private long partialCubeFullMask;
    private long mandatoryColumnMask;
    private List<HierarchyMask> hierarchyMasks;
    private List<Long> joints;//each long is a group
    private long jointDimsMask;
    private long normalDimsMask;
    private long hierarchyDimsMask;
    private List<Long> normalDims;//each long is a single dim
    private CubeDesc cubeDesc;
    private boolean isMandatoryOnlyValid;
    private HashMap<Long, Long> dim2JointMap;

    public void init(CubeDesc cubeDesc, RowKeyDesc rowKeyDesc) {
        this.cubeDesc = cubeDesc;
        this.isMandatoryOnlyValid = cubeDesc.getConfig().getCubeAggrGroupIsMandatoryOnlyValid();

        if (this.includes == null || this.includes.length == 0 || this.selectRule == null) {
            throw new IllegalStateException("AggregationGroup incomplete");
        }

        normalizeColumnNames();

        buildPartialCubeFullMask(rowKeyDesc);
        buildMandatoryColumnMask(rowKeyDesc);
        buildJointColumnMask(rowKeyDesc);
        buildJointDimsMask();
        buildHierarchyMasks(rowKeyDesc);
        buildHierarchyDimsMask();
        buildNormalDimsMask();
    }

    private void normalizeColumnNames() {
        Preconditions.checkNotNull(includes);
        normalizeColumnNames(includes);

        Preconditions.checkNotNull(selectRule.mandatoryDims);
        normalizeColumnNames(selectRule.mandatoryDims);

        if (selectRule.hierarchyDims == null)
            selectRule.hierarchyDims = new String[0][];
        for (String[] cols : selectRule.hierarchyDims) {
            Preconditions.checkNotNull(cols);
            normalizeColumnNames(cols);
        }

        if (selectRule.jointDims == null)
            selectRule.jointDims = new String[0][];
        for (String[] cols : selectRule.jointDims) {
            Preconditions.checkNotNull(cols);
            normalizeColumnNames(cols);
        }
    }

    private void normalizeColumnNames(String[] names) {
        if (names == null)
            return;

        for (int i = 0; i < names.length; i++) {
            TblColRef col = cubeDesc.getModel().findColumn(names[i]);
            names[i] = col.getIdentity();
        }

        // check no dup
        Set<String> set = new HashSet<>(Arrays.asList(names));
        if (set.size() < names.length)
            throw new IllegalStateException(
                    "Columns in aggrgroup must not contain duplication: " + Arrays.asList(names));
    }

    private void buildPartialCubeFullMask(RowKeyDesc rowKeyDesc) {
        Preconditions.checkState(this.includes != null);
        Preconditions.checkState(this.includes.length != 0);

        partialCubeFullMask = 0L;
        for (String dim : this.includes) {
            TblColRef hColumn = cubeDesc.getModel().findColumn(dim);
            Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
            long bit = 1L << index;
            partialCubeFullMask |= bit;
        }
    }

    private void buildJointColumnMask(RowKeyDesc rowKeyDesc) {
        joints = Lists.newArrayList();
        dim2JointMap = Maps.newHashMap();

        if (this.selectRule.jointDims == null || this.selectRule.jointDims.length == 0) {
            return;
        }

        for (String[] jointDims : this.selectRule.jointDims) {
            if (jointDims == null || jointDims.length == 0) {
                continue;
            }

            long joint = 0L;
            for (int i = 0; i < jointDims.length; i++) {
                TblColRef hColumn = cubeDesc.getModel().findColumn(jointDims[i]);
                Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
                long bit = 1L << index;
                joint |= bit;
            }

            Preconditions.checkState(joint != 0);
            joints.add(joint);
        }

        for (long jt : joints) {
            for (int i = 0; i < 64; i++) {
                if (((1L << i) & jt) != 0) {
                    dim2JointMap.put((1L << i), jt);
                }
            }
        }
    }

    private void buildMandatoryColumnMask(RowKeyDesc rowKeyDesc) {
        mandatoryColumnMask = 0L;

        String[] mandatory_dims = this.selectRule.mandatoryDims;
        if (mandatory_dims == null || mandatory_dims.length == 0) {
            return;
        }

        for (String dim : mandatory_dims) {
            TblColRef hColumn = cubeDesc.getModel().findColumn(dim);
            Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
            mandatoryColumnMask |= (1L << index);
        }
    }

    private void buildHierarchyMasks(RowKeyDesc rowKeyDesc) {
        this.hierarchyMasks = new ArrayList<HierarchyMask>();

        if (this.selectRule.hierarchyDims == null || this.selectRule.hierarchyDims.length == 0) {
            return;
        }

        for (String[] hierarchy_dims : this.selectRule.hierarchyDims) {
            HierarchyMask mask = new HierarchyMask();
            if (hierarchy_dims == null || hierarchy_dims.length == 0) {
                continue;
            }

            ArrayList<Long> allMaskList = new ArrayList<Long>();
            ArrayList<Long> dimList = new ArrayList<Long>();
            for (int i = 0; i < hierarchy_dims.length; i++) {
                TblColRef hColumn = cubeDesc.getModel().findColumn(hierarchy_dims[i]);
                Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
                long bit = 1L << index;

                // combine joint as logic dim
                if (dim2JointMap.get(bit) != null) {
                    bit = dim2JointMap.get(bit);
                }

                mask.fullMask |= bit;
                allMaskList.add(mask.fullMask);
                dimList.add(bit);
            }

            Preconditions.checkState(allMaskList.size() == dimList.size());
            mask.allMasks = new long[allMaskList.size()];
            mask.dims = new long[dimList.size()];
            for (int i = 0; i < allMaskList.size(); i++) {
                mask.allMasks[i] = allMaskList.get(i);
                mask.dims[i] = dimList.get(i);
            }

            this.hierarchyMasks.add(mask);
        }
    }

    private void buildNormalDimsMask() {
        //no joint, no hierarchy, no mandatory
        long leftover = partialCubeFullMask & ~mandatoryColumnMask;
        leftover &= ~this.jointDimsMask;
        for (HierarchyMask hierarchyMask : this.hierarchyMasks) {
            leftover &= ~hierarchyMask.fullMask;
        }

        this.normalDimsMask = leftover;
        this.normalDims = bits(leftover);
    }

    private void buildHierarchyDimsMask() {
        long ret = 0;
        for (HierarchyMask mask : hierarchyMasks) {
            ret |= mask.fullMask;
        }
        this.hierarchyDimsMask = ret;
    }

    private List<Long> bits(long x) {
        List<Long> r = Lists.newArrayList();
        long l = x;
        while (l != 0) {
            long bit = Long.lowestOneBit(l);
            r.add(bit);
            l ^= bit;
        }
        return r;
    }

    public void buildJointDimsMask() {
        long ret = 0;
        for (long x : joints) {
            ret |= x;
        }
        this.jointDimsMask = ret;
    }

    public long getMandatoryColumnMask() {
        return mandatoryColumnMask;
    }

    public List<HierarchyMask> getHierarchyMasks() {
        return hierarchyMasks;
    }

    public int getBuildLevel() {
        int ret = 1;//base cuboid => partial cube root
        if (this.getPartialCubeFullMask() == Cuboid.getBaseCuboidId(cubeDesc)) {
            ret -= 1;//if partial cube's root is base cuboid, then one round less agg
        }

        ret += getNormalDims().size();
        for (HierarchyMask hierarchyMask : this.hierarchyMasks) {
            ret += hierarchyMask.allMasks.length;
        }
        for (Long joint : joints) {
            if ((joint & this.getHierarchyDimsMask()) == 0) {
                ret += 1;
            }
        }

        return ret;
    }

    /** Compute cuboid combination for aggregation group */
    public long calculateCuboidCombination() {
        long combination = 1;

        try {
            if (this.getDimCap() > 0) {
                CuboidScheduler cuboidScheduler = cubeDesc.getInitialCuboidScheduler();
                combination = cuboidScheduler.calculateCuboidsForAggGroup(this).size();
            } else {
                Set<String> includeDims = new TreeSet<>(Arrays.asList(includes));
                Set<String> mandatoryDims = new TreeSet<>(Arrays.asList(selectRule.mandatoryDims));
    
                Set<String> hierarchyDims = new TreeSet<>();
                for (String[] ss : selectRule.hierarchyDims) {
                    hierarchyDims.addAll(Arrays.asList(ss));
                    combination = LongMath.checkedMultiply(combination, (ss.length + 1));
                }
    
                Set<String> jointDims = new TreeSet<>();
                for (String[] ss : selectRule.jointDims) {
                    jointDims.addAll(Arrays.asList(ss));
                }
                combination = LongMath.checkedMultiply(combination, (1L << selectRule.jointDims.length));
    
                Set<String> normalDims = new TreeSet<>();
                normalDims.addAll(includeDims);
                normalDims.removeAll(mandatoryDims);
                normalDims.removeAll(hierarchyDims);
                normalDims.removeAll(jointDims);
    
                combination = LongMath.checkedMultiply(combination, (1L << normalDims.size()));
    
                if (cubeDesc.getConfig().getCubeAggrGroupIsMandatoryOnlyValid() && !mandatoryDims.isEmpty()) {
                    combination += 1;
                }
                combination -= 1; // not include cuboid 0
            }
            
            if (combination < 0)
                throw new ArithmeticException();
            
        } catch (ArithmeticException ae) {
            // long overflow, give max value
            combination = Long.MAX_VALUE;
        }

        if (combination < 0) { // overflow
            combination = Long.MAX_VALUE - 1;
        }
        return combination;
    }

    public Long translateToOnTreeCuboid(long cuboidID) {
        if ((cuboidID & ~getPartialCubeFullMask()) > 0) {
            //the partial cube might not contain all required dims
            return null;
        }

        // add mandantory
        cuboidID = cuboidID | getMandatoryColumnMask();

        // add hierarchy
        for (HierarchyMask hierarchyMask : getHierarchyMasks()) {
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
        for (Long joint : getJoints()) {
            if (((cuboidID | joint) != cuboidID) && ((cuboidID & ~joint) != cuboidID)) {
                cuboidID = cuboidID | joint;
            }
        }

        if (!isOnTree(cuboidID)) {
            // no column, add one column
            long nonJointDims = removeBits((getPartialCubeFullMask() ^ getMandatoryColumnMask()), getJoints());
            if (nonJointDims != 0) {
                long nonJointNonHierarchy = removeBits(nonJointDims,
                        Collections2.transform(getHierarchyMasks(), new Function<HierarchyMask, Long>() {
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
                    long allJointDims = getJointDimsMask();
                    for (HierarchyMask hierarchyMask : getHierarchyMasks()) {
                        long dim = hierarchyMask.allMasks[0];
                        if ((dim & allJointDims) == 0) {
                            return cuboidID | dim;
                        }
                    }
                }
            }
            if (getJoints().size() > 0) {
                cuboidID = cuboidID | Collections.min(getJoints(), Cuboid.cuboidSelectComparator);
            }
            if (!isOnTree(cuboidID)) {
                // kylin.cube.aggrgroup.is-mandatory-only-valid can be false
                return null;
            }
        }
        return cuboidID;
    }

    private long removeBits(long original, Collection<Long> toRemove) {
        long ret = original;
        for (Long joint : toRemove) {
            ret = ret & ~joint;
        }
        return ret;
    }

    public boolean isOnTree(long cuboidID) {
        if (cuboidID <= 0) {
            return false; //cuboid must be greater than 0
        }
        if ((cuboidID & ~partialCubeFullMask) != 0) {
            return false; //a cuboid's parent within agg is at most partialCubeFullMask
        }

        return checkMandatoryColumns(cuboidID) && checkHierarchy(cuboidID) && checkJoint(cuboidID);
    }

    private boolean checkMandatoryColumns(long cuboidID) {
        if ((cuboidID & mandatoryColumnMask) != mandatoryColumnMask) {
            return false;
        } else {
            //base cuboid is always valid
            if (cuboidID == Cuboid.getBaseCuboidId(cubeDesc)) {
                return true;
            }

            //cuboid with only mandatory columns maybe valid
            return isMandatoryOnlyValid || (cuboidID & ~mandatoryColumnMask) != 0;
        }
    }

    private boolean checkJoint(long cuboidID) {
        for (long joint : joints) {
            long common = cuboidID & joint;
            if (!(common == 0 || common == joint)) {
                return false;
            }
        }
        return true;
    }

    private boolean checkHierarchy(long cuboidID) {
        // if no hierarchy defined in metadata
        if (hierarchyMasks == null || hierarchyMasks.size() == 0) {
            return true;
        }

        for (HierarchyMask hierarchy : hierarchyMasks) {
            long result = cuboidID & hierarchy.fullMask;
            if (result > 0) {
                boolean meetHierarcy = false;
                for (long mask : hierarchy.allMasks) {
                    if (result == mask) {
                        meetHierarcy = true;
                        break;
                    }
                }

                if (!meetHierarcy) {
                    return false;
                }
            }
        }
        return true;
    }

    public void setIncludes(String[] includes) {
        this.includes = includes;
    }

    public void setSelectRule(SelectRule selectRule) {
        this.selectRule = selectRule;
    }

    public List<Long> getJoints() {
        return joints;
    }

    public long getJointDimsMask() {
        return jointDimsMask;
    }

    public long getNormalDimsMask() {
        return normalDimsMask;
    }

    public long getHierarchyDimsMask() {
        return hierarchyDimsMask;
    }

    public List<Long> getNormalDims() {
        return normalDims;
    }

    public long getPartialCubeFullMask() {
        return partialCubeFullMask;
    }

    public String[] getIncludes() {
        return includes;
    }

    public SelectRule getSelectRule() {
        return selectRule;
    }

    public boolean isMandatoryOnlyValid() {
        return isMandatoryOnlyValid;
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
    }

    public int getDimCap() {
        return this.selectRule.dimCap == null ? 0 : this.selectRule.dimCap;
    }


}
