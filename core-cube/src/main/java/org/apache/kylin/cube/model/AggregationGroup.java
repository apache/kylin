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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class AggregationGroup {
    public static class HierarchyMask {
        public long fullMask;// 00000111
        public long[] allMasks;// 00000100,00000110,00000111
        public long[] dims;// 00000100,00000010,00000001
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

    public void init(CubeDesc cubeDesc, RowKeyDesc rowKeyDesc) {
        this.cubeDesc = cubeDesc;
        this.isMandatoryOnlyValid = cubeDesc.getConfig().getCubeAggrGroupIsMandatoryOnlyValid();
        Map<String, TblColRef> colNameAbbr = cubeDesc.buildColumnNameAbbreviation();

        if (this.includes == null || this.includes.length == 0 || this.selectRule == null) {
            throw new IllegalStateException("AggregationGroup incomplete");
        }

        buildPartialCubeFullMask(colNameAbbr, rowKeyDesc);
        buildMandatoryColumnMask(colNameAbbr, rowKeyDesc);
        buildHierarchyMasks(colNameAbbr, rowKeyDesc);
        buildJointColumnMask(colNameAbbr, rowKeyDesc);
        buildJointDimsMask();
        buildNormalDimsMask();
        buildHierarchyDimsMask();

    }

    private void buildPartialCubeFullMask(Map<String, TblColRef> colNameAbbr, RowKeyDesc rowKeyDesc) {
        Preconditions.checkState(this.includes != null);
        Preconditions.checkState(this.includes.length != 0);

        partialCubeFullMask = 0L;
        for (String dim : this.includes) {
            TblColRef hColumn = colNameAbbr.get(dim);
            Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
            long bit = 1L << index;
            partialCubeFullMask |= bit;
        }
    }

    private void buildJointColumnMask(Map<String, TblColRef> colNameAbbr, RowKeyDesc rowKeyDesc) {
        joints = Lists.newArrayList();

        if (this.selectRule.joint_dims == null || this.selectRule.joint_dims.length == 0) {
            return;
        }

        for (String[] joint_dims : this.selectRule.joint_dims) {
            if (joint_dims == null || joint_dims.length == 0) {
                continue;
            }

            long joint = 0L;
            for (int i = 0; i < joint_dims.length; i++) {
                TblColRef hColumn = colNameAbbr.get(joint_dims[i]);
                Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
                long bit = 1L << index;
                joint |= bit;
            }

            Preconditions.checkState(joint != 0);
            joints.add(joint);
        }
    }

    private void buildMandatoryColumnMask(Map<String, TblColRef> colNameAbbr, RowKeyDesc rowKeyDesc) {
        mandatoryColumnMask = 0L;

        String[] mandatory_dims = this.selectRule.mandatory_dims;
        if (mandatory_dims == null || mandatory_dims.length == 0) {
            return;
        }

        for (String dim : mandatory_dims) {
            TblColRef hColumn = colNameAbbr.get(dim);
            Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
            mandatoryColumnMask |= 1 << index;
        }

    }

    private void buildHierarchyMasks(Map<String, TblColRef> colNameAbbr, RowKeyDesc rowKeyDesc) {
        this.hierarchyMasks = new ArrayList<HierarchyMask>();

        if (this.selectRule.hierarchy_dims == null || this.selectRule.hierarchy_dims.length == 0) {
            return;
        }

        for (String[] hierarchy_dims : this.selectRule.hierarchy_dims) {
            HierarchyMask mask = new HierarchyMask();
            if (hierarchy_dims == null || hierarchy_dims.length == 0) {
                continue;
            }

            ArrayList<Long> allMaskList = new ArrayList<Long>();
            ArrayList<Long> dimList = new ArrayList<Long>();
            for (int i = 0; i < hierarchy_dims.length; i++) {
                TblColRef hColumn = colNameAbbr.get(hierarchy_dims[i]);
                Integer index = rowKeyDesc.getColumnBitIndex(hColumn);
                long bit = 1L << index;

                //                if ((tailMask & bit) > 0)
                //                    continue; // ignore levels in tail, they don't participate
                //                // aggregation group combination anyway

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
}
