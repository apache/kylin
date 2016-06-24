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

package org.apache.kylin.cube.model.v1_4_0;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class RowKeyDesc {

    public static class HierarchyMask {
        public long fullMask;
        public long[] allMasks;
    }

    public static class AggrGroupMask {
        public AggrGroupMask(int size) {
            groupOneBitMasks = new long[size];
        }

        public long groupMask;
        public long[] groupOneBitMasks;
        public long uniqueMask;
        public long leftoverMask;
    }

    @JsonProperty("rowkey_columns")
    private RowKeyColDesc[] rowkeyColumns;
    @JsonProperty("aggregation_groups")
    private String[][] aggregationGroups;

    // computed content
    private CubeDesc cubeDesc;
    private Map<TblColRef, RowKeyColDesc> columnMap;

    private long fullMask;
    private long mandatoryColumnMask;
    private AggrGroupMask[] aggrGroupMasks;
    private long aggrGroupFullMask;
    private long hierarchyFullMask;
    private long tailMask;

    private List<HierarchyMask> hierarchyMasks;

    public RowKeyColDesc[] getRowKeyColumns() {
        return rowkeyColumns;
    }

    // search a specific row key col
    public int getRowKeyIndexByColumnName(String columnName) {
        if (this.rowkeyColumns == null)
            return -1;

        for (int i = 0; i < this.rowkeyColumns.length; ++i) {
            RowKeyColDesc desc = this.rowkeyColumns[i];
            if (desc.getColumn().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public int getNCuboidBuildLevels() {
        // N aggregation columns requires N levels of cuboid build
        // - N columns requires N-1 levels build
        // - zero tail cuboid needs one more additional level
        Set<String> aggDims = new HashSet<String>();
        for (String[] aggrGroup : aggregationGroups) {
            for (String dim : aggrGroup) {
                aggDims.add(dim);
            }
        }
        return aggDims.size();
    }

    public String[][] getAggregationGroups() {
        return aggregationGroups;
    }

    public CubeDesc getCubeRef() {
        return cubeDesc;
    }

    public void setCubeRef(CubeDesc cubeRef) {
        this.cubeDesc = cubeRef;
    }

    public long getFullMask() {
        return fullMask;
    }

    public long getMandatoryColumnMask() {
        return mandatoryColumnMask;
    }

    public long getAggrGroupFullMask() {
        return aggrGroupFullMask;
    }

    public AggrGroupMask[] getAggrGroupMasks() {
        return aggrGroupMasks;
    }

    public List<HierarchyMask> getHierarchyMasks() {
        return hierarchyMasks;
    }

    public long getHierarchyFullMask() {
        return hierarchyFullMask;
    }

    public long getTailMask() {
        return tailMask;
    }

    public int getColumnBitIndex(TblColRef col) {
        return getColDesc(col).getBitIndex();
    }

    public int getColumnLength(TblColRef col) {
        return getColDesc(col).getLength();
    }

    public String getDictionary(TblColRef col) {
        return getColDesc(col).getDictionary();
    }

    private RowKeyColDesc getColDesc(TblColRef col) {
        RowKeyColDesc desc = columnMap.get(col);
        if (desc == null)
            throw new NullPointerException("Column " + col + " does not exist in row key desc");
        return desc;
    }

    public boolean isUseDictionary(int index) {
        String useDictionary = rowkeyColumns[index].getDictionary();
        return useDictionary(useDictionary);
    }

    public boolean isUseDictionary(TblColRef col) {
        String useDictionary = getDictionary(col);
        return useDictionary(useDictionary);
    }

    private boolean useDictionary(String useDictionary) {
        return !StringUtils.isBlank(useDictionary) && !"false".equals(useDictionary);
    }

    public void init(CubeDesc cube) {
        setCubeRef(cube);
        Map<String, TblColRef> colNameAbbr = cube.buildColumnNameAbbreviation();

        buildRowKey(colNameAbbr);
        buildAggregationGroups(colNameAbbr);
        buildHierarchyMasks();
    }

    @Override
    public String toString() {
        return "RowKeyDesc [rowkeyColumns=" + Arrays.toString(rowkeyColumns) + ", aggregationGroups=" + Arrays.toString(aggregationGroups) + "]";
    }

    private void buildRowKey(Map<String, TblColRef> colNameAbbr) {
        columnMap = new HashMap<TblColRef, RowKeyColDesc>();
        mandatoryColumnMask = 0;

        for (int i = 0; i < rowkeyColumns.length; i++) {
            RowKeyColDesc rowKeyColDesc = rowkeyColumns[i];
            String column = rowKeyColDesc.getColumn();
            rowKeyColDesc.setColumn(column.toUpperCase());
            rowKeyColDesc.setBitIndex(rowkeyColumns.length - i - 1);
            rowKeyColDesc.setColRef(colNameAbbr.get(column));
            if (rowKeyColDesc.getColRef() == null) {
                throw new IllegalArgumentException("Cannot find rowkey column " + column + " in cube " + cubeDesc);
            }

            columnMap.put(rowKeyColDesc.getColRef(), rowKeyColDesc);

            if (rowKeyColDesc.isMandatory()) {
                mandatoryColumnMask |= 1L << rowKeyColDesc.getBitIndex();
            }
        }
    }

    private void buildAggregationGroups(Map<String, TblColRef> colNameAbbr) {
        if (aggregationGroups == null) {
            aggregationGroups = new String[0][];
        }

        for (int i = 0; i < aggregationGroups.length; i++) {
            StringUtil.toUpperCaseArray(aggregationGroups[i], this.aggregationGroups[i]);
        }

        for (int i = 0; i < this.rowkeyColumns.length; i++) {
            int index = rowkeyColumns[i].getBitIndex();
            this.fullMask |= 1L << index;
        }

        this.aggrGroupMasks = new AggrGroupMask[aggregationGroups.length];
        for (int i = 0; i < this.aggregationGroups.length; i++) {
            String[] aggGrp = this.aggregationGroups[i];
            AggrGroupMask mask = new AggrGroupMask(aggGrp.length);

            for (int j = 0; j < aggGrp.length; j++) {
                TblColRef aggCol = colNameAbbr.get(aggGrp[j].toUpperCase());
                if (aggCol == null) {
                    throw new IllegalArgumentException("Can't find aggregation column " + aggGrp[j] + " in  cube " + this.cubeDesc.getName());
                }
                Integer index = getColumnBitIndex(aggCol);
                mask.groupMask |= 1L << index;
                mask.groupOneBitMasks[j] = 1L << index;
                this.aggrGroupFullMask |= 1L << index;
            }
            this.aggrGroupMasks[i] = mask;
        }

        this.tailMask = fullMask ^ mandatoryColumnMask ^ aggrGroupFullMask;

        // unique mask = (bits in this group) - (bits in following groups)
        // leftover mask = (tail bits) + (bits in following groups) - (bits in
        // this group)
        for (int i = 0; i < aggrGroupMasks.length; i++) {
            AggrGroupMask mask = aggrGroupMasks[i];

            mask.uniqueMask = mask.groupMask;
            for (int j = i + 1; j < aggrGroupMasks.length; j++) {
                mask.uniqueMask &= ~aggrGroupMasks[j].groupMask;
            }

            mask.leftoverMask = tailMask;
            for (int j = i + 1; j < aggrGroupMasks.length; j++) {
                mask.leftoverMask |= aggrGroupMasks[j].groupMask;
            }
            mask.leftoverMask &= ~mask.groupMask;
        }
    }

    private void buildHierarchyMasks() {
        this.hierarchyMasks = new ArrayList<HierarchyMask>();

        for (DimensionDesc dimension : this.cubeDesc.getDimensions()) {
            HierarchyDesc[] hierarchies = dimension.getHierarchy();
            if (hierarchies == null || hierarchies.length == 0)
                continue;

            HierarchyMask mask = new HierarchyMask();
            ArrayList<Long> allMaskList = new ArrayList<Long>();
            for (int i = 0; i < hierarchies.length; i++) {
                TblColRef hColumn = hierarchies[i].getColumnRef();
                Integer index = getColumnBitIndex(hColumn);
                long bit = 1L << index;

                if ((tailMask & bit) > 0)
                    continue; // ignore levels in tail, they don't participate
                // aggregation group combination anyway

                mask.fullMask |= bit;
                this.hierarchyFullMask |= bit;
                allMaskList.add(mask.fullMask);
            }

            mask.allMasks = new long[allMaskList.size()];
            for (int i = 0; i < allMaskList.size(); i++)
                mask.allMasks[i] = allMaskList.get(i);

            this.hierarchyMasks.add(mask);
        }
    }

}
