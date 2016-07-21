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

package org.apache.kylin.cube.upgrade.V1_5_1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.cube.model.v1_4_0.CubeDesc;
import org.apache.kylin.cube.model.v1_4_0.DimensionDesc;
import org.apache.kylin.cube.model.v1_4_0.HBaseMappingDesc;
import org.apache.kylin.cube.model.v1_4_0.RowKeyColDesc;
import org.apache.kylin.cube.model.v1_4_0.RowKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class CubeDescUpgrade_v_1_5_1 {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CubeDescUpgrade_v_1_5_1.class);

    private static final Serializer<CubeDesc> oldCubeDescSerializer = new JsonSerializer<>(CubeDesc.class);

    private ResourceStore store;
    private String resourcePath;

    private List<String[]> oldHierarchies = Lists.newArrayList();
    private List<String> oldMandatories = Lists.newArrayList();
    private String[][] oldAggGroup = null;
    private Set<String> allRowKeyCols = newIgnoreCaseSet(null);

    public CubeDescUpgrade_v_1_5_1(String resourcePath, ResourceStore resourceStore) {
        this.resourcePath = resourcePath;
        this.store = resourceStore;
    }

    public org.apache.kylin.cube.model.CubeDesc upgrade() throws IOException {
        CubeDesc oldModel = loadOldCubeDesc(resourcePath);

        org.apache.kylin.cube.model.CubeDesc newModel = new org.apache.kylin.cube.model.CubeDesc();
        copyUnChangedProperties(oldModel, newModel);
        upgradeDimension(oldModel, newModel);
        upgradeRowKey(oldModel, newModel);
        upgradeHBaseMapping(oldModel, newModel);
        upgradeAggregationGroup(newModel);//must do at last

        return newModel;
    }

    private CubeDesc loadOldCubeDesc(String path) throws IOException {
        CubeDesc ndesc = store.getResource(path, CubeDesc.class, oldCubeDescSerializer);

        if (StringUtils.isBlank(ndesc.getName())) {
            throw new IllegalStateException("CubeDesc name must not be blank");
        }

        return ndesc;
    }

    private Set<String> newIgnoreCaseSet(Set<String> input) {
        Set<String> ret = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (input != null)
            ret.addAll(input);
        return ret;
    }

    private String[] toArray(Set<String> input) {
        return input.toArray(new String[input.size()]);
    }

    private boolean rowKeyColExistsInMultipleAggGroup() {
        if (oldAggGroup == null)
            return false;

        int total = 0;
        Set<String> overall = newIgnoreCaseSet(null);
        for (String[] group : oldAggGroup) {
            Set<String> temp = newIgnoreCaseSet(null);
            for (String entry : group) {

                overall.add(entry);
                temp.add(entry);
            }
            total += temp.size();
        }
        return overall.size() != total;
    }

    private void upgradeAggregationGroup(org.apache.kylin.cube.model.CubeDesc newModel) {

        List<AggregationGroup> aggs = Lists.newArrayList();
        if (oldAggGroup == null || oldAggGroup.length == 0) {
            oldAggGroup = new String[1][];
            oldAggGroup[0] = toArray(allRowKeyCols);
        }

        if (rowKeyColExistsInMultipleAggGroup()) {
            throw new IllegalArgumentException("rowKeyColExistsInMultipleAggGroup!");
        }

        Set<String> visited = newIgnoreCaseSet(null);

        for (String[] group : oldAggGroup) {
            AggregationGroup agg = new AggregationGroup();

            Set<String> remaining = newIgnoreCaseSet(allRowKeyCols);
            remaining.removeAll(visited);

            Set<String> joint = newIgnoreCaseSet(remaining);
            joint.removeAll(oldMandatories);

            Set<String> groupAsSet = newIgnoreCaseSet(null);
            for (String entry : group) {
                groupAsSet.add(entry);
            }
            visited.addAll(groupAsSet);
            joint.removeAll(groupAsSet);

            List<String> mandatories = Lists.newArrayList();
            List<String[]> hierarchies = Lists.newArrayList();

            for (String s : oldMandatories) {
                mandatories.add(s);
            }

            for (String[] h : oldHierarchies) {
                if (groupAsSet.containsAll(Arrays.asList(h))) {
                    hierarchies.add(h);
                }
            }

            agg.setIncludes(toArray(remaining));

            SelectRule selectRule = new SelectRule();
            selectRule.hierarchy_dims = hierarchies.toArray(new String[hierarchies.size()][]);
            if (joint.size() != 0) {
                selectRule.joint_dims = new String[1][];
                selectRule.joint_dims[0] = joint.toArray(new String[joint.size()]);
            } else {
                selectRule.joint_dims = new String[0][];
            }
            selectRule.mandatory_dims = mandatories.toArray(new String[mandatories.size()]);
            agg.setSelectRule(selectRule);

            aggs.add(agg);

        }
        newModel.setAggregationGroups(aggs);
    }

    private void upgradeDimension(CubeDesc oldModel, org.apache.kylin.cube.model.CubeDesc newModel) {
        List<DimensionDesc> oldDimensions = oldModel.getDimensions();
        if (oldDimensions == null) {
            throw new IllegalArgumentException("dimensions is null");
        }
        List<org.apache.kylin.cube.model.DimensionDesc> newDimensions = Lists.newArrayList();

        for (DimensionDesc oldDim : oldDimensions) {
            if (oldDim.isDerived()) {
                org.apache.kylin.cube.model.DimensionDesc newDim = new org.apache.kylin.cube.model.DimensionDesc();

                newDim.setName(oldDim.getName());
                newDim.setTable(oldDim.getTable());
                newDim.setColumn("{FK}");
                newDim.setDerived(oldDim.getDerived());

                newDimensions.add(newDim);
            } else {
                if (oldDim.isHierarchy()) {
                    oldHierarchies.add(oldDim.getColumn());
                }

                for (String columnStr : oldDim.getColumn()) {
                    org.apache.kylin.cube.model.DimensionDesc newDim = new org.apache.kylin.cube.model.DimensionDesc();

                    newDim.setName(oldDim.getName());
                    newDim.setTable(oldDim.getTable());
                    newDim.setColumn(columnStr);
                    newDim.setDerived(null);

                    newDimensions.add(newDim);
                }
            }
        }

        newModel.setDimensions(newDimensions);
    }

    private void upgradeRowKey(CubeDesc oldModel, org.apache.kylin.cube.model.CubeDesc newModel) {
        RowKeyDesc oldRowKey = oldModel.getRowkey();
        if (oldRowKey == null) {
            throw new IllegalArgumentException("RowKeyDesc is null");
        }

        if (oldRowKey.getRowKeyColumns() == null) {
            throw new IllegalArgumentException("RowKeyDesc.getRowKeyColumns is null");
        }

        org.apache.kylin.cube.model.RowKeyDesc newRowKey = new org.apache.kylin.cube.model.RowKeyDesc();
        org.apache.kylin.cube.model.RowKeyColDesc[] cols = new org.apache.kylin.cube.model.RowKeyColDesc[oldRowKey.getRowKeyColumns().length];
        int index = 0;
        for (RowKeyColDesc oldRowKeyCol : oldRowKey.getRowKeyColumns()) {
            org.apache.kylin.cube.model.RowKeyColDesc newRowKeyCol = new org.apache.kylin.cube.model.RowKeyColDesc();

            allRowKeyCols.add(oldRowKeyCol.getColumn());
            if (oldRowKeyCol.isMandatory()) {
                oldMandatories.add(oldRowKeyCol.getColumn());
            }

            newRowKeyCol.setColumn(oldRowKeyCol.getColumn());
            if (oldRowKeyCol.getDictionary() != null && "true".equalsIgnoreCase(oldRowKeyCol.getDictionary())) {
                newRowKeyCol.setEncoding("dict");
            } else if (oldRowKeyCol.getLength() > 0) {
                newRowKeyCol.setEncoding("fixed_length:" + oldRowKeyCol.getLength());
            } else {
                throw new IllegalArgumentException("Unknow encoding: Dictionary " + oldRowKeyCol.getDictionary() + ", length: " + oldRowKeyCol.getLength());
            }
            cols[index++] = newRowKeyCol;
        }
        oldAggGroup = oldRowKey.getAggregationGroups();

        newRowKey.setRowkeyColumns(cols);
        newModel.setRowkey(newRowKey);
    }

    private void upgradeHBaseMapping(CubeDesc oldModel, org.apache.kylin.cube.model.CubeDesc newModel) {
        HBaseMappingDesc hbaseMappingDesc = oldModel.getHBaseMapping();
        try {

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            JsonUtil.writeValueIndent(os, hbaseMappingDesc);
            byte[] blob = os.toByteArray();
            ByteArrayInputStream is = new ByteArrayInputStream(blob);
            org.apache.kylin.cube.model.HBaseMappingDesc newHBaseMappingDesc = JsonUtil.readValue(is, org.apache.kylin.cube.model.HBaseMappingDesc.class);
            newModel.setHbaseMapping(newHBaseMappingDesc);

        } catch (IOException e) {
            throw new RuntimeException("error when copying HBaseMappingDesc");
        }
    }

    private void copyUnChangedProperties(CubeDesc oldModel, org.apache.kylin.cube.model.CubeDesc newModel) {
        newModel.setUuid(oldModel.getUuid());
        newModel.setLastModified(oldModel.getLastModified());

        newModel.setName(oldModel.getName());
        newModel.setModelName(oldModel.getModelName());
        newModel.setDescription(oldModel.getDescription());
        newModel.setNullStrings(oldModel.getNullStrings());
        newModel.setMeasures(oldModel.getMeasures());
        newModel.setNotifyList(oldModel.getNotifyList());
        newModel.setStatusNeedNotify(oldModel.getStatusNeedNotify());

        newModel.setPartitionDateStart(oldModel.getPartitionDateStart());
        newModel.setPartitionDateEnd(oldModel.getPartitionDateEnd());
        newModel.setAutoMergeTimeRanges(oldModel.getAutoMergeTimeRanges());
        newModel.setRetentionRange(oldModel.getRetentionRange());
        newModel.setStorageType(oldModel.getStorageType());
        newModel.setEngineType(oldModel.getEngineType());
    }

}
