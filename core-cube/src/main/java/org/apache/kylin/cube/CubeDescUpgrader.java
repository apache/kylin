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

package org.apache.kylin.cube;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.HierarchyDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.cube.model.v1.CubeDesc;
import org.apache.kylin.cube.model.v1.CubePartitionDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CubeDescUpgrader {

    private String resourcePath;

    @SuppressWarnings("unused")
    private static final Log logger = LogFactory.getLog(CubeDescUpgrader.class);

    private static final Serializer<CubeDesc> CUBE_DESC_SERIALIZER_V1 = new JsonSerializer<CubeDesc>(CubeDesc.class);

    public CubeDescUpgrader(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public org.apache.kylin.cube.model.CubeDesc upgrade() throws IOException {
        CubeDesc oldModel = loadOldCubeDesc(resourcePath);

        org.apache.kylin.cube.model.CubeDesc newModel = new org.apache.kylin.cube.model.CubeDesc();

        copyUnChangedProperties(oldModel, newModel);

        DataModelDesc model = extractDataModel(oldModel, newModel);
        newModel.setModel(model);

        updateDimensions(oldModel, newModel);

        updateRowkeyDictionary(oldModel, newModel);

        return newModel;
    }

    private void updateRowkeyDictionary(CubeDesc oldModel, org.apache.kylin.cube.model.CubeDesc newModel) {

        RowKeyDesc rowKey = newModel.getRowkey();

        for (RowKeyColDesc rowkeyCol : rowKey.getRowKeyColumns()) {
            if (rowkeyCol.getDictionary() != null && rowkeyCol.getDictionary().length() > 0)
                rowkeyCol.setDictionary("true");
        }

    }

    private void copyUnChangedProperties(CubeDesc oldModel, org.apache.kylin.cube.model.CubeDesc newModel) {

        newModel.setUuid(oldModel.getUuid());
        newModel.setName(oldModel.getName());
        newModel.setDescription(oldModel.getDescription());
        newModel.setNullStrings(oldModel.getNullStrings());

        newModel.setMeasures(oldModel.getMeasures());
        newModel.setRowkey(oldModel.getRowkey());
        newModel.setHbaseMapping(oldModel.getHBaseMapping());

        newModel.setSignature(oldModel.getSignature());

        newModel.setNotifyList(oldModel.getNotifyList());
        newModel.setLastModified(oldModel.getLastModified());
    }

    private DimensionDesc newDimensionDesc(org.apache.kylin.cube.model.v1.DimensionDesc dim, int dimId, String name) {
        DimensionDesc newDim = new DimensionDesc();

        newDim.setId(dimId);
        newDim.setName(name);
        newDim.setTable(getMetadataManager().appendDBName(dim.getTable()));

        return newDim;
    }

    private void updateDimensions(CubeDesc oldModel, org.apache.kylin.cube.model.CubeDesc newModel) {
        List<org.apache.kylin.cube.model.v1.DimensionDesc> oldDimensions = oldModel.getDimensions();

        List<DimensionDesc> newDimensions = Lists.newArrayList();
        newModel.setDimensions(newDimensions);

        int dimId = 0;
        for (org.apache.kylin.cube.model.v1.DimensionDesc dim : oldDimensions) {

            DimensionDesc newDim = null;
            // if a dimension defines "column", "derived" and "hierarchy" at the same time, separate it into three dimensions;

            boolean needNameSuffix = false;
            if (dim.getColumn() != null && !"{FK}".equals(dim.getColumn())) {
                //column on fact table
                newDim = newDimensionDesc(dim, dimId++, dim.getName());
                newDimensions.add(newDim);
                newDim.setColumn(new String[] { dim.getColumn() });
                needNameSuffix = true;
            } else if (ArrayUtils.isEmpty(dim.getDerived()) && ArrayUtils.isEmpty(dim.getHierarchy())) {
                // user defines a lookup table, but didn't use any column other than the pk, in this case, convert to use fact table's fk
                newDim = newDimensionDesc(dim, dimId++, dim.getName());
                newDimensions.add(newDim);
                newDim.setTable(getMetadataManager().appendDBName(newModel.getFactTable()));

                newDim.setColumn(dim.getJoin().getForeignKey());
            }

            if (!ArrayUtils.isEmpty(dim.getDerived())) {
                newDim = newDimensionDesc(dim, dimId++, dim.getName() + (needNameSuffix ? "_DERIVED" : ""));
                newDimensions.add(newDim);
                newDim.setDerived(dim.getDerived());
                newDim.setColumn(null); // derived column must come from a lookup table; in this case the fk will be the dimension column, no need to explicitly declare it;
                needNameSuffix = true;
            }

            if (!ArrayUtils.isEmpty(dim.getHierarchy())) {
                newDim = newDimensionDesc(dim, dimId++, dim.getName() + (needNameSuffix ? "_HIERARCHY" : ""));
                newDimensions.add(newDim);

                newDim.setHierarchy(true);

                List<String> columns = Lists.newArrayList();
                for (HierarchyDesc hierarch : dim.getHierarchy()) {
                    String col = hierarch.getColumn();
                    columns.add(col);
                }

                newDim.setColumn(columns.toArray(new String[columns.size()]));
            }

        }
    }

    private DataModelDesc extractDataModel(CubeDesc oldModel, org.apache.kylin.cube.model.CubeDesc newModel) {

        DataModelDesc dm = new DataModelDesc();
        dm.setUuid(UUID.randomUUID().toString());
        String factTable = oldModel.getFactTable();
        dm.setName(oldModel.getName());
        dm.setFactTable(getMetadataManager().appendDBName(factTable));

        newModel.setModelName(dm.getName());

        List<org.apache.kylin.cube.model.v1.DimensionDesc> oldDimensions = oldModel.getDimensions();

        List<LookupDesc> lookups = Lists.newArrayList();
        for (org.apache.kylin.cube.model.v1.DimensionDesc dim : oldDimensions) {
            JoinDesc join = dim.getJoin();
            if (join != null && !StringUtils.isEmpty(join.getType()) && join.getForeignKey() != null && join.getForeignKey().length > 0) {
                LookupDesc lookup = new LookupDesc();
                lookup.setJoin(join);
                String table = dim.getTable();
                lookup.setTable(getMetadataManager().appendDBName(table));

                lookups.add(lookup);
            }
        }

        dm.setLookups(lookups.toArray(new LookupDesc[lookups.size()]));
        dm.setFilterCondition(oldModel.getFilterCondition());
        updatePartitionDesc(oldModel, dm);

        if (oldModel.getCapacity() == CubeDesc.CubeCapacity.SMALL) {
            dm.setCapacity(DataModelDesc.RealizationCapacity.SMALL);
        } else if (oldModel.getCapacity() == CubeDesc.CubeCapacity.MEDIUM) {
            dm.setCapacity(DataModelDesc.RealizationCapacity.MEDIUM);
        } else if (oldModel.getCapacity() == CubeDesc.CubeCapacity.LARGE) {
            dm.setCapacity(DataModelDesc.RealizationCapacity.LARGE);
        }

        return dm;
    }

    private void updatePartitionDesc(CubeDesc oldModel, DataModelDesc dm) {

        CubePartitionDesc partition = oldModel.getCubePartitionDesc();
        PartitionDesc newPartition = new PartitionDesc();

        if (partition.getPartitionDateColumn() != null) {
            String partitionCol = partition.getPartitionDateColumn();

            String[] tablecolumn = partitionCol.split("\\.");
            if (tablecolumn != null && tablecolumn.length == 2) {
                // pattern is <tablename>.<colname>
                String tableFullName = getMetadataManager().appendDBName(tablecolumn[0]);
                newPartition.setPartitionDateColumn(tableFullName + "." + tablecolumn[1]);
            } else {

                if (partitionCol.indexOf(".") < 0) {
                    // pattern is <colname>
                    partitionCol = dm.getFactTable() + "." + partitionCol;
                }

                newPartition.setPartitionDateColumn(partitionCol);
            }
        }

        // only append is supported
        newPartition.setCubePartitionType(PartitionDesc.PartitionType.APPEND);

        newPartition.setPartitionDateStart(partition.getPartitionDateStart());

        dm.setPartitionDesc(newPartition);
    }

    private CubeDesc loadOldCubeDesc(String path) throws IOException {
        ResourceStore store = getStore();

        CubeDesc ndesc = store.getResource(path, CubeDesc.class, CUBE_DESC_SERIALIZER_V1);

        if (StringUtils.isBlank(ndesc.getName())) {
            throw new IllegalStateException("CubeDesc name must not be blank");
        }

        Map<String, TableDesc> tableMap = getMetadataManager().getAllTablesMap();
        Map<String, TableDesc> newMap = Maps.newHashMap();
        for (Entry<String, TableDesc> entry : tableMap.entrySet()) {
            String t = entry.getKey();

            if (t.indexOf(".") > 0) {
                newMap.put(t.substring(t.indexOf(".") + 1), entry.getValue());

            }
        }
        ndesc.init(KylinConfig.getInstanceFromEnv(), newMap);

        if (ndesc.getError().isEmpty() == false) {
            throw new IllegalStateException("Cube desc at " + path + " has issues: " + ndesc.getError());
        }

        return ndesc;
    }

    private static MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    protected static ResourceStore getStore() {
        return ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    }
}
