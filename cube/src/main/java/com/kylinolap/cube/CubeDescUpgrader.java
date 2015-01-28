package com.kylinolap.cube;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.cube.model.HierarchyDesc;
import com.kylinolap.cube.model.RowKeyColDesc;
import com.kylinolap.cube.model.RowKeyDesc;
import com.kylinolap.cube.model.v1.CubePartitionDesc;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.DataModelDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.LookupDesc;
import com.kylinolap.metadata.model.TableDesc;

public class CubeDescUpgrader {

    private String resourcePath;

    private static final Log logger = LogFactory.getLog(CubeDescUpgrader.class);

    private static final Serializer<com.kylinolap.cube.model.v1.CubeDesc> CUBE_DESC_SERIALIZER_V1 = new JsonSerializer<com.kylinolap.cube.model.v1.CubeDesc>(com.kylinolap.cube.model.v1.CubeDesc.class);

    public CubeDescUpgrader(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public com.kylinolap.cube.model.CubeDesc upgrade() throws IOException {
        com.kylinolap.cube.model.v1.CubeDesc oldModel = loadOldCubeDesc(resourcePath);

        com.kylinolap.cube.model.CubeDesc newModel = new com.kylinolap.cube.model.CubeDesc();

        copyUnChangedProperties(oldModel, newModel);

        DataModelDesc model = extractDataModel(oldModel, newModel);
        newModel.setModel(model);

        updateDimensions(oldModel, newModel);

        updateRowkeyDictionary(oldModel, newModel);

        return newModel;
    }

    private void updateRowkeyDictionary(com.kylinolap.cube.model.v1.CubeDesc oldModel, com.kylinolap.cube.model.CubeDesc newModel) {

        RowKeyDesc rowKey = newModel.getRowkey();

        for (RowKeyColDesc rowkeyCol : rowKey.getRowKeyColumns()) {
            if (rowkeyCol.getDictionary() != null && rowkeyCol.getDictionary().length() > 0)
                rowkeyCol.setDictionary("true");
        }

    }

    private void copyUnChangedProperties(com.kylinolap.cube.model.v1.CubeDesc oldModel, com.kylinolap.cube.model.CubeDesc newModel) {

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

    private com.kylinolap.cube.model.DimensionDesc newDimensionDesc(com.kylinolap.cube.model.v1.DimensionDesc dim, int dimId, String name) {
        com.kylinolap.cube.model.DimensionDesc newDim = new com.kylinolap.cube.model.DimensionDesc();

        newDim.setId(dimId);
        newDim.setName(name);
        newDim.setTable(getMetadataManager().appendDBName(dim.getTable()));

        return newDim;
    }

    private void updateDimensions(com.kylinolap.cube.model.v1.CubeDesc oldModel, com.kylinolap.cube.model.CubeDesc newModel) {
        List<com.kylinolap.cube.model.v1.DimensionDesc> oldDimensions = oldModel.getDimensions();

        List<com.kylinolap.cube.model.DimensionDesc> newDimensions = Lists.newArrayList();
        newModel.setDimensions(newDimensions);

        int dimId = 0;
        for (com.kylinolap.cube.model.v1.DimensionDesc dim : oldDimensions) {

            com.kylinolap.cube.model.DimensionDesc newDim = null;
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

    private DataModelDesc extractDataModel(com.kylinolap.cube.model.v1.CubeDesc oldModel, com.kylinolap.cube.model.CubeDesc newModel) {

        DataModelDesc dm = new DataModelDesc();
        dm.setUuid(UUID.randomUUID().toString());
        String factTable = oldModel.getFactTable();
        dm.setName(oldModel.getName());
        dm.setFactTable(getMetadataManager().appendDBName(factTable));

        newModel.setModelName(dm.getName());

        List<com.kylinolap.cube.model.v1.DimensionDesc> oldDimensions = oldModel.getDimensions();

        List<LookupDesc> lookups = Lists.newArrayList();
        for (com.kylinolap.cube.model.v1.DimensionDesc dim : oldDimensions) {
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
        

        if (oldModel.getCapacity() == com.kylinolap.cube.model.v1.CubeDesc.CubeCapacity.SMALL) {
            dm.setCapacity(com.kylinolap.metadata.model.DataModelDesc.RealizationCapacity.SMALL);
        } else if (oldModel.getCapacity() == com.kylinolap.cube.model.v1.CubeDesc.CubeCapacity.MEDIUM) {
            dm.setCapacity(com.kylinolap.metadata.model.DataModelDesc.RealizationCapacity.MEDIUM);
        } else if (oldModel.getCapacity() == com.kylinolap.cube.model.v1.CubeDesc.CubeCapacity.LARGE) {
            dm.setCapacity(com.kylinolap.metadata.model.DataModelDesc.RealizationCapacity.LARGE);
        }
        
        return dm;
    }

    private void updatePartitionDesc(com.kylinolap.cube.model.v1.CubeDesc oldModel, com.kylinolap.metadata.model.DataModelDesc dm) {

        com.kylinolap.cube.model.v1.CubePartitionDesc partition = oldModel.getCubePartitionDesc();
        com.kylinolap.metadata.model.PartitionDesc newPartition = new com.kylinolap.metadata.model.PartitionDesc();

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

        if (partition.getCubePartitionType() == com.kylinolap.cube.model.v1.CubePartitionDesc.CubePartitionType.APPEND) {
            newPartition.setCubePartitionType(com.kylinolap.metadata.model.PartitionDesc.PartitionType.APPEND);
        } else if (partition.getCubePartitionType() == CubePartitionDesc.CubePartitionType.UPDATE_INSERT) {
            newPartition.setCubePartitionType(com.kylinolap.metadata.model.PartitionDesc.PartitionType.UPDATE_INSERT);

        }

        newPartition.setPartitionDateStart(partition.getPartitionDateStart());

        dm.setPartitionDesc(newPartition);
    }

    private com.kylinolap.cube.model.v1.CubeDesc loadOldCubeDesc(String path) throws IOException {
        ResourceStore store = getStore();

        com.kylinolap.cube.model.v1.CubeDesc ndesc = store.getResource(path, com.kylinolap.cube.model.v1.CubeDesc.class, CUBE_DESC_SERIALIZER_V1);

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
