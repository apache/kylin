package com.kylinolap.job.hadoop.hive;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.metadata.model.DataModelDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.LookupDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.sun.org.apache.xml.internal.utils.StringComparable;

/**
 * Created by Hongbin Ma(Binmahone) on 12/30/14.
 */
public class IIJoinedFlatTableDesc implements IJoinedFlatTableDesc {

    private IIDesc iiDesc;
    private String tableName;
    private List<IntermediateColumnDesc> columnList = Lists.newArrayList();
    private Map<String, String> tableAliasMap;

    public IIJoinedFlatTableDesc(IIDesc iiDesc) {
        this.iiDesc = iiDesc;
        parseIIDesc();
    }

    private void parseIIDesc() {
        this.tableName = "kylin_intermediate_ii_" + iiDesc.getName();

        int columnIndex = 0;
        for (TblColRef col : iiDesc.listAllColumns()) {
            columnList.add(new IntermediateColumnDesc(String.valueOf(columnIndex), col));
            columnIndex++;
        }
        buildTableAliasMap();
    }

    private void buildTableAliasMap() {
        tableAliasMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        tableAliasMap.put(iiDesc.getFactTableName(), FACT_TABLE_ALIAS);

        int i = 1;
        for (LookupDesc lookupDesc : iiDesc.getModel().getLookups()) {
            JoinDesc join = lookupDesc.getJoin();
            if (join != null) {
                tableAliasMap.put(lookupDesc.getTable(), LOOKUP_TABLE_ALAIS_PREFIX + i);
                i++;
            }
        }
    }

    @Override
    public String getTableName(String jobUUID) {
        return tableName + "_" + jobUUID.replace("-", "_");
    }

    public List<IntermediateColumnDesc> getColumnList() {
        return columnList;
    }

    @Override
    public DataModelDesc getDataModel() {
        return iiDesc.getModel();
    }

    @Override
    public CubeDesc.RealizationCapacity getCapacity() {
        return CubeDesc.RealizationCapacity.SMALL;
    }

    @Override
    public String getTableAlias(String tableName) {
        return tableAliasMap.get(tableName);
    }
}
