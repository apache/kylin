/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.hadoop.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.DimensionDesc;
import com.kylinolap.cube.model.MeasureDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.TblColRef;

/**
 * @author George Song (ysong1)
 */
public class JoinedFlatTableDesc {

    private String tableName;
    private final CubeDesc cubeDesc;
    private final CubeSegment cubeSegment;

    private int[] rowKeyColumnIndexes; // the column index on flat table
    private int[][] measureColumnIndexes; // [i] is the i.th measure related
                                          // column index on flat table
    
    // Map for table alais; key: table name; value: alias;
    private Map<String, String> tableAliasMap;
    
    public static final String FACT_TABLE_ALIAS = "FACT_TABLE";
    public static final String LOOKUP_TABLE_ALAIS_PREFIX = "LOOKUP_";

    public JoinedFlatTableDesc(CubeDesc cubeDesc, CubeSegment cubeSegment) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        parseCubeDesc();
    }

    /**
     * @return the cubeSegment
     */
    public CubeSegment getCubeSegment() {
        return cubeSegment;
    }

    private List<IntermediateColumnDesc> columnList = new ArrayList<IntermediateColumnDesc>();

    public List<IntermediateColumnDesc> getColumnList() {
        return columnList;
    }

    // check what columns from hive tables are required, and index them
    private void parseCubeDesc() {
        int rowkeyColCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        if (cubeSegment == null) {
            this.tableName = "kylin_intermediate_" + cubeDesc.getName();
        } else {
            this.tableName = "kylin_intermediate_" + cubeDesc.getName() + "_" + cubeSegment.getName();
        }

        Map<String, Integer> dimensionIndexMap = new HashMap<String, Integer>();
        int columnIndex = 0;
        for (TblColRef col : cubeDesc.listDimensionColumnsExcludingDerived()) {
            dimensionIndexMap.put(col.getName(), columnIndex);
            columnList.add(new IntermediateColumnDesc(String.valueOf(columnIndex), col.getName(), col.getDatatype(), col.getTable()));
            columnIndex++;
        }

        // build index
        List<TblColRef> cuboidColumns = baseCuboid.getColumns();
        rowKeyColumnIndexes = new int[rowkeyColCount];
        for (int i = 0; i < rowkeyColCount; i++) {
            String colName = cuboidColumns.get(i).getName();
            Integer dimIdx = dimensionIndexMap.get(colName);
            if (dimIdx == null) {
                throw new RuntimeException("Can't find column " + colName);
            }
            rowKeyColumnIndexes[i] = dimIdx;
        }

        List<MeasureDesc> measures = cubeDesc.getMeasures();
        int measureSize = measures.size();
        measureColumnIndexes = new int[measureSize][];
        for (int i = 0; i < measureSize; i++) {
            FunctionDesc func = measures.get(i).getFunction();
            List<TblColRef> colRefs = func.getParameter().getColRefs();
            if (colRefs == null) {
                measureColumnIndexes[i] = null;
            } else {
                measureColumnIndexes[i] = new int[colRefs.size()];
                for (int j = 0; j < colRefs.size(); j++) {
                    TblColRef c = colRefs.get(j);
                    measureColumnIndexes[i][j] = contains(columnList, c);
                    if (measureColumnIndexes[i][j] < 0) {
                        measureColumnIndexes[i][j] = columnIndex;
                        columnList.add(new IntermediateColumnDesc(String.valueOf(columnIndex), c.getName(), c.getDatatype(), c.getTable()));
                        columnIndex++;
                    }
                }
            }
        }
        
        buileTableAliasMap();
    }
    
    private void buileTableAliasMap() {
        tableAliasMap = new HashMap<String, String>();
        
        tableAliasMap.put(cubeDesc.getFactTable(), FACT_TABLE_ALIAS);
        
        int i=1;
        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            JoinDesc join = dim.getJoin();
            if(join != null) {
                tableAliasMap.put(dim.getTable(), LOOKUP_TABLE_ALAIS_PREFIX + i);
                i++;
            }
            
        }
    }
    
    public String getTableAlias(String tableName) {
        return tableAliasMap.get(tableName);
    }

    private int contains(List<IntermediateColumnDesc> columnList, TblColRef c) {
        for (int i = 0; i < columnList.size(); i++) {
            IntermediateColumnDesc col = columnList.get(i);
            if (col.getColumnName().equals(c.getName()) && col.getTableName().equals(c.getTable()))
                return i;
        }
        return -1;
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
    }

    public String getTableName(String jobUUID) {
        return tableName + "_" + jobUUID.replace("-", "_");
    }

    public int[] getRowKeyColumnIndexes() {
        return rowKeyColumnIndexes;
    }

    public int[][] getMeasureColumnIndexes() {
        return measureColumnIndexes;
    }

    public static class IntermediateColumnDesc {
        private String id;
        private String columnName;
        private String dataType;
        private String tableName;
        private String databaseName;

        public IntermediateColumnDesc(String id, String columnName, String dataType, String tableName) {
            this.id = id;
            this.columnName = columnName;
            this.dataType = dataType;
            this.tableName = tableName;
        }

        public String getId() {
            return id;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getDataType() {
            return dataType;
        }

        public String getTableName() {
            return tableName;
        }
        
        public String getDatabaseName() {
            return databaseName;
        }
    }
}
