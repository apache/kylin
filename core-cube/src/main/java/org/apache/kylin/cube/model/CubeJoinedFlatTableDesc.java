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

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class CubeJoinedFlatTableDesc implements IJoinedFlatTableDesc {

    private String tableName;
    private final CubeDesc cubeDesc;
    private final CubeSegment cubeSegment;

    private int columnCount;
    private int[] rowKeyColumnIndexes; // the column index on flat table
    private int[][] measureColumnIndexes; // [i] is the i.th measure related column index on flat table

    private List<TblColRef> columnList = Lists.newArrayList();
    private Map<TblColRef, Integer> columnIndexMap;

    public CubeJoinedFlatTableDesc(CubeDesc cubeDesc) {
        this(cubeDesc, null);
    }
    
    public CubeJoinedFlatTableDesc(CubeSegment cubeSegment) {
        this(cubeSegment.getCubeDesc(), cubeSegment);
    }
    
    private CubeJoinedFlatTableDesc(CubeDesc cubeDesc, CubeSegment cubeSegment /* can be null */) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;
        this.columnIndexMap = Maps.newHashMap();
        parseCubeDesc();
    }

    // check what columns from hive tables are required, and index them
    private void parseCubeDesc() {
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        if (cubeSegment == null) {
            this.tableName = "kylin_intermediate_" + cubeDesc.getName();
        } else {
            this.tableName = "kylin_intermediate_" + cubeDesc.getName() + "_" + cubeSegment.getUuid().replaceAll("-", "_");
        }

        int columnIndex = 0;
        for (TblColRef col : cubeDesc.listDimensionColumnsExcludingDerived(false)) {
            columnIndexMap.put(col, columnIndex);
            columnList.add(col);
            columnIndex++;
        }

        // build index for rowkey columns
        List<TblColRef> cuboidColumns = baseCuboid.getColumns();
        int rowkeyColCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        rowKeyColumnIndexes = new int[rowkeyColCount];
        for (int i = 0; i < rowkeyColCount; i++) {
            TblColRef col = cuboidColumns.get(i);
            Integer dimIdx = columnIndexMap.get(col);
            if (dimIdx == null) {
                throw new RuntimeException("Can't find column " + col);
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
                    measureColumnIndexes[i][j] = columnList.indexOf(c);
                    if (measureColumnIndexes[i][j] < 0) {
                        measureColumnIndexes[i][j] = columnIndex;
                        columnIndexMap.put(c, columnIndex);
                        columnList.add(c);
                        columnIndex++;
                    }
                }
            }
        }

        if (cubeDesc.getDictionaries() != null) {
            for (DictionaryDesc dictDesc : cubeDesc.getDictionaries()) {
                TblColRef c = dictDesc.getColumnRef();
                if (columnList.indexOf(c) < 0) {
                    columnIndexMap.put(c, columnIndex);
                    columnList.add(c);
                    columnIndex++;
                }
                if (dictDesc.getResuseColumnRef() != null) {
                    c = dictDesc.getResuseColumnRef();
                    if (columnList.indexOf(c) < 0) {
                        columnIndexMap.put(c, columnIndex);
                        columnList.add(c);
                        columnIndex++;
                    }
                }
            }
        }

        columnCount = columnIndex;
    }

    // sanity check the input record (in bytes) matches what's expected
    public void sanityCheck(BytesSplitter bytesSplitter) {
        if (columnCount != bytesSplitter.getBufferSize()) {
            throw new IllegalArgumentException("Expect " + columnCount + " columns, but see " + bytesSplitter.getBufferSize() + " -- " + bytesSplitter);
        }

        // TODO: check data types here
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
    }

    public int[] getRowKeyColumnIndexes() {
        return rowKeyColumnIndexes;
    }

    public int[][] getMeasureColumnIndexes() {
        return measureColumnIndexes;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return columnList;
    }

    @Override
    public DataModelDesc getDataModel() {
        return cubeDesc.getModel();
    }

    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef);
        if (index == null)
            throw new IllegalArgumentException("Column " + colRef.toString() + " wasn't found on flat table.");

        return index.intValue();
    }

    @Override
    public long getSourceOffsetStart() {
        return cubeSegment.getSourceOffsetStart();
    }

    @Override
    public long getSourceOffsetEnd() {
        return cubeSegment.getSourceOffsetEnd();
    }

    @Override
    public TblColRef getDistributedBy() {
        return cubeDesc.getDistributedByColumn();
    }

}
