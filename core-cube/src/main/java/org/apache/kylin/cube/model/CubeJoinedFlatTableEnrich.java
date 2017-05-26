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

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.List;

/**
 * An enrich of IJoinedFlatTableDesc for cubes
 */
public class CubeJoinedFlatTableEnrich implements IJoinedFlatTableDesc, java.io.Serializable {

    private CubeDesc cubeDesc;
    private IJoinedFlatTableDesc flatDesc;
    private int[] rowKeyColumnIndexes; // the column index on flat table
    private int[][] measureColumnIndexes; // [i] is the i.th measure related column index on flat table

    public CubeJoinedFlatTableEnrich(IJoinedFlatTableDesc flatDesc, CubeDesc cubeDesc) {
        // != works due to object cache
        if (cubeDesc.getModel() != flatDesc.getDataModel())
            throw new IllegalArgumentException();

        this.cubeDesc = cubeDesc;
        this.flatDesc = flatDesc;
        parseCubeDesc();
    }

    // check what columns from hive tables are required, and index them
    private void parseCubeDesc() {
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        // build index for rowkey columns
        List<TblColRef> cuboidColumns = baseCuboid.getColumns();
        int rowkeyColCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        rowKeyColumnIndexes = new int[rowkeyColCount];
        for (int i = 0; i < rowkeyColCount; i++) {
            TblColRef col = cuboidColumns.get(i);
            rowKeyColumnIndexes[i] = flatDesc.getColumnIndex(col);
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
                    measureColumnIndexes[i][j] = flatDesc.getColumnIndex(c);
                }
            }
        }
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
        return flatDesc.getTableName();
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return flatDesc.getAllColumns();
    }

    @Override
    public DataModelDesc getDataModel() {
        return flatDesc.getDataModel();
    }

    @Override
    public int getColumnIndex(TblColRef colRef) {
        return flatDesc.getColumnIndex(colRef);
    }

    @Override
    public long getSourceOffsetStart() {
        return flatDesc.getSourceOffsetStart();
    }

    @Override
    public long getSourceOffsetEnd() {
        return flatDesc.getSourceOffsetEnd();
    }

    @Override
    public TblColRef getDistributedBy() {
        return flatDesc.getDistributedBy();
    }

    @Override
    public ISegment getSegment() {
        return flatDesc.getSegment();
    }

    @Override
    public TblColRef getClusterBy() {
        return flatDesc.getClusterBy();
    }

}
