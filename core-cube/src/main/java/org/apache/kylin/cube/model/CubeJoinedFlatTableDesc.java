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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
@SuppressWarnings("serial")
public class CubeJoinedFlatTableDesc implements IJoinedFlatTableDesc, Serializable {

    protected String tableName;
    protected final CubeDesc cubeDesc;
    protected final CubeSegment cubeSegment;
    protected final boolean includingDerived;

    private int columnCount = 0;
    private List<TblColRef> columnList = Lists.newArrayList();
    private Map<TblColRef, Integer> columnIndexMap = Maps.newHashMap();

    public CubeJoinedFlatTableDesc(CubeDesc cubeDesc) {
        this(cubeDesc, null, false);
    }

    public CubeJoinedFlatTableDesc(CubeDesc cubeDesc, boolean includingDerived) {
        this(cubeDesc, null, includingDerived);
    }

    public CubeJoinedFlatTableDesc(CubeSegment cubeSegment) {
        this(cubeSegment.getCubeDesc(), cubeSegment, false);
    }

    public CubeJoinedFlatTableDesc(CubeSegment cubeSegment, boolean includingDerived) {
        this(cubeSegment.getCubeDesc(), cubeSegment, includingDerived);
    }

    private CubeJoinedFlatTableDesc(CubeDesc cubeDesc, CubeSegment cubeSegment /* can be null */,
            boolean includingDerived) {
        this.cubeDesc = cubeDesc;
        this.cubeSegment = cubeSegment;

        this.tableName = makeTableName(cubeDesc, cubeSegment);
        this.includingDerived = includingDerived;
        initParseCubeDesc();
    }

    protected String makeTableName(CubeDesc cubeDesc, CubeSegment cubeSegment) {
        if (cubeSegment == null) {
            return cubeDesc.getConfig().getHiveIntermediateTablePrefix() + cubeDesc.getName().toLowerCase(Locale.ROOT);
        } else {
            return cubeDesc.getConfig().getHiveIntermediateTablePrefix() + cubeDesc.getName().toLowerCase(Locale.ROOT) + "_"
                    + cubeSegment.getUuid().replaceAll("-", "_");
        }
    }

    protected final void initAddColumn(TblColRef col) {
        if (columnIndexMap.containsKey(col))
            return;

        int columnIndex = columnIndexMap.size();
        columnIndexMap.put(col, columnIndex);
        columnList.add(col);
        columnCount = columnIndexMap.size();

        Preconditions.checkState(columnIndexMap.size() == columnList.size());
    }

    // check what columns from hive tables are required, and index them
    protected void initParseCubeDesc() {
        if (this.includingDerived) {
            for (TblColRef col : cubeDesc.listDimensionColumnsIncludingDerived()) {
                initAddColumn(col);
            }
        } else {
            for (TblColRef col : cubeDesc.listDimensionColumnsExcludingDerived(false)) {
                initAddColumn(col);
            }
        }

        List<MeasureDesc> measures = cubeDesc.getMeasures();
        int measureSize = measures.size();
        for (int i = 0; i < measureSize; i++) {
            FunctionDesc func = measures.get(i).getFunction();
            List<TblColRef> colRefs = func.getParameter().getColRefs();
            if (colRefs != null) {
                for (int j = 0; j < colRefs.size(); j++) {
                    TblColRef c = colRefs.get(j);
                    initAddColumn(c);
                }
            }
        }

        if (cubeDesc.getDictionaries() != null) {
            for (DictionaryDesc dictDesc : cubeDesc.getDictionaries()) {
                TblColRef c = dictDesc.getColumnRef();
                initAddColumn(c);
                if (dictDesc.getResuseColumnRef() != null) {
                    c = dictDesc.getResuseColumnRef();
                    initAddColumn(c);
                }
            }
        }
    }

    @Override
    public List<TblColRef> getFactColumns() {
        final List<TblColRef> factColumns = Lists.newArrayList();
        for (TblColRef col : this.getAllColumns()) {
            if (col.getTableRef().equals(getDataModel().getRootFactTable())) {
                // only fetch the columns from fact table
                factColumns.add(col);
            }
        }
        return factColumns;
    }

    public int getColumnCount() {
        return columnCount;
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

    @Override
    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef);
        if (index == null)
            return -1;

        return index.intValue();
    }

    @Override
    public SegmentRange getSegRange() {
        if (cubeSegment.isOffsetCube()) {
            return null;
        }
        return cubeSegment.getSegRange();
    }

    @Override
    public TblColRef getDistributedBy() {
        return cubeDesc.getDistributedByColumn();
    }

    @Override
    public ISegment getSegment() {
        return cubeSegment;
    }

    @Override
    public boolean useAlias() {
        return true;
    }

    @Override
    public TblColRef getClusterBy() {
        return cubeDesc.getClusteredByColumn();
    }

}
