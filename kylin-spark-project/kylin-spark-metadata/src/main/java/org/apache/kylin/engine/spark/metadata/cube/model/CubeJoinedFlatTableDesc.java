/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.metadata.model.ISegment;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CubeJoinedFlatTableDesc implements IJoinedFlatTableDesc{
    protected final String tableName;
    protected final Cube cube;
    protected final SegmentRange segmentRange;
    protected final boolean needJoin;

    private Map<String, Integer> columnIndexMap = Maps.newHashMap();
    private List<TblColRef> columns = Lists.newLinkedList();
    private List<Integer> indices = Lists.newArrayList();

    public CubeJoinedFlatTableDesc(Cube cube) {
        this(cube, null, true);
    }

    public CubeJoinedFlatTableDesc(DataSegment segment, Boolean needJoinLookup) {
        this(segment.getCube(), segment.getSegRange(), needJoinLookup);
    }

    public CubeJoinedFlatTableDesc(Cube cube, SegmentRange segmentRange, Boolean needJoinLookup) {
        this.cube = cube;
        this.segmentRange = segmentRange;
        this.tableName = makeTableName();
        this.needJoin = needJoinLookup;

        initParseCube();

        initIndices();
    }

    protected String makeTableName() {
        if (segmentRange == null) {
            return "kylin_intermediate_" + cube.getUuid().toLowerCase(Locale.ROOT);
        } else {
            return "kylin_intermediate_" + cube.getUuid().toLowerCase(Locale.ROOT) + "_" + segmentRange.toString();
        }
    }

    protected final void initAddColumn(TblColRef col) {
        DataModel model = getDataModel();
        TableRef factTable = model.getRootFactTable();
        if (!needJoin && !factTable.getTableName().equalsIgnoreCase(col.getTableRef().getTableName())) {
            return;
        }

        if (columnIndexMap.containsKey(col.getIdentity()))
            return;

        columnIndexMap.put(col.getIdentity(), columnIndexMap.size());
        columns.add(col);
    }

    // check what columns from hive tables are required, and index them
    private void initParseCube() {
        for (Map.Entry<Integer, TblColRef> dimEntry : cube.getEffectiveDimCols().entrySet()) {
            initAddColumn(dimEntry.getValue());
        }

        for (Map.Entry<Integer, DataModel.Measure> measureEntry : cube.getModel().getEffectiveMeasureMap().entrySet()) {
            FunctionDesc func = measureEntry.getValue().getFunction();
            List<TblColRef> colRefs = func.getColRefs();
            if (colRefs != null) {
                for (TblColRef colRef : colRefs) {
                    initAddColumn(colRef);
                }
            }
        }

        // TODO: add dictionary columns
    }

    public List<Integer> getIndices() {
        return indices;
    }

    public void initIndices() {
        for (TblColRef tblColRef : columns) {
            int id = cube.getModel().getColumnIdByColumnName(tblColRef.getIdentity());
            if (-1 == id)
                throw new IllegalArgumentException(
                        "Column: " + tblColRef.getIdentity() + " is not in model: " + cube.getModel().getUuid());
            indices.add(id);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public List<TblColRef> getAllColumns() {
        return columns;
    }

    public DataModel getDataModel() {
        return cube.getModel();
    }

    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef.getIdentity());
        if (index == null)
            return -1;

        return index;
    }

    public SegmentRange getSegRange() {
        return segmentRange;
    }

    public TblColRef getDistributedBy() {
        return null;
    }

    public ISegment getSegment() {
        return null;
    }

    public TblColRef getClusterBy() {
        return null;
    }

}
