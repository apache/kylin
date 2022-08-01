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

package org.apache.kylin.metadata.cube.model;

import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;

@SuppressWarnings("serial")
public class NCubeJoinedFlatTableDesc implements IJoinedFlatTableDesc, Serializable {

    protected final String tableName;
    protected final IndexPlan indexPlan;
    protected final SegmentRange segmentRange;
    protected final boolean needJoin;

    private Map<String, Integer> columnIndexMap = Maps.newHashMap();
    private List<TblColRef> columns = Lists.newLinkedList();
    private Set<TblColRef> usedColumns = Sets.newLinkedHashSet();
    private List<Integer> indices = Lists.newArrayList();

    public NCubeJoinedFlatTableDesc(IndexPlan indexPlan) {
        this(indexPlan, null, true);
    }

    public NCubeJoinedFlatTableDesc(NDataSegment segment) {
        this(segment.getIndexPlan(), segment.getSegRange(), true);
    }

    public NCubeJoinedFlatTableDesc(IndexPlan indexPlan, @Nullable SegmentRange segmentRange, Boolean needJoinLookup) {
        this.indexPlan = indexPlan;
        this.segmentRange = segmentRange;
        this.tableName = makeTableName();
        this.needJoin = needJoinLookup;

        initParseIndexPlan();
        initIndices();
    }

    protected String makeTableName() {
        if (segmentRange == null) {
            return "kylin_intermediate_" + indexPlan.getUuid().toLowerCase(Locale.ROOT);
        } else {
            return "kylin_intermediate_" + indexPlan.getUuid().toLowerCase(Locale.ROOT) + "_" + segmentRange.toString();
        }
    }

    protected final void initAddColumn(TblColRef col) {
        if (shouldNotAddColumn(col) || columnIndexMap.containsKey(col.getIdentity())) {
            return;
        }
        columnIndexMap.put(col.getIdentity(), columnIndexMap.size());
        columns.add(col);
    }

    private void initAddUsedColumn(TblColRef col) {
        if (shouldNotAddColumn(col)) {
            return;
        }
        usedColumns.add(col);
    }

    private boolean shouldNotAddColumn(TblColRef col) {
        val model = getDataModel();
        val factTable = model.getRootFactTable();
        return !needJoin && !factTable.getTableName().equalsIgnoreCase(col.getTableRef().getTableName());
    }

    // check what columns from hive tables are required, and index them
    private void initParseIndexPlan() {
        boolean flatTableEnabled = KylinConfig.getInstanceFromEnv().isPersistFlatTableEnabled();
        Map<Integer, TblColRef> usedDimensions = indexPlan.getEffectiveDimCols();
        Map<Integer, TblColRef> effectiveDimensions = flatTableEnabled ? indexPlan.getModel().getEffectiveDimensions()
                : usedDimensions;

        Map<Integer, NDataModel.Measure> usedMeasures = indexPlan.getEffectiveMeasures();
        Map<Integer, NDataModel.Measure> effectiveMeasures = flatTableEnabled
                ? indexPlan.getModel().getEffectiveMeasures()
                : usedMeasures;
        if (flatTableEnabled) {
            effectiveDimensions.forEach((k, v) -> initAddColumn(v));
            effectiveMeasures.forEach((k, v) -> {
                FunctionDesc func = v.getFunction();
                List<TblColRef> colRefs = func.getColRefs();
                colRefs.forEach(this::initAddColumn);
            });
            usedDimensions.forEach((k, v) -> initAddUsedColumn(v));
            usedMeasures.forEach((k, v) -> {
                FunctionDesc func = v.getFunction();
                List<TblColRef> colRefs = func.getColRefs();
                colRefs.forEach(this::initAddUsedColumn);
            });
        } else {
            usedDimensions.forEach((k, v) -> {
                initAddColumn(v);
                initAddUsedColumn(v);
            });
            usedMeasures.forEach((k, v) -> {
                FunctionDesc func = v.getFunction();
                List<TblColRef> colRefs = func.getColRefs();
                colRefs.forEach(colRef -> {
                    initAddColumn(colRef);
                    initAddUsedColumn(colRef);
                });
            });
        }

        // TODO: add dictionary columns
    }

    public List<Integer> getIndices() {
        return indices;
    }

    public void initIndices() {
        for (TblColRef tblColRef : columns) {
            int id = indexPlan.getModel().getColumnIdByColumnName(tblColRef.getIdentity());
            if (-1 == id)
                throw new IllegalArgumentException(
                        "Column: " + tblColRef.getIdentity() + " is not in model: " + indexPlan.getModel().getUuid());
            indices.add(id);
        }
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return columns;
    }

    public Set<TblColRef> getUsedColumns() {
        return usedColumns;
    }

    @Override
    public NDataModel getDataModel() {
        return indexPlan.getModel();
    }

    @Override
    public int getColumnIndex(TblColRef colRef) {
        Integer index = columnIndexMap.get(colRef.getIdentity());
        if (index == null)
            return -1;

        return index;
    }

    @Override
    public SegmentRange getSegRange() {
        return segmentRange;
    }

    @Override
    public TblColRef getDistributedBy() {
        return null;
    }

    @Override
    public ISegment getSegment() {
        return null;
    }

    @Override
    public TblColRef getClusterBy() {
        return null;
    }

}
