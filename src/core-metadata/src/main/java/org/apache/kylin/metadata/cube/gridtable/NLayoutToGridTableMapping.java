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

package org.apache.kylin.metadata.cube.gridtable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.gridtable.GridTableMapping;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.NDataModel;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class NLayoutToGridTableMapping extends GridTableMapping {
    private final LayoutEntity layoutEntity;
    private boolean isBatchOfHybrid = false;

    public NLayoutToGridTableMapping(LayoutEntity layoutEntity) {
        this.layoutEntity = layoutEntity;
        init();
    }

    public NLayoutToGridTableMapping(LayoutEntity layoutEntity, boolean isBatchOfHybrid) {
        this.layoutEntity = layoutEntity;
        this.isBatchOfHybrid = isBatchOfHybrid;
        init();
    }

    private void init() {
        // FIXME: currently only support all dimensions in one column family
        int gtColIdx = 0;
        gtDataTypes = Lists.newArrayList();
        gtColBlocks = Lists.newArrayList();

        // dimensions
        dim2gt = Maps.newHashMap();
        BitSet pk = new BitSet();
        List<TblColRef> columns = new ArrayList<>();
        if (isBatchOfHybrid) {
            columns.addAll(layoutEntity.getStreamingColumns().values());
        } else {
            columns.addAll(layoutEntity.getColumns());
        }
        for (TblColRef dimension : columns) {
            gtDataTypes.add(dimension.getType());
            dim2gt.put(dimension, gtColIdx);
            pk.set(gtColIdx);
            gtColIdx++;
        }
        gtPrimaryKey = new ImmutableBitSet(pk);
        gtColBlocks.add(gtPrimaryKey);

        nDimensions = gtColIdx;
        assert nDimensions == layoutEntity.getColumns().size();

        // column blocks of metrics
        ArrayList<BitSet> metricsColBlocks = Lists.newArrayList();
        for (int i = 0; i < layoutEntity.getOrderedMeasures().size(); i++) {
            metricsColBlocks.add(new BitSet());
        }

        // metrics
        metrics2gt = Maps.newHashMap();
        int mColBlock = 0;
        List<NDataModel.Measure> measureDescs = new ArrayList<>();
        if (isBatchOfHybrid) {
            measureDescs.addAll(layoutEntity.getStreamingMeasures().values());
        } else {
            measureDescs.addAll(layoutEntity.getOrderedMeasures().values());
        }
        for (NDataModel.Measure measure : measureDescs) {
            // Count distinct & holistic count distinct are equals() but different.
            // Ensure the holistic version if exists is always the first.
            FunctionDesc func = measure.getFunction();
            metrics2gt.put(func, gtColIdx);
            gtDataTypes.add(func.getReturnDataType());

            // map to column block
            metricsColBlocks.get(mColBlock++).set(gtColIdx++);
        }

        for (BitSet set : metricsColBlocks) {
            gtColBlocks.add(new ImmutableBitSet(set));
        }

        nMetrics = gtColIdx - nDimensions;
        assert nMetrics == layoutEntity.getOrderedMeasures().size();
    }

    @Override
    public List<TblColRef> getCuboidDimensionsInGTOrder() {
        return layoutEntity.getColumns();
    }

    @Override
    public DimensionEncoding[] getDimensionEncodings(IDimensionEncodingMap dimEncMap) {
        List<TblColRef> dims = layoutEntity.getColumns();
        DimensionEncoding[] dimEncs = new DimensionEncoding[dims.size()];
        for (int i = 0; i < dimEncs.length; i++) {
            dimEncs[i] = dimEncMap.get(dims.get(i));
        }
        return dimEncs;
    }

    @Override
    public Map<Integer, Integer> getDependentMetricsMap() {
        Map<Integer, Integer> result = Maps.newHashMap();
        Collection<NDataModel.Measure> measures = layoutEntity.getOrderedMeasures().values();
        for (NDataModel.Measure child : layoutEntity.getOrderedMeasures().values()) {
            if (child.getDependentMeasureRef() != null) {
                boolean ok = false;
                for (NDataModel.Measure parent : measures) {
                    if (parent.getName().equals(child.getDependentMeasureRef())) {
                        int childIndex = getIndexOf(child.getFunction());
                        int parentIndex = getIndexOf(parent.getFunction());
                        result.put(childIndex, parentIndex);
                        ok = true;
                        break;
                    }
                }
                if (!ok)
                    throw new IllegalStateException("Cannot find dependent measure: " + child.getDependentMeasureRef());
            }
        }
        return result.isEmpty() ? Collections.<Integer, Integer> emptyMap() : result;
    }

    @Override
    public String getTableName() {
        return "Cuboid " + layoutEntity.getId();
    }
}
