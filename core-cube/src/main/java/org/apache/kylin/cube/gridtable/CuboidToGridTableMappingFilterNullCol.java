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

package org.apache.kylin.cube.gridtable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.common.util.BitSets;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class CuboidToGridTableMappingFilterNullCol extends CuboidToGridTableMapping{

    final private CuboidToGridTableMapping internalMapping;
    private Collection<FunctionDesc> nullMetrics;
    private Collection<TblColRef> nullDimensions;
    private int nullMetricsNum;
    private int nullDimensionsNum;
    private ImmutableBitSet nullMetricsIdxSet;

    public CuboidToGridTableMappingFilterNullCol(CuboidToGridTableMapping internalMapping, Collection<FunctionDesc> nullMetrics) {
        this(internalMapping, nullMetrics, Collections.EMPTY_LIST);
    }

    private CuboidToGridTableMappingFilterNullCol(CuboidToGridTableMapping internalMapping, Collection<FunctionDesc> nullMetrics, Collection<TblColRef> nullDimensions) {
        this.internalMapping = internalMapping;
        this.nullMetrics = nullMetrics;
        this.nullDimensions = nullDimensions;
        this.nullMetricsNum = nullMetrics.size();
        this.nullDimensionsNum = nullDimensions.size();
        int[] nullMetricsIdx = internalMapping.getMetricsIndexes(nullMetrics);
        this.nullMetricsIdxSet = new ImmutableBitSet(BitSets.valueOf(nullMetricsIdx));
    }

    @Override
    public int getColumnCount() {
        return internalMapping.getColumnCount() - nullMetricsNum - nullDimensionsNum;
    }

    @Override
    public DataType[] getDataTypes() {
        return internalMapping.getDataTypes();
    }

    @Override
    // TODO-yuzhang filter null dimensions
    public ImmutableBitSet getPrimaryKey() {
        return internalMapping.getPrimaryKey();
    }

    @Override
    public ImmutableBitSet[] getColumnBlocks() {
        ImmutableBitSet[] result = internalMapping.getColumnBlocks();
        result[0] = getPrimaryKey();
        for (int i = 1; i < result.length; i++){
            result[i] = result[i].andNot(nullMetricsIdxSet);
        }
        return result;
    }

    @Override
    public int getIndexOf(TblColRef dimension) {
        if (this.nullDimensions.contains(dimension)){
            return -1;
        }
        return internalMapping.getIndexOf(dimension);
    }

    @Override
    public int getIndexOf(FunctionDesc metric) {
        int idx = internalMapping.getIndexOf(metric);
        if (nullMetricsIdxSet.get(idx)){
            return -1;
        }else{
            return idx;
        }
    }

    @Override
    public List<TblColRef> getCuboidDimensionsInGTOrder() {
        List<TblColRef> dimsInGTOrder = Lists.newArrayList(internalMapping.getCuboidDimensionsInGTOrder());
        dimsInGTOrder.removeAll(nullDimensions);
        return dimsInGTOrder;
    }

    @Override
    public DimensionEncoding[] getDimensionEncodings(IDimensionEncodingMap dimEncMap) {
        List<TblColRef> dims = getCuboidDimensionsInGTOrder();
        DimensionEncoding[] dimEncs = new DimensionEncoding[dims.size()];
        for (int i = 0; i < dimEncs.length; i++) {
            dimEncs[i] = dimEncMap.get(dims.get(i));
        }
        return dimEncs;
    }

    @Override
    public Map<Integer, Integer> getDependentMetricsMap() {
        return Collections.<Integer, Integer> emptyMap();
    }

    @Override
    public Map<TblColRef, Integer> getDim2gt() {
        Map<TblColRef, Integer> result = Maps.newHashMap(internalMapping.getDim2gt());
        nullDimensions.stream().forEach(d -> result.remove(d));
        return result;
    }

    @Override
    public ImmutableBitSet makeGridTableColumns(Collection<? extends FunctionDesc> metrics) {
        BitSet result = new BitSet();
        for (FunctionDesc metric : metrics) {
            int idx = getIndexOf(metric);
            if (idx < 0) {
                continue;
            }
            result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    @Override
    public String[] makeAggrFuncs(Collection<FunctionDesc> metrics) {
        List<FunctionDesc> metricList = Lists.newArrayListWithCapacity(metrics.size());
        metrics.stream().forEach(m -> {
            if (getIndexOf(m) >= 0){
               metricList.add(m);
            }
        });
        return super.makeAggrFuncs(metricList);
    }
}
