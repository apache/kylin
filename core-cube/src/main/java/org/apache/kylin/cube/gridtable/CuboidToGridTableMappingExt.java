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

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.DynamicFunctionDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class CuboidToGridTableMappingExt extends CuboidToGridTableMapping {
    private final List<TblColRef> dynDims;
    private final List<DynamicFunctionDesc> dynFuncs;

    private ImmutableBitSet dynamicDims;

    private List<DataType> dynGtDataTypes;
    private List<ImmutableBitSet> dynGtColBlocks;

    private Map<TblColRef, Integer> dynDim2gt;

    private Map<FunctionDesc, Integer> dynMetrics2gt;

    public CuboidToGridTableMappingExt(Cuboid cuboid, List<TblColRef> dynDims, List<DynamicFunctionDesc> dynFuncs) {
        super(cuboid);
        this.dynDims = dynDims;
        this.dynFuncs = dynFuncs;
        initialize();
    }

    private void initialize() {
        dynGtDataTypes = Lists.newArrayList();
        dynGtColBlocks = Lists.newArrayList();
        dynDim2gt = Maps.newHashMap();
        dynMetrics2gt = Maps.newHashMap();

        int gtColIdx = super.getColumnCount();

        BitSet rtColBlock = new BitSet();
        // dynamic dimensions
        for (TblColRef rtDim : dynDims) {
            dynDim2gt.put(rtDim, gtColIdx);
            dynGtDataTypes.add(rtDim.getType());
            rtColBlock.set(gtColIdx);
            gtColIdx++;
        }
        dynamicDims = new ImmutableBitSet(rtColBlock);

        // dynamic metrics
        for (DynamicFunctionDesc rtFunc : dynFuncs) {
            dynMetrics2gt.put(rtFunc, gtColIdx);
            dynGtDataTypes.add(rtFunc.getReturnDataType());
            rtColBlock.set(gtColIdx);
            gtColIdx++;
        }

        dynGtColBlocks.add(new ImmutableBitSet(rtColBlock));
    }

    public ImmutableBitSet getDynamicDims() {
        return dynamicDims;
    }

    @Override
    public int getColumnCount() {
        return super.getColumnCount() + dynDims.size() + dynFuncs.size();
    }

    @Override
    public DataType[] getDataTypes() {
        return (DataType[]) ArrayUtils.addAll(super.getDataTypes(),
                dynGtDataTypes.toArray(new DataType[dynGtDataTypes.size()]));
    }

    @Override
    public ImmutableBitSet[] getColumnBlocks() {
        return (ImmutableBitSet[]) ArrayUtils.addAll(super.getColumnBlocks(),
                dynGtColBlocks.toArray(new ImmutableBitSet[dynGtColBlocks.size()]));
    }

    @Override
    public int getIndexOf(TblColRef dimension) {
        Integer i = super.getIndexOf(dimension);
        if (i < 0) {
            i = dynDim2gt.get(dimension);
        }
        return i == null ? -1 : i;
    }

    @Override
    public int getIndexOf(FunctionDesc metric) {
        Integer r = super.getIndexOf(metric);
        if (r < 0) {
            r = dynMetrics2gt.get(metric);
        }
        return r == null ? -1 : r;
    }

    @Override
    public int[] getMetricsIndexes(Collection<FunctionDesc> metrics) {
        int[] result = new int[metrics.size()];
        int i = 0;
        for (FunctionDesc metric : metrics) {
            result[i++] = getIndexOf(metric);
        }
        return result;
    }
}