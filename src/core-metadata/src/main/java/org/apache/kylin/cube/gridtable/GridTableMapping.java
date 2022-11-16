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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;

public abstract class GridTableMapping {
    protected List<DataType> gtDataTypes;
    protected List<ImmutableBitSet> gtColBlocks;

    protected int nDimensions;
    protected Map<TblColRef, Integer> dim2gt;
    protected ImmutableBitSet gtPrimaryKey;

    protected int nMetrics;
    protected Map<FunctionDesc, Integer> metrics2gt; // because count distinct may have a holistic version

    public int getColumnCount() {
        return nDimensions + nMetrics;
    }

    public DataType[] getDataTypes() {
        return gtDataTypes.toArray(new DataType[gtDataTypes.size()]);
    }

    public ImmutableBitSet getPrimaryKey() {
        return gtPrimaryKey;
    }

    public int getIndexOf(TblColRef dimension) {
        Integer i = dim2gt.get(dimension);
        return i == null ? -1 : i.intValue();
    }

    public int[] getDimIndices(Collection<TblColRef> dims) {
        int[] result = new int[dims.size()];
        int i = 0;
        for (TblColRef dim : dims) {
            result[i++] = getIndexOf(dim);
        }
        return result;
    }

    public int getIndexOf(FunctionDesc metric) {
        Integer r = metrics2gt.get(metric);
        if (r == null) {
            r = handlerCountReplace(metric);
        }
        return r == null ? -1 : r;
    }

    public Integer handlerCountReplace(FunctionDesc metric) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (config.isReplaceColCountWithCountStar() && FunctionDesc.FUNC_COUNT.equals(metric.getExpression())) {
            Iterator<FunctionDesc> functionDescIterator = metrics2gt.keySet().iterator();
            while (functionDescIterator.hasNext()) {
                FunctionDesc functionDesc = functionDescIterator.next();
                if (FunctionDesc.FUNC_COUNT.equals(functionDesc.getExpression())
                        && functionDesc.getParameters().size() == 1
                        && "1".equals(functionDesc.getParameters().get(0).getValue())) {
                    return metrics2gt.get(functionDesc);
                }
            }
        }
        return null;
    }

    public int[] getMetricsIndices(Collection<FunctionDesc> metrics) {
        int[] result = new int[metrics.size()];
        int i = 0;
        for (FunctionDesc metric : metrics) {
            result[i++] = getIndexOf(metric);
        }
        return result;
    }

    public abstract List<TblColRef> getCuboidDimensionsInGTOrder();

    public abstract DimensionEncoding[] getDimensionEncodings(IDimensionEncodingMap dimEncMap);

    public abstract Map<Integer, Integer> getDependentMetricsMap();

    public abstract String getTableName();

    public ImmutableBitSet makeGridTableColumns(Set<TblColRef> dimensions) {
        BitSet result = new BitSet();
        for (TblColRef dim : dimensions) {
            int idx = this.getIndexOf(dim);
            if (idx >= 0)
                result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    public ImmutableBitSet makeGridTableColumns(Collection<FunctionDesc> metrics) {
        BitSet result = new BitSet();
        for (FunctionDesc metric : metrics) {
            int idx = this.getIndexOf(metric);
            if (idx < 0)
                throw new IllegalStateException(metric + " not found in " + this);
            result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    public String[] makeAggrFuncs(Collection<FunctionDesc> metrics) {

        //metrics are represented in ImmutableBitSet, which loses order information
        //sort the aggrFuns to align with metrics natural order
        List<FunctionDesc> metricList = Lists.newArrayList(metrics);
        Collections.sort(metricList, new Comparator<FunctionDesc>() {
            @Override
            public int compare(FunctionDesc o1, FunctionDesc o2) {
                int a = GridTableMapping.this.getIndexOf(o1);
                int b = GridTableMapping.this.getIndexOf(o2);
                return a - b;
            }
        });

        String[] result = new String[metricList.size()];
        int i = 0;
        for (FunctionDesc metric : metricList) {
            result[i++] = metric.getExpression();
        }
        return result;
    }
}
