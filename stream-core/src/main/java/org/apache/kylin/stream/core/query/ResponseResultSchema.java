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

package org.apache.kylin.stream.core.query;

import java.util.Map;
import java.util.Set;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class ResponseResultSchema {
    private CubeDesc cubeDesc;

    private TblColRef[] dimensions;
    private FunctionDesc[] metrics;
    private MeasureDesc[] measures;

    private DataType[] dimDataTypes;
    private DataType[] metricsDataTypes;

    private int nDimensions;
    private Map<TblColRef, Integer> dimColIdxMap;

    private int nMetrics;
    private Map<TblColRef, Integer> metricsColIdxMap;

    public ResponseResultSchema(CubeDesc cubeDesc, Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics) {
        this.cubeDesc = cubeDesc;
        init(selectedDimensions, selectedMetrics);
    }

    private void init(Set<TblColRef> selectedDimensions, Set<FunctionDesc> selectedMetrics) {
        this.dimensions = new TblColRef[selectedDimensions.size()];
        this.metrics = new FunctionDesc[selectedMetrics.size()];
        this.measures = new MeasureDesc[selectedMetrics.size()];
        this.dimDataTypes = new DataType[dimensions.length];
        this.metricsDataTypes = new DataType[metrics.length];
        // sort dimensions according to the rowKey definition
        dimColIdxMap = Maps.newHashMap();
        RowKeyDesc rowKeyDesc = cubeDesc.getRowkey();
        int colIdx = 0;
        for (RowKeyColDesc rowKeyColDesc : rowKeyDesc.getRowKeyColumns()) {
            TblColRef dimension = rowKeyColDesc.getColRef();
            if (selectedDimensions.contains(dimension)) {
                dimensions[colIdx] = dimension;
                dimDataTypes[colIdx] = dimension.getType();
                dimColIdxMap.put(dimension, colIdx);
                colIdx++;
            }
        }

        nDimensions = colIdx;

        colIdx = 0;
        // metrics
        metricsColIdxMap = Maps.newHashMap();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            FunctionDesc func = measure.getFunction();
            if (selectedMetrics.contains(func)) {
                metrics[colIdx] = func;
                measures[colIdx] = measure;
                metricsColIdxMap.put(func.getParameter().getColRef(), colIdx);
                metricsDataTypes[colIdx] = func.getReturnDataType();
                colIdx++;
            }
        }

        nMetrics = colIdx;
    }

    public int getColumnCount() {
        return nDimensions + nMetrics;
    }

    public int getDimensionCount() {
        return nDimensions;
    }

    public int getMetricsCount() {
        return nMetrics;
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
    }

    public DataType[] getMetricsDataTypes() {
        return metricsDataTypes;
    }

    public DataType getMetricsDataType(int i) {
        return metricsDataTypes[i];
    }

    public int getIndexOfDimension(TblColRef dimension) {
        Integer i = dimColIdxMap.get(dimension);
        return i == null ? -1 : i.intValue();
    }

    public int getIndexOfMetrics(TblColRef metricsColumn) {
        Integer i = metricsColIdxMap.get(metricsColumn);
        return i == null ? -1 : i.intValue();
    }

    public TblColRef[] getDimensions() {
        return dimensions;
    }

    public FunctionDesc[] getMetrics() {
        return metrics;
    }

    public MeasureDesc[] getMeasureDescs() {
        return measures;
    }

    public String[] getAggrFuncs() {
        String[] result = new String[metrics.length];
        for (int i = 0; i < metrics.length; i++) {
            result[i] = metrics[i].getExpression();
        }
        return result;
    }

    public DimensionEncoding[] getDimensionEncodings(IDimensionEncodingMap dimEncMap) {
        DimensionEncoding[] dimEncs = new DimensionEncoding[dimensions.length];
        for (int i = 0; i < dimEncs.length; i++) {
            dimEncs[i] = dimEncMap.get(dimensions[i]);
        }
        return dimEncs;
    }
}
