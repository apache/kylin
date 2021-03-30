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

package org.apache.kylin.stream.core.storage.columnar;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class ParsedStreamingCubeInfo {
    public final CubeInstance cubeInstance;
    public final CubeDesc cubeDesc;
    public final CubeJoinedFlatTableEnrich intermediateTableDesc;
    public final MeasureDesc[] measureDescs;
    public final MeasureIngester<?>[] measureIngesters;
    public final String[] metricsAggrFuncs;
    public final int dimCount;
    public final int measureCount;
    public final TblColRef[] dimensions;
    public final TblColRef[] dimensionsUseDictEncoding;

    public final Cuboid basicCuboid;
    public List<CuboidInfo> additionalCuboidsToBuild;
    public CuboidToGridTableMapping basicCuboidMapping;

    private Map<String, TblColRef> dimensionsMap = Maps.newHashMap();

    public ParsedStreamingCubeInfo(CubeInstance cubeInstance) {
        this.cubeInstance = cubeInstance;
        this.cubeDesc = cubeInstance.getDescriptor();
        this.basicCuboid = Cuboid.getBaseCuboid(cubeDesc);
        this.intermediateTableDesc = new CubeJoinedFlatTableEnrich(new CubeJoinedFlatTableDesc(cubeDesc), cubeDesc);
        this.measureCount = cubeDesc.getMeasures().size();
        this.measureDescs = cubeDesc.getMeasures().toArray(new MeasureDesc[measureCount]);
        this.measureIngesters = MeasureIngester.create(cubeDesc.getMeasures());
        this.dimensions = basicCuboid.getColumns().toArray(new TblColRef[basicCuboid.getColumns().size()]);
        this.dimCount = dimensions.length;
        this.basicCuboidMapping = new CuboidToGridTableMapping(basicCuboid);

        boolean buildAdditionalCuboids = cubeDesc.getConfig().isStreamingBuildAdditionalCuboids();
        Set<Long> mandatoryCuboids = cubeDesc.getMandatoryCuboids();
        if (buildAdditionalCuboids) {
            additionalCuboidsToBuild = Lists.newArrayListWithCapacity(mandatoryCuboids.size());
            for (long cuboidID : mandatoryCuboids) {
                CuboidInfo cuboidInfo = new CuboidInfo(cuboidID);
                cuboidInfo.init(cubeDesc, intermediateTableDesc);
                additionalCuboidsToBuild.add(cuboidInfo);
            }
        }

        List<TblColRef> dimUseDictList = Lists.newArrayList();
        for (TblColRef column : dimensions) {
            dimensionsMap.put(column.getName(), column);
            if (cubeDesc.getRowkey().isUseDictionary(column)) {
                dimUseDictList.add(column);
            }
        }
        this.dimensionsUseDictEncoding = dimUseDictList.toArray(new TblColRef[dimUseDictList.size()]);

        List<String> metricsAggrFuncsList = Lists.newArrayListWithCapacity(measureCount);
        for (int i = 0; i < measureCount; i++) {
            MeasureDesc measureDesc = measureDescs[i];
            metricsAggrFuncsList.add(measureDesc.getFunction().getExpression());
        }
        this.metricsAggrFuncs = metricsAggrFuncsList.toArray(new String[metricsAggrFuncsList.size()]);
    }

    public CubeInstance getCubeInstance() {
        return cubeInstance;
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
    }

    public TblColRef[] getDimensions() {
        return dimensions;
    }

    public MeasureDesc[] getMeasures() {
        return measureDescs;
    }

    public MeasureIngester<?>[] getMeasureIngesters() {
        return measureIngesters;
    }

    public DataType[] getAllDataTypes() {
        return basicCuboidMapping.getDataTypes();
    }

    public DataTypeSerializer getMeasureTypeSerializer(int measureIdx) {
        DataType type = measureDescs[measureIdx].getFunction().getReturnDataType();
        return DataTypeSerializer.create(type);
    }

    public List<CuboidInfo> getAdditionalCuboidsToBuild() {
        return additionalCuboidsToBuild;
    }

    public CuboidToGridTableMapping getBasicCuboidToGridTableMapping() {
        return basicCuboidMapping;
    }

    public CuboidInfo getCuboidInfo(long cuboidId) {
        CuboidInfo cuboidInfo = new CuboidInfo(cuboidId);
        cuboidInfo.init(cubeDesc, intermediateTableDesc);

        return cuboidInfo;
    }

    public int getMetricIndexInAllMetrics(FunctionDesc metric) {
        return basicCuboidMapping.getIndexOf(metric) - dimensions.length;
    }

    public TblColRef getDimensionByName(String dimensionName) {
        return dimensionsMap.get(dimensionName);
    }

    public static DimensionEncoding[] getDimensionEncodings(CubeDesc cubeDesc, TblColRef[] dimensions,
            Map<TblColRef, Dictionary<String>> dimDictMap) {
        DimensionEncoding[] result = new DimensionEncoding[dimensions.length];
        for (int i = 0; i < dimensions.length; i++) {
            TblColRef dimension = dimensions[i];
            RowKeyColDesc colDesc = cubeDesc.getRowkey().getColDesc(dimension);
            if (colDesc.isUsingDictionary()) {
                @SuppressWarnings({ "unchecked" })
                Dictionary<String> dict = dimDictMap.get(dimension);
                if (dict == null) {
                    throw new RuntimeException("No dictionary found for dict-encoding column " + dimension);
                } else {
                    result[i] = new DictionaryDimEnc(dict);
                }
            } else {
                result[i] = DimensionEncodingFactory.create(colDesc.getEncodingName(), colDesc.getEncodingArgs(),
                        colDesc.getEncodingVersion());
            }
        }
        return result;
    }

    public static ColumnarMetricsEncoding[] getMetricsEncodings(MeasureDesc[] measures) {
        ColumnarMetricsEncoding[] result = new ColumnarMetricsEncoding[measures.length];
        for (int i = 0; i < measures.length; i++) {
            result[i] = ColumnarMetricsEncodingFactory.create(measures[i].getFunction().getReturnDataType());
        }
        return result;
    }

    public static class CuboidInfo {
        private long cuboidID;
        private int[] columnsIndex;
        private List<TblColRef> dimensions;

        public CuboidInfo(long cuboidID) {
            this.cuboidID = cuboidID;
        }

        public void init(CubeDesc cubeDesc, CubeJoinedFlatTableEnrich intermediateTableDesc) {
            dimensions = Lists.newArrayList();
            columnsIndex = new int[Long.bitCount(cuboidID)];

            int colIdx = 0;
            RowKeyColDesc[] allColumns = cubeDesc.getRowkey().getRowKeyColumns();
            for (int i = 0; i < allColumns.length; i++) {
                // NOTE: the order of column in list!!!
                long bitmask = 1L << allColumns[i].getBitIndex();
                if ((cuboidID & bitmask) != 0) {
                    TblColRef colRef = allColumns[i].getColRef();
                    dimensions.add(colRef);
                    columnsIndex[colIdx] = intermediateTableDesc.getColumnIndex(colRef);
                    colIdx++;
                }
            }
        }

        public long getCuboidID() {
            return cuboidID;
        }

        public int[] getColumnsIndex() {
            if (columnsIndex == null) {
                throw new IllegalStateException("it is not initialized");
            }
            return columnsIndex;
        }

        public TblColRef[] getDimensions() {
            if (dimensions == null) {
                throw new IllegalStateException("it is not initialized");
            }
            return dimensions.toArray(new TblColRef[dimensions.size()]);
        }

        public int getDimCount() {
            return dimensions.size();
        }

        public int getIndexOf(TblColRef dimension) {
            return dimensions.indexOf(dimension);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            CuboidInfo that = (CuboidInfo) o;

            return cuboidID == that.cuboidID;

        }

        @Override
        public int hashCode() {
            return (int) (cuboidID ^ (cuboidID >>> 32));
        }
    }

    public void resetAggrs() {
        for (int i = 0; i < cubeDesc.getMeasures().size(); i++) {
            measureIngesters[i].reset();
        }
    }
}
