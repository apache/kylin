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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.storage.columnar.protocol.CuboidMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimensionMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.MetricMetaInfo;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class FragmentCuboidReader implements Iterable<RawRecord> {
    private long rowCount;
    private long readRowCount = 0;
    private int dimCnt;
    private int metricCnt;
    private ColumnDataReader[] dimensionDataReaders;
    private ColumnDataReader[] metricDataReaders;

    public FragmentCuboidReader(CubeDesc cubeDesc, FragmentData fragmentData, CuboidMetaInfo cuboidMetaInfo,
            TblColRef[] dimensions, MeasureDesc[] measures, DimensionEncoding[] dimEncodings) {
        this.dimCnt = dimensions.length;
        this.metricCnt = measures.length;
        this.dimensionDataReaders = new ColumnDataReader[dimCnt];
        this.metricDataReaders = new ColumnDataReader[metricCnt];
        this.rowCount = cuboidMetaInfo.getNumberOfRows();
        Map<String, DimensionMetaInfo> dimensionMetaInfoMap = getDimensionMetaMap(cuboidMetaInfo);
        Map<String, MetricMetaInfo> metricMetaInfoMap = getMetricMetaMap(cuboidMetaInfo);
        int i = 0;
        for (TblColRef dimension : dimensions) {
            DimensionMetaInfo dimensionMetaInfo = dimensionMetaInfoMap.get(dimension.getName());
            ColumnDataReader dimensionDataReader = getDimensionDataReader(dimensionMetaInfo,
                    (int) cuboidMetaInfo.getNumberOfRows(), dimension, dimEncodings[i],
                    fragmentData.getDataReadBuffer());
            dimensionDataReaders[i] = dimensionDataReader;
            i++;
        }

        i = 0;
        for (MeasureDesc measure : measures) {
            MetricMetaInfo metricMetaInfo = metricMetaInfoMap.get(measure.getName());
            ColumnDataReader metricDataReader = getMetricsDataReader(measure, metricMetaInfo,
                    (int) cuboidMetaInfo.getNumberOfRows(), fragmentData.getDataReadBuffer());
            metricDataReaders[i] = metricDataReader;
            i++;
        }
    }

    /**
     * get the specified row in the cuboid data
     * @param rowNum
     * @return
     */
    public RawRecord read(int rowNum) {
        if (rowNum > rowCount - 1) {
            throw new IllegalStateException("cannot read row:" + rowNum + ", total row cnt is:" + rowCount);
        }
        RawRecord rawRecord = new RawRecord(dimCnt, metricCnt);
        for (int i = 0; i < dimCnt; i++) {
            rawRecord.setDimension(i, dimensionDataReaders[i].read(rowNum));
        }
        for (int i = 0; i < metricCnt; i++) {
            rawRecord.setMetric(i, metricDataReaders[i].read(rowNum));
        }
        readRowCount++;
        return rawRecord;
    }

    private Map<String, DimensionMetaInfo> getDimensionMetaMap(CuboidMetaInfo cuboidMetaInfo) {
        Map<String, DimensionMetaInfo> result = Maps.newHashMap();
        List<DimensionMetaInfo> dimensionMetaInfoList = cuboidMetaInfo.getDimensionsInfo();
        for (DimensionMetaInfo dimensionMetaInfo : dimensionMetaInfoList) {
            result.put(dimensionMetaInfo.getName(), dimensionMetaInfo);
        }
        return result;
    }

    private Map<String, MetricMetaInfo> getMetricMetaMap(CuboidMetaInfo cuboidMetaInfo) {
        Map<String, MetricMetaInfo> result = Maps.newHashMap();
        List<MetricMetaInfo> metricMetaInfoList = cuboidMetaInfo.getMetricsInfo();
        for (MetricMetaInfo metricMetaInfo : metricMetaInfoList) {
            result.put(metricMetaInfo.getName(), metricMetaInfo);
        }
        return result;
    }

    @Override
    public Iterator<RawRecord> iterator() {
        final RawRecord oneRawRecord = new RawRecord(dimCnt, metricCnt);
        final Iterator<byte[]>[] dimValItr = new Iterator[dimensionDataReaders.length];
        for (int i = 0; i < dimensionDataReaders.length; i++) {
            dimValItr[i] = dimensionDataReaders[i].iterator();
        }

        final Iterator<byte[]>[] metricsValItr = new Iterator[metricDataReaders.length];
        for (int i = 0; i < metricDataReaders.length; i++) {
            metricsValItr[i] = metricDataReaders[i].iterator();
        }
        return new Iterator<RawRecord>() {
            @Override
            public boolean hasNext() {
                return readRowCount < rowCount;
            }

            @Override
            public RawRecord next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                for (int i = 0; i < dimensionDataReaders.length; i++) {
                    oneRawRecord.setDimension(i, dimValItr[i].next());
                }
                for (int i = 0; i < metricDataReaders.length; i++) {
                    oneRawRecord.setMetric(i, metricsValItr[i].next());
                }
                readRowCount++;
                return oneRawRecord;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("unSupported");
            }
        };
    }

    public long getReadRowCount() {
        return readRowCount;
    }

    public ColumnDataReader getDimensionDataReader(DimensionMetaInfo dimensionMetaInfo, int numberOfRows,
            TblColRef dimension, DimensionEncoding dimensionEncoding, ByteBuffer dataReadBuffer) {
        ColumnarStoreDimDesc cStoreDimDesc = new ColumnarStoreDimDesc(dimensionEncoding.getLengthOfEncoding(),
                dimensionMetaInfo.getCompressionType());

        return cStoreDimDesc.getDimReader(dataReadBuffer, dimensionMetaInfo.getStartOffset(),
                dimensionMetaInfo.getDataLength(), numberOfRows);
    }

    public ColumnDataReader getMetricsDataReader(MeasureDesc measure, MetricMetaInfo metricMetaInfo, int numberOfRows,
            ByteBuffer dataReadBuffer) {
        DataType type = measure.getFunction().getReturnDataType();

        ColumnarMetricsEncoding metricsEncoding = ColumnarMetricsEncodingFactory.create(type);
        ColumnarStoreMetricsDesc cStoreMetricsDesc = new ColumnarStoreMetricsDesc(metricsEncoding,
                metricMetaInfo.getCompressionType());
        return cStoreMetricsDesc.getMetricsReader(dataReadBuffer, metricMetaInfo.getStartOffset(),
                metricMetaInfo.getMetricLength(), numberOfRows);
    }

}
