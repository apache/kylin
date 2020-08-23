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

package org.apache.kylin.engine.mr.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dict.DictionarySerializer;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.storage.columnar.ColumnDataReader;
import org.apache.kylin.stream.core.storage.columnar.ColumnarMetricsEncoding;
import org.apache.kylin.stream.core.storage.columnar.ColumnarMetricsEncodingFactory;
import org.apache.kylin.stream.core.storage.columnar.ColumnarStoreDimDesc;
import org.apache.kylin.stream.core.storage.columnar.ColumnarStoreMetricsDesc;
import org.apache.kylin.stream.core.storage.columnar.protocol.CuboidMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimDictionaryMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimensionMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.MetricMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableMap;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class RowRecordReader extends ColumnarFilesReader {
    private static final Logger logger = LoggerFactory.getLogger(RowRecordReader.class);

    private List<ColumnDataReader> dimensionColumnReaders;
    private List<ColumnDataReader> metricsColumnReaders;
    private List<Iterator<byte[]>> dimensionColumnReaderItrs;
    private List<Iterator<byte[]>> metricsColumnReaderItrs;
    private List<DimensionEncoding> dimensionEncodings;
    private List<MetricsDataTransformer> metricsDataTransformers;

    private CubeDesc cubeDesc;
    private String[] rowDimensionValues;
    private byte[][] rowMetricsValues;
    private RowRecord currentRowRecord;

    public RowRecordReader(CubeDesc cubeDesc, Path path, FileSystem fileSystem) throws IOException {
        super(fileSystem, path);
        this.cubeDesc = cubeDesc;
        this.folderPath = path;
        this.fs = fileSystem;
        this.currentRowRecord = new RowRecord();
        initReaders();
    }

    public void initReaders() throws IOException {
        List<DimensionMetaInfo> allDimensions;
        CuboidMetaInfo basicCuboidMetaInfo;
        Map<String, DimensionEncoding> dimensionEncodingMap;
        try (FSDataInputStream in = fs.open(metaFilePath)) {
            FragmentMetaInfo fragmentMetaInfo = JsonUtil.readValue(in, FragmentMetaInfo.class);
            basicCuboidMetaInfo = fragmentMetaInfo.getBasicCuboidMetaInfo();
            allDimensions = basicCuboidMetaInfo.getDimensionsInfo();
            dimensionEncodingMap = getDimensionEncodings(fragmentMetaInfo, allDimensions, dataFilePath);
        }

        dimensionColumnReaders = Lists.newArrayList();
        dimensionColumnReaderItrs = Lists.newArrayList();
        dimensionEncodings = Lists.newArrayList();
        for (DimensionMetaInfo dimensionMetaInfo : allDimensions) {
            String dimName = dimensionMetaInfo.getName();
            DimensionEncoding dimEncoding = dimensionEncodingMap.get(dimName);
            ColumnarStoreDimDesc dimDesc = new ColumnarStoreDimDesc(dimEncoding.getLengthOfEncoding(),
                    dimensionMetaInfo.getCompressionType());
            ColumnDataReader dimDataReader = dimDesc.getDimReaderFromFSInput(fs, dataFilePath,
                    dimensionMetaInfo.getStartOffset(), dimensionMetaInfo.getDataLength(),
                    (int) basicCuboidMetaInfo.getNumberOfRows());
            dimensionColumnReaders.add(dimDataReader);
            dimensionColumnReaderItrs.add(dimDataReader.iterator());
            dimensionEncodings.add(dimEncoding);
        }
        rowDimensionValues = new String[dimensionColumnReaders.size()];

        metricsColumnReaders = Lists.newArrayList();
        metricsColumnReaderItrs = Lists.newArrayList();
        metricsDataTransformers = Lists.newArrayList();
        for (MetricMetaInfo metricMetaInfo : basicCuboidMetaInfo.getMetricsInfo()) {

            MeasureDesc measure = findMeasure(metricMetaInfo.getName());
            DataType metricsDataType = measure.getFunction().getReturnDataType();
            ColumnarMetricsEncoding metricsEncoding = ColumnarMetricsEncodingFactory.create(metricsDataType);
            ColumnarStoreMetricsDesc metricsDesc = new ColumnarStoreMetricsDesc(metricsEncoding,
                    metricMetaInfo.getCompressionType());
            ColumnDataReader metricsDataReader = metricsDesc.getMetricsReaderFromFSInput(fs, dataFilePath,
                    metricMetaInfo.getStartOffset(), metricMetaInfo.getMetricLength(),
                    (int) basicCuboidMetaInfo.getNumberOfRows());
            metricsColumnReaders.add(metricsDataReader);
            metricsColumnReaderItrs.add(metricsDataReader.iterator());
            metricsDataTransformers.add(new MetricsDataTransformer(metricsEncoding.asDataTypeSerializer(),
                    DataTypeSerializer.create(metricsDataType)));
        }
        rowMetricsValues = new byte[metricsColumnReaders.size()][];
    }

    private MeasureDesc findMeasure(String name) {
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        for (MeasureDesc measure : measures) {
            if (name.equals(measure.getName())) {
                return measure;
            }
        }
        return null;
    }

    private Map<String, DimensionEncoding> getDimensionEncodings(FragmentMetaInfo fragmentMetaInfo,
            List<DimensionMetaInfo> allDimensions, Path dataPath) throws IOException {
        Map<String, Dictionary> dictionaryMap;
        try (FSDataInputStream dictInputStream = fs.open(dataPath)) {
            dictionaryMap = readAllDimensionsDictionary(fragmentMetaInfo, dictInputStream);
        }

        Map<String, DimensionEncoding> result = Maps.newHashMap();
        for (DimensionMetaInfo dimension : allDimensions) {
            TblColRef col = cubeDesc.getModel().findColumn(dimension.getName());
            RowKeyColDesc colDesc = cubeDesc.getRowkey().getColDesc(col);
            if (colDesc.isUsingDictionary()) {
                @SuppressWarnings({ "unchecked" })
                Dictionary<String> dict = dictionaryMap.get(dimension.getName());
                if (dict == null) {
                    logger.error("No dictionary found for dict-encoding column " + col);
                    throw new IllegalStateException("No dictionary found for dict-encoding column " + col);
                } else {
                    result.put(dimension.getName(), new DictionaryDimEnc(dict));
                }
            } else {
                result.put(
                        dimension.getName(),
                        DimensionEncodingFactory.create(colDesc.getEncodingName(), colDesc.getEncodingArgs(),
                                colDesc.getEncodingVersion()));
            }
        }
        return result;
    }

    public Map<String, Dictionary> readAllDimensionsDictionary(FragmentMetaInfo fragmentMetaInfo,
            FSDataInputStream dataInputStream) throws IOException {
        List<DimDictionaryMetaInfo> dimDictMetaInfos = fragmentMetaInfo.getDimDictionaryMetaInfos();
        ImmutableMap.Builder<String, Dictionary> builder = ImmutableMap.builder();
        for (DimDictionaryMetaInfo dimDictMetaInfo : dimDictMetaInfos) {
            dataInputStream.seek(dimDictMetaInfo.getStartOffset());
            Dictionary dict = DictionarySerializer.deserialize(dataInputStream);
            builder.put(dimDictMetaInfo.getDimName(), dict);
        }
        return builder.build();
    }

    public boolean hasNextRow() {
        if (hasNextDimensionsRow() && hasNextMetricsRow()) {
            currentRowRecord.setDimensions(rowDimensionValues);
            currentRowRecord.setMetrics(rowMetricsValues);
            return true;
        }
        return false;
    }

    public RowRecord nextRow() {
        return currentRowRecord;
    }

    private boolean hasNextDimensionsRow() {
        for (int i = 0; i < dimensionColumnReaders.size(); i++) {
            Iterator<byte[]> itr = dimensionColumnReaderItrs.get(i);
            if (!itr.hasNext()) {
                return false;
            }
            byte[] colValue = itr.next();
            if (colValue == null) {
                rowDimensionValues[i] = null;
            } else {
                rowDimensionValues[i] = dimensionEncodings.get(i).decode(colValue, 0, colValue.length);
            }
        }
        return true;
    }

    private boolean hasNextMetricsRow() {
        for (int i = 0; i < metricsColumnReaders.size(); i++) {
            Iterator<byte[]> itr = metricsColumnReaderItrs.get(i);
            if (!itr.hasNext()) {
                return false;
            }
            byte[] colValue = itr.next();
            if (colValue == null) {
                rowMetricsValues[i] = null;
            } else {
                rowMetricsValues[i] = metricsDataTransformers.get(i).transformFromColumnarMetrics(colValue);
            }
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        for (ColumnDataReader dimensionColumnReader : dimensionColumnReaders) {
            dimensionColumnReader.close();
        }
        for (ColumnDataReader metricsColumnReader : metricsColumnReaders) {
            metricsColumnReader.close();
        }
    }

    public static class MetricsDataTransformer {
        private DataTypeSerializer columnarMetricsSerializer;
        private DataTypeSerializer rowBasedMetricsSerializer;
        private boolean isSame = false;
        private ByteBuffer byteBuffer;

        public MetricsDataTransformer(DataTypeSerializer columnarMetricsSerializer,
                DataTypeSerializer rowBasedMetricsSerializer) {
            this.columnarMetricsSerializer = columnarMetricsSerializer;
            this.rowBasedMetricsSerializer = rowBasedMetricsSerializer;
            if (columnarMetricsSerializer.getClass() == rowBasedMetricsSerializer.getClass()) {
                isSame = true;
            } else {
                byteBuffer = ByteBuffer.allocate(rowBasedMetricsSerializer.maxLength());
            }
        }

        public byte[] transformFromColumnarMetrics(byte[] metricsValue) {
            if (isSame) {
                return metricsValue;
            }
            byteBuffer.clear();
            Object val = columnarMetricsSerializer.deserialize(ByteBuffer.wrap(metricsValue));
            rowBasedMetricsSerializer.serialize(val, byteBuffer);
            return Arrays.copyOf(byteBuffer.array(), byteBuffer.position());
        }
    }
}
