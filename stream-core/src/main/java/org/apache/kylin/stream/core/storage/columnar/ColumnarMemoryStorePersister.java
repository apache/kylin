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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionarySerializer;
import org.apache.kylin.dict.IterableDictionaryValueEnumerator;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.storage.columnar.ParsedStreamingCubeInfo.CuboidInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.CuboidMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimDictionaryMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimensionMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.MetricMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.invertindex.FixLenIIColumnDescriptor;
import org.apache.kylin.stream.core.storage.columnar.invertindex.IIColumnDescriptor;
import org.apache.kylin.stream.core.storage.columnar.invertindex.SeqIIColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Collections2;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.shaded.com.google.common.io.CountingOutputStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ColumnarMemoryStorePersister {
    private static Logger logger = LoggerFactory.getLogger(ColumnarMemoryStorePersister.class);
    private CubeDesc cubeDesc;
    private CubeInstance cubeInstance;
    private String segmentName;

    protected final TblColRef[] dimensions;
    protected final MeasureDesc[] measures;
    protected final Set<TblColRef> dimensionsUseDictEncoding;

    protected final long baseCuboidId;

    public ColumnarMemoryStorePersister(ParsedStreamingCubeInfo parsedCubeInfo, String segmentName) {
        this.cubeInstance = parsedCubeInfo.cubeInstance;
        this.cubeDesc = cubeInstance.getDescriptor();
        this.segmentName = segmentName;

        this.baseCuboidId = parsedCubeInfo.basicCuboid.getId();
        this.dimensions = parsedCubeInfo.dimensions;
        this.measures = parsedCubeInfo.measureDescs;
        this.dimensionsUseDictEncoding = Sets.newHashSet(parsedCubeInfo.dimensionsUseDictEncoding);
    }

    /**
     * Build dictionary, Inverted indexes and persist streaming columnar data structure to disk.
     *
     */
    public void persist(SegmentMemoryStore memoryStore, DataSegmentFragment fragment) {
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        logger.info("Start persist memory store for cube:{}, segment:{}, rowCnt:{}", cubeInstance.getName(), segmentName, memoryStore.getRowCount());
        try {
            persistDataFragment(memoryStore, fragment);
            stopwatch.stop();
            logger.info("Finish persist memory store for cube:{} segment:{}, take: {}ms", cubeInstance.getName(),
                    segmentName, stopwatch.elapsed(MILLISECONDS));
        } catch (Exception e) {
            logger.error("Error persist DataSegment.", e);
        }
    }

    private void persistDataFragment(SegmentMemoryStore memoryStore, DataSegmentFragment fragment) throws Exception {
        FragmentMetaInfo fragmentMeta = new FragmentMetaInfo();
        Map<String, CuboidMetaInfo> cuboidMetaInfoMap = Maps.newHashMap();

        fragmentMeta.setFragmentId(fragment.getFragmentId().toString());
        fragmentMeta.setMinEventTime(memoryStore.getMinEventTime());
        fragmentMeta.setMaxEventTime(memoryStore.getMaxEventTime());
        fragmentMeta.setOriginNumOfRows(memoryStore.getOriginRowCount());
        FileOutputStream fragmentOutputStream = FileUtils.openOutputStream(fragment.getDataFile());

        try (CountingOutputStream fragmentOut = new CountingOutputStream(new BufferedOutputStream(fragmentOutputStream))) {
            ConcurrentMap<String[], MeasureAggregator[]> basicCuboidData = memoryStore.getBasicCuboidData();
            List<List<Object>> basicCuboidColumnarValues = transformToColumnar(baseCuboidId, dimensions.length,
                    basicCuboidData);
            // persist dictionaries
            Map<TblColRef, Dictionary<String>> dictMap = buildAndPersistDictionaries(fragmentMeta,
                    basicCuboidColumnarValues, fragmentOut);
            // persist basic cuboid
            CuboidMetaInfo basicCuboidMeta = persistCuboidData(baseCuboidId, dimensions, dictMap,
                    basicCuboidColumnarValues, fragmentOut);

            fragmentMeta.setBasicCuboidMetaInfo(basicCuboidMeta);
            long totalRowCnt = basicCuboidMeta.getNumberOfRows();

            // persist additional cuboids
            Map<CuboidInfo, ConcurrentMap<String[], MeasureAggregator[]>> additionalCuboidsData = memoryStore
                    .getAdditionalCuboidsData();
            if (additionalCuboidsData != null && additionalCuboidsData.size() > 0) {
                for (Entry<CuboidInfo, ConcurrentMap<String[], MeasureAggregator[]>> cuboidDataEntry : additionalCuboidsData
                        .entrySet()) {
                    CuboidInfo cuboidInfo = cuboidDataEntry.getKey();
                    ConcurrentMap<String[], MeasureAggregator[]> cuboidData = cuboidDataEntry.getValue();
                    List<List<Object>> cuboidColumnarValues = transformToColumnar(cuboidInfo.getCuboidID(),
                            cuboidInfo.getDimCount(), cuboidData);
                    CuboidMetaInfo cuboidMeta = persistCuboidData(cuboidInfo.getCuboidID(), cuboidInfo.getDimensions(),
                            dictMap, cuboidColumnarValues, fragmentOut);
                    cuboidMetaInfoMap.put(String.valueOf(cuboidInfo.getCuboidID()), cuboidMeta);
                    totalRowCnt += cuboidMeta.getNumberOfRows();
                }
            }

            fragmentMeta.setNumberOfRows(totalRowCnt);
            fragmentMeta.setCuboidMetaInfoMap(cuboidMetaInfoMap);
        }

        FileOutputStream metaOutputStream = FileUtils.openOutputStream(fragment.getMetaFile());
        JsonUtil.writeValueIndent(metaOutputStream, fragmentMeta);
        metaOutputStream.flush();
        metaOutputStream.close();
    }

    /**
     *  Transform the internal aggBufMap to columnar format which includes all the dimensions and metrics.
     *
     * @return
     */
    private List<List<Object>> transformToColumnar(long cuboidId, int dimCnt,
            ConcurrentMap<String[], MeasureAggregator[]> aggBufMap) {
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        int columnsNum = dimCnt + measures.length;
        List<List<Object>> columnarValues = Lists.newArrayListWithExpectedSize(columnsNum);
        for (int i = 0; i <= columnsNum; i++) {
            List<Object> valueList = Lists.newLinkedList();
            columnarValues.add(valueList);
        }

        for (Entry<String[], MeasureAggregator[]> entry : aggBufMap.entrySet()) {
            String[] row = entry.getKey();
            MeasureAggregator<?>[] measures = entry.getValue();

            for (int i = 0; i < row.length; i++) {
                String cell = row[i];
                List<Object> dimValueList = columnarValues.get(i);
                dimValueList.add(cell); // todo consider null case
            }

            for (int j = 0; j < measures.length; j++) {
                MeasureAggregator<?> measure = measures[j];
                List<Object> measureValueList = columnarValues.get(dimCnt + j);
                measureValueList.add(measure.getState());
            }
        }
        stopwatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("cuboid-{} transform to columnar, take {} ms", cuboidId, stopwatch.elapsed(MILLISECONDS));
        }
        return columnarValues;
    }

    private Map<TblColRef, Dictionary<String>> buildAndPersistDictionaries(FragmentMetaInfo fragmentMetaInfo,
            List<List<Object>> allColumnarValues, CountingOutputStream fragmentOut) throws IOException {
        Map<TblColRef, Dictionary<String>> dictMaps = Maps.newHashMap();
        List<DimDictionaryMetaInfo> dimDictionaryMetaInfos = Lists.newArrayList();
        for (int i = 0; i < dimensions.length; i++) {
            TblColRef dimension = dimensions[i];
            List<Object> dimValueList = allColumnarValues.get(i);
            Dictionary<String> dict;
            DimDictionaryMetaInfo dimDictionaryMetaInfo = new DimDictionaryMetaInfo();
            if (dimensionsUseDictEncoding.contains(dimension)) {
                dict = buildDictionary(dimension, dimValueList);
                dictMaps.put(dimension, dict);

                dimDictionaryMetaInfo.setDimName(dimension.getName());
                dimDictionaryMetaInfo.setDictType(dict.getClass().getName());
                dimDictionaryMetaInfo.setStartOffset((int) fragmentOut.getCount());

                DictionarySerializer.serialize(dict, fragmentOut);
                dimDictionaryMetaInfo.setDictLength((int) fragmentOut.getCount()
                        - dimDictionaryMetaInfo.getStartOffset());
                dimDictionaryMetaInfos.add(dimDictionaryMetaInfo);
            }
        }
        fragmentMetaInfo.setDimDictionaryMetaInfos(dimDictionaryMetaInfos);
        return dictMaps;
    }

    private CuboidMetaInfo persistCuboidData(long cuboidID, TblColRef[] dimensions,
            Map<TblColRef, Dictionary<String>> dictMaps, List<List<Object>> columnarCuboidValues,
            CountingOutputStream fragmentOutput) throws Exception {
        CuboidMetaInfo cuboidMeta = new CuboidMetaInfo();
        int dimCnt = dimensions.length;
        List<DimensionMetaInfo> dimensionMetaList = Lists.newArrayListWithExpectedSize(dimCnt);
        cuboidMeta.setDimensionsInfo(dimensionMetaList);
        cuboidMeta.setNumberOfDim(dimCnt);
        List<MetricMetaInfo> metricMetaInfoList = Lists.newArrayListWithCapacity(measures.length);
        cuboidMeta.setMetricsInfo(metricMetaInfoList);
        cuboidMeta.setNumberOfMetrics(measures.length);

        long rowNum = -1;
        for (int i = 0; i < dimCnt; i++) {
            if (rowNum == -1) {
                rowNum = columnarCuboidValues.get(i).size();
            }
            persistDimension(cuboidID, columnarCuboidValues.get(i), dimensionMetaList, fragmentOutput,
                    dimensions[i], dictMaps);
        }

        for (int i = 0; i < measures.length; i++) {
            persistMetric(cuboidID, columnarCuboidValues.get(dimCnt + i), metricMetaInfoList, i, fragmentOutput);
        }
        cuboidMeta.setNumberOfRows(rowNum);
        return cuboidMeta;
    }

    /**
     * This method is used to persist the dimension data to disk file, first part is the dictionary, second part is the dimension value, third part is the index.
     *
     * @param dimValueList
     * @param dimensionMetaList
     * @param indexOut
     * @param dimension
     * @param dictMaps
     * @throws IOException
     */
    private void persistDimension(long cuboidId, List<Object> dimValueList, List<DimensionMetaInfo> dimensionMetaList,
            CountingOutputStream indexOut, TblColRef dimension, Map<TblColRef, Dictionary<String>> dictMaps)
            throws IOException {
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();

        DimensionMetaInfo dimensionMeta = new DimensionMetaInfo();
        dimensionMetaList.add(dimensionMeta);

        DimensionEncoding encoding;
        IIColumnDescriptor columnDescriptor;
        if (dimensionsUseDictEncoding.contains(dimension)) {
            Dictionary<String> dict = dictMaps.get(dimension);
            encoding = new DictionaryDimEnc(dict);
            if (dict instanceof TrieDictionary) {
                columnDescriptor = new SeqIIColumnDescriptor(dimension.getName(), dict.getMinId(), dict.getMaxId());
            } else {
                columnDescriptor = new FixLenIIColumnDescriptor(dimension.getName(), encoding.getLengthOfEncoding());
            }
        } else {
            RowKeyColDesc colDesc = cubeDesc.getRowkey().getColDesc(dimension);
            encoding = DimensionEncodingFactory.create(colDesc.getEncodingName(), colDesc.getEncodingArgs(),
                    colDesc.getEncodingVersion());
            columnDescriptor = new FixLenIIColumnDescriptor(dimension.getName(), encoding.getLengthOfEncoding());
        }
        dimensionMeta.setName(dimension.getName());
        dimensionMeta.setStartOffset((int) indexOut.getCount());
        int fixEncodingLen = encoding.getLengthOfEncoding();

        DataOutputStream dataOut = new DataOutputStream(indexOut);
        ColumnarStoreDimDesc cStoreDimDesc = getColumnarStoreDimDesc(dimension, encoding);
        ColumnDataWriter columnDataWriter = cStoreDimDesc.getDimWriter(dataOut, dimValueList.size());

        //Raw values are stored on disk files with fixed length encoding to make it easy for inverted index to search and scan.
        for (Object cell : dimValueList) {
            byte[] fixLenBytes = new byte[fixEncodingLen];
            if (cell != null) {
                encoding.encode((String) cell, fixLenBytes, 0);
            } else {
                encoding.encode(null, fixLenBytes, 0);
                dimensionMeta.setHasNull(true);
            }
            columnDescriptor.getWriter().addValue(fixLenBytes);
            columnDataWriter.write(fixLenBytes);
        }
        columnDataWriter.flush();
        dimensionMeta.setDataLength(dataOut.size());
        columnDescriptor.getWriter().write(indexOut);
        dimensionMeta.setIndexLength((int) indexOut.getCount() - dimensionMeta.getStartOffset()
                - dimensionMeta.getDataLength());
        dimensionMeta.setCompression(cStoreDimDesc.getCompression().name());

        stopwatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("cuboid-{} saved dimension:{}, took: {}ms", cuboidId, dimension.getName(),
                    stopwatch.elapsed(MILLISECONDS));
        }
    }

    private ColumnarStoreDimDesc getColumnarStoreDimDesc(TblColRef dimension, DimensionEncoding encoding) {
        return ColumnarStoreDimDesc.getDefaultCStoreDimDesc(cubeDesc, dimension.getName(), encoding);
    }

    /**
     * This method is used to persist the metrics data to disk file.
     *
     * @param metricValueList
     * @param metricMetaInfoList
     * @param indexOut
     * @throws IOException
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void persistMetric(long cuboidId, List<Object> metricValueList, List<MetricMetaInfo> metricMetaInfoList,
            int metricIdx, CountingOutputStream indexOut) throws IOException {
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();

        MetricMetaInfo metricMeta = new MetricMetaInfo();
        metricMetaInfoList.add(metricMeta);
        String measureName = measures[metricIdx].getName();
        metricMeta.setName(measureName);
        metricMeta.setCol(metricIdx);
        metricMeta.setStartOffset((int) indexOut.getCount());

        DataType type = measures[metricIdx].getFunction().getReturnDataType();

        ColumnarMetricsEncoding metricsEncoding = ColumnarMetricsEncodingFactory.create(type);
        DataTypeSerializer serializer = metricsEncoding.asDataTypeSerializer();
        DataOutputStream metricsOut = new DataOutputStream(indexOut);

        int maxLength = serializer.maxLength();
        metricMeta.setMaxSerializeLength(maxLength);
        ByteBuffer metricsBuf = ByteBuffer.allocate(maxLength);
        ColumnarStoreMetricsDesc cStoreMetricsDesc = getColumnarStoreMetricsDesc(metricsEncoding);
        ColumnDataWriter metricsWriter = cStoreMetricsDesc.getMetricsWriter(metricsOut, metricValueList.size());
//        metricMeta.setStoreInFixedLength(false);
        for (Object metricValue : metricValueList) {
            metricsBuf.clear();
            serializer.serialize(metricValue, metricsBuf);
            byte[] metricBytes = Arrays.copyOf(metricsBuf.array(), metricsBuf.position());
            metricsWriter.write(metricBytes);
        }
        metricsWriter.flush();
        metricMeta.setMetricLength(metricsOut.size());
        metricMeta.setCompression(cStoreMetricsDesc.getCompression().name());
        stopwatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("cuboid-{} saved measure:{}, took: {}ms", cuboidId, measureName, stopwatch.elapsed(MILLISECONDS));
        }
    }

    private ColumnarStoreMetricsDesc getColumnarStoreMetricsDesc(ColumnarMetricsEncoding metricsEncoding) {
        return ColumnarStoreMetricsDesc.getDefaultCStoreMetricsDesc(metricsEncoding);
    }

    private Dictionary<String> buildDictionary(TblColRef dim, List<Object> inputValues) throws IOException {
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        final Collection<String> values = Collections2.transform(Sets.newHashSet(inputValues),
                new Function<Object, String>() {
                    @Nullable
                    @Override
                    public String apply(Object input) {
                        String value = (String) input;
                        return value;
                    }
                });
        final Dictionary<String> dict = DictionaryGenerator.buildDictionary(dim.getType(),
                new IterableDictionaryValueEnumerator(values));
        stopwatch.stop();
        if (logger.isDebugEnabled()) {
            logger.debug("BuildDictionary for column : " + dim.getName() + " took : " + stopwatch.elapsed(MILLISECONDS)
                    + " ms ");
        }
        return dict;
    }
}
