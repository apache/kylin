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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionarySerializer;
import org.apache.kylin.dict.MultipleDictionaryValueEnumerator;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.storage.columnar.ParsedStreamingCubeInfo.CuboidInfo;
import org.apache.kylin.stream.core.storage.columnar.invertindex.FixLenIIColumnDescriptor;
import org.apache.kylin.stream.core.storage.columnar.invertindex.IIColumnDescriptor;
import org.apache.kylin.stream.core.storage.columnar.invertindex.SeqIIColumnDescriptor;
import org.apache.kylin.stream.core.storage.columnar.protocol.CuboidMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimDictionaryMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.DimensionMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.MetricMetaInfo;
import org.apache.kylin.stream.core.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.io.ByteStreams;
import org.apache.kylin.shaded.com.google.common.io.CountingOutputStream;

public class FragmentFilesMerger {
    private static Logger logger = LoggerFactory.getLogger(FragmentFilesMerger.class);

    private ParsedStreamingCubeInfo parsedCubeInfo;
    private File segmentFolder;

    private File mergeWorkingDirectory;

    public FragmentFilesMerger(ParsedStreamingCubeInfo parsedCubeInfo, File segmentFolder) {
        this.parsedCubeInfo = parsedCubeInfo;
        this.segmentFolder = segmentFolder;
        this.mergeWorkingDirectory = new File(segmentFolder, ".merge-" + System.currentTimeMillis());
    }

    public FragmentsMergeResult merge(List<DataSegmentFragment> fragmentList) throws IOException {
        if (!mergeWorkingDirectory.exists()) {
            mergeWorkingDirectory.mkdirs();
        } else {
            logger.info("clean the merge working dir:{}", mergeWorkingDirectory.getAbsolutePath());
            FileUtils.cleanDirectory(mergeWorkingDirectory);
        }

        Collections.sort(fragmentList);
        FragmentId mergedFragmentId = new FragmentId(fragmentList.get(0).getFragmentId().getStartId(),
                fragmentList.get(fragmentList.size() - 1).getFragmentId().getEndId());
        List<FragmentData> fragmentDataList = Lists.newArrayList();
        Map<TblColRef, List<Dictionary<String>>> dimDictListMap = Maps.newHashMap();
        Map<FragmentId, Map<TblColRef, Dictionary<String>>> fragmentDictionaryMaps = Maps.newHashMap();
        List<Long> additionalCuboidsToMerge = null;
        long minMergedFragmentEventTime = Long.MAX_VALUE;
        long maxMergedFragmentEventTime = 0;
        long originNumOfRows = 0;
        for (DataSegmentFragment fragment : fragmentList) {
            FragmentData fragmentData = ColumnarStoreCache.getInstance().startReadFragmentData(fragment);
            FragmentMetaInfo fragmentMetaInfo = fragmentData.getFragmentMetaInfo();
            long fragmentMinTime = fragmentMetaInfo.getMinEventTime();
            long fragmentMaxTime = fragmentMetaInfo.getMaxEventTime();
            originNumOfRows += fragmentMetaInfo.getOriginNumOfRows();
            if (fragmentMinTime < minMergedFragmentEventTime) {
                minMergedFragmentEventTime = fragmentMinTime;
            }
            if (fragmentMaxTime > maxMergedFragmentEventTime) {
                maxMergedFragmentEventTime = fragmentMaxTime;
            }
            if (additionalCuboidsToMerge == null) {
                Map<String, CuboidMetaInfo> cuboidMetaInfoMap = fragmentMetaInfo.getCuboidMetaInfoMap();
                if (cuboidMetaInfoMap != null) {
                    additionalCuboidsToMerge = Lists.transform(Lists.newArrayList(cuboidMetaInfoMap.keySet()),
                            new Function<String, Long>() {
                                @Nullable
                                @Override
                                public Long apply(@Nullable String input) {
                                    return Long.valueOf(input);
                                }
                            });
                } else {
                    additionalCuboidsToMerge = Lists.newArrayList();
                }
            }
            fragmentDataList.add(fragmentData);
            Map<TblColRef, Dictionary<String>> dictionaryMap = fragmentData
                    .getDimensionDictionaries(parsedCubeInfo.dimensionsUseDictEncoding);
            fragmentDictionaryMaps.put(fragment.getFragmentId(), dictionaryMap);
            for (Entry<TblColRef, Dictionary<String>> dimDictEntry : dictionaryMap.entrySet()) {
                List<Dictionary<String>> dictionaryList = dimDictListMap.get(dimDictEntry.getKey());
                if (dictionaryList == null) {
                    dictionaryList = Lists.newArrayList();
                    dimDictListMap.put(dimDictEntry.getKey(), dictionaryList);
                }
                dictionaryList.add(dimDictEntry.getValue());
            }
        }
        File mergedFragmentDataFile = new File(mergeWorkingDirectory, mergedFragmentId + Constants.DATA_FILE_SUFFIX);
        File mergedFragmentMetaFile = new File(mergeWorkingDirectory, mergedFragmentId + Constants.META_FILE_SUFFIX);
        try {
            FragmentMetaInfo mergedFragmentMeta = new FragmentMetaInfo();
            CountingOutputStream fragmentDataOutput = new CountingOutputStream(
                    new BufferedOutputStream(FileUtils.openOutputStream(mergedFragmentDataFile)));
            // merge dictionaries
            Map<TblColRef, Dictionary<String>> mergedDictMap = mergeAndPersistDictionaries(mergedFragmentMeta,
                    dimDictListMap, fragmentDataOutput);

            // merge basicCuboid
            logger.info("merge basic cuboid");
            CuboidMetaInfo basicCuboidMeta = mergeAndPersistCuboidData(fragmentDataList, fragmentDictionaryMaps,
                    mergedDictMap, fragmentDataOutput, parsedCubeInfo.basicCuboid.getId());
            mergedFragmentMeta.setBasicCuboidMetaInfo(basicCuboidMeta);
            long totalRowCnt = basicCuboidMeta.getNumberOfRows();
            // merge additional cuboids
            Map<String, CuboidMetaInfo> cuboidMetaInfoMap = Maps.newHashMap();
            for (Long cuboidId : additionalCuboidsToMerge) {
                logger.info("merge cuboid:{}", cuboidId);
                CuboidMetaInfo cuboidMeta = mergeAndPersistCuboidData(fragmentDataList, fragmentDictionaryMaps,
                        mergedDictMap, fragmentDataOutput, cuboidId);
                cuboidMetaInfoMap.put(String.valueOf(cuboidId), cuboidMeta);
                totalRowCnt += cuboidMeta.getNumberOfRows();
            }
            mergedFragmentMeta.setMaxEventTime(maxMergedFragmentEventTime);
            mergedFragmentMeta.setMinEventTime(minMergedFragmentEventTime);
            mergedFragmentMeta.setCuboidMetaInfoMap(cuboidMetaInfoMap);
            mergedFragmentMeta.setFragmentId(mergedFragmentId.toString());
            mergedFragmentMeta.setNumberOfRows(totalRowCnt);
            mergedFragmentMeta.setOriginNumOfRows(originNumOfRows);
            fragmentDataOutput.flush();
            fragmentDataOutput.close();

            FileOutputStream metaOutputStream = FileUtils.openOutputStream(mergedFragmentMetaFile);
            JsonUtil.writeValueIndent(metaOutputStream, mergedFragmentMeta);
            metaOutputStream.flush();
            metaOutputStream.close();
        } finally {
            for (DataSegmentFragment fragment : fragmentList) {
                ColumnarStoreCache.getInstance().finishReadFragmentData(fragment);
            }
        }
        FragmentsMergeResult result = new FragmentsMergeResult(fragmentList, mergedFragmentId, mergedFragmentMetaFile,
                mergedFragmentDataFile);
        return result;
    }

    public void cleanMergeDirectory() {
        FileUtils.deleteQuietly(mergeWorkingDirectory);
    }

    private Map<TblColRef, Dictionary<String>> mergeAndPersistDictionaries(FragmentMetaInfo fragmentMetaInfo,
            Map<TblColRef, List<Dictionary<String>>> dimDictListMap, CountingOutputStream fragmentOut)
            throws IOException {
        logger.info("merge dimension dictionaries");
        Map<TblColRef, Dictionary<String>> mergedDictMap = Maps.newHashMap();
        List<DimDictionaryMetaInfo> dimDictionaryMetaInfos = Lists.newArrayList();
        for (TblColRef dimension : parsedCubeInfo.dimensionsUseDictEncoding) {
            List<Dictionary<String>> dicts = dimDictListMap.get(dimension);
            MultipleDictionaryValueEnumerator multipleDictionaryValueEnumerator = new MultipleDictionaryValueEnumerator(
                    dimension.getType(), dicts);
            Dictionary<String> mergedDict = DictionaryGenerator.buildDictionary(dimension.getType(),
                    multipleDictionaryValueEnumerator);
            mergedDictMap.put(dimension, mergedDict);

            DimDictionaryMetaInfo dimDictionaryMetaInfo = new DimDictionaryMetaInfo();
            dimDictionaryMetaInfo.setDimName(dimension.getName());
            dimDictionaryMetaInfo.setDictType(mergedDict.getClass().getName());
            dimDictionaryMetaInfo.setStartOffset((int) fragmentOut.getCount());

            DictionarySerializer.serialize(mergedDict, fragmentOut);
            dimDictionaryMetaInfo.setDictLength((int) fragmentOut.getCount() - dimDictionaryMetaInfo.getStartOffset());
            dimDictionaryMetaInfos.add(dimDictionaryMetaInfo);
        }
        fragmentMetaInfo.setDimDictionaryMetaInfos(dimDictionaryMetaInfos);
        return mergedDictMap;
    }

    private CuboidMetaInfo mergeAndPersistCuboidData(List<FragmentData> fragmentDataList,
            Map<FragmentId, Map<TblColRef, Dictionary<String>>> fragmentDictionaryMaps,
            Map<TblColRef, Dictionary<String>> mergedDictMap, CountingOutputStream fragmentDataOutput, long cuboidId)
            throws IOException {
        List<FragmentCuboidReader> fragmentCuboidReaders = Lists.newArrayList();
        List<DimensionEncoding[]> fragmentsDimensionEncodings = Lists.newArrayList();

        CuboidInfo cuboidInfo = parsedCubeInfo.getCuboidInfo(cuboidId);
        TblColRef[] dimensions = cuboidInfo.getDimensions();
        int dimCount = dimensions.length;

        for (FragmentData fragmentData : fragmentDataList) {
            FragmentMetaInfo fragmentMetaInfo = fragmentData.getFragmentMetaInfo();
            CuboidMetaInfo cuboidMetaInfo;
            if (cuboidId == parsedCubeInfo.basicCuboid.getId()) {
                cuboidMetaInfo = fragmentMetaInfo.getBasicCuboidMetaInfo();
            } else {
                cuboidMetaInfo = fragmentMetaInfo.getCuboidMetaInfo(cuboidId);
            }
            Map<TblColRef, Dictionary<String>> dictMap = fragmentDictionaryMaps
                    .get(FragmentId.parse(fragmentMetaInfo.getFragmentId()));
            DimensionEncoding[] dimensionEncodings = ParsedStreamingCubeInfo
                    .getDimensionEncodings(parsedCubeInfo.cubeDesc, dimensions, dictMap);
            FragmentCuboidReader fragmentCuboidReader = new FragmentCuboidReader(parsedCubeInfo.cubeDesc, fragmentData,
                    cuboidMetaInfo, cuboidInfo.getDimensions(), parsedCubeInfo.measureDescs, dimensionEncodings);
            fragmentCuboidReaders.add(fragmentCuboidReader);
            fragmentsDimensionEncodings.add(dimensionEncodings);
        }
        MeasureAggregators measureAggregators = new MeasureAggregators(parsedCubeInfo.measureDescs);

        DimensionEncoding[] mergedDimEncodings = ParsedStreamingCubeInfo.getDimensionEncodings(parsedCubeInfo.cubeDesc,
                cuboidInfo.getDimensions(), mergedDictMap);

        IIColumnDescriptor[] invertIndexColDescs = new IIColumnDescriptor[dimCount];
        for (int i = 0; i < mergedDimEncodings.length; i++) {
            TblColRef dim = dimensions[i];
            DimensionEncoding encoding = mergedDimEncodings[i];
            if (encoding instanceof DictionaryDimEnc) {
                DictionaryDimEnc dictDimEnc = (DictionaryDimEnc) encoding;
                Dictionary<String> dict = dictDimEnc.getDictionary();
                if (dict instanceof TrieDictionary) {
                    invertIndexColDescs[i] = new SeqIIColumnDescriptor(dim.getName(), dict.getMinId(), dict.getMaxId());
                } else {
                    invertIndexColDescs[i] = new FixLenIIColumnDescriptor(dim.getName(),
                            encoding.getLengthOfEncoding());
                }
            } else {
                invertIndexColDescs[i] = new FixLenIIColumnDescriptor(dim.getName(), encoding.getLengthOfEncoding());
            }
        }

        CuboidColumnDataWriter[] dimDataWriters = new CuboidColumnDataWriter[dimCount];
        CuboidMetricDataWriter[] metricDataWriters = new CuboidMetricDataWriter[parsedCubeInfo.measureCount];
        ColumnarMetricsEncoding[] metricsEncodings = new ColumnarMetricsEncoding[parsedCubeInfo.measureCount];

        for (int i = 0; i < dimDataWriters.length; i++) {
            dimDataWriters[i] = new CuboidColumnDataWriter(cuboidId, dimensions[i].getName());
        }

        for (int i = 0; i < metricDataWriters.length; i++) {
            metricDataWriters[i] = new CuboidMetricDataWriter(cuboidId, parsedCubeInfo.measureDescs[i].getName(),
                    parsedCubeInfo.getMeasureTypeSerializer(i).maxLength());
            metricsEncodings[i] = ColumnarMetricsEncodingFactory
                    .create(parsedCubeInfo.measureDescs[i].getFunction().getReturnDataType());
        }

        FragmentCuboidDataMerger fragmentCuboidDataMerger = new FragmentCuboidDataMerger(cuboidInfo,
                fragmentCuboidReaders, fragmentsDimensionEncodings, mergedDimEncodings, measureAggregators,
                metricsEncodings);

        logger.info("start to merge and write dimension data");
        int rowCnt = 0;
        while (fragmentCuboidDataMerger.hasNext()) {
            RawRecord rawRecord = fragmentCuboidDataMerger.next();
            for (int i = 0; i < rawRecord.getDimensions().length; i++) {
                byte[] bytes = rawRecord.getDimensions()[i];
                dimDataWriters[i].write(bytes);
            }
            for (int i = 0; i < rawRecord.getMetrics().length; i++) {
                metricDataWriters[i].write(rawRecord.getMetrics()[i]);
            }
            rowCnt++;
        }
        for (int i = 0; i < dimDataWriters.length; i++) {
            dimDataWriters[i].close();
        }
        for (int i = 0; i < metricDataWriters.length; i++) {
            metricDataWriters[i].close();
        }
        logger.info("all dimensions data wrote to separate file");
        logger.info("start to merge dimension data and build invert index");

        CuboidMetaInfo cuboidMeta = new CuboidMetaInfo();
        cuboidMeta.setNumberOfRows(rowCnt);
        cuboidMeta.setNumberOfDim(dimCount);
        cuboidMeta.setNumberOfMetrics(parsedCubeInfo.measureCount);
        List<DimensionMetaInfo> dimensionMetaList = Lists.newArrayList();
        List<MetricMetaInfo> metricMetaList = Lists.newArrayList();
        cuboidMeta.setDimensionsInfo(dimensionMetaList);
        cuboidMeta.setMetricsInfo(metricMetaList);

        for (int i = 0; i < dimDataWriters.length; i++) {
            DimensionEncoding encoding = mergedDimEncodings[i];
            int dimFixLen = encoding.getLengthOfEncoding();
            InputStream dimInput = new BufferedInputStream(
                    FileUtils.openInputStream(dimDataWriters[i].getOutputFile()));
            try {
                DimensionMetaInfo dimensionMeta = new DimensionMetaInfo();
                dimensionMeta.setName(dimensions[i].getName());
                int startOffset = (int) fragmentDataOutput.getCount();
                dimensionMeta.setStartOffset(startOffset);

                ColumnarStoreDimDesc cStoreDimDesc = ColumnarStoreDimDesc
                        .getDefaultCStoreDimDesc(parsedCubeInfo.cubeDesc, dimensions[i].getName(), encoding);
                ColumnDataWriter columnDataWriter = cStoreDimDesc.getDimWriter(fragmentDataOutput, rowCnt);
                for (int j = 0; j < rowCnt; j++) {
                    byte[] dimValue = new byte[dimFixLen];

                    int offset = 0;
                    int bytesRead;
                    while ((bytesRead = dimInput.read(dimValue, offset, dimValue.length - offset)) != -1) {
                        offset += bytesRead;
                        if (offset >= dimValue.length) {
                            break;
                        }
                    }

                    if (DimensionEncoding.isNull(dimValue, 0, dimValue.length)) {
                        dimensionMeta.setHasNull(true);
                    }
                    invertIndexColDescs[i].getWriter().addValue(dimValue);
                    columnDataWriter.write(dimValue);
                }
                columnDataWriter.flush();
                int dimLen = (int) fragmentDataOutput.getCount() - startOffset;
                dimensionMeta.setDataLength(dimLen);
                invertIndexColDescs[i].getWriter().write(fragmentDataOutput);
                dimensionMeta.setIndexLength((int) fragmentDataOutput.getCount() - startOffset - dimLen);
                dimensionMeta.setCompression(cStoreDimDesc.getCompression().name());
                dimensionMetaList.add(dimensionMeta);
            } finally {
                if (null != dimInput) {
                    dimInput.close();
                }
            }
        }

        for (int i = 0; i < metricDataWriters.length; i++) {
            DataInputStream metricInput = new DataInputStream(
                    new BufferedInputStream(FileUtils.openInputStream(metricDataWriters[i].getOutputFile())));
            try {
                ColumnarMetricsEncoding metricsEncoding = ColumnarMetricsEncodingFactory
                        .create(parsedCubeInfo.measureDescs[i].getFunction().getReturnDataType());
                ColumnarStoreMetricsDesc cStoreMetricsDesc = ColumnarStoreMetricsDesc
                        .getDefaultCStoreMetricsDesc(metricsEncoding);
                ColumnDataWriter columnDataWriter = cStoreMetricsDesc.getMetricsWriter(fragmentDataOutput, rowCnt);
                MetricMetaInfo metricMeta = new MetricMetaInfo();
                metricMeta.setName(parsedCubeInfo.measureDescs[i].getName());
                int startOffset = (int) fragmentDataOutput.getCount();
                metricMeta.setStartOffset(startOffset);
                for (int j = 0; j < rowCnt; j++) {
                    int metricLen = metricInput.readInt();
                    byte[] metricValue = new byte[metricLen];

                    int offset = 0;
                    int bytesRead;
                    while ((bytesRead = metricInput.read(metricValue, offset, metricValue.length - offset)) != -1) {
                        offset += bytesRead;
                        if (offset >= metricValue.length) {
                            break;
                        }
                    }

                    columnDataWriter.write(metricValue);
                }
                columnDataWriter.flush();
                int metricsLen = (int) fragmentDataOutput.getCount() - startOffset;
                metricMeta.setMetricLength(metricsLen);
                metricMeta.setMaxSerializeLength(metricDataWriters[i].getMaxValueLen());
                metricMeta.setCompression(cStoreMetricsDesc.getCompression().name());
                metricMetaList.add(metricMeta);

                ByteStreams.copy(metricInput, fragmentDataOutput);
            } finally {
                if (null != metricInput) {
                    metricInput.close();
                }
            }
        }
        return cuboidMeta;
    }

    public class CuboidColumnDataWriter {
        private long cuboidId;
        private String colName;
        private File tmpColDataFile;
        private CountingOutputStream output;

        public CuboidColumnDataWriter(long cuboidId, String colName) throws IOException {
            this.cuboidId = cuboidId;
            this.colName = colName;

            this.tmpColDataFile = new File(mergeWorkingDirectory, cuboidId + "-" + colName + ".data");
            this.output = new CountingOutputStream(
                    new BufferedOutputStream(FileUtils.openOutputStream(tmpColDataFile)));
        }

        public void write(byte[] value) throws IOException {
            output.write(value);
        }

        public void close() throws IOException {
            output.close();
        }

        public long getLength() {
            return output.getCount();
        }

        public File getOutputFile() {
            return tmpColDataFile;
        }
    }

    public class CuboidMetricDataWriter {
        private long cuboidId;
        private String metricName;
        private File tmpMetricDataFile;
        private DataOutputStream output;
        private CountingOutputStream countingOutput;
        private int maxValLen;

        public CuboidMetricDataWriter(long cuboidId, String metricName, int maxValLen) throws IOException {
            this.cuboidId = cuboidId;
            this.metricName = metricName;
            this.maxValLen = maxValLen;
            this.tmpMetricDataFile = new File(mergeWorkingDirectory, cuboidId + "-" + metricName + ".data");
            this.countingOutput = new CountingOutputStream(
                    new BufferedOutputStream(FileUtils.openOutputStream(tmpMetricDataFile)));
            this.output = new DataOutputStream(countingOutput);
        }

        public void write(byte[] value) throws IOException {
            output.writeInt(value.length);
            output.write(value);
        }

        public void close() throws IOException {
            output.close();
        }

        public int getMaxValueLen() {
            return maxValLen;
        }

        public long getLength() {
            return countingOutput.getCount();
        }

        public File getOutputFile() {
            return tmpMetricDataFile;
        }
    }

    public class FragmentCuboidDataMerger implements Iterator<RawRecord> {
        private List<DimensionEncoding[]> fragmentsDimensionEncodings;
        private DimensionEncoding[] mergedDimensionEncodings;
        private List<RecordDecoder> fragmentsRecordDecoders;
        private List<Iterator<RawRecord>> fragmentsCuboidRecords;
        private PriorityQueue<Pair<DecodedRecord, Integer>> minHeap;
        private MeasureAggregators resultAggrs;
        private DataTypeSerializer[] metricsSerializers;
        private RawRecord oneRawRecord;
        private ByteBuffer metricsBuf;

        public FragmentCuboidDataMerger(CuboidInfo cuboidInfo, List<FragmentCuboidReader> fragmentCuboidReaders,
                List<DimensionEncoding[]> fragmentsDimensionEncodings, DimensionEncoding[] mergedDimEncodings,
                MeasureAggregators resultAggrs, ColumnarMetricsEncoding[] metricsEncodings) {
            this.mergedDimensionEncodings = mergedDimEncodings;
            this.fragmentsDimensionEncodings = fragmentsDimensionEncodings;
            this.fragmentsRecordDecoders = Lists.newArrayList();
            for (DimensionEncoding[] fragmentDimensionEncodings : fragmentsDimensionEncodings) {
                fragmentsRecordDecoders.add(new RecordDecoder(fragmentDimensionEncodings));
            }

            this.fragmentsCuboidRecords = Lists.newArrayListWithCapacity(fragmentCuboidReaders.size());
            for (FragmentCuboidReader reader : fragmentCuboidReaders) {
                fragmentsCuboidRecords.add(reader.iterator());
            }
            this.resultAggrs = resultAggrs;
            this.metricsSerializers = new DataTypeSerializer[metricsEncodings.length];
            for (int i = 0; i < metricsEncodings.length; i++) {
                metricsSerializers[i] = metricsEncodings[i].asDataTypeSerializer();
            }
            this.minHeap = new PriorityQueue<>(fragmentCuboidReaders.size(),
                    new Comparator<Pair<DecodedRecord, Integer>>() {
                        @Override
                        public int compare(Pair<DecodedRecord, Integer> o1, Pair<DecodedRecord, Integer> o2) {
                            return StringArrayComparator.INSTANCE.compare(o1.getFirst().dimensions,
                                    o2.getFirst().dimensions);
                        }
                    });

            this.oneRawRecord = new RawRecord(cuboidInfo.getDimCount(), parsedCubeInfo.measureCount);
            for (int i = 0; i < fragmentCuboidReaders.size(); i++) {
                enqueueFromFragment(i);
            }
            metricsBuf = ByteBuffer.allocate(getMaxMetricsLength());
        }

        public int getMaxMetricsLength() {
            int result = -1;
            for (int i = 0; i < metricsSerializers.length; i++) {
                int maxLength = metricsSerializers[i].maxLength();
                if (result < maxLength) {
                    result = maxLength;
                }
            }
            return result;
        }

        @Override
        public boolean hasNext() {
            return !minHeap.isEmpty();
        }

        @Override
        public RawRecord next() {
            Pair<DecodedRecord, Integer> currRecordEntry = minHeap.poll();
            DecodedRecord currRecord = currRecordEntry.getFirst();

            enqueueFromFragment(currRecordEntry.getSecond());
            boolean needAggregate = false;
            boolean first = true;
            while ((!minHeap.isEmpty()) && StringArrayComparator.INSTANCE.compare(currRecord.dimensions,
                    minHeap.peek().getFirst().dimensions) == 0) {
                if (first) {
                    doAggregate(currRecord);
                    first = false;
                    needAggregate = true;
                }
                Pair<DecodedRecord, Integer> nextRecord = minHeap.poll();
                doAggregate(nextRecord.getFirst());
                enqueueFromFragment(nextRecord.getSecond());
            }
            byte[][] newEncodedDimVals = encodeToNewDimValues(currRecord.dimensions);
            if (!needAggregate) {
                return new RawRecord(newEncodedDimVals, currRecord.metrics);
            }

            for (int i = 0; i < oneRawRecord.getDimensions().length; i++) {
                oneRawRecord.setDimension(i, newEncodedDimVals[i]);
            }
            Object[] metricValues = new Object[parsedCubeInfo.measureCount];
            resultAggrs.collectStates(metricValues);
            for (int i = 0; i < metricValues.length; i++) {
                metricsBuf.clear();
                metricsSerializers[i].serialize(metricValues[i], metricsBuf);
                byte[] metricBytes = Arrays.copyOf(metricsBuf.array(), metricsBuf.position());
                oneRawRecord.setMetric(i, metricBytes);
            }
            resultAggrs.reset();
            return oneRawRecord;
        }

        private byte[][] encodeToNewDimValues(String[] dimensionValues) {
            byte[][] result = new byte[dimensionValues.length][];
            for (int i = 0; i < dimensionValues.length; i++) {
                DimensionEncoding dimensionEncoding = mergedDimensionEncodings[i];
                byte[] bytes = new byte[dimensionEncoding.getLengthOfEncoding()];
                dimensionEncoding.encode(dimensionValues[i], bytes, 0);
                result[i] = bytes;
            }
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("unSupport operation");
        }

        private void doAggregate(DecodedRecord record) {
            Object[] metricValues = new Object[parsedCubeInfo.measureCount];
            decode(record.metrics, metricValues);
            resultAggrs.aggregate(metricValues);
        }

        public void decode(byte[][] metricsBytes, Object[] result) {
            for (int i = 0; i < metricsSerializers.length; i++) {
                result[i] = metricsSerializers[i].deserialize(ByteBuffer.wrap(metricsBytes[i]));
            }
        }

        private void enqueueFromFragment(int index) {
            Iterator<RawRecord> fragmentCuboidRecords = fragmentsCuboidRecords.get(index);
            RecordDecoder recordDecoder = fragmentsRecordDecoders.get(index);
            if (fragmentCuboidRecords.hasNext()) {
                RawRecord rawRecord = fragmentCuboidRecords.next();
                minHeap.offer(new Pair<>(recordDecoder.decode(rawRecord), index));
            }
        }
    }

    private static class RecordDecoder {
        private DimensionEncoding[] dimEncodings;

        public RecordDecoder(DimensionEncoding[] dimEncodings) {
            this.dimEncodings = dimEncodings;
        }

        public DecodedRecord decode(RawRecord rawRecord) {
            byte[][] rawDimValues = rawRecord.getDimensions();
            String[] dimValues = new String[rawDimValues.length];
            for (int i = 0; i < dimValues.length; i++) {
                byte[] dimVal = rawDimValues[i];
                dimValues[i] = dimEncodings[i].decode(dimVal, 0, dimVal.length);
            }
            byte[][] metricsValues = rawRecord.getMetrics();
            return new DecodedRecord(dimValues, Arrays.copyOf(metricsValues, metricsValues.length));
        }
    }

    private static class DecodedRecord {
        String[] dimensions;
        byte[][] metrics;

        DecodedRecord(String[] dimensions, byte[][] metrics) {
            this.dimensions = dimensions;
            this.metrics = metrics;
        }
    }

}
