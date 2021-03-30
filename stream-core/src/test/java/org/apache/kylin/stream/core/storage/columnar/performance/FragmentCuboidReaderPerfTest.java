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

package org.apache.kylin.stream.core.storage.columnar.performance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.storage.TestHelper;
import org.apache.kylin.stream.core.storage.columnar.ColumnarSegmentStore;
import org.apache.kylin.stream.core.storage.columnar.DataSegmentFragment;
import org.apache.kylin.stream.core.storage.columnar.FragmentCuboidReader;
import org.apache.kylin.stream.core.storage.columnar.FragmentData;
import org.apache.kylin.stream.core.storage.columnar.ParsedStreamingCubeInfo;
import org.apache.kylin.stream.core.storage.columnar.RawRecord;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FragmentCuboidReaderPerfTest extends LocalFileMetadataTestCase {
    private static final String cubeName = "test_streaming_v2_cube";

    private String baseStorePath;
    private CubeInstance cubeInstance;
    private ParsedStreamingCubeInfo parsedStreamingCubeInfo;
    private TestHelper testHelper;
    private DataSegmentFragment[] fragments;

    public FragmentCuboidReaderPerfTest() throws Exception {
        this.createTestMetadata();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        this.cubeInstance = CubeManager.getInstance(getTestConfig()).reloadCubeQuietly(cubeName);
        this.parsedStreamingCubeInfo = new ParsedStreamingCubeInfo(cubeInstance);
        ColumnarSegmentStore segmentStore1 = new ColumnarSegmentStore(baseStorePath, cubeInstance,
                "20180730070000_20180730080000");
        ColumnarSegmentStore segmentStore2 = new ColumnarSegmentStore(baseStorePath, cubeInstance,
                "20180730080000_20180730090000");
        segmentStore1.init();
        segmentStore2.init();
        List<DataSegmentFragment> allFragments = Lists.newArrayList();
        allFragments.addAll(segmentStore1.getAllFragments());
        allFragments.addAll(segmentStore2.getAllFragments());
        this.fragments = allFragments.toArray(new DataSegmentFragment[allFragments.size()]);

        this.testHelper = new TestHelper(cubeInstance);
    }

    public static void main(String[] args) throws Exception {
        FragmentCuboidReaderPerfTest test = new FragmentCuboidReaderPerfTest();
        test.scanPerformance();
        test.readRowPerformance();
        test.cleanData();
    }

    public void scanPerformance() throws Exception {
        Pair<List<TblColRef>, List<MeasureDesc>> readDimAndMetrics = getReadDimensionsAndMetrics();

        for (int i = 1; i < 5; i++) {
            scan(i, readDimAndMetrics.getFirst(), readDimAndMetrics.getSecond().toArray(new MeasureDesc[0]));
        }
    }

    private void scan(int time, List<TblColRef> dimensions, MeasureDesc[] metrics) throws IOException {
        System.out.println("start " + time + " scan, " + dimensions.size() + " dimensions," + metrics.length
                + " measures");
        TblColRef[] dimArray = dimensions.toArray(new TblColRef[dimensions.size()]);
        List<FragmentData> fragmentDatas = Lists.newArrayList();
        List<FragmentCuboidReader> fragmentCuboidReaders = Lists.newArrayList();
        for (int i = 0; i < fragments.length; i++) {
            FragmentMetaInfo fragmentMetaInfo = fragments[i].getMetaInfo();
            FragmentData fragmentData = new FragmentData(fragmentMetaInfo, fragments[i].getDataFile());
            Map<TblColRef, Dictionary<String>> dictionaryMap = fragmentData
                    .getDimensionDictionaries(parsedStreamingCubeInfo.dimensionsUseDictEncoding);
            DimensionEncoding[] dimensionEncodings = ParsedStreamingCubeInfo.getDimensionEncodings(
                    parsedStreamingCubeInfo.cubeDesc, dimArray, dictionaryMap);
            FragmentCuboidReader fragmentCuboidReader = new FragmentCuboidReader(parsedStreamingCubeInfo.cubeDesc,
                    fragmentData, fragmentMetaInfo.getBasicCuboidMetaInfo(), dimArray, metrics, dimensionEncodings);
            fragmentDatas.add(fragmentData);
            fragmentCuboidReaders.add(fragmentCuboidReader);
        }

        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        int rowNum = 0;
        long scanTime = 0;
        for (int i = 0; i < fragments.length; i++) {
            FragmentData fragmentData = fragmentDatas.get(i);
            FragmentMetaInfo fragmentMetaInfo = fragmentData.getFragmentMetaInfo();
            Map<TblColRef, Dictionary<String>> dictionaryMap = fragmentData
                    .getDimensionDictionaries(parsedStreamingCubeInfo.dimensionsUseDictEncoding);
            DimensionEncoding[] dimensionEncodings = ParsedStreamingCubeInfo.getDimensionEncodings(
                    parsedStreamingCubeInfo.cubeDesc, dimArray, dictionaryMap);
            FragmentCuboidReader fragmentCuboidReader = new FragmentCuboidReader(parsedStreamingCubeInfo.cubeDesc,
                    fragmentData, fragmentMetaInfo.getBasicCuboidMetaInfo(), dimArray, metrics, dimensionEncodings);
            long scanStartTime = System.currentTimeMillis();
            for (RawRecord rawRecord : fragmentCuboidReader) {
                rowNum++;
            }
            scanTime += System.currentTimeMillis() - scanStartTime;
        }
        sw.stop();
        long takeTime = sw.elapsed(MILLISECONDS);
        System.out.println(time + " scan finished, total rows:" + rowNum);
        System.out.println(time + " scan took:" + takeTime + ", scan time: " + scanTime + ", rowsPerSec:"
                + (rowNum / takeTime) * 1000);
    }

    public void readRowPerformance() throws Exception {
        Pair<List<TblColRef>, List<MeasureDesc>> readDimAndMetrics = getReadDimensionsAndMetrics();
        for (int i = 1; i < 21; i++) {
            readRow(i, readDimAndMetrics.getFirst(), readDimAndMetrics.getSecond().toArray(new MeasureDesc[0]));
        }
    }

    private void readRow(int time, List<TblColRef> dimensions, MeasureDesc[] metrics) throws IOException {
        System.out.println("start " + time + " read, " + dimensions.size() + " dimensions," + metrics.length
                + " measures");
        TblColRef[] dimArray = dimensions.toArray(new TblColRef[dimensions.size()]);
        Random rand = new Random();
        int randReadNum = 100;
        int[][] readRows = new int[fragments.length][randReadNum];
        List<FragmentCuboidReader> fragmentCuboidReaders = Lists.newArrayList();
        for (int i = 0; i < fragments.length; i++) {
            FragmentMetaInfo fragmentMetaInfo = fragments[i].getMetaInfo();
            FragmentData fragmentData = new FragmentData(fragmentMetaInfo, fragments[i].getDataFile());
            Map<TblColRef, Dictionary<String>> dictionaryMap = fragmentData
                    .getDimensionDictionaries(parsedStreamingCubeInfo.dimensionsUseDictEncoding);
            DimensionEncoding[] dimensionEncodings = ParsedStreamingCubeInfo.getDimensionEncodings(
                    parsedStreamingCubeInfo.cubeDesc, dimArray, dictionaryMap);
            FragmentCuboidReader fragmentCuboidReader = new FragmentCuboidReader(parsedStreamingCubeInfo.cubeDesc,
                    fragmentData, fragmentMetaInfo.getBasicCuboidMetaInfo(), dimArray, metrics, dimensionEncodings);
            fragmentCuboidReaders.add(fragmentCuboidReader);
            for (int j = 0; j < randReadNum; j++) {
                readRows[i][j] = rand.nextInt((int) fragmentMetaInfo.getNumberOfRows());
            }
        }
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        int rowNum = 0;

        for (int i = 0; i < fragments.length; i++) {
            for (int j = 0; j < readRows.length; j++) {
                fragmentCuboidReaders.get(i).read(readRows[i][j]);
            }
        }
        sw.stop();
        long takeTime = sw.elapsed(MILLISECONDS);
        System.out.println(time + " scan finished, total rows:" + rowNum);
        System.out.println(time + " scan took:" + takeTime + ",rowsPerSec:" + (rowNum / takeTime) * 1000);
    }

    public Pair<List<TblColRef>, List<MeasureDesc>> getReadDimensionsAndMetrics() {
        Set<TblColRef> dimensionsSet = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        List<TblColRef> dimensions = new ArrayList<>(dimensionsSet);
        FunctionDesc metricFunc = testHelper.simulateMetric("STREAMING_V2_TABLE.GMV", "SUM", "decimal(19,6)");
        List<MeasureDesc> allMetrics = cubeInstance.getDescriptor().getMeasures();
        List<MeasureDesc> targetMetrics = Lists.newArrayList();
        for (MeasureDesc metric : allMetrics) {
                                    if (metric.getFunction().equals(metricFunc)) {
                                        targetMetrics.add(metric);
                                    }
            if (metric.getName().equalsIgnoreCase("_COUNT_")) {
//                targetMetrics.add(metric);
            }
        }
        return new Pair<>(dimensions, targetMetrics);
    }

    public void cleanData() throws Exception {
        this.cleanupTestMetadata();
    }

}
