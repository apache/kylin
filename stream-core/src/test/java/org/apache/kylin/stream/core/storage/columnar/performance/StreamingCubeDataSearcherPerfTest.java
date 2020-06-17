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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.query.IStreamingSearchResult;
import org.apache.kylin.stream.core.query.StreamingCubeDataSearcher;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.MockPositionHandler;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.core.storage.TestHelper;
import org.apache.kylin.stream.core.storage.columnar.ParsedStreamingCubeInfo;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StreamingCubeDataSearcherPerfTest extends LocalFileMetadataTestCase {
    private static final String cubeName = "test_streaming_v2_cube";

    private String baseStorePath;
    private CubeInstance cubeInstance;
    private ParsedStreamingCubeInfo parsedStreamingCubeInfo;
    private TestHelper testHelper;
    private int totalRows;
    private StreamingSegmentManager streamingSegmentManager;
    private StreamingCubeDataSearcher searcher;

    public StreamingCubeDataSearcherPerfTest() throws Exception {
        this.createTestMetadata();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        this.cubeInstance = CubeManager.getInstance(getTestConfig()).reloadCubeQuietly(cubeName);
        this.parsedStreamingCubeInfo = new ParsedStreamingCubeInfo(cubeInstance);
        setupCubeConfig();
        this.testHelper = new TestHelper(cubeInstance);
        this.streamingSegmentManager = new StreamingSegmentManager(baseStorePath, cubeInstance,
                new MockPositionHandler(), null);
        this.searcher = streamingSegmentManager.getSearcher();
    }

    private void setupCubeConfig() {
        KylinConfig kylinConfig = cubeInstance.getConfig();
        kylinConfig.setProperty("kylin.stream.index.maxrows", "1000000");
    }

    public static void main(String[] args) throws Exception {
        StreamingCubeDataSearcherPerfTest test = new StreamingCubeDataSearcherPerfTest();
        test.prepareData();
        test.searchPerformance();
        //        test.scanPerformance();
    }

    public void searchPerformance() throws Exception {
        StreamingQueryProfile.set(new StreamingQueryProfile("test-query-id", System.currentTimeMillis()));
        KylinConfig.getInstanceFromEnv().setProperty("kylin.stream.receiver.use-threads-per-query", "1");
        int times = Integer.MAX_VALUE;
        for (int i = 1; i < times; i++) {
            search(i);
        }

        for (int i = 1; i < times; i++) {
            iiSearch(i);
        }
    }

    private void search(int time) throws IOException {
        System.out.println("start " + time + " search");
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        Set<TblColRef> groups = testHelper.simulateDimensions();
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        long startTime = DateFormat.stringToMillis("2018-07-30 07:00:00");
        long endTime = DateFormat.stringToMillis("2018-07-30 08:00:00");
        TupleFilter filter = testHelper.buildTimeRangeFilter("STREAMING_V2_TABLE.MINUTE_START",
                String.valueOf(startTime), String.valueOf(endTime));
        StreamingSearchContext searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc, dimensions,
                groups, metrics, filter, null);
        IStreamingSearchResult searchResult = searcher.doSearch(searchRequest, 0L, true);
        for (Record record : searchResult) {
            System.out.println(record);
        }
        sw.stop();
        long takeTime = sw.elapsed(MILLISECONDS);
        System.out.println(time + " search finished, took:" + takeTime);
    }

    private void iiSearch(int time) throws IOException {
        System.out.println("start " + time + " invertIndex search");
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START",
                "STREAMING_V2_TABLE.ITM");
        Set<TblColRef> groups = testHelper.simulateDimensions();
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        long startTime = DateFormat.stringToMillis("2018-07-30 07:00:00");
        long endTime = DateFormat.stringToMillis("2018-07-30 09:00:00");
        TupleFilter timeFilter = testHelper.buildTimeRangeFilter("STREAMING_V2_TABLE.MINUTE_START",
                String.valueOf(startTime), String.valueOf(endTime));
        TupleFilter itemFilter = testHelper.buildEQFilter("STREAMING_V2_TABLE.ITM", "ITM0000000000");
        TupleFilter filter = testHelper.buildAndFilter(timeFilter, itemFilter);
        StreamingSearchContext searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc, dimensions,
                groups, metrics, filter, null);
        IStreamingSearchResult searchResult = searcher.doSearch(searchRequest, 0L, true);
        for (Record record : searchResult) {
            System.out.println(record);
        }
        sw.stop();
        long takeTime = sw.elapsed(MILLISECONDS);
        System.out.println(time + " search finished, took:" + takeTime);
    }

    public void scanPerformance() throws Exception {
        StreamingQueryProfile queryProfile = new StreamingQueryProfile("test-query-id", System.currentTimeMillis());
        queryProfile.setStorageBehavior(StorageSideBehavior.RAW_SCAN);
        StreamingQueryProfile.set(queryProfile);
        for (int i = 1; i < 500; i++) {
            scan(i);
        }
    }

    private void scan(int time) throws IOException {
        System.out.println("start " + time + " scan");
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE");
        Set<TblColRef> groups = testHelper.simulateDimensions();
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        long startTime = DateFormat.stringToMillis("2018-07-30 07:00:00");
        long endTime = DateFormat.stringToMillis("2018-07-30 08:00:00");
        TupleFilter filter = testHelper.buildTimeRangeFilter("STREAMING_V2_TABLE.MINUTE_START",
                String.valueOf(startTime), String.valueOf(endTime));
        StreamingSearchContext searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc, dimensions,
                groups, metrics, null, null);
        IStreamingSearchResult searchResult = searcher.doSearch(searchRequest, 0L, true);
        long scanRowCnt = 0;
        for (Record record : searchResult) {
            scanRowCnt++;
        }
        sw.stop();
        long takeTime = sw.elapsed(MILLISECONDS);
        System.out.println(time + " search finished, scan row cnt:" + scanRowCnt + ", took:" + takeTime
                + ",numRowsPerSec:" + scanRowCnt * 1000 / takeTime);
    }

    public void prepareData() {
        streamingSegmentManager.restoreSegmentsFromLocal();
    }

    public void cleanData() throws Exception {
        this.cleanupTestMetadata();
        FileUtils.deleteQuietly(new File(baseStorePath));
    }

}
