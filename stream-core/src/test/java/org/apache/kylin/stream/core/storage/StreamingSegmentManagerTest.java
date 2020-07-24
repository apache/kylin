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

package org.apache.kylin.stream.core.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.query.IStreamingSearchResult;
import org.apache.kylin.stream.core.query.StreamingCubeDataSearcher;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.columnar.StreamingDataSimulator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class StreamingSegmentManagerTest extends LocalFileMetadataTestCase {

    private static final String cubeName = "test_streaming_v2_cube";
    private static Logger logger = LoggerFactory.getLogger(StreamingSegmentManagerTest.class);

    private CubeInstance cubeInstance;
    private CubeDesc cubeDesc;
    private String baseStorePath;
    private TestHelper testHelper;
    private StreamingSegmentManager streamingSegmentManager;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        setUpTestKylinCube();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        this.streamingSegmentManager = new StreamingSegmentManager(baseStorePath, cubeInstance,
                new MockPositionHandler(), null);
        this.cleanupSegments();
        this.testHelper = new TestHelper(cubeInstance);
        StreamingQueryProfile.set(new StreamingQueryProfile("test-query-id", System.currentTimeMillis()));
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    protected void setUpTestKylinCube() {
        this.cubeInstance = getCubeManager().reloadCubeQuietly(cubeName);
        this.cubeDesc = cubeInstance.getDescriptor();
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getTestConfig());
    }

    @Test
    public void testAddEventAndScan() {
        genEvents(80000);
        StreamingCubeDataSearcher searcher = streamingSegmentManager.getSearcher();

        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE");
        Set<TblColRef> groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE");
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateMetric("STREAMING_V2_TABLE.GMV", "SUM", "decimal(19,6)"));

        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics, null,
                null);
        IStreamingSearchResult segmentResults1 = searcher.doSearch(searchRequest, -1, true);
        int recordNum = 0;
        for (Record record : segmentResults1) {
            recordNum++;
        }
        assertEquals(10, recordNum);

        dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.ITM");
        groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.ITM");

        searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics, null,
                null);
        segmentResults1 = searcher.doSearch(searchRequest, -1, true);
        recordNum = 0;
        for (Record record : segmentResults1) {
            recordNum++;
        }
        assertEquals(80000, recordNum);

        dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START", "STREAMING_V2_TABLE.SITE");
        groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE");
        CompareTupleFilter filter1 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.MINUTE_START",
                FilterOperatorEnum.GTE, "2018-07-30 20:00:00");
        CompareTupleFilter filter2 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.MINUTE_START",
                FilterOperatorEnum.LT, "2018-07-30 20:04:00");
        TupleFilter filter = testHelper.buildAndFilter(filter1, filter2);
        metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics, filter,
                null);
        segmentResults1 = searcher.doSearch(searchRequest, -1, true);
        recordNum = 0;
        for (Record record : segmentResults1) {
            recordNum++;
            long cnt = (Long)record.getMetrics()[0];
            assertEquals(4000, cnt);
        }
        assertEquals(10, recordNum);

        CompareTupleFilter filter3 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.MINUTE_START",
            FilterOperatorEnum.GTE, String.valueOf(DateFormat.stringToMillis("2018-07-30 20:04:00")));
        CompareTupleFilter filter4 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.MINUTE_START",
            FilterOperatorEnum.LTE, String.valueOf(DateFormat.stringToMillis("2018-07-30 20:06:00")));
        filter = testHelper.buildAndFilter(filter3, filter4);
        groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        dimensions = groups;
        searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics, filter,
            null);
        segmentResults1 = searcher.doSearch(searchRequest, -1, true);
        recordNum = 0;
        for (Record record : segmentResults1) {
            recordNum++;
            System.out.println(record);
        }
        assertEquals(3, recordNum);
    }

    @Test
    public void testIndexFilter() {
        genEvents(80000);
        StreamingCubeDataSearcher searcher = streamingSegmentManager.getSearcher();
        String startTimeStr = "2018-07-30 20:00:00";
        long startTime = DateFormat.stringToMillis(startTimeStr);
        CompareTupleFilter filter1 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.MINUTE_START",
            FilterOperatorEnum.GTE, startTimeStr);
        CompareTupleFilter filter2 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.MINUTE_START",
            FilterOperatorEnum.LTE, "2018-07-30 20:04:00");
        TupleFilter filter = testHelper.buildAndFilter(filter1, filter2);
        TupleFilter filter3 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.SITE",
            FilterOperatorEnum.EQ, "SITE0");
        filter = testHelper.buildAndFilter(filter, filter3);
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START", "STREAMING_V2_TABLE.SITE");
        Set<TblColRef> groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics, filter,
            null);
        IStreamingSearchResult segmentResults1 = searcher.doSearch(searchRequest, -1, true);
        int recordNum = 0;
        for (Record record : segmentResults1) {
            long cnt = (Long)record.getMetrics()[0];
            assertEquals(String.valueOf(startTime + 60 * 1000 * recordNum), record.getDimensions()[0]);
            assertEquals(1000, cnt);
            recordNum++;
            System.out.println(record);
        }
        assertEquals(5, recordNum);
    }

    @Test
    public void testOneValueAggregation() {
        genEvents(80000);
        StreamingCubeDataSearcher searcher = streamingSegmentManager.getSearcher();
        String startTimeStr = "2018-07-30 20:00:00";
        long startTime = DateFormat.stringToMillis(startTimeStr);
        String endTimeStr = "2018-07-30 20:04:00";
        long endTime = DateFormat.stringToMillis(endTimeStr);
        CompareTupleFilter filter1 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.MINUTE_START",
            FilterOperatorEnum.GTE, startTimeStr);
        CompareTupleFilter filter2 = testHelper.buildCompareFilter("STREAMING_V2_TABLE.MINUTE_START",
            FilterOperatorEnum.LT, endTimeStr);
        TupleFilter filter = testHelper.buildAndFilter(filter1, filter2);
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        Set<TblColRef> groups = Sets.newHashSet();
        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics, filter,
            null);
        IStreamingSearchResult segmentResults1 = searcher.doSearch(searchRequest, -1, true);
        for (Record record : segmentResults1) {
            long minStart = Long.valueOf(record.getDimensions()[0]);
            assertTrue(startTime <= minStart && minStart < endTime);
            System.out.println(record);
        }
    }

    private void genEvents(int numEvents) {
        long time = DateFormat.stringToMillis("2018-07-30 20:00:00");
        StreamingDataSimulator simulator = new StreamingDataSimulator();
        Iterator<StreamingMessage> messageIterator = simulator.simulate(numEvents, time);
        while (messageIterator.hasNext()) {
            StreamingMessage message = messageIterator.next();
            streamingSegmentManager.addEvent(message);
        }
    }

    private void cleanupSegments() {
        FileUtils.deleteQuietly(new File(baseStorePath));
    }
}
