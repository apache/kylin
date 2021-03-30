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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.query.ResponseResultSchema;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.SingleThreadResultCollector;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.storage.TestHelper;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class FragmentFileSearcherTest extends LocalFileMetadataTestCase {
    private static final String cubeName = "test_streaming_v2_cube";

    private String baseStorePath;
    private CubeInstance cubeInstance;
    private String segmentName;
    private ParsedStreamingCubeInfo parsedStreamingCubeInfo;
    private DataSegmentFragment fragment;
    private FragmentFileSearcher fragmentFileSearcher;
    private TestHelper testHelper;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        this.cubeInstance = CubeManager.getInstance(getTestConfig()).reloadCubeQuietly(cubeName);
        this.segmentName = "20171018100000_20171018110000";
        this.parsedStreamingCubeInfo = new ParsedStreamingCubeInfo(cubeInstance);
        this.fragment = new DataSegmentFragment(baseStorePath, cubeName, segmentName, new FragmentId(0));
        PropertyConfigurator.configure("../build/conf/kylin-tools-log4j.properties");
        prepareData();
        fragmentFileSearcher = new FragmentFileSearcher(fragment, new FragmentData(fragment.getMetaInfo(),
                fragment.getDataFile()));
        this.testHelper = new TestHelper(cubeInstance);
        StreamingQueryProfile.set(new StreamingQueryProfile("test-query-id", System.currentTimeMillis()));
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        cleanupData();
    }

    @Test
    public void testIterator() throws Exception {
        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START", "STREAMING_V2_TABLE.SITE");
        Set<TblColRef> groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        Set<FunctionDesc> metrics = testHelper.simulateMetrics();
        StreamingSearchContext searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc,
                dimensions, groups, metrics, null, null);
        ResultCollector resultCollector = new SingleThreadResultCollector();
        fragmentFileSearcher.search(searchRequest, resultCollector);
        for (Record record : resultCollector) {
            Object[] values = record.getMetrics();
            PercentileCounter counter = (PercentileCounter) values[values.length - 1];
            values[values.length - 1] = estPercentileValue(counter);
            System.out.println(record);
        }

        dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE", "STREAMING_V2_TABLE.ITM",
                "STREAMING_V2_TABLE.MINUTE_START");

        groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE");
        searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc, dimensions, groups, metrics,
                null, null);
        resultCollector = new SingleThreadResultCollector();
        fragmentFileSearcher.search(searchRequest, resultCollector);
        int count = 0;
        for (Record record : resultCollector) {
            Object[] values = record.getMetrics();
            if (count < 5) {
                PercentileCounter counter = (PercentileCounter) values[values.length - 1];
                values[values.length - 1] = estPercentileValue(counter);
                System.out.println(record);
            }
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    public void testInvertIndexSearch() throws Exception {
        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE",
                "STREAMING_V2_TABLE.ITM", "STREAMING_V2_TABLE.MINUTE_START");
        Set<TblColRef> groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE");
        Set<FunctionDesc> metrics = testHelper.simulateMetrics();
        String itmValue = "ITM0000009000";
        CompareTupleFilter itemFilter = testHelper.buildEQFilter("STREAMING_V2_TABLE.ITM", itmValue);
        StreamingSearchContext searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc,
                dimensions, groups, metrics, itemFilter, null);
        ResponseResultSchema schema = searchRequest.getRespResultSchema();
        int itmDimIdx = schema.getIndexOfDimension(itemFilter.getColumn());
        ResultCollector resultCollector = new SingleThreadResultCollector();
        fragmentFileSearcher.search(searchRequest, resultCollector);
        for (Record record : resultCollector) {
            assertEquals(itmValue, record.getDimensions()[itmDimIdx]);
            Object[] values = record.getMetrics();
            PercentileCounter counter = (PercentileCounter) values[values.length - 1];
            values[values.length - 1] = estPercentileValue(counter);
            System.out.println(Lists.newArrayList(values));
        }

        String siteValue = "SITE0";
        CompareTupleFilter siteFilter = testHelper.buildEQFilter("STREAMING_V2_TABLE.SITE", siteValue);
        int siteDimIdx = schema.getIndexOfDimension(siteFilter.getColumn());
        TupleFilter andFilter = testHelper.buildAndFilter(itemFilter, siteFilter);
        searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc, dimensions, groups, metrics,
                andFilter, null);
        resultCollector = new SingleThreadResultCollector();
        fragmentFileSearcher.search(searchRequest, resultCollector);
        for (Record record : resultCollector) {
            assertEquals(itmValue, record.getDimensions()[itmDimIdx]);
            assertEquals(siteValue, record.getDimensions()[siteDimIdx]);
            Object[] values = record.getMetrics();
            PercentileCounter counter = (PercentileCounter) values[values.length - 1];
            values[values.length - 1] = estPercentileValue(counter);
            System.out.println(Lists.newArrayList(values));
        }

        TupleFilter likeFilter = testHelper.buildLikeFilter("STREAMING_V2_TABLE.ITM", "ITM000001%");
        metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc,
                dimensions, groups, metrics, likeFilter, null);
        resultCollector = new SingleThreadResultCollector();
        fragmentFileSearcher.search(searchRequest, resultCollector);
        long count = 0;
        int rowsNum = 0;
        for (Record record : resultCollector) {
            count += (Long)record.getMetrics()[0];
            rowsNum ++;
        }
        assertEquals(10000, count);
        assertEquals(10, rowsNum);
    }

    private double estPercentileValue(PercentileCounter counter) {
        PercentileCounter counter1 = new PercentileCounter(100, 0.5);
        counter1.merge(counter);
        return counter1.getResultEstimate();
    }

    protected void prepareData() {
        // build additional cuboids
        KylinConfigExt configExt = (KylinConfigExt) cubeInstance.getDescriptor().getConfig();
        configExt.getExtendedOverrides().put("kylin.stream.build.additional.cuboids", "true");

        ColumnarMemoryStorePersister memStorePersister = new ColumnarMemoryStorePersister(parsedStreamingCubeInfo,
                segmentName);
        StreamingDataSimulator simulator = new StreamingDataSimulator(
                StreamingDataSimulator.getDefaultCardinalityMap(), 10000);
        Iterator<StreamingMessage> streamingMessages = simulator.simulate(50000, System.currentTimeMillis());
        SegmentMemoryStore memoryStore = new SegmentMemoryStore(new ParsedStreamingCubeInfo(cubeInstance), segmentName);
        while (streamingMessages.hasNext()) {
            memoryStore.index(streamingMessages.next());
        }

        memStorePersister.persist(memoryStore, fragment);
    }

    private void cleanupData() throws IOException {
        FileUtils.deleteQuietly(new File(baseStorePath));
    }

}
