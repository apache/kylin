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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.SingleThreadResultCollector;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.storage.TestHelper;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class FragmentFilesMergerTest extends LocalFileMetadataTestCase {
    private static final String cubeName = "test_streaming_v2_cube";

    private String baseStorePath;
    private CubeInstance cubeInstance;
    private String segmentName;
    private ParsedStreamingCubeInfo parsedStreamingCubeInfo;
    private FragmentFilesMerger fragmentFilesMerger;
    private TestHelper testHelper;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        this.cubeInstance = getCubeManager().reloadCubeQuietly(cubeName);
        this.segmentName = "20171218100000_20171218110000";
        this.parsedStreamingCubeInfo = new ParsedStreamingCubeInfo(cubeInstance);
        this.fragmentFilesMerger = new FragmentFilesMerger(parsedStreamingCubeInfo, new File(new File(baseStorePath,
                cubeName), segmentName));
        this.testHelper = new TestHelper(cubeInstance);
        cleanupData();
        PropertyConfigurator.configure("../build/conf/kylin-tools-log4j.properties");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        cleanupData();
    }

    @Test
    public void testMerge() throws Exception {
        List<DataSegmentFragment> fragments = createFragmentFiles(0, 5, new StreamingDataSimulator());
        FragmentsMergeResult mergeResult = fragmentFilesMerger.merge(fragments);
        File mergedFragmentMetaFile = mergeResult.getMergedFragmentMetaFile();
        FragmentMetaInfo fragmentMetaInfo = JsonUtil.readValue(mergedFragmentMetaFile, FragmentMetaInfo.class);
        assertEquals(5 * 50000, fragmentMetaInfo.getNumberOfRows());
        assertEquals(5 * 50000, fragmentMetaInfo.getOriginNumOfRows());

        FragmentData fragmentData = new FragmentData(fragmentMetaInfo, mergeResult.getMergedFragmentDataFile());
        Set<TblColRef> dims = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE");
        TupleFilter siteFilter = testHelper.buildEQFilter("SITE", "SITE0");
        Set<FunctionDesc> metrics = Sets.newHashSet();

        StreamingQueryProfile.set(new StreamingQueryProfile("test-query-id", System.currentTimeMillis()));
        DataSegmentFragment fragment = new DataSegmentFragment(baseStorePath, cubeName, segmentName, new FragmentId(0));
        FragmentFileSearcher fragmentFileSearcher = new FragmentFileSearcher(fragment, fragmentData);
        //        InvertIndexSearcher iiSearcher = new InvertIndexSearcher(cuboidMetaInfo, Lists.newArrayList(dims), fragmentData.getDataReadBuffer());
        //        IndexSearchResult searchResult = iiSearcher.search(scanFilter);
        StreamingSearchContext searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc, dims,
                Sets.<TblColRef> newHashSet(), metrics, siteFilter, null);
        ResultCollector resultCollector = new SingleThreadResultCollector();
        fragmentFileSearcher.search(searchRequest, resultCollector);
        int rowCnt = 0;
        for (Record record : resultCollector) {
            rowCnt++;
        }
        assertEquals(5 * 5000, rowCnt);

        dims = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE", "STREAMING_V2_TABLE.MINUTE_START");
        Set<TblColRef> groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc, dims, groups, metrics, null,
                null);
        resultCollector = new SingleThreadResultCollector();
        fragmentFileSearcher.search(searchRequest, resultCollector);
        int totalOriginCnt = 0;
        for (Record record : resultCollector) {
            System.out.println(record);
            totalOriginCnt += (Long) record.getMetrics()[0];
        }
        assertEquals(5 * 50000, totalOriginCnt);
    }

    @Test
    public void testMerge2() throws Exception {
        int fragmentNum = 4;
        int eventCntPerMin = 100000;
        StreamingDataSimulator simulator = new StreamingDataSimulator(getTestCardinalityMap(), eventCntPerMin);
        List<DataSegmentFragment> fragments = createFragmentFiles(5, fragmentNum, simulator);
        int originRowCnt = fragmentNum * 50000;
        FragmentsMergeResult mergeResult = fragmentFilesMerger.merge(fragments);
        File mergedFragmentMetaFile = mergeResult.getMergedFragmentMetaFile();
        File mergedFragmentDataFile = mergeResult.getMergedFragmentDataFile();
        FragmentMetaInfo fragmentMetaInfo = JsonUtil.readValue(mergedFragmentMetaFile, FragmentMetaInfo.class);
        assertEquals(160000, fragmentMetaInfo.getNumberOfRows());
        assertEquals(originRowCnt, fragmentMetaInfo.getOriginNumOfRows());

        FragmentData fragmentData = new FragmentData(fragmentMetaInfo, mergedFragmentDataFile);
        Set<TblColRef> dims = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        TupleFilter siteFilter = null;
        Set<TblColRef> groups = testHelper.simulateDimensions("STREAMING_V2_TABLE.MINUTE_START");
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateCountMetric());

        StreamingQueryProfile.set(new StreamingQueryProfile("test-query-id", System.currentTimeMillis()));
        DataSegmentFragment fragment = new DataSegmentFragment(baseStorePath, cubeName, segmentName, new FragmentId(0));
        FragmentFileSearcher fragmentFileSearcher = new FragmentFileSearcher(fragment, fragmentData);
        StreamingSearchContext searchRequest = new StreamingSearchContext(parsedStreamingCubeInfo.cubeDesc, dims,
                groups, metrics, siteFilter, null);
        ResultCollector resultCollector = new SingleThreadResultCollector();
        fragmentFileSearcher.search(searchRequest, resultCollector);
        int rowCnt = 0;
        int totalOriginCnt = 0;
        for (Record record : resultCollector) {
            rowCnt++;
            long everyMinuteCnt = (Long) record.getMetrics()[0];
            assertEquals(eventCntPerMin, (int) everyMinuteCnt);
            System.out.println(record);
            totalOriginCnt += everyMinuteCnt;
        }
        assertEquals(2, rowCnt);
        assertEquals(originRowCnt, totalOriginCnt);
    }

    @Test
    public void testMergeWithAdditionalCuboids() throws Exception {
        setBuildAdditionalCuboids();
        StreamingDataSimulator simulator = new StreamingDataSimulator(getTestCardinalityMap(), 200000);
        List<DataSegmentFragment> fragments = createFragmentFiles(9, 3, simulator);
        FragmentsMergeResult mergeResult = fragmentFilesMerger.merge(fragments);
        File mergedFragmentMetaFile = mergeResult.getMergedFragmentMetaFile();
        File mergedFragmentDataFile = mergeResult.getMergedFragmentDataFile();
        FragmentMetaInfo fragmentMetaInfo = JsonUtil.readValue(mergedFragmentMetaFile, FragmentMetaInfo.class);
        assertEquals(160010, fragmentMetaInfo.getNumberOfRows());
    }

    private void setBuildAdditionalCuboids() {
        KylinConfigExt configExt = (KylinConfigExt) cubeInstance.getDescriptor().getConfig();
        configExt.getExtendedOverrides().put("kylin.stream.build.additional.cuboids", "true");
    }

    private Map<String, Integer> getTestCardinalityMap() {
        Map<String, Integer> result = new HashMap();

        result.put("SITE", 10);
        result.put("ITM", 80000);
        return result;
    }

    private List<DataSegmentFragment> createFragmentFiles(int startFragmentId, int fragmentNum,
            StreamingDataSimulator simulator) {
        List<DataSegmentFragment> result = Lists.newArrayListWithCapacity(fragmentNum);
        ColumnarMemoryStorePersister memStorePersister = new ColumnarMemoryStorePersister(parsedStreamingCubeInfo,
                segmentName);
        int eventCntInMem = 50000;
        long startTime = DateFormat.stringToMillis("2018-07-30 20:00:00");
        Iterator<StreamingMessage> streamingMessages = simulator.simulate(eventCntInMem * fragmentNum, startTime);
        for (int i = 0; i < fragmentNum; i++) {
            DataSegmentFragment fragment = new DataSegmentFragment(baseStorePath, cubeName, segmentName,
                    new FragmentId(startFragmentId + i));
            int eventCnt = 0;
            SegmentMemoryStore memoryStore = new SegmentMemoryStore(new ParsedStreamingCubeInfo(cubeInstance),
                    segmentName);
            while (streamingMessages.hasNext() && eventCnt < eventCntInMem) {
                memoryStore.index(streamingMessages.next());
                eventCnt++;
            }
            memStorePersister.persist(memoryStore, fragment);
            result.add(fragment);
        }
        return result;
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getTestConfig());
    }

    private void cleanupData() throws IOException {
        fragmentFilesMerger.cleanMergeDirectory();
        FileUtils.deleteQuietly(new File(baseStorePath));
    }
}
