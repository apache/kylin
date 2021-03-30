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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter.FilterOperatorEnum;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.SingleThreadResultCollector;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.storage.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class SegmentMemoryStoreTest extends LocalFileMetadataTestCase {

    private static final String cubeName = "test_streaming_v2_cube";

    private CubeInstance cubeInstance;
    private String segmentName;
    private CubeDesc cubeDesc;
    private SegmentMemoryStore memoryStore;
    private TestHelper testHelper;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        setUpTestKylinCube();
        this.segmentName = "20171218100000_20171218110000";
        this.memoryStore = new SegmentMemoryStore(new ParsedStreamingCubeInfo(cubeInstance), segmentName);
        this.testHelper = new TestHelper(cubeInstance);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    protected void setUpTestKylinCube() {
        this.cubeInstance = getCubeManager().reloadCubeQuietly(cubeName);
        this.cubeDesc = cubeInstance.getDescriptor();
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getTestConfig());
    }

    @Test
    public void testIndexEvent() {
        prepareDataToMemoryStore(50000);
        assertEquals(50000, memoryStore.getRowCount());
    }

    @Test
    public void testIndexEventForMultipleCuboids() {
        setBuildAdditionalCuboids();
        int eventCnt = 50000;
        prepareDataToMemoryStore(eventCnt);
        int expectedCnt = eventCnt + eventCnt + 10;
        assertEquals(expectedCnt, memoryStore.getRowCount());
    }

    @Test
    public void testSearchBasicCuboid() throws Exception {
        StreamingQueryProfile profile = new StreamingQueryProfile("test-query-id", System.currentTimeMillis());
        StreamingQueryProfile.set(profile);

        int eventCnt = 50000;
        prepareDataToMemoryStore(eventCnt);
        Set<TblColRef> dimensions = testHelper.simulateDimensions("DAY_START", "SITE");
        Set<TblColRef> groups = testHelper.simulateDimensions("DAY_START", "SITE");
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics,
                null, null);
        ResultCollector resultCollector = new SingleThreadResultCollector();
        memoryStore.search(searchRequest, resultCollector);
        int returnRecordCnt = 0;
        int returnColNum = 0;
        for (Record record : resultCollector) {
            returnRecordCnt++;
            returnColNum = record.getDimensions().length + record.getMetrics().length;
        }
        assertEquals(eventCnt, returnRecordCnt);
        assertEquals(3, returnColNum);
    }

    @Test
    public void testSearchWithFilter() throws Exception {
        StreamingQueryProfile profile = new StreamingQueryProfile("test-query-id", System.currentTimeMillis());
        StreamingQueryProfile.set(profile);

        int eventCnt = 50000;
        prepareDataToMemoryStore(eventCnt);
        Set<TblColRef> dimensions = testHelper.simulateDimensions("DAY_START", "SITE");
        Set<TblColRef> groups = testHelper.simulateDimensions("DAY_START", "SITE");
        Set<FunctionDesc> metrics = Sets.newHashSet(testHelper.simulateCountMetric());
        TupleFilter filter = testHelper.buildEQFilter("SITE", "SITE0");
        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics,
                filter, null);
        ResultCollector resultCollector = new SingleThreadResultCollector();
        memoryStore.search(searchRequest, resultCollector);
        int returnRecordCnt = 0;
        int returnColNum = 0;
        for (Record record : resultCollector) {
            returnRecordCnt++;
            returnColNum = record.getDimensions().length + record.getMetrics().length;
        }
        assertEquals(eventCnt/10, returnRecordCnt);
        assertEquals(3, returnColNum);

        dimensions = testHelper.simulateDimensions("DAY_START", "SITE", "ITM");
        filter = testHelper.buildLikeFilter("ITM", "ITM000001%");
        searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics,
                filter, null);
        resultCollector = new SingleThreadResultCollector();
        memoryStore.search(searchRequest, resultCollector);
        returnRecordCnt = 0;
        for (Record record : resultCollector) {
            returnRecordCnt++;
        }
        assertEquals(10000, returnRecordCnt);

        filter = testHelper.buildLowerFilter("ITM", FilterOperatorEnum.EQ, "itm0000010000");
        searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics,
                filter, null);
        resultCollector = new SingleThreadResultCollector();
        memoryStore.search(searchRequest, resultCollector);
        returnRecordCnt = 0;
        for (Record record : resultCollector) {
            returnRecordCnt++;
        }
        assertEquals(1, returnRecordCnt);
    }

    @Test
    public void testSearchSpecificCuboid() throws Exception {
        StreamingQueryProfile profile = new StreamingQueryProfile("test-query-id", System.currentTimeMillis());
        StreamingQueryProfile.set(profile);

        setBuildAdditionalCuboids();
        int eventCnt = 50000;
        prepareDataToMemoryStore(eventCnt);
        Set<TblColRef> dimensions = simulateColumns("SITE");
        Set<TblColRef> groups = Sets.newHashSet();
        Set<FunctionDesc> metrics = simulateMetrics();
        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics,
                null, null);
        assertEquals(1L, searchRequest.getHitCuboid());
        ResultCollector resultCollector = new SingleThreadResultCollector();
        memoryStore.search(searchRequest, resultCollector);
        int returnRecordCnt = 0;
        int returnColNum = 0;
        for (Record record : resultCollector) {
            returnRecordCnt++;
            returnColNum = record.getDimensions().length + record.getMetrics().length;
        }
        assertEquals(10, returnRecordCnt);
        assertEquals(2, returnColNum);
    }

    private void setBuildAdditionalCuboids() {
        KylinConfigExt configExt = (KylinConfigExt) cubeInstance.getDescriptor().getConfig();
        configExt.getExtendedOverrides().put("kylin.stream.build.additional.cuboids", "true");
        this.memoryStore = new SegmentMemoryStore(new ParsedStreamingCubeInfo(cubeInstance), segmentName);
    }

    private void prepareDataToMemoryStore(int eventCnt) {
        Iterator<StreamingMessage> streamingMessages = new StreamingDataSimulator().simulate(eventCnt,
                System.currentTimeMillis());
        while (streamingMessages.hasNext()) {
            memoryStore.index(streamingMessages.next());
        }
    }

    private Map<String, Integer> getColCardMap() {
        Map<String, Integer> result = new HashMap();

        result.put("SITE", 10);
        result.put("ITM", Integer.MAX_VALUE);
        return result;
    }

    private Set<TblColRef> simulateColumns(String... columnNames) {
        Set<TblColRef> columns = Sets.newHashSet();
        for (String columnName : columnNames) {
            if (!columnName.contains(".")) {
                columnName = "STREAMING_V2_TABLE." + columnName;
            }
            TblColRef cf1 = cubeDesc.getModel().findColumn(columnName);
            columns.add(cf1);
        }
        return columns;
    }

    private Set<FunctionDesc> simulateMetrics() {
        List<FunctionDesc> functions = Lists.newArrayList();

        TblColRef gmvCol = cubeDesc.getModel().findColumn("STREAMING_V2_TABLE.GMV");
        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = ParameterDesc.newInstance(gmvCol);
        f1.setParameter(p1);
        f1.setReturnType("decimal(19,6)");
        functions.add(f1);

        return Sets.newHashSet(functions);
    }

}
