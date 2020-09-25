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
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.SingleThreadResultCollector;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.MockPositionHandler;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.storage.StreamingCubeSegment;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.core.storage.TestHelper;
import org.apache.kylin.stream.core.storage.columnar.ColumnarSegmentStore;
import org.apache.kylin.stream.core.storage.columnar.StreamingDataSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PerformanceTest extends LocalFileMetadataTestCase {
    private static final String cubeName = "test_streaming_v2_cube";
    private static Logger logger = LoggerFactory.getLogger(PerformanceTest.class);

    private CubeInstance cubeInstance;
    private CubeDesc cubeDesc;
    private String baseStorePath;
    private StreamingSegmentManager cubeDataStore;
    private TestHelper testHelper;

    public PerformanceTest() throws Exception {
        this.createTestMetadata();
        setUpTestKylinCube();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        this.cubeDataStore = new StreamingSegmentManager(baseStorePath, cubeInstance, new MockPositionHandler(), null);
        testHelper = new TestHelper(cubeInstance);
    }

    public static void main(String[] args) throws Exception {
        PerformanceTest test = new PerformanceTest();
        test.run();
    }

    public void run() {
        System.out.println("Start generating data");
        StreamingQueryProfile.set(new StreamingQueryProfile("test-query-id", System.currentTimeMillis()));
        StreamingDataSimulator simulator = new StreamingDataSimulator();
        long time = System.currentTimeMillis();
        int rowCnt = 10000000;
        Iterator<StreamingMessage> messageItr = simulator.simulate(rowCnt, time);
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        while (messageItr.hasNext()) {
            StreamingMessage message = messageItr.next();
            cubeDataStore.addEvent(message);
        }
        long takeTime = sw.elapsed(MILLISECONDS);
        System.out.println("Index took:" + takeTime + ",qps:" + rowCnt / (takeTime / 1000));
        sw.reset();

        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE");
        Set<TblColRef> groups = dimensions;
        Set<FunctionDesc> metrics = testHelper.simulateMetrics();
        System.out.println("Scan with no filter:");
        try {
            System.out.println("start first scan");
            sw.start();
            final Iterator<Record> firstScan = scan(cubeDataStore, null, dimensions, groups, metrics);
            int rowNum = 0;
            while (firstScan.hasNext()) {
                Record record = firstScan.next();
                rowNum++;
            }

            takeTime = sw.elapsed(MILLISECONDS);
            System.out.println("scan finished, total rows:" + rowNum);
            System.out.println("first scan took:" + takeTime + ",rowsPerSec:" + (rowNum / takeTime) * 1000);

            System.out.println("start second scan");
            sw.reset();
            sw.start();
            final Iterator<Record> secondScan = scan(cubeDataStore, null, dimensions, groups, metrics);
            rowNum = 0;
            while (secondScan.hasNext()) {
                Record record = secondScan.next();
                rowNum++;
            }

            takeTime = sw.elapsed(MILLISECONDS);
            System.out.println("total rows:" + rowNum);
            System.out.println("second scan took:" + takeTime + ",rowsPerSec:" + (rowNum / takeTime) * 1000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        cleanupData();
    }

    private Iterator<Record> scan(StreamingSegmentManager cubeDataStore, TupleFilter tupleFilter,
            Set<TblColRef> dimensions, Set<TblColRef> groups, Set<FunctionDesc> metrics) throws IOException {
        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics,
                tupleFilter, null);
        ResultCollector resultCollector = getResultCollector();
        for (StreamingCubeSegment queryableSegment : cubeDataStore.getAllSegments()) {
            ColumnarSegmentStore segmentStore = new ColumnarSegmentStore(baseStorePath, cubeInstance,
                    queryableSegment.getSegmentName());
                segmentStore.init();
                segmentStore.search(searchRequest, resultCollector);

        }

        return resultCollector.iterator();
    }

    private ResultCollector getResultCollector() {
        return new SingleThreadResultCollector();
    }

    protected void setUpTestKylinCube() {
        this.cubeInstance = getCubeManager().reloadCubeQuietly(cubeName);
        this.cubeDesc = cubeInstance.getDescriptor();
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getTestConfig());
    }

    private void cleanupData() {
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File(baseStorePath));
    }

}
