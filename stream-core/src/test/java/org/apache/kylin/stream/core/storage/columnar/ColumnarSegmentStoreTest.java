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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class ColumnarSegmentStoreTest extends LocalFileMetadataTestCase {

    private static final String cubeName = "test_streaming_v2_cube";
    private static Logger logger = LoggerFactory.getLogger(ColumnarSegmentStoreTest.class);

    private CubeInstance cubeInstance;
    private CubeDesc cubeDesc;
    private String baseStorePath;
    private String segmentName;
    private ColumnarSegmentStore segmentStore;
    private TestHelper testHelper;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        cleanupData();
        this.cubeInstance = getCubeManager().getCube(cubeName);
        this.cubeDesc = cubeInstance.getDescriptor();
        this.segmentName = "20171218100000_20171218110000";
        this.segmentStore = new ColumnarSegmentStore(baseStorePath, cubeInstance, segmentName);
        this.testHelper = new TestHelper(cubeInstance);
    }

    private void prepareTestData() {
        Iterator<StreamingMessage> messages = new StreamingDataSimulator().simulate(200000, System.currentTimeMillis());
        while (messages.hasNext()) {
            segmentStore.addEvent(messages.next());
        }
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getTestConfig());
    }

    @Test
    public void testSearch() throws Exception {
        kickSearchRequests(2);
        prepareTestData();
        segmentStore.doMergeFragments(Lists.newArrayList(segmentStore.getAllFragments()));
        Thread.sleep(5000);
        cleanupData();
    }

    private void kickSearchRequests(int parallel) {
        for (int i = 0; i < parallel; i++) {
            new SearchClient(i).start();
        }
    }

    private void scanStore() throws IOException {
        Set<TblColRef> dimensions = testHelper.simulateDimensions(new String[] { "STREAMING_V2_TABLE.SITE" });
        Set<TblColRef> groups = testHelper.simulateDimensions(new String[] { "STREAMING_V2_TABLE.SITE" });
        Set<FunctionDesc> metrics = testHelper.simulateMetrics();
        StreamingSearchContext searchRequest = new StreamingSearchContext(cubeDesc, dimensions, groups, metrics,
                null, null);
        ResultCollector resultCollector = new SingleThreadResultCollector();
        segmentStore.search(searchRequest, resultCollector);
        int count = 0;
        for (Record record : resultCollector) {
            count++;
        }
        resultCollector.close();
    }

    @Test
    public void testFindFragmentsToMerge() {
        List<DataSegmentFragment> allFragments = Lists.newArrayList();
        KylinConfig config = getTestConfig();
        config.setProperty("kylin.stream.segment-min-fragments", "5");
        config.setProperty("kylin.stream.max-fragment-size-mb", "100");
        segmentStore.latestCheckpointFragment = 1000;
        List<DataSegmentFragment> result = segmentStore.chooseFragmentsToMerge(config, allFragments);
        assertTrue(result.isEmpty());
        for (int i = 0; i < 10; i++) {
            allFragments.add(new MockFragment(new FragmentId(i), 15));
        }
        result = segmentStore.chooseFragmentsToMerge(config, allFragments);
        assertEquals(6, result.size());

        allFragments.clear();
        for (int i = 0; i < 50; i++) {
            allFragments.add(new MockFragment(new FragmentId(i), 1));
        }
        result = segmentStore.chooseFragmentsToMerge(config, allFragments);
        assertEquals(46, result.size());

        allFragments.clear();
        allFragments.add(new MockFragment(new FragmentId(0), 100));
        for (int i = 1; i < 10; i++) {
            allFragments.add(new MockFragment(new FragmentId(i), 15));
        }
        result = segmentStore.chooseFragmentsToMerge(config, allFragments);
        assertEquals(6, result.size());

        allFragments.clear();
        allFragments.add(new MockFragment(new FragmentId(0, 5), 50));
        for (int i = 6; i < 20; i++) {
            allFragments.add(new MockFragment(new FragmentId(i), 15));
        }
        result = segmentStore.chooseFragmentsToMerge(config, allFragments);
        assertTrue(result.get(0).getFragmentId().equals(new FragmentId(6)));
        assertEquals(6, result.size());
    }

    private void cleanupData() throws IOException {
        FileUtils.deleteQuietly(new File(baseStorePath));
    }

    private class SearchClient extends Thread {
        private int id;

        public SearchClient(int id) {
            super("thread-" + id);
            this.id = id;
            this.setDaemon(true);
        }

        @Override
        public void run() {
            StreamingQueryProfile.set(new StreamingQueryProfile("test-query-id", System.currentTimeMillis()));
            System.out.println("search client " + id + " start");
            int round = 1;
            while (true) {
                try {
                    scanStore();
                    System.out.println("client:" + id + " round" + round + " scan finished");
                    round++;
                    Thread.sleep(100);
                } catch (Exception e) {
                    logger.error("error", e);
                }
            }
        }
    }

    private class MockFragment extends DataSegmentFragment {
        private long mockedDataFileSize;

        public MockFragment(FragmentId fragmentId, int mockedDataFileSizeMb) {
            super(baseStorePath, cubeName, segmentName, fragmentId);
            this.mockedDataFileSize = mockedDataFileSizeMb * 1024 * 1024;
        }

        public long getDataFileSize() {
            return mockedDataFileSize;
        }
    }
}
