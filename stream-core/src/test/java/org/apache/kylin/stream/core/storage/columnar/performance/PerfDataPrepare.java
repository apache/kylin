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
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.storage.MockPositionHandler;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.core.storage.columnar.StreamingDataSimulator;

public class PerfDataPrepare extends LocalFileMetadataTestCase {
    private static final String cubeName = "test_streaming_v2_cube";

    private String baseStorePath;
    private CubeInstance cubeInstance;
    private int totalRows;
    private StreamingSegmentManager streamingSegmentManager;

    public PerfDataPrepare(int totalRows) throws Exception {
        this.createTestMetadata();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        this.cubeInstance = CubeManager.getInstance(getTestConfig()).reloadCubeQuietly(cubeName);
        this.totalRows = totalRows;
        setupCubeConfig();
        this.streamingSegmentManager = new StreamingSegmentManager(baseStorePath, cubeInstance, new MockPositionHandler(), null);
    }

    private void setupCubeConfig() {
        KylinConfig kylinConfig = cubeInstance.getConfig();
        kylinConfig.setProperty("kylin.stream.index.maxrows", "1000000");
    }

    public static void main(String[] args) throws Exception {
        PerfDataPrepare test = new PerfDataPrepare(72000000);
        test.prepareData();
        test.cleanTmpMeta();
    }

    public void prepareData() {
        System.out.println("start preparing data...");
        long startTime = System.currentTimeMillis();
        StreamingDataSimulator simulator = new StreamingDataSimulator(
                StreamingDataSimulator.getDefaultCardinalityMap(), 600000);
        long eventStartTime = DateFormat.stringToMillis("2018-07-30 07:00:00");
        Iterator<StreamingMessage> streamingMessages = simulator.simulate(totalRows, eventStartTime);

        while (streamingMessages.hasNext()) {
            streamingSegmentManager.addEvent(streamingMessages.next());
        }
        streamingSegmentManager.checkpoint();
        System.out.println("data is prepared. take time:" + (System.currentTimeMillis() - startTime));
    }

    public void cleanData() throws Exception {
        FileUtils.deleteQuietly(new File(baseStorePath));
    }

    public void cleanTmpMeta() {
        cleanupTestMetadata();
    }

}
