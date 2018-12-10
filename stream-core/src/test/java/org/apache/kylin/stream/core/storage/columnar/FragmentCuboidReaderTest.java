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
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.storage.columnar.ParsedStreamingCubeInfo.CuboidInfo;
import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FragmentCuboidReaderTest extends LocalFileMetadataTestCase {
    private static final String cubeName = "test_streaming_v2_cube";

    private String baseStorePath;
    private CubeInstance cubeInstance;
    private String segmentName;
    private ParsedStreamingCubeInfo parsedStreamingCubeInfo;
    private DataSegmentFragment fragment;

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
    }

    @Test
    public void testIterateAndRead() throws Exception {
        FragmentMetaInfo fragmentMetaInfo = fragment.getMetaInfo();
        FragmentData fragmentData = new FragmentData(fragmentMetaInfo, fragment.getDataFile());
        CuboidInfo cuboidInfo = parsedStreamingCubeInfo.getCuboidInfo(parsedStreamingCubeInfo.basicCuboid.getId());
        Map<TblColRef, Dictionary<String>> dictionaryMap = fragmentData
                .getDimensionDictionaries(parsedStreamingCubeInfo.dimensionsUseDictEncoding);
        TblColRef[] dimensions = cuboidInfo.getDimensions();
        DimensionEncoding[] dimensionEncodings = ParsedStreamingCubeInfo.getDimensionEncodings(
                parsedStreamingCubeInfo.cubeDesc, dimensions, dictionaryMap);
        FragmentCuboidReader fragmentCuboidReader = new FragmentCuboidReader(parsedStreamingCubeInfo.cubeDesc,
                fragmentData, fragmentMetaInfo.getBasicCuboidMetaInfo(), cuboidInfo.getDimensions(),
                parsedStreamingCubeInfo.measureDescs, dimensionEncodings);

        int i = 0;
        for (RawRecord rawRecord : fragmentCuboidReader) {
            System.out.println(rawRecord);
            if (i > 10) {
                break;
            }
            i++;
        }
        RawRecord rawRecord = fragmentCuboidReader.read(9999);
        byte[] itmVal = rawRecord.getDimensions()[0];
        assertEquals("ITM0000009999", dimensionEncodings[0].decode(itmVal, 0, itmVal.length));

        cuboidInfo = parsedStreamingCubeInfo.getCuboidInfo(17);
        dictionaryMap = fragmentData.getDimensionDictionaries(parsedStreamingCubeInfo.dimensionsUseDictEncoding);
        dimensions = cuboidInfo.getDimensions();
        dimensionEncodings = ParsedStreamingCubeInfo.getDimensionEncodings(parsedStreamingCubeInfo.cubeDesc,
                dimensions, dictionaryMap);
        fragmentCuboidReader = new FragmentCuboidReader(parsedStreamingCubeInfo.cubeDesc, fragmentData,
                fragmentMetaInfo.getBasicCuboidMetaInfo(), cuboidInfo.getDimensions(),
                parsedStreamingCubeInfo.measureDescs, dimensionEncodings);

        int j = 0;
        for (RawRecord newRawRecord : fragmentCuboidReader) {
            System.out.println(newRawRecord);
            if (j >= 10) {
                break;
            }
            j++;
        }
    }

    private void prepareData() {
        ColumnarMemoryStorePersister memStorePersister = new ColumnarMemoryStorePersister(parsedStreamingCubeInfo,
                segmentName);
        StreamingDataSimulator simulator = new StreamingDataSimulator(
                StreamingDataSimulator.getDefaultCardinalityMap(), 100000);
        Iterator<StreamingMessage> streamingMessages = simulator.simulate(50000, System.currentTimeMillis());
        SegmentMemoryStore memoryStore = new SegmentMemoryStore(new ParsedStreamingCubeInfo(cubeInstance), segmentName);
        while (streamingMessages.hasNext()) {
            memoryStore.index(streamingMessages.next());
        }

        memStorePersister.persist(memoryStore, fragment);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        cleanupData();
    }

    private void cleanupData() throws IOException {
        FileUtils.deleteQuietly(new File(baseStorePath));
    }

}
