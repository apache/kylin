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

package org.apache.kylin.engine.mr.streaming;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.storage.columnar.ColumnarMemoryStorePersister;
import org.apache.kylin.stream.core.storage.columnar.DataSegmentFragment;
import org.apache.kylin.stream.core.storage.columnar.FragmentData;
import org.apache.kylin.stream.core.storage.columnar.FragmentFileSearcher;
import org.apache.kylin.stream.core.storage.columnar.FragmentId;
import org.apache.kylin.stream.core.storage.columnar.ParsedStreamingCubeInfo;
import org.apache.kylin.stream.core.storage.columnar.SegmentMemoryStore;
import org.apache.kylin.stream.core.storage.columnar.StreamingDataSimulator;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RowRecordReaderTest extends LocalFileMetadataTestCase {
    private static final String cubeName = "test_streaming_v2_cube";

    private String baseStorePath;
    private CubeInstance cubeInstance;
    private String segmentName;
    private ParsedStreamingCubeInfo parsedStreamingCubeInfo;
    private DataSegmentFragment fragment;
    private FragmentFileSearcher fragmentFileSearcher;
    private CubeDesc cubeDesc;
    private int eventCnt = 50000;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.baseStorePath = KylinConfig.getInstanceFromEnv().getStreamingIndexPath();
        this.cubeInstance = CubeManager.getInstance(getTestConfig()).reloadCube(cubeName);
        this.cubeDesc = cubeInstance.getDescriptor();
        this.segmentName = "20171018100000_20171018110000";
        this.parsedStreamingCubeInfo = new ParsedStreamingCubeInfo(cubeInstance);
        this.fragment = new DataSegmentFragment(baseStorePath, cubeName, segmentName, new FragmentId(0));
        PropertyConfigurator.configure("../build/conf/kylin-tools-log4j.properties");
        prepareData();
        fragmentFileSearcher = new FragmentFileSearcher(fragment, new FragmentData(fragment.getMetaInfo(),
                fragment.getDataFile()));
        StreamingQueryProfile.set(new StreamingQueryProfile("test-query-id", System.currentTimeMillis()));
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        cleanupData();
    }

    @Test
    public void testIterator() throws Exception {
        Path path = new Path(fragment.getDataFile().getParentFile().getAbsolutePath());
        FileSystem fs = FileSystem.getLocal(new Configuration());
        RowRecordReader rowRecordReader = new RowRecordReader(cubeDesc, path, fs);
        List<MeasureDesc> measures = cubeDesc.getMeasures();
        List<DataTypeSerializer> dataTypeSerializers = Lists.newArrayListWithCapacity(measures.size());
        for (MeasureDesc measure : measures) {
            dataTypeSerializers.add(DataTypeSerializer.create(measure.getFunction().getReturnDataType()));
        }
        int rowNum = 0;
        while (rowRecordReader.hasNextRow()) {
            RowRecord record = rowRecordReader.nextRow();
            if (rowNum < 10) {
                String[] dimensions = record.getDimensions();
                byte[][] metrics = record.getMetrics();
                Object[] metricValues = new Object[metrics.length];
                for (int i = 0; i < metrics.length; i++) {
                    metricValues[i] = dataTypeSerializers.get(i).deserialize(ByteBuffer.wrap(metrics[i]));
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < dimensions.length; i++) {
                    sb.append(dimensions[i]);
                    sb.append(",");
                }
                for (int i = 0; i < metricValues.length; i++) {
                    sb.append(metricValues[i].toString());
                    sb.append(",");
                }
                System.out.println(sb.toString());
            }
            rowNum++;
        }
        assertEquals(eventCnt, rowNum);
    }

    protected void prepareData() {
        // build additional cuboids
        KylinConfigExt configExt = (KylinConfigExt) cubeInstance.getDescriptor().getConfig();
        configExt.getExtendedOverrides().put("kylin.stream.build.additional.cuboids", "true");

        ColumnarMemoryStorePersister memStorePersister = new ColumnarMemoryStorePersister(parsedStreamingCubeInfo,
                segmentName);
        StreamingDataSimulator simulator = new StreamingDataSimulator(
                StreamingDataSimulator.getDefaultCardinalityMap(), 100000);
        Iterator<StreamingMessage> streamingMessages = simulator.simulate(eventCnt, System.currentTimeMillis());
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
