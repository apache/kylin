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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.zip.DataFormatException;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.query.ResponseResultSchema;
import org.apache.kylin.stream.core.util.RecordsSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class RecordsSerDeTest extends LocalFileMetadataTestCase {

    private static final String cubeName = "test_streaming_v2_cube";

    private CubeDesc cubeDesc;
    private CubeInstance cubeInstance;
    private TestHelper testHelper;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.cubeInstance = CubeManager.getInstance(getTestConfig()).getCube(cubeName);
        this.cubeDesc = cubeInstance.getDescriptor();
        this.testHelper = new TestHelper(cubeInstance);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void serDeTest() throws IOException, DataFormatException {
        Set<TblColRef> dimensions = testHelper.simulateDimensions("STREAMING_V2_TABLE.SITE",
                "STREAMING_V2_TABLE.MINUTE_START");
        Set<FunctionDesc> metrics = Sets.newHashSet();

        metrics.add(testHelper.simulateCountMetric());

        ResponseResultSchema schema = new ResponseResultSchema(cubeDesc, dimensions, metrics);
        RecordsSerializer serializer = new RecordsSerializer(schema);
        List<Record> records = new ArrayList<>();
        int rowsNum = 10;
        for (int i = 0; i < rowsNum; i++) {
            Record record = new Record(dimensions.size(), metrics.size());
            record.setDimension(0, "site" + i);
            record.setDimension(1, "" + i);
            record.setMetric(0, 10000L + i);
            records.add(record);
        }
        Pair<byte[], Long> serializedData = serializer.serialize(records.iterator(), Integer.MAX_VALUE);

        Iterator<Record> desRecords = serializer.deserialize(serializedData.getFirst());
        while (desRecords.hasNext()) {
            Record record = desRecords.next();
            System.out.println(record);
        }
    }
}
