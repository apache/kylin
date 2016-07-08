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

package org.apache.kylin.engine.mr.steps;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

public class NDCuboidMapperTest extends LocalFileMetadataTestCase {
    MapReduceDriver<Text, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();

        // hack for distributed cache
        FileUtils.deleteDirectory(new File("./meta"));
        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl()), new File("./meta"));

        NDCuboidMapper mapper = new NDCuboidMapper();
        CuboidReducer reducer = new CuboidReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteDirectory(new File("./meta"));
    }

    @Test
    public void testMapReduceWithSlr() throws IOException {

        String cubeName = "test_kylin_cube_with_slr_1_new_segment";
        String segmentID = "198va32a-a33e-4b69-83dd-0bb8b1f8c53b";
        mapReduceDriver.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
        mapReduceDriver.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

        byte[] key = { 0, 0, 0, 0, 0, 0, 0, 0, 1, -1, 0, -104, -106, -128, 11, 54, -105, 55, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 13, 71, 114, 65, 66, 73, 78, 9, 9, 9, 9, 9, 9, 9, 9, 0, 10, 0 };
        byte[] value = { 14, 7, 23, -16, 56, 92, 114, -80, 118, 14, 7, 23, -16, 56, 92, 114, -80, 118, 14, 7, 23, -16, 56, 92, 114, -80, 118, 1, 1 };
        Pair<Text, Text> input1 = new Pair<Text, Text>(new Text(key), new Text(value));

        mapReduceDriver.addInput(input1);

        List<Pair<Text, Text>> result = mapReduceDriver.run();

        assertEquals(4, result.size());

        byte[] resultKey = { 0, 0, 0, 0, 0, 0, 0, 0, 1, 127, 0, -104, -106, -128, 55, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 13, 71, 114, 65, 66, 73, 78, 9, 9, 9, 9, 9, 9, 9, 9, 0, 10, 0 };
        byte[] resultValue = { 14, 7, 23, -16, 56, 92, 114, -80, 118, 14, 7, 23, -16, 56, 92, 114, -80, 118, 14, 7, 23, -16, 56, 92, 114, -80, 118, 1, 1 };
        Pair<Text, Text> output1 = new Pair<Text, Text>(new Text(resultKey), new Text(resultValue));

        //As we will truncate decimal(KYLIN-766), value will no longer equals to resultValue
        Collection<Text> keys = Collections2.transform(result, new Function<Pair<Text, Text>, Text>() {
            @Nullable
            @Override
            public Text apply(Pair<Text, Text> input) {
                return input.getFirst();
            }
        });
        assertTrue(keys.contains(output1.getFirst()));
        assertTrue(!result.contains(output1));

        long[] keySet = new long[result.size()];

        System.out.println(Bytes.toLong(new byte[] { 0, 0, 0, 0, 0, 0, 1, -1 }));
        for (int i = 0; i < result.size(); i++) {
            byte[] bytes = new byte[result.get(i).getFirst().getLength()];
            System.arraycopy(result.get(i).getFirst().getBytes(), RowConstants.ROWKEY_SHARDID_LEN, bytes, 0, result.get(i).getFirst().getLength() - RowConstants.ROWKEY_SHARDID_LEN);
            System.out.println(Bytes.toLong(bytes));
            keySet[i] = Bytes.toLong(bytes);
        }

        // refer to CuboidSchedulerTest.testGetSpanningCuboid()
        assertArrayEquals(new long[] { 383, 447, 503, 504 }, keySet);

    }
}
