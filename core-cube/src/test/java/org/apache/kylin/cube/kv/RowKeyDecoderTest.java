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

package org.apache.kylin.cube.kv;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RowKeyDecoderTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.clearCache();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testDecodeWithoutSlr() throws Exception {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITHOUT_SLR_READY");

        RowKeyDecoder rowKeyDecoder = new RowKeyDecoder(cube.getFirstSegment());

        byte[] key = { 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, 11, 55, -13, 13, 22, 34, 121, 70, 80, 45, 71, 84, 67, 9, 9, 9, 9, 9, 9, 0, 10, 5 };

        rowKeyDecoder.decode(key);
        List<String> values = rowKeyDecoder.getValues();
        assertEquals("[" + millis("2012-12-15") + ", 11848, Health & Beauty, Fragrances, Women, FP-GTC, 0, 15]", values.toString());
    }

    @Test
    public void testDecodeWithSlr() throws Exception {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITH_SLR_READY");

        RowKeyDecoder rowKeyDecoder = new RowKeyDecoder(cube.getFirstSegment());

        byte[] key = { 0, 0, 0, 0, 0, 0, 0, 0, 1, -1, 0, -104, -106, -128, 11, 54, -105, 50, 48, 50, 49, 51, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 13, 71, 114, 65, 66, 73, 78, 9, 9, 9, 9, 9, 9, 9, 9, 0, 10, 0 };

        rowKeyDecoder.decode(key);
        List<String> values = rowKeyDecoder.getValues();
        assertEquals("[10000000, " + millis("2012-01-02") + ", 20213, Collectibles, Postcards, US StateCities & Towns, ABIN, 0, -99]", values.toString());
    }

    @Test
    public void testEncodeAndDecodeWithUtf8() throws IOException {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITHOUT_SLR_READY");
        CubeDesc cubeDesc = cube.getDescriptor();

        byte[][] data = new byte[8][];
        data[0] = Bytes.toBytes("2012-12-15");
        data[1] = Bytes.toBytes("11848");
        data[2] = Bytes.toBytes("Health & Beauty");
        data[3] = Bytes.toBytes("Fragrances");
        data[4] = Bytes.toBytes("Women");
        data[5] = Bytes.toBytes("刊登格式测试");// UTF-8
        data[6] = Bytes.toBytes("0");
        data[7] = Bytes.toBytes("15");

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(cube.getFirstSegment(), baseCuboid);

        byte[] encodedKey = rowKeyEncoder.encode(data);
        assertEquals(22 + rowKeyEncoder.getHeaderLength(), encodedKey.length);

        RowKeyDecoder rowKeyDecoder = new RowKeyDecoder(cube.getFirstSegment());
        rowKeyDecoder.decode(encodedKey);
        List<String> values = rowKeyDecoder.getValues();
        assertEquals("[" + millis("2012-12-15") + ", 11848, Health & Beauty, Fragrances, Women, 刊登格式, 0, 15]", values.toString());
    }

    private String millis(String dateStr) {
        return String.valueOf(DateFormat.stringToMillis(dateStr));
    }

}
