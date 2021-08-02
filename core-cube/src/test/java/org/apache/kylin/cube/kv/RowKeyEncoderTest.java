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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class RowKeyEncoderTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testEncodeWithoutSlr() throws Exception {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITHOUT_SLR_READY");
        // CubeSegment seg = cube.getTheOnlySegment();
        CubeDesc cubeDesc = cube.getDescriptor();
        // String data =
        // "2013-08-18Abbigliamento e accessoriDonna: AccessoriSciarpFoulard e ScialliAuctionItalyRegular";
        String[] data = new String[8];
        data[0] = "2012-12-15";
        data[1] = "11848";
        data[2] = "Health & Beauty";
        data[3] = "Fragrances";
        data[4] = "Women";
        data[5] = "FP-GTC";
        data[6] = "0";
        data[7] = "15";

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findForMandatory(cubeDesc, baseCuboidId);
        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(cube.getFirstSegment(), baseCuboid);

        byte[] encodedKey = rowKeyEncoder.encode(data);
        assertEquals(22 + rowKeyEncoder.getHeaderLength(), encodedKey.length);
        byte[] cuboidId = Arrays.copyOfRange(encodedKey, RowConstants.ROWKEY_SHARDID_LEN, rowKeyEncoder.getHeaderLength());
        byte[] rest = Arrays.copyOfRange(encodedKey, rowKeyEncoder.getHeaderLength(), encodedKey.length);
        assertEquals(255, Bytes.toLong(cuboidId));
        assertArrayEquals(new byte[] { 11, 55, -13, 13, 22, 34, 121, 70, 80, 45, 71, 84, 67, 9, 9, 9, 9, 9, 9, 0, 10, 5 }, rest);
    }

    @Ignore
    @Test
    public void testEncodeWithSlr() throws Exception {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITH_SLR_READY");
        // CubeSegment seg = cube.getTheOnlySegment();
        CubeDesc cubeDesc = cube.getDescriptor();
        // String data =
        // "1234567892013-08-18Abbigliamento e accessoriDonna: AccessoriSciarpFoulard e ScialliAuctionItalyRegular";
        String[] data = new String[9];
        data[0] = "123456789";
        data[1] = "2012-12-15";
        data[2] = "11848";
        data[3] = "Health & Beauty";
        data[4] = "Fragrances";
        data[5] = "Women";
        data[6] = "FP-GTC";
        data[7] = "0";
        data[8] = "15";

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findForMandatory(cubeDesc, baseCuboidId);
        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(cube.getFirstSegment(), baseCuboid);

        byte[] encodedKey = rowKeyEncoder.encode(data);
        assertEquals(43 + rowKeyEncoder.getHeaderLength(), encodedKey.length);
        byte[] shard = Arrays.copyOfRange(encodedKey, 0, RowConstants.ROWKEY_SHARDID_LEN);
        @SuppressWarnings("unused")
        byte[] sellerId = Arrays.copyOfRange(encodedKey, rowKeyEncoder.getHeaderLength(), 4 + rowKeyEncoder.getHeaderLength());
        byte[] cuboidId = Arrays.copyOfRange(encodedKey, RowConstants.ROWKEY_SHARDID_LEN, rowKeyEncoder.getHeaderLength());
        byte[] rest = Arrays.copyOfRange(encodedKey, 4 + rowKeyEncoder.getHeaderLength(), encodedKey.length);
        assertEquals(0, Bytes.toShort(shard));
        //        assertTrue(Bytes.toString(sellerId).startsWith("123456789"));
        assertEquals(511, Bytes.toLong(cuboidId));
        assertArrayEquals(new byte[] { 11, 55, -13, 49, 49, 56, 52, 56, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 22, 34, 121, 70, 80, 45, 71, 84, 67, 9, 9, 9, 9, 9, 9, 0, 10, 5 }, rest);
    }

    @Ignore
    @Test
    public void testEncodeWithSlr2() throws Exception {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITH_SLR_READY");
        // CubeSegment seg = cube.getTheOnlySegment();
        CubeDesc cubeDesc = cube.getDescriptor();
        // String data =
        // "1234567892013-08-18Abbigliamento e accessoriDonna: AccessoriSciarpFoulard e ScialliAuctionItalyRegular";
        String[] data = new String[9];
        data[0] = "123456789";
        data[1] = null;
        data[2] = null;
        data[3] = null;
        data[4] = null;
        data[5] = null;
        data[6] = null;
        data[7] = null;
        data[8] = null;

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findForMandatory(cubeDesc, baseCuboidId);
        RowKeyEncoder rowKeyEncoder = new RowKeyEncoder(cube.getFirstSegment(), baseCuboid);

        byte[] encodedKey = rowKeyEncoder.encode(data);
        assertEquals(43 + rowKeyEncoder.getHeaderLength(), encodedKey.length);
        byte[] shard = Arrays.copyOfRange(encodedKey, 0, RowConstants.ROWKEY_SHARDID_LEN);
        byte[] cuboidId = Arrays.copyOfRange(encodedKey, RowConstants.ROWKEY_SHARDID_LEN, rowKeyEncoder.getHeaderLength());
        @SuppressWarnings("unused")
        byte[] sellerId = Arrays.copyOfRange(encodedKey, rowKeyEncoder.getHeaderLength(), 18 + rowKeyEncoder.getHeaderLength());
        byte[] rest = Arrays.copyOfRange(encodedKey, 4 + rowKeyEncoder.getHeaderLength(), encodedKey.length);
        assertEquals(0, Bytes.toShort(shard));
        //assertTrue(Bytes.toString(sellerId).startsWith("123456789"));
        assertEquals(511, Bytes.toLong(cuboidId));
        assertArrayEquals(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 }, rest);
    }
}
