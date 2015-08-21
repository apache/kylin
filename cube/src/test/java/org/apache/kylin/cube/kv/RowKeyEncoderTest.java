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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author George Song (ysong1)
 * 
 */
public class RowKeyEncoderTest extends LocalFileMetadataTestCase {

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
    public void testEncodeWithoutSlr() throws Exception {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITHOUT_SLR_READY");
        // CubeSegment seg = cube.getTheOnlySegment();
        CubeDesc cubeDesc = cube.getDescriptor();
        // String data =
        // "2013-08-18Abbigliamento e accessoriDonna: AccessoriSciarpFoulard e ScialliAuctionItalyRegular";
        byte[][] data = new byte[8][];
        data[0] = Bytes.toBytes("2012-12-15");
        data[1] = Bytes.toBytes("11848");
        data[2] = Bytes.toBytes("Health & Beauty");
        data[3] = Bytes.toBytes("Fragrances");
        data[4] = Bytes.toBytes("Women");
        data[5] = Bytes.toBytes("FP-GTC");
        data[6] = Bytes.toBytes("0");
        data[7] = Bytes.toBytes("15");

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        AbstractRowKeyEncoder rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cube.getFirstSegment(), baseCuboid);

        byte[] encodedKey = rowKeyEncoder.encode(data);
        assertEquals(30, encodedKey.length);
        byte[] cuboidId = Arrays.copyOfRange(encodedKey, 0, 8);
        byte[] rest = Arrays.copyOfRange(encodedKey, 8, encodedKey.length);
        assertEquals(255, Bytes.toLong(cuboidId));
        assertArrayEquals(new byte[] { 11, 55, -13, 13, 22, 34, 121, 70, 80, 45, 71, 84, 67, 9, 9, 9, 9, 9, 9, 0, 10, 5 }, rest);
    }

    @Test
    public void testEncodeWithSlr() throws Exception {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITH_SLR_READY");
        // CubeSegment seg = cube.getTheOnlySegment();
        CubeDesc cubeDesc = cube.getDescriptor();
        // String data =
        // "1234567892013-08-18Abbigliamento e accessoriDonna: AccessoriSciarpFoulard e ScialliAuctionItalyRegular";
        byte[][] data = new byte[9][];
        data[0] = Bytes.toBytes("123456789");
        data[1] = Bytes.toBytes("2012-12-15");
        data[2] = Bytes.toBytes("11848");
        data[3] = Bytes.toBytes("Health & Beauty");
        data[4] = Bytes.toBytes("Fragrances");
        data[5] = Bytes.toBytes("Women");
        data[6] = Bytes.toBytes("FP-GTC");
        data[7] = Bytes.toBytes("0");
        data[8] = Bytes.toBytes("15");

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        AbstractRowKeyEncoder rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cube.getFirstSegment(), baseCuboid);

        byte[] encodedKey = rowKeyEncoder.encode(data);
        assertEquals(48, encodedKey.length);
        byte[] sellerId = Arrays.copyOfRange(encodedKey, 8, 26);
        byte[] cuboidId = Arrays.copyOfRange(encodedKey, 0, 8);
        byte[] rest = Arrays.copyOfRange(encodedKey, 26, encodedKey.length);
        assertTrue(Bytes.toString(sellerId).startsWith("123456789"));
        assertEquals(511, Bytes.toLong(cuboidId));
        assertArrayEquals(new byte[] { 11, 55, -13, 13, 22, 34, 121, 70, 80, 45, 71, 84, 67, 9, 9, 9, 9, 9, 9, 0, 10, 5 }, rest);
    }

    @Test
    public void testEncodeWithSlr2() throws Exception {
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITH_SLR_READY");
        // CubeSegment seg = cube.getTheOnlySegment();
        CubeDesc cubeDesc = cube.getDescriptor();
        // String data =
        // "1234567892013-08-18Abbigliamento e accessoriDonna: AccessoriSciarpFoulard e ScialliAuctionItalyRegular";
        byte[][] data = new byte[9][];
        data[0] = Bytes.toBytes("123456789");
        data[1] = null;
        data[2] = null;
        data[3] = null;
        data[4] = null;
        data[5] = null;
        data[6] = null;
        data[7] = null;
        data[8] = null;

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        AbstractRowKeyEncoder rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cube.getFirstSegment(), baseCuboid);

        byte[] encodedKey = rowKeyEncoder.encode(data);
        assertEquals(48, encodedKey.length);
        byte[] sellerId = Arrays.copyOfRange(encodedKey, 8, 26);
        byte[] cuboidId = Arrays.copyOfRange(encodedKey, 0, 8);
        byte[] rest = Arrays.copyOfRange(encodedKey, 26, encodedKey.length);
        assertTrue(Bytes.toString(sellerId).startsWith("123456789"));
        assertEquals(511, Bytes.toLong(cuboidId));
        assertArrayEquals(new byte[] { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 }, rest);
    }
}
