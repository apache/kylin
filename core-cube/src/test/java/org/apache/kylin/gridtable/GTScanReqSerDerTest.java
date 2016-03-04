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

package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GTScanReqSerDerTest extends LocalFileMetadataTestCase {

    private ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);

    @Before
    public void setUp() throws Exception {
        buffer.clear();
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testImmutableBitSet() {
        ImmutableBitSet x = new ImmutableBitSet(10, 100);
        ImmutableBitSet.serializer.serialize(x, buffer);

        buffer.flip();

        ImmutableBitSet sx = ImmutableBitSet.serializer.deserialize(buffer);
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(sx.get(i));
        }
        for (int i = 10; i < 100; i++) {
            Assert.assertTrue(sx.get(i));
        }
    }

    @Test
    public void testBasicInfo() {
        GTInfo info = UnitTestSupport.basicInfo();
        GTInfo.serializer.serialize(info, buffer);
        buffer.flip();

        GTInfo sInfo = GTInfo.serializer.deserialize(buffer);
        this.compareTwoGTInfo(info, sInfo);
    }

    @Test
    public void testAdvancedInfo() {
        GTInfo info = UnitTestSupport.advancedInfo();
        GTInfo.serializer.serialize(info, buffer);
        buffer.flip();

        GTInfo sInfo = GTInfo.serializer.deserialize(buffer);
        this.compareTwoGTInfo(info, sInfo);
    }

    @Test
    public void testGTInfo() {
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube("test_kylin_cube_with_slr_ready");
        CubeSegment segment = cube.getFirstSegment();

        GTInfo info = CubeGridTable.newGTInfo(segment, Cuboid.getBaseCuboidId(cube.getDescriptor()));
        GTInfo.serializer.serialize(info, buffer);
        buffer.flip();

        GTInfo sInfo = GTInfo.serializer.deserialize(buffer);
        this.compareTwoGTInfo(info, sInfo);
    }

    private void compareTwoGTInfo(GTInfo info, GTInfo sInfo) {
        Assert.assertEquals(info.tableName, sInfo.tableName);
        Assert.assertEquals(info.primaryKey, sInfo.primaryKey);

        for (int i = 0; i < info.colTypes.length; i++) {
            Assert.assertEquals(info.codeSystem.maxCodeLength(i), sInfo.codeSystem.maxCodeLength(i));
            Assert.assertTrue(info.codeSystem.maxCodeLength(i) > 0);
            Assert.assertEquals(info.colRef(i), sInfo.colRef(i));
        }
        Assert.assertArrayEquals(info.colBlocks, sInfo.colBlocks);
        Assert.assertEquals(info.getRowBlockSize(), sInfo.getRowBlockSize());
        Assert.assertEquals(info.rowBlockSize, sInfo.rowBlockSize);

    }
}
