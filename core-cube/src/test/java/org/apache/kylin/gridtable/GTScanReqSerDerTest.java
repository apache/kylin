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
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

@Disabled 
public class GTScanReqSerDerTest extends LocalFileMetadataTestCase {

    private ByteBuffer buffer = ByteBuffer.allocate(BytesSerializer.SERIALIZE_BUFFER_SIZE);

    @BeforeEach
    public void setUp() throws Exception {
        buffer.clear();
        this.createTestMetadata();
    }

    @AfterEach
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    void testImmutableBitSet() {
        ImmutableBitSet x = new ImmutableBitSet(10, 100);
        ImmutableBitSet.serializer.serialize(x, buffer);

        buffer.flip();

        ImmutableBitSet sx = ImmutableBitSet.serializer.deserialize(buffer);
        for (int i = 0; i < 10; i++) {
            Assertions.assertFalse(sx.get(i));
        }
        for (int i = 10; i < 100; i++) {
            Assertions.assertTrue(sx.get(i));
        }
    }

    @Test
    void testBasicInfo() {
        GTInfo info = UnitTestSupport.basicInfo();
        GTInfo.serializer.serialize(info, buffer);
        buffer.flip();

        GTInfo sInfo = GTInfo.serializer.deserialize(buffer);
        this.compareTwoGTInfo(info, sInfo);
    }

    @Test
    void testAdvancedInfo() {
        GTInfo info = UnitTestSupport.advancedInfo();
        GTInfo.serializer.serialize(info, buffer);
        buffer.flip();

        GTInfo sInfo = GTInfo.serializer.deserialize(buffer);
        this.compareTwoGTInfo(info, sInfo);
    }

    @Test
    void testGTInfo() {
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube("test_kylin_cube_with_slr_ready");
        CubeSegment segment = cube.getFirstSegment();

        Cuboid baseCuboid = Cuboid.getBaseCuboid(cube.getDescriptor());
        GTInfo info = CubeGridTable.newGTInfo(baseCuboid, new CubeDimEncMap(segment));
        GTInfo.serializer.serialize(info, buffer);
        buffer.flip();

        GTInfo sInfo = GTInfo.serializer.deserialize(buffer);
        this.compareTwoGTInfo(info, sInfo);
    }

    private void compareTwoGTInfo(GTInfo info, GTInfo sInfo) {
        Assertions.assertEquals(info.tableName, sInfo.tableName);
        Assertions.assertEquals(info.primaryKey, sInfo.primaryKey);

        for (int i = 0; i < info.colTypes.length; i++) {
            Assertions.assertEquals(info.codeSystem.maxCodeLength(i), sInfo.codeSystem.maxCodeLength(i));
            Assertions.assertTrue(info.codeSystem.maxCodeLength(i) > 0);
            Assertions.assertEquals(info.colRef(i), sInfo.colRef(i));
        }
        Assertions.assertArrayEquals(info.colBlocks, sInfo.colBlocks);
        Assertions.assertEquals(info.getRowBlockSize(), sInfo.getRowBlockSize());
        Assertions.assertEquals(info.rowBlockSize, sInfo.rowBlockSize);

    }
}
