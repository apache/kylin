/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase.observer;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.storage.hbase.observer.SRowType;

/**
 * @author yangli9
 * 
 */
public class RowTypeTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSerialize() {

        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_without_slr_ready");
        CubeDesc cubeDesc = cube.getDescriptor();
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid cuboid = Cuboid.findById(cubeDesc, baseCuboidId);

        SRowType rowType = SRowType.fromCuboid(cube.getLatestReadySegment(), cuboid);
        byte[] bytes = SRowType.serialize(rowType);
        SRowType copy = SRowType.deserialize(bytes);

        assertTrue(Arrays.equals(rowType.columns, copy.columns));
        assertTrue(Arrays.equals(rowType.columnSizes, copy.columnSizes));
    }
}
