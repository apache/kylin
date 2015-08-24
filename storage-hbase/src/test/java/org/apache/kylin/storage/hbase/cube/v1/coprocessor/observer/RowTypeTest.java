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

package org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

        CoprocessorRowType rowType = CoprocessorRowType.fromCuboid(cube.getLatestReadySegment(), cuboid);
        byte[] bytes = CoprocessorRowType.serialize(rowType);
        CoprocessorRowType copy = CoprocessorRowType.deserialize(bytes);

        assertTrue(Arrays.equals(rowType.columns, copy.columns));
        assertTrue(Arrays.equals(rowType.columnSizes, copy.columnSizes));
    }
}
