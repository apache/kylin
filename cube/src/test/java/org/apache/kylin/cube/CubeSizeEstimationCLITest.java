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

package org.apache.kylin.cube;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.estimation.CubeSizeEstimationCLI;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by honma on 9/1/14.
 */
public class CubeSizeEstimationCLITest extends LocalFileMetadataTestCase {

    String cubeName = "test_kylin_cube_with_slr_ready";
    long[] cardinality;
    CubeDesc cubeDesc;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.clearCache();

        String cubeName = "test_kylin_cube_with_slr_ready";
        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        cubeDesc = cubeInstance.getDescriptor();
        cardinality = new long[] { 100, 100, 100, 10000, 1000, 100, 100, 100, 100 };

    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void test() {
        long[] x = new long[] { 5, 8, 5, 115, 122, 127, 137, 236, 101 };
        CubeSizeEstimationCLI.estimatedCubeSize(cubeName, x);
    }

    @Test
    public void baseCuboidTest() {
        long cuboidID = getCuboidID(0, 1, 2, 3, 4, 5, 6, 7, 8);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (10000000000000000L * (32 + 39));
    }

    @Test
    public void cuboidTest1() {
        long cuboidID = getCuboidID(0, 1, 2, 4, 5, 6, 7, 8);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (1000000000000000L * (32 + 37));
    }

    @Test
    public void cuboidTest2() {

        long cuboidID = getCuboidID(0);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (100L * (1 + 32));
    }

    @Test
    public void cuboidTest3() {
        long cuboidID = getCuboidID(4, 5);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (1000L * (3 + 32));
    }

    @Test
    public void cuboidTest4() {
        long cuboidID = getCuboidID(4, 5, 6);
        long size = CubeSizeEstimationCLI.estimateCuboidSpace(cuboidID, cardinality, cubeDesc);

        assert size == (100000L * (4 + 32));
    }

    private long getCuboidID(int... bitIndice) {
        long ret = 0;
        long mask = 1L;
        for (int index : bitIndice) {
            ret |= mask << index;
        }
        return ret;
    }
}
