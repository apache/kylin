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

package org.apache.kylin.cube.common;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class RowKeySplitterTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testWithSlr() throws Exception {
        //has shard
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITH_SLR_READY");

        RowKeySplitter rowKeySplitter = new RowKeySplitter(cube.getFirstSegment(), 11, 20);
        // base cuboid rowkey
        byte[] input = { 0, 0, 0, 0, 0, 0, 0, 0, 1, -1, 0, -104, -106, -128, 11, 54, -105, 55, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 13, 71, 114, 65, 66, 73, 78, 9, 9, 9, 9, 9, 9, 9, 9, 0, 10, 0 };
        rowKeySplitter.split(input);

        assertEquals(11, rowKeySplitter.getBufferSize());
    }

    @Test
    public void testWithoutSlr() throws Exception {
        //no shard
        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("TEST_KYLIN_CUBE_WITHOUT_SLR_READY");

        RowKeySplitter rowKeySplitter = new RowKeySplitter(cube.getFirstSegment(), 11, 20);
        // base cuboid rowkey
        byte[] input = { 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, 11, 55, -13, 13, 22, 34, 121, 70, 80, 45, 71, 84, 67, 9, 9, 9, 9, 9, 9, 0, 10, 5 };
        rowKeySplitter.split(input);

        assertEquals(10, rowKeySplitter.getBufferSize());
    }
}
