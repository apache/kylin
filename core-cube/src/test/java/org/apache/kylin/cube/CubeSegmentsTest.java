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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CubeSegmentsTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testAppendNonPartitioned() throws IOException {
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_empty");

        // override partition desc
        cube.getModel().setPartitionDesc(new PartitionDesc());

        // first append, creates a new & single segment
        CubeSegment seg = mgr.appendSegment(cube);
        assertEquals(new TSRange(0L, Long.MAX_VALUE), seg.getTSRange());
        assertEquals(new TSRange(0L, Long.MAX_VALUE), seg.getSegRange());
        
        assertEquals(0, cube.getSegments().size()); // older cube not changed
        cube = mgr.getCube(cube.getName());
        assertEquals(1, cube.getSegments().size()); // the updated cube

        // second append, throw IllegalStateException because the first segment is not built
        try {
            mgr.appendSegment(cube);
            fail();
        } catch (IllegalStateException ex) {
            // good
        }
    }

    @Test
    public void testAppendNonPartitioned2() throws IOException {
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_ready");

        // override partition desc
        cube.getModel().setPartitionDesc(new PartitionDesc());

        // assert one ready segment
        assertEquals(1, cube.getSegments().size());
        CubeSegment seg = cube.getSegments(SegmentStatusEnum.READY).get(0);
        assertEquals(SegmentStatusEnum.READY, seg.getStatus());

        // append again, for non-partitioned cube, it becomes a full refresh
        CubeSegment seg2 = mgr.appendSegment(cube);
        assertEquals(new TSRange(0L, Long.MAX_VALUE), seg2.getTSRange());
        assertEquals(new TSRange(0L, Long.MAX_VALUE), seg2.getSegRange());
        
        assertEquals(1, cube.getSegments().size()); // older cube not changed
        cube = mgr.getCube(cube.getName());
        assertEquals(2, cube.getSegments().size()); // the updated cube

        // non-partitioned cannot merge, throw exception
        try {
            mgr.mergeSegments(cube, null, new SegmentRange(0L, Long.MAX_VALUE), false);
            fail();
        } catch (IllegalStateException ex) {
            // good
        }
    }

    @Test
    public void testPartitioned() throws IOException {
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_left_join_empty");

        // no segment at first
        assertEquals(0, cube.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, new TSRange(0L, 1000L));
        cube = readySegment(cube, seg1);

        // append second
        CubeSegment seg2 = mgr.appendSegment(cube, new TSRange(1000L, 2000L));
        cube = readySegment(cube, seg2);

        assertEquals(2, cube.getSegments().size());
        assertEquals(new TSRange(1000L, 2000L), seg2.getTSRange());
        assertEquals(new TSRange(1000L, 2000L), seg2.getSegRange());
        assertEquals(SegmentStatusEnum.NEW, seg2.getStatus()); // older version of seg2
        assertEquals(SegmentStatusEnum.READY, cube.getSegments().get(1).getStatus()); // newer version of seg2

        // merge first and second
        CubeSegment merge = mgr.mergeSegments(cube, new TSRange(0L, 2000L), null, true);
        assertEquals(2, cube.getSegments().size()); // older version of cube

        cube = mgr.getCube(cube.getName()); // get the newer version of cube
        assertEquals(3, cube.getSegments().size());
        assertEquals(new TSRange(0L, 2000L), merge.getTSRange());
        assertEquals(new TSRange(0L, 2000L), merge.getSegRange());
        assertEquals(SegmentStatusEnum.NEW, merge.getStatus());

        // segments are strictly ordered
        assertEquals(seg1.getUuid(), cube.getSegments().get(0).getUuid());
        assertEquals(merge.getUuid(), cube.getSegments().get(1).getUuid());
        assertEquals(seg2.getUuid(), cube.getSegments().get(2).getUuid());

        // drop the merge
        cube = mgr.updateCubeDropSegments(cube, merge);

        // try merge at start/end at middle of segments
        try {
            mgr.mergeSegments(cube, new TSRange(500L, 2500L), null, true);
            fail();
        } catch (IllegalArgumentException ex) {
            // good
        }

        CubeSegment merge2 = mgr.mergeSegments(cube, new TSRange(0L, 2500L), null, true);
        cube = mgr.getCube(cube.getName()); // get the newer version of cube
        assertEquals(3, cube.getSegments().size());
        assertEquals(new TSRange(0L, 2000L), merge2.getTSRange());
        assertEquals(new TSRange(0L, 2000L), merge2.getSegRange());
    }

    @Test
    public void testAllowGap() throws IOException {

        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_left_join_empty");

        // no segment at first
        assertEquals(0, cube.getSegments().size());

        // append the first
        CubeSegment seg1 = mgr.appendSegment(cube, new TSRange(0L, 1000L));
        cube = readySegment(cube, seg1);
        assertEquals(1, cube.getSegments().size());

        // append the third
        CubeSegment seg3 = mgr.appendSegment(cube, new TSRange(2000L, 3000L));
        cube = readySegment(cube, seg3);
        assertEquals(2, cube.getSegments().size());

        // reject overlap
        try {
            mgr.appendSegment(cube, new TSRange(1000L, 2500L));
            fail();
        } catch (IllegalStateException ex) {
            // good
        }

        // append the second
        CubeSegment seg2 = mgr.appendSegment(cube, new TSRange(1000L, 2000L));
        cube = readySegment(cube, seg2);
        assertEquals(3, cube.getSegments().size());
    }
    
    private CubeInstance readySegment(CubeInstance cube, CubeSegment seg) throws IOException {
        return mgr().updateCubeSegStatus(seg, SegmentStatusEnum.READY);
    }

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }
}
