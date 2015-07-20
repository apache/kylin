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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
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

    @Test(expected = IllegalStateException.class)
    public void testAppendNonPartitioned() throws IOException {
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_empty");

        // first append, creates a new & single segment
        CubeSegment seg = mgr.appendSegments(cube, 0);
        assertEquals(0, seg.getDateRangeStart());
        assertEquals(Long.MAX_VALUE, seg.getDateRangeEnd());
        assertEquals(1, cube.getSegments().size());

        // second append, throw IllegalStateException because the first segment is not built
        CubeSegment seg2 = mgr.appendSegments(cube, 0);
        discard(seg2);
    }

    @Test(expected = IllegalStateException.class)
    public void testAppendNonPartitioned2() throws IOException {
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_ready");

        // assert one ready segment
        assertEquals(1, cube.getSegments().size());
        CubeSegment seg = cube.getSegment(SegmentStatusEnum.READY).get(0);
        assertEquals(SegmentStatusEnum.READY, seg.getStatus());

        // append again, for non-partitioned cube, it becomes a full refresh
        CubeSegment seg2 = mgr.appendSegments(cube, 0);
        assertEquals(0, seg2.getDateRangeStart());
        assertEquals(Long.MAX_VALUE, seg2.getDateRangeEnd());
        assertEquals(2, cube.getSegments().size());
        
        // non-partitioned cannot merge, throw exception
        mgr.mergeSegments(cube, 0, Long.MAX_VALUE, false);
    }

    @Test
    public void testPartitioned() throws IOException {
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_left_join_empty");

        // no segment at first
        assertEquals(0, cube.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegments(cube, 1000);
        seg1.setStatus(SegmentStatusEnum.READY);

        // append second
        CubeSegment seg2 = mgr.appendSegments(cube, 2000);

        assertEquals(2, cube.getSegments().size());
        assertEquals(1000, seg2.getDateRangeStart());
        assertEquals(2000, seg2.getDateRangeEnd());
        assertEquals(SegmentStatusEnum.NEW, seg2.getStatus());
        seg2.setStatus(SegmentStatusEnum.READY);
        
        // merge first and second
        CubeSegment merge = mgr.mergeSegments(cube, 0, 2000, true);

        assertEquals(3, cube.getSegments().size());
        assertEquals(0, merge.getDateRangeStart());
        assertEquals(2000, merge.getDateRangeEnd());
        assertEquals(SegmentStatusEnum.NEW, merge.getStatus());
        
        // segments are strictly ordered
        assertEquals(seg1, cube.getSegments().get(0));
        assertEquals(merge, cube.getSegments().get(1));
        assertEquals(seg2, cube.getSegments().get(2));
        
        // drop the merge
        cube.getSegments().remove(merge);
        
        // try merge at start/end at middle of segments
        CubeSegment merge2 = mgr.mergeSegments(cube, 500, 1500, false);
        assertEquals(3, cube.getSegments().size());
        assertEquals(0, merge2.getDateRangeStart());
        assertEquals(2000, merge2.getDateRangeEnd());
    }

    @Test
    public void testSpecialAppendAndMerge() throws IOException {
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube("test_kylin_cube_without_slr_left_join_empty");

        // no segment at first
        assertEquals(0, cube.getSegments().size());

        try {
            mgr.appendAndMergeSegments(cube, 1000);
            fail();
        } catch (IllegalStateException ex) {
            // expected, append and merge requires at least one segment
        }
        
        // append first
        CubeSegment seg1 = mgr.appendSegments(cube, 1000);
        seg1.setStatus(SegmentStatusEnum.READY);

        // append second and merge with first
        Pair<CubeSegment, CubeSegment> appendAndMergeSegments = mgr.appendAndMergeSegments(cube, 2000);
        CubeSegment seg2 = appendAndMergeSegments.getFirst();
        CubeSegment merge = appendAndMergeSegments.getSecond();

        assertEquals(3, cube.getSegments().size());
        assertEquals(0, seg1.getDateRangeStart());
        assertEquals(1000, seg1.getDateRangeEnd());
        assertEquals(1000, seg2.getDateRangeStart());
        assertEquals(2000, seg2.getDateRangeEnd());
        assertEquals(0, merge.getDateRangeStart());
        assertEquals(2000, merge.getDateRangeEnd());
        
        // segments are strictly ordered
        assertEquals(seg1, cube.getSegments().get(0));
        assertEquals(merge, cube.getSegments().get(1));
        assertEquals(seg2, cube.getSegments().get(2));
    }
    
    private void discard(Object o) {
        // throw away input parameter
    }

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }
}
