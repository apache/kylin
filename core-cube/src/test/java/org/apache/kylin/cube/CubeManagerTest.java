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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * @author yangli9
 */
public class CubeManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {

        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_without_slr_ready");
        CubeDesc desc = cube.getDescriptor();
        System.out.println(JsonUtil.writeValueAsIndentString(desc));

        String signature = desc.calculateSignature();
        desc.getModel().getPartitionDesc().setPartitionDateColumn("test_column");
        assertTrue(signature.equals(desc.calculateSignature()));
    }

    @Test
    public void testCreateAndDrop() throws Exception {

        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        ProjectManager prjMgr = ProjectManager.getInstance(config);
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/a_whole_new_cube.json");

        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        CubeInstance createdCube = cubeMgr.createCube("a_whole_new_cube", ProjectInstance.DEFAULT_PROJECT_NAME, desc, null);
        assertTrue(createdCube == cubeMgr.getCube("a_whole_new_cube"));

        assertTrue(prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(createdCube));

        CubeInstance droppedCube = CubeManager.getInstance(getTestConfig()).dropCube("a_whole_new_cube", false);
        assertTrue(createdCube == droppedCube);

        assertTrue(!prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(droppedCube));

        assertNull(CubeManager.getInstance(getTestConfig()).getCube("a_whole_new_cube"));
    }

    @Test
    public void testAutoMergeNormal() throws Exception {
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_empty");

        cube.getDescriptor().setAutoMergeTimeRanges(new long[] { 2000, 6000 });
        mgr.updateCube(new CubeUpdate(cube));

        assertTrue(cube.needAutoMerge());

        // no segment at first
        assertEquals(0, cube.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000, 0, 0, null, null);
        seg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg2 = mgr.appendSegment(cube, 1000, 2000, 0, 0, null, null);
        seg2.setStatus(SegmentStatusEnum.READY);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);

        mgr.updateCube(cubeBuilder);

        assertEquals(2, cube.getSegments().size());

        Pair<Long, Long> mergedSeg = mgr.autoMergeCubeSegments(cube);

        assertTrue(mergedSeg != null);

    }


    @Test
    public void testConcurrentBuildAndMerge() throws Exception {
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_empty");
        System.setProperty("kylin.cube.max-building-segments", "10");
        // no segment at first
        assertEquals(0, cube.getSegments().size());

        Map m1 =  Maps.newHashMap();
        m1.put(1, 1000L);
        Map m2 =  Maps.newHashMap();
        m2.put(1, 2000L);
        Map m3 =  Maps.newHashMap();
        m3.put(1, 3000L);
        Map m4 =  Maps.newHashMap();
        m4.put(1, 4000L);

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 0, 0, 1000, null, m1);
        seg1.setStatus(SegmentStatusEnum.READY);


        CubeSegment seg2 = mgr.appendSegment(cube, 0, 0, 1000, 2000, m1, m2);
        seg2.setStatus(SegmentStatusEnum.READY);


        CubeSegment seg3 = mgr.mergeSegments(cube, 0, 0, 0000, 2000, true);
        seg3.setStatus(SegmentStatusEnum.NEW);


        CubeSegment seg4 = mgr.appendSegment(cube, 0, 0, 2000, 3000, m2, m3);
        seg4.setStatus(SegmentStatusEnum.NEW);
        seg4.setLastBuildJobID("test");
        seg4.setStorageLocationIdentifier("test");


        CubeSegment seg5 = mgr.appendSegment(cube, 0, 0, 3000, 4000, m3, m4);
        seg5.setStatus(SegmentStatusEnum.READY);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);

        mgr.updateCube(cubeBuilder);


        mgr.promoteNewlyBuiltSegments(cube, seg4);

        assertTrue(cube.getSegments().size() == 5);

        assertTrue(cube.getSegmentById(seg1.getUuid()) != null && cube.getSegmentById(seg1.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg2.getUuid()) != null && cube.getSegmentById(seg2.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg3.getUuid()) != null && cube.getSegmentById(seg3.getUuid()).getStatus() == SegmentStatusEnum.NEW);
        assertTrue(cube.getSegmentById(seg4.getUuid()) != null && cube.getSegmentById(seg4.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg5.getUuid()) != null && cube.getSegmentById(seg5.getUuid()).getStatus() == SegmentStatusEnum.READY);

    }


    @Test
    public void testConcurrentMergeAndMerge() throws Exception {
        System.setProperty("kylin.cube.max-building-segments", "10");
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_empty");

        // no segment at first
        assertEquals(0, cube.getSegments().size());
        Map m1 =  Maps.newHashMap();
        m1.put(1, 1000L);
        Map m2 =  Maps.newHashMap();
        m2.put(1, 2000L);
        Map m3 =  Maps.newHashMap();
        m3.put(1, 3000L);
        Map m4 =  Maps.newHashMap();
        m4.put(1, 4000L);

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 0, 0, 1000, null, m1);
        seg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg2 = mgr.appendSegment(cube, 0, 0, 1000, 2000, m1, m2);
        seg2.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg3 = mgr.appendSegment(cube, 0, 0, 2000, 3000, m2, m3);
        seg3.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg4 = mgr.appendSegment(cube, 0, 0, 3000, 4000, m3, m4);
        seg4.setStatus(SegmentStatusEnum.READY);



        CubeSegment merge1 = mgr.mergeSegments(cube, 0, 0, 0, 2000, true);
        merge1.setStatus(SegmentStatusEnum.NEW);
        merge1.setLastBuildJobID("test");
        merge1.setStorageLocationIdentifier("test");

        CubeSegment merge2 = mgr.mergeSegments(cube, 0, 0, 2000, 4000, true);
        merge2.setStatus(SegmentStatusEnum.NEW);
        merge2.setLastBuildJobID("test");
        merge2.setStorageLocationIdentifier("test");

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        mgr.updateCube(cubeBuilder);


        mgr.promoteNewlyBuiltSegments(cube, merge1);

        assertTrue(cube.getSegments().size() == 4);

        assertTrue(cube.getSegmentById(seg1.getUuid()) == null);
        assertTrue(cube.getSegmentById(seg2.getUuid()) == null);
        assertTrue(cube.getSegmentById(merge1.getUuid()) != null && cube.getSegmentById(merge1.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg3.getUuid()) != null && cube.getSegmentById(seg3.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg4.getUuid()) != null && cube.getSegmentById(seg4.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(merge2.getUuid()) != null && cube.getSegmentById(merge2.getUuid()).getStatus() == SegmentStatusEnum.NEW);

    }

    @Test
    public void testGetAllCubes() throws Exception {
        final ResourceStore store = ResourceStore.getStore(getTestConfig());
        final NavigableSet<String> cubePath = store.listResources(ResourceStore.CUBE_RESOURCE_ROOT);
        assertTrue(cubePath.size() > 1);

        final List<CubeInstance> cubes = store.getAllResources(ResourceStore.CUBE_RESOURCE_ROOT, CubeInstance.class, CubeManager.CUBE_SERIALIZER);
        assertEquals(cubePath.size(), cubes.size());
    }

    @Test
    public void testAutoMergeWithGap() throws Exception {
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_empty");

        cube.getDescriptor().setAutoMergeTimeRanges(new long[] { 2000, 6000 });
        mgr.updateCube(new CubeUpdate(cube));

        assertTrue(cube.needAutoMerge());

        // no segment at first
        assertEquals(0, cube.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000);
        seg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg3 = mgr.appendSegment(cube, 2000, 4000);
        seg3.setStatus(SegmentStatusEnum.READY);

        assertEquals(2, cube.getSegments().size());

        Pair<Long, Long> mergedSeg = mgr.autoMergeCubeSegments(cube);

        assertTrue(mergedSeg == null);

        // append a new seg which will be merged

        CubeSegment seg4 = mgr.appendSegment(cube, 4000, 8000);
        seg4.setStatus(SegmentStatusEnum.READY);

        assertEquals(3, cube.getSegments().size());

        mergedSeg = mgr.autoMergeCubeSegments(cube);

        assertTrue(mergedSeg != null);
        assertTrue(mergedSeg.getFirst() == 2000 && mergedSeg.getSecond() == 8000);

        // fill the gap

        CubeSegment seg2 = mgr.appendSegment(cube, 1000, 2000);
        seg2.setStatus(SegmentStatusEnum.READY);

        assertEquals(4, cube.getSegments().size());

        mergedSeg = mgr.autoMergeCubeSegments(cube);

        assertTrue(mergedSeg != null);
        assertTrue(mergedSeg.getFirst() == 0 && mergedSeg.getSecond() == 8000);
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }
}
