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
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000, 0, 0);
        seg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg2 = mgr.appendSegment(cube, 0, 2000, 0, 0);
        seg2.setStatus(SegmentStatusEnum.READY);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);

        mgr.updateCube(cubeBuilder);

        assertEquals(2, cube.getSegments().size());

        Pair<Long, Long> mergedSeg = mgr.autoMergeCubeSegments(cube);

        assertTrue(mergedSeg != null);

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
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000, 0, 0);
        seg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg3 = mgr.appendSegment(cube, 2000, 4000, 0, 0);
        seg3.setStatus(SegmentStatusEnum.READY);

        assertEquals(2, cube.getSegments().size());

        Pair<Long, Long> mergedSeg = mgr.autoMergeCubeSegments(cube);

        assertTrue(mergedSeg == null);

        // append a new seg which will be merged

        CubeSegment seg4 = mgr.appendSegment(cube, 4000, 8000, 0, 0);
        seg4.setStatus(SegmentStatusEnum.READY);

        assertEquals(3, cube.getSegments().size());

        mergedSeg = mgr.autoMergeCubeSegments(cube);

        assertTrue(mergedSeg != null);
        assertTrue(mergedSeg.getFirst() == 2000 && mergedSeg.getSecond() == 8000);

        // fill the gap

        CubeSegment seg2 = mgr.appendSegment(cube, 1000, 2000, 0, 0);
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
