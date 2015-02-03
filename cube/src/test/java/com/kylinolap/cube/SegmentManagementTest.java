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

package com.kylinolap.cube;

import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.model.CubeBuildTypeEnum;
import com.kylinolap.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

/**
 * @author ysong1
 * 
 */
@Ignore("we are not going to support upsert any more, need to rewrite ut")
public class SegmentManagementTest extends LocalFileMetadataTestCase {

    CubeManager cubeMgr = null;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.clearCache();
        CubeManager.clearCache();
        ProjectManager.clearCache();
        cubeMgr = CubeManager.getInstance(getTestConfig());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    private void createNewCube(CubeDesc cubeDesc) throws IOException {
        ResourceStore store = getStore();
        // clean legacy in case last run failed
        store.deleteResource("/cube/a_whole_new_cube.json");

        CubeInstance createdCube = cubeMgr.createCube("a_whole_new_cube", ProjectInstance.DEFAULT_PROJECT_NAME, cubeDesc, "username");
        assertTrue(createdCube == cubeMgr.getCube("a_whole_new_cube"));

        System.out.println(JsonUtil.writeValueAsIndentString(createdCube));
    }

    @Test
    public void testInitialAndAppend() throws ParseException, IOException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(RealizationStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.appendSegments(cubeInstance, dateEnd);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(SegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals(desc.getModel().getPartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_1", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(desc.getModel().getPartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // incremental build
        System.out.println("Incremental Build");
        long dateEnd2 = f.parse("2013-12-12").getTime();
        CubeSegment incrementalSegment = cubeMgr.appendSegments(cubeInstance, dateEnd2);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(2, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.NEW).size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getModel().getPartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd2, cubeInstance.getAllocatedEndDate());
        assertEquals(dateEnd, cubeInstance.getBuildingSegments().get(0).getDateRangeStart());
        assertEquals(dateEnd2, cubeInstance.getBuildingSegments().get(0).getDateRangeEnd());

        // incremental build success
        System.out.println("Incremental Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, incrementalSegment.getName(), "job_2", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(2, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getModel().getPartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd2, cubeInstance.getAllocatedEndDate());
    }

    @Test
    public void testRebuildSegment() throws IOException {
        CubeInstance cubeInstance = cubeMgr.getCube("test_kylin_cube_with_slr_ready");

        // rebuild segment
        System.out.println("Rebuild Segment");
        CubeSegment rebuildSegment = cubeMgr.appendSegments(cubeInstance, 1386806400000L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(SegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.NEW).size());
        assertEquals(1, cubeInstance.getRebuildingSegments().size());
        assertEquals(1364688000000L, cubeInstance.getAllocatedStartDate());
        assertEquals(1386806400000L, cubeInstance.getAllocatedEndDate());

        // rebuild success
        System.out.println("Rebuild Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, rebuildSegment.getName(), "job_3", System.currentTimeMillis(), 111, 222, 333);
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(1364688000000L, cubeInstance.getAllocatedStartDate());
        assertEquals(1386806400000L, cubeInstance.getAllocatedEndDate());
        assertEquals("job_3", cubeInstance.getSegments().get(0).getLastBuildJobID());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidRebuild() throws IOException {
        CubeInstance cubeInstance = cubeMgr.getCube("test_kylin_cube_with_slr_ready");

        // rebuild segment
        System.out.println("Rebuild Segment");
        cubeMgr.appendSegments(cubeInstance, 1386806400000L);
    }

    @Test
    public void testMergeSegments() throws IOException {
        CubeInstance cubeInstance = cubeMgr.getCube("test_kylin_cube_with_slr_ready_2_segments");

        // merge segments
        System.out.println("Merge Segment");
        CubeSegment mergedSegment = cubeMgr.mergeSegments(cubeInstance, 1384240200000L, 1386835200000L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(SegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(2, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.NEW).size());
        assertEquals(2, cubeInstance.getMergingSegments().size());
        assertEquals(1384240200000L, cubeInstance.getAllocatedStartDate());
        assertEquals(1386835200000L, cubeInstance.getAllocatedEndDate());

        // build success
        System.out.println("Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.MERGE, mergedSegment.getName(), "job_4", System.currentTimeMillis(), 123, 20000L, 1216024L);
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(1384240200000L, cubeInstance.getAllocatedStartDate());
        assertEquals(1386835200000L, cubeInstance.getAllocatedEndDate());
        assertEquals("job_4", cubeInstance.getSegments().get(0).getLastBuildJobID());
        assertEquals(20000L, cubeInstance.getSegments().get(0).getInputRecords());
        assertEquals(1216024L, cubeInstance.getSegments().get(0).getInputRecordsSize());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
    }

    @Test
    public void testNonPartitionedCube() throws ParseException, IOException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_without_slr_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(RealizationStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.appendSegments(cubeInstance, dateEnd);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(SegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals("FULL_BUILD", initialSegment.getName());
        assertEquals(desc.getModel().getPartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_5", System.currentTimeMillis(), 111L, 222L, 333L);
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedStartDate());
        assertEquals(0, cubeInstance.getAllocatedEndDate());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));

        // rebuild segment
        System.out.println("Rebuild Segment");
        CubeSegment rebuildSegment = cubeMgr.appendSegments(cubeInstance, 1386806400000L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(SegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.NEW).size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(1, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedStartDate());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // rebuild success
        System.out.println("Rebuild Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, rebuildSegment.getName(), "job_6", System.currentTimeMillis(), 111, 222, 333);
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedStartDate());
        assertEquals(0, cubeInstance.getAllocatedEndDate());
        assertEquals("job_6", cubeInstance.getSegments().get(0).getLastBuildJobID());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidAppend() throws ParseException, IOException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(RealizationStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.appendSegments(cubeInstance, dateEnd);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(SegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals(desc.getModel().getPartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_1", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(RealizationStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(SegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(desc.getModel().getPartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // incremental build
        System.out.println("Invalid Incremental Build");
        long dateEnd2 = f.parse("2013-12-12").getTime();
        cubeMgr.appendSegments(cubeInstance, dateEnd2);

    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }
}
