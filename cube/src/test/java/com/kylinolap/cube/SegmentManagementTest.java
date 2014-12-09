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
import java.util.List;
import java.util.TimeZone;

import com.kylinolap.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.project.CubeRealizationManager;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author ysong1
 * 
 */
public class SegmentManagementTest extends LocalFileMetadataTestCase {

    CubeManager cubeMgr = null;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.removeInstance(this.getTestConfig());
        CubeManager.removeInstance(this.getTestConfig());
        CubeRealizationManager.removeInstance(this.getTestConfig());
        cubeMgr = CubeManager.getInstance(this.getTestConfig());
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
    public void testInitialAndAppend() throws ParseException, IOException, CubeIntegrityException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, desc.getCubePartitionDesc().getPartitionDateStart(), dateEnd).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(CubeSegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_1", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // incremental build
        System.out.println("Incremental Build");
        long dateEnd2 = f.parse("2013-12-12").getTime();
        CubeSegment incrementalSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, dateEnd, dateEnd2).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(2, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd2, cubeInstance.getAllocatedEndDate());
        assertEquals(dateEnd, cubeInstance.getBuildingSegments().get(0).getDateRangeStart());
        assertEquals(dateEnd2, cubeInstance.getBuildingSegments().get(0).getDateRangeEnd());

        // incremental build success
        System.out.println("Incremental Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, incrementalSegment.getName(), "job_2", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(2, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd2, cubeInstance.getAllocatedEndDate());
    }

    @Test
    public void testRebuildSegment() throws IOException, CubeIntegrityException {
        CubeInstance cubeInstance = cubeMgr.getCube("test_kylin_cube_with_slr_ready");

        // rebuild segment
        System.out.println("Rebuild Segment");
        CubeSegment rebuildSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, 1364688000000L, 1386806400000L).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(CubeSegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(1, cubeInstance.getRebuildingSegments().size());
        assertEquals(1364688000000L, cubeInstance.getAllocatedStartDate());
        assertEquals(1386806400000L, cubeInstance.getAllocatedEndDate());

        // rebuild success
        System.out.println("Rebuild Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, rebuildSegment.getName(), "job_3", System.currentTimeMillis(), 111, 222, 333);
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(1364688000000L, cubeInstance.getAllocatedStartDate());
        assertEquals(1386806400000L, cubeInstance.getAllocatedEndDate());
        assertEquals("job_3", cubeInstance.getSegments().get(0).getLastBuildJobID());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
    }

    @Test(expected = CubeIntegrityException.class)
    public void testInvalidRebuild() throws IOException, CubeIntegrityException {
        CubeInstance cubeInstance = cubeMgr.getCube("test_kylin_cube_with_slr_ready");

        // rebuild segment
        System.out.println("Rebuild Segment");
        cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, 1364688000000L + 1000L, 1386806400000L).get(0);
    }

    @Test
    public void testMergeSegments() throws IOException, CubeIntegrityException {
        CubeInstance cubeInstance = cubeMgr.getCube("test_kylin_cube_with_slr_ready_2_segments");

        // merge segments
        System.out.println("Merge Segment");
        CubeSegment mergedSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.MERGE, 1384240200000L, 1386835200000L).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(CubeSegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(2, cubeInstance.getMergingSegments().size());
        assertEquals(1384240200000L, cubeInstance.getAllocatedStartDate());
        assertEquals(1386835200000L, cubeInstance.getAllocatedEndDate());

        // build success
        System.out.println("Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.MERGE, mergedSegment.getName(), "job_4", System.currentTimeMillis(), 123, 20000L, 1216024L);
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(1384240200000L, cubeInstance.getAllocatedStartDate());
        assertEquals(1386835200000L, cubeInstance.getAllocatedEndDate());
        assertEquals("job_4", cubeInstance.getSegments().get(0).getLastBuildJobID());
        assertEquals(20000L, cubeInstance.getSegments().get(0).getSourceRecords());
        assertEquals(1216024L, cubeInstance.getSegments().get(0).getSourceRecordsSize());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
    }

    @Test
    public void testNonPartitionedCube() throws ParseException, IOException, CubeIntegrityException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_without_slr_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, desc.getCubePartitionDesc().getPartitionDateStart(), dateEnd).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(CubeSegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals("FULL_BUILD", initialSegment.getName());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_5", System.currentTimeMillis(), 111L, 222L, 333L);
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedStartDate());
        assertEquals(0, cubeInstance.getAllocatedEndDate());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));

        // rebuild segment
        System.out.println("Rebuild Segment");
        CubeSegment rebuildSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, 1364688000000L, 1386806400000L).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(CubeSegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(1, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedStartDate());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // rebuild success
        System.out.println("Rebuild Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, rebuildSegment.getName(), "job_6", System.currentTimeMillis(), 111, 222, 333);
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedStartDate());
        assertEquals(0, cubeInstance.getAllocatedEndDate());
        assertEquals("job_6", cubeInstance.getSegments().get(0).getLastBuildJobID());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
    }

    @Test(expected = CubeIntegrityException.class)
    public void testInvalidAppend() throws ParseException, IOException, CubeIntegrityException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, desc.getCubePartitionDesc().getPartitionDateStart(), dateEnd).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(CubeSegmentStatusEnum.NEW, cubeInstance.getBuildingSegments().get(0).getStatus());
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_1", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // incremental build
        System.out.println("Invalid Incremental Build");
        long dateEnd2 = f.parse("2013-12-12").getTime();
        cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, dateEnd + 1000, dateEnd2);

    }

    @Test
    public void testInitialAndUpsert() throws ParseException, IOException, CubeIntegrityException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_without_slr_left_join_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, desc.getCubePartitionDesc().getPartitionDateStart(), dateEnd).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        for (CubeSegment cubeSegment : cubeInstance.getBuildingSegments()) {
            assertEquals(CubeSegmentStatusEnum.NEW, cubeSegment.getStatus());
        }
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_1", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // upsert build
        System.out.println("Upsert Build");
        long start = f.parse("2013-11-01").getTime();
        long dateEnd2 = f.parse("2013-12-12").getTime();
        List<CubeSegment> upsertSegments = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, start, dateEnd2);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(2, upsertSegments.size());
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(3, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(2, cubeInstance.getBuildingSegments().size());
        assertEquals(1, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd2, cubeInstance.getAllocatedEndDate());
        assertEquals(0L, cubeInstance.getBuildingSegments().get(0).getDateRangeStart());
        assertEquals(dateEnd2, cubeInstance.getBuildingSegments().get(1).getDateRangeEnd());

        // upsert build success
        System.out.println("Upsert Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments.get(0).getName(), "job_2", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments.get(1).getName(), "job_3", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(2, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd2, cubeInstance.getAllocatedEndDate());

        // upsert build again
        System.out.println("Upsert Build");
        long start2 = f.parse("2013-12-01").getTime();
        long dateEnd3 = f.parse("2013-12-31").getTime();
        List<CubeSegment> upsertSegments2 = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, start2, dateEnd3);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(2, upsertSegments2.size());
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(4, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(2, cubeInstance.getBuildingSegments().size());
        assertEquals(1, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(dateEnd3, cubeInstance.getAllocatedEndDate());
        // building segment 1 from 2013-11-01 to 2013-12-01
        assertEquals(f.parse("2013-11-01").getTime(), cubeInstance.getBuildingSegments().get(0).getDateRangeStart());
        assertEquals(f.parse("2013-12-01").getTime(), cubeInstance.getBuildingSegments().get(0).getDateRangeEnd());
        // building segment 2 from 2013-12-01 to 2013-12-31
        assertEquals(f.parse("2013-12-01").getTime(), cubeInstance.getBuildingSegments().get(1).getDateRangeStart());
        assertEquals(f.parse("2013-12-31").getTime(), cubeInstance.getBuildingSegments().get(1).getDateRangeEnd());

        // upsert build success
        System.out.println("Upsert Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments2.get(1).getName(), "job_5", System.currentTimeMillis(), 111L, 222L, 333L);
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments2.get(0).getName(), "job_4", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(3, cubeInstance.getSegments().size());
        assertEquals(3, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd3, cubeInstance.getAllocatedEndDate());
        // segment 1 from 1970-01-01 to 2013-11-01
        assertEquals(cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getSegments().get(0).getDateRangeStart());
        assertEquals(f.parse("2013-11-01").getTime(), cubeInstance.getSegments().get(0).getDateRangeEnd());
        // segment 2 from 2013-11-01 to 2013-12-01
        assertEquals(f.parse("2013-11-01").getTime(), cubeInstance.getSegments().get(1).getDateRangeStart());
        assertEquals(f.parse("2013-12-01").getTime(), cubeInstance.getSegments().get(1).getDateRangeEnd());
        // segment 3 from 2013-12-01 to 2013-12-31
        assertEquals(f.parse("2013-12-01").getTime(), cubeInstance.getSegments().get(2).getDateRangeStart());
        assertEquals(f.parse("2013-12-31").getTime(), cubeInstance.getSegments().get(2).getDateRangeEnd());

        // upsert build again
        System.out.println("Upsert Build");
        List<CubeSegment> upsertSegments3 = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, f.parse("2013-10-01").getTime(), f.parse("2014-02-01").getTime());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(2, upsertSegments3.size());
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(5, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(2, cubeInstance.getBuildingSegments().size());
        assertEquals(3, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(f.parse("2014-02-01").getTime(), cubeInstance.getAllocatedEndDate());
        // building segment 1 from 2013-11-01 to 2013-10-01
        assertEquals(cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getBuildingSegments().get(0).getDateRangeStart());
        assertEquals(f.parse("2013-10-01").getTime(), cubeInstance.getBuildingSegments().get(0).getDateRangeEnd());
        // building segment 2 from 2013-10-01 to 2014-02-01
        assertEquals(f.parse("2013-10-01").getTime(), cubeInstance.getBuildingSegments().get(1).getDateRangeStart());
        assertEquals(f.parse("2014-02-01").getTime(), cubeInstance.getBuildingSegments().get(1).getDateRangeEnd());

        // upsert build success
        System.out.println("Upsert Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments3.get(1).getName(), "job_7", System.currentTimeMillis(), 111L, 222L, 333L);
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments3.get(0).getName(), "job_6", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(2, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(f.parse("2014-02-01").getTime(), cubeInstance.getAllocatedEndDate());
        // segment 1 from 1970-01-01 to 2013-10-01
        assertEquals(cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getSegments().get(0).getDateRangeStart());
        assertEquals(f.parse("2013-10-01").getTime(), cubeInstance.getSegments().get(0).getDateRangeEnd());
        // segment 2 from 2013-10-01 to 2014-02-01
        assertEquals(f.parse("2013-10-01").getTime(), cubeInstance.getSegments().get(1).getDateRangeStart());
        assertEquals(f.parse("2014-02-01").getTime(), cubeInstance.getSegments().get(1).getDateRangeEnd());
    }

    @Test
    public void testInitialAndUpsert2() throws ParseException, IOException, CubeIntegrityException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_without_slr_left_join_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-01-01").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, desc.getCubePartitionDesc().getPartitionDateStart(), dateEnd).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        for (CubeSegment cubeSegment : cubeInstance.getBuildingSegments()) {
            assertEquals(CubeSegmentStatusEnum.NEW, cubeSegment.getStatus());
        }
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_1", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // upsert build
        System.out.println("Upsert Build");
        long start = f.parse("2013-01-01").getTime();
        long dateEnd2 = f.parse("2014-01-01").getTime();
        List<CubeSegment> upsertSegments = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, start, dateEnd2);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(1, upsertSegments.size());
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(2, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd2, cubeInstance.getAllocatedEndDate());
        assertEquals(start, cubeInstance.getBuildingSegments().get(0).getDateRangeStart());
        assertEquals(dateEnd2, cubeInstance.getBuildingSegments().get(0).getDateRangeEnd());

        // upsert build success
        System.out.println("Upsert Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments.get(0).getName(), "job_2", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));

        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(2, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd2, cubeInstance.getAllocatedEndDate());

        // upsert build again
        System.out.println("Upsert Build");
        List<CubeSegment> upsertSegments3 = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, f.parse("2013-10-01").getTime(), f.parse("2014-02-01").getTime());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(2, upsertSegments3.size());
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(4, cubeInstance.getSegments().size());
        assertEquals(2, cubeInstance.getSegments(CubeSegmentStatusEnum.NEW).size());
        assertEquals(2, cubeInstance.getBuildingSegments().size());
        assertEquals(1, cubeInstance.getRebuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(f.parse("2014-02-01").getTime(), cubeInstance.getAllocatedEndDate());
        // building segment 1 from 2013-11-01 to 2013-10-01
        assertEquals(f.parse("2013-01-01").getTime(), cubeInstance.getBuildingSegments().get(0).getDateRangeStart());
        assertEquals(f.parse("2013-10-01").getTime(), cubeInstance.getBuildingSegments().get(0).getDateRangeEnd());
        // building segment 2 from 2013-10-01 to 2014-02-01
        assertEquals(f.parse("2013-10-01").getTime(), cubeInstance.getBuildingSegments().get(1).getDateRangeStart());
        assertEquals(f.parse("2014-02-01").getTime(), cubeInstance.getBuildingSegments().get(1).getDateRangeEnd());

        // upsert build success
        System.out.println("Upsert Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments3.get(1).getName(), "job_7", System.currentTimeMillis(), 111L, 222L, 333L);
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, upsertSegments3.get(0).getName(), "job_6", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(3, cubeInstance.getSegments().size());
        assertEquals(3, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getMergingSegments().size());
        assertEquals(cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(f.parse("2014-02-01").getTime(), cubeInstance.getAllocatedEndDate());
        // segment 1 from 1970-01-01 to 2013-10-01
        assertEquals(cubeInstance.getDescriptor().getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getSegments().get(0).getDateRangeStart());
        assertEquals(f.parse("2013-01-01").getTime(), cubeInstance.getSegments().get(0).getDateRangeEnd());
        // segment 2 from 2013-10-01 to 2014-02-01
        assertEquals(f.parse("2013-01-01").getTime(), cubeInstance.getSegments().get(1).getDateRangeStart());
        assertEquals(f.parse("2013-10-01").getTime(), cubeInstance.getSegments().get(1).getDateRangeEnd());
        // segment 3 from 2013-10-01 to 2014-02-01
        assertEquals(f.parse("2013-10-01").getTime(), cubeInstance.getSegments().get(2).getDateRangeStart());
        assertEquals(f.parse("2014-02-01").getTime(), cubeInstance.getSegments().get(2).getDateRangeEnd());
    }

    @Test(expected = CubeIntegrityException.class)
    public void testInvalidUpsert() throws IOException, CubeIntegrityException, ParseException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_without_slr_left_join_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, desc.getCubePartitionDesc().getPartitionDateStart(), dateEnd).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        for (CubeSegment cubeSegment : cubeInstance.getBuildingSegments()) {
            assertEquals(CubeSegmentStatusEnum.NEW, cubeSegment.getStatus());
        }
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_1", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // upsert build
        // time gap in new segment
        System.out.println("Upsert Build");
        long start = f.parse("2013-11-13").getTime();
        long dateEnd2 = f.parse("2013-12-12").getTime();
        cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, start, dateEnd2);
    }

    @Test(expected = CubeIntegrityException.class)
    public void testInvalidUpsert2() throws IOException, CubeIntegrityException, ParseException {
        // create a new cube
        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_without_slr_left_join_desc");
        createNewCube(desc);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long dateEnd = f.parse("2013-11-12").getTime();

        CubeInstance cubeInstance = cubeMgr.getCube("a_whole_new_cube");
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        assertEquals(0, cubeInstance.getSegments().size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getAllocatedEndDate());

        // initial build
        System.out.println("Initial Build");
        CubeSegment initialSegment = cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, desc.getCubePartitionDesc().getPartitionDateStart(), dateEnd).get(0);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.DISABLED, cubeInstance.getStatus());
        for (CubeSegment cubeSegment : cubeInstance.getBuildingSegments()) {
            assertEquals(CubeSegmentStatusEnum.NEW, cubeSegment.getStatus());
        }
        assertEquals(1, cubeInstance.getSegments().size());
        assertEquals(1, cubeInstance.getBuildingSegments().size());
        assertEquals(0, cubeInstance.getRebuildingSegments().size());
        assertTrue("".equals(initialSegment.getStorageLocationIdentifier()) == false);
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // initial build success
        System.out.println("Initial Build Success");
        cubeMgr.updateSegmentOnJobSucceed(cubeInstance, CubeBuildTypeEnum.BUILD, initialSegment.getName(), "job_1", System.currentTimeMillis(), 111L, 222L, 333L);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
        assertEquals(CubeStatusEnum.READY, cubeInstance.getStatus());
        assertEquals(1, cubeInstance.getSegments(CubeSegmentStatusEnum.READY).size());
        assertEquals(0, cubeInstance.getBuildingSegments().size());
        assertEquals(desc.getCubePartitionDesc().getPartitionDateStart(), cubeInstance.getAllocatedStartDate());
        assertEquals(dateEnd, cubeInstance.getAllocatedEndDate());

        // upsert build
        // time gap in new segment
        System.out.println("Upsert Build");
        long start = f.parse("2013-11-01").getTime();
        long dateEnd2 = f.parse("2013-11-02").getTime();

        cubeMgr.allocateSegments(cubeInstance, CubeBuildTypeEnum.BUILD, start, dateEnd2);
        System.out.println(JsonUtil.writeValueAsIndentString(cubeInstance));
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }
}
