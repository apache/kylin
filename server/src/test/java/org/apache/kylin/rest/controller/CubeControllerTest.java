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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.response.CubeInstanceResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.rest.service.StreamingService;
import org.apache.kylin.rest.util.ValidateUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * @author xduo
 */
public class CubeControllerTest extends ServiceTestBase {

    private CubeController cubeController;
    private CubeDescController cubeDescController;

    @Autowired
    @Qualifier("cubeMgmtService")
    CubeService cubeService;

    @Autowired
    @Qualifier("jobService")
    JobService jobService;

    @Autowired
    @Qualifier("streamingMgmtService")
    StreamingService streamingService;

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @Before
    public void setup() throws Exception {
        super.setup();

        cubeController = new CubeController();
        cubeController.setCubeService(cubeService);
        cubeController.setJobService(jobService);
        cubeController.setValidateUtil(validateUtil);

        cubeDescController = new CubeDescController();
        cubeDescController.setCubeService(cubeService);

    }

    @Test
    public void testBasics() throws IOException {
        CubeDesc[] cubes = cubeDescController.getCube("test_kylin_cube_with_slr_ready");
        Assert.assertNotNull(cubes);
        Assert.assertNotNull(cubeController.getSql("test_kylin_cube_with_slr_ready", "20130331080000_20131212080000"));
        Assert.assertNotNull(cubeController.getCubes(null, null, null, 0, 5));

        CubeDesc cube = cubes[0];
        CubeDesc newCube = new CubeDesc();
        String newCubeName = cube.getName() + "_test_save";

        try {
            cubeController.deleteCube(newCubeName);
        } catch (Exception e) {
            // it may not exist, ignore the exception
        }

        newCube.setName(newCubeName);
        newCube.setModelName(cube.getModelName());
        newCube.setModel(cube.getModel());
        newCube.setDimensions(cube.getDimensions());
        newCube.setHbaseMapping(cube.getHbaseMapping());
        newCube.setMeasures(cube.getMeasures());
        newCube.setRowkey(cube.getRowkey());
        newCube.setAggregationGroups(cube.getAggregationGroups());

        newCube.getModel().setLastModified(0);

        ObjectMapper cubeDescMapper = new ObjectMapper();
        StringWriter cubeDescWriter = new StringWriter();
        cubeDescMapper.writeValue(cubeDescWriter, newCube);

        ObjectMapper modelDescMapper = new ObjectMapper();
        StringWriter modelDescWriter = new StringWriter();
        modelDescMapper.writeValue(modelDescWriter, newCube.getModel());

        CubeRequest cubeRequest = new CubeRequest();
        cubeRequest.setCubeDescData(cubeDescWriter.toString());
        cubeRequest.setCubeName(newCube.getName());
        cubeRequest = cubeController.saveCubeDesc(cubeRequest);

        List<String> notifyList = Lists.newArrayList();
        notifyList.add("john@example.com");
        cubeController.updateNotifyList(newCubeName, notifyList);
        try {
            cubeController.updateCubeOwner(newCubeName, "new_user");
        } catch (InternalErrorException e) {
            Assert.assertEquals("Operation failed, user:new_user not exists, please add first.",
                e.getMessage());
        }
        cubeController.updateCubeOwner(newCubeName, "MODELER");
        List<CubeInstanceResponse> cubeInstances = cubeController.getCubes(newCubeName, cube.getModelName(), "default",
                1, 0);

        CubeInstance cubeInstance = cubeController.getCube(cubeInstances.get(0).getName());
        Assert.assertTrue(cubeInstance.getDescriptor().getNotifyList().contains("john@example.com"));
        Assert.assertEquals("MODELER", cubeInstance.getOwner());
        Assert.assertEquals(495, cubeInstance.getCost());
        cubeController.deleteCube(newCubeName);
    }

    @Test(expected = InternalErrorException.class)
    public void testDeleteSegmentNew() throws IOException {
        String cubeName = "test_kylin_cube_with_slr_ready_3_segments";
        CubeDesc[] cubes = cubeDescController.getCube(cubeName);
        Assert.assertNotNull(cubes);

        String segmentName = "20131212000000_20140112000000";

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        cubeController.disableCube(cubeName);

        CubeSegment toDelete = null;
        for (CubeSegment seg : cube.getSegments()) {
            if (seg.getName().equals(segmentName)) {
                toDelete = seg;
                break;
            }
        }

        Assert.assertNotNull(toDelete);
        String segId = toDelete.getUuid();

        cubeController.deleteSegment(cubeName, segmentName);

        // delete success, no related job 'NEW' segment can be delete
        if (cubeService.isOrphonSegment(cube, segId)){
            throw new InternalErrorException();
        }
        cubeController.enableCube(cubeName);
    }

    @Test(expected = NotFoundException.class)
    public void testDeleteSegmentNotExist() throws IOException {
        String cubeName = "test_kylin_cube_with_slr_ready_3_segments";
        CubeDesc[] cubes = cubeDescController.getCube(cubeName);
        Assert.assertNotNull(cubes);

        cubeController.disableCube(cubeName);

        cubeController.deleteSegment(cubeName, "not_exist_segment");

        cubeController.enableCube(cubeName);
    }

    @Test
    public void testDeleteSegmentFromHead() throws IOException {
        String cubeName = "test_kylin_cube_with_slr_ready_3_segments";
        CubeDesc[] cubes = cubeDescController.getCube(cubeName);
        Assert.assertNotNull(cubes);
        int segNumber = cubeService.getCubeManager().getCube(cubeName).getSegments().size();

        cubeController.disableCube(cubeName);

        cubeController.deleteSegment(cubeName, "19691231160000_20131112000000");

        int newSegNumber = cubeService.getCubeManager().getCube(cubeName).getSegments().size();

        Assert.assertEquals(segNumber, newSegNumber + 1);

        cubeController.enableCube(cubeName);
    }

    @Test
    public void testGetHoles() throws IOException {
        String cubeName = "test_kylin_cube_with_slr_ready_3_segments";
        CubeDesc[] cubes = cubeDescController.getCube(cubeName);
        Assert.assertNotNull(cubes);

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        List<CubeSegment> segments = cube.getSegments();

        cubeController.disableCube(cubeName);

        final long dateEnd = segments.get(segments.size() - 1).getTSRange().end.v;

        final long ONEDAY = 24 * 60 * 60000;
        cubeService.getCubeManager().appendSegment(cube, new TSRange(dateEnd + ONEDAY, dateEnd + ONEDAY * 2));

        List<CubeSegment> holes = cubeController.getHoles(cubeName);

        Assert.assertEquals(1, holes.size());

        CubeSegment hole = holes.get(0);

        Assert.assertEquals(hole.getTSRange(), new TSRange(dateEnd, dateEnd + ONEDAY));
    }

    @Test
    public void testGetCubes() {
        List<CubeInstanceResponse> cubes = cubeController.getCubes(null, null, null, 1, 0);
        Assert.assertEquals(1, cubes.size());
    }

    @Test
    public void testGetSql() {
        GeneralResponse response = cubeController.getSql("test_kylin_cube_with_slr_ready");
        String sql = response.getProperty("sql");
        CubeDesc cubeDesc = cubeDescController.getDesc("test_kylin_cube_with_slr_ready");

        for (DimensionDesc dimensionDesc : cubeDesc.getDimensions()) {
            if (dimensionDesc.getDerived() != null) {
                for (String derivedDimension : dimensionDesc.getDerived()) {
                    Assert.assertTrue(sql.contains(derivedDimension));
                }
            }
        }
    }

    @Test
    public void tesDeleteDescBrokenCube() throws Exception {
        final String cubeName = "ci_left_join_cube";
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        cubeDesc.setModel(null);
        cubeInstance.setStatus(RealizationStatusEnum.DESCBROKEN);
        cubeController.deleteCube(cubeName);
        Assert.assertNull(cubeService.getCubeManager().getCube(cubeName));
    }

}
