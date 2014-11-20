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

package com.kylinolap.rest.controller;

import java.io.IOException;
import java.io.StringWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.rest.request.CubeRequest;
import com.kylinolap.rest.service.CubeService;
import com.kylinolap.rest.service.JobService;
import com.kylinolap.rest.service.TestBase;

/**
 * @author xduo
 */
public class CubeControllerTest extends TestBase {

    private CubeController cubeController;
    private CubeDescController cubeDescController;

    @Autowired
    CubeService cubeService;
    @Autowired
    JobService jobService;

    @Before
    public void setup() throws Exception {
        super.setUp();

        cubeController = new CubeController();
        cubeController.setCubeService(cubeService);
        cubeController.setJobService(jobService);
        cubeDescController = new CubeDescController();
        cubeDescController.setCubeService(cubeService);
    }

    @Test
    public void testBasics() throws IOException {
        CubeDesc[] cubes = (CubeDesc[]) cubeDescController.getCube("test_kylin_cube_with_slr_ready");
        Assert.assertNotNull(cubes);
        Assert.assertNotNull(cubeController.getSql("test_kylin_cube_with_slr_ready", "20130331080000_20131212080000"));
        Assert.assertNotNull(cubeController.getCubes(null, null, 0, 5));

        CubeDesc cube = cubes[0];
        CubeDesc newCube = new CubeDesc();
        String newCubeName = cube.getName() + "_test_save";
        newCube.setName(newCubeName);
        newCube.setDimensions(cube.getDimensions());
        newCube.setHBaseMapping(cube.getHBaseMapping());
        newCube.setMeasures(cube.getMeasures());
        newCube.setConfig(cube.getConfig());
        newCube.setFactTable(cube.getFactTable());
        newCube.setRowkey(cube.getRowkey());

        ObjectMapper mapper = new ObjectMapper();
        StringWriter stringWriter = new StringWriter();
        mapper.writeValue(stringWriter, newCube);

        CubeRequest cubeRequest = new CubeRequest();
        cubeRequest.setCubeDescData(stringWriter.toString());
        cubeRequest = cubeController.saveCubeDesc(cubeRequest);

        cubeController.deleteCube(newCubeName);
    }

}
