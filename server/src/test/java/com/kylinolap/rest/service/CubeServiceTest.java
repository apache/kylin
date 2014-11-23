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

package com.kylinolap.rest.service;

import java.net.UnknownHostException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.project.ProjectInstance;
import com.kylinolap.job.exception.JobException;

/**
 * @author xduo
 * 
 */
public class CubeServiceTest extends ServiceTestBase {

    @Autowired
    CubeService cubeService;

    @Test
    public void testBasics() throws JsonProcessingException, JobException, UnknownHostException {
        Assert.assertNotNull(cubeService.getJobManager());
        Assert.assertNotNull(cubeService.getConfig());
        Assert.assertNotNull(cubeService.getKylinConfig());
        Assert.assertNotNull(cubeService.getMetadataManager());
        Assert.assertNotNull(cubeService.getOLAPDataSource(ProjectInstance.DEFAULT_PROJECT_NAME));

        Assert.assertTrue(CubeService.getCubeDescNameFromCube("testCube").equals("testCube_desc"));
        Assert.assertTrue(CubeService.getCubeNameFromDesc("testCube_desc").equals("testCube"));

        List<CubeInstance> cubes = cubeService.getCubes(null, null, null, null);
        Assert.assertNotNull(cubes);
        CubeInstance cube = cubes.get(0);
        cubeService.isCubeDescEditable(cube.getDescriptor());
        cubeService.isCubeEditable(cube);

        cubes = cubeService.getCubes(null, null, 1, 0);
        Assert.assertTrue(cubes.size() == 1);
    }
}
