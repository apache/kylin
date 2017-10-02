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

package org.apache.kylin.rest.service;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.QueryConnection;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * @author xduo
 */
public class CubeServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("cubeMgmtService")
    CubeService cubeService;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @Test
    public void testBasics() throws JsonProcessingException, JobException, UnknownHostException, SQLException {
        Assert.assertNotNull(cubeService.getConfig());
        Assert.assertNotNull(cubeService.getConfig());
        Assert.assertNotNull(cubeService.getDataModelManager());
        Assert.assertNotNull(QueryConnection.getConnection(ProjectInstance.DEFAULT_PROJECT_NAME));

        List<CubeInstance> cubes = cubeService.listAllCubes(null, null, null, true);
        Assert.assertNotNull(cubes);
        CubeInstance cube = cubes.get(0);
    }
}
