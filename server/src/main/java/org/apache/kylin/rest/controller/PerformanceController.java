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

import net.sf.ehcache.CacheManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.PerformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;

/**
 * Handle query requests.
 *
 * @author xduo
 */
@Controller
@RequestMapping(value = "/performance")
public class PerformanceController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(PerformanceController.class);


    @Autowired
    private PerformService performService;

    @Autowired
    private CubeService cubeService;

    @Autowired
    private CacheManager cacheManager;

    @RequestMapping(value = "/eachDayPercentile", method = RequestMethod.GET)
    @ResponseBody
    public List<String[]> eachDayPercentile() throws IOException {
        return performService.eachDayPercentile();
    }

    @RequestMapping(value = "/projectPercentile", method = RequestMethod.GET)
    @ResponseBody
    public List<String[]> projectPercentile() throws IOException {
        return performService.projectPercentile();
    }


    @RequestMapping(value = "/last30DayPercentile", method = RequestMethod.GET)
    @ResponseBody
    public List<String[]> last30DayPercentile() throws IOException {
        return performService.last30DayPercentile();
    }

    @RequestMapping(value = "/cubesStorage", method = {RequestMethod.GET})
    @ResponseBody
    public List<CubeInstance> getCubeStorage() {
        return cubeService.listAllCubes(null, null);
    }


    @RequestMapping(value = "/totalQueryUser", method = {RequestMethod.GET})
    @ResponseBody
    public List<String[]> totalQueryUser() throws IOException {
        return performService.getTotalQueryUser();
    }

    @RequestMapping(value = "/dailyQueryCount", method = {RequestMethod.GET})
    @ResponseBody
    public List<String[]> dailyQueryCount() throws IOException {
        return performService.dailyQueryCount();
    }

    @RequestMapping(value = "/avgDayQuery", method = {RequestMethod.GET})
    @ResponseBody
    public List<String[]> avgDayQuery() throws IOException {
        return performService.avgDayQuery();
    }

    @RequestMapping(value = "/listCubes", method = {RequestMethod.GET})
    @ResponseBody
    public List<CubeInstance> getCubes() {
        return cubeService.listAllCubes(null,null);
    }



    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    public void setPerformService(PerformService performService) {
        this.performService = performService;
    }

}
