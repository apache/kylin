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

import java.util.Collection;
import java.util.List;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.rest.request.HybridRequest;
import org.apache.kylin.rest.response.HybridRespone;
import org.apache.kylin.rest.service.HybridService;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/hybrids")
public class HybridController extends BasicController {

    @Autowired
    private HybridService hybridService;

    @RequestMapping(value = "", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public HybridRespone create(@RequestBody HybridRequest request) {
        checkRequiredArg("hybrid", request.getHybrid());
        checkRequiredArg("project", request.getProject());
        checkRequiredArg("model", request.getModel());
        checkRequiredArg("cubes", request.getCubes());
        HybridInstance hybridInstance = hybridService.createHybridInstance(request.getHybrid(), request.getProject(), request.getModel(),
                request.getCubes());
        return hybridInstance2response(hybridInstance);
    }

    @RequestMapping(value = "", method = RequestMethod.PUT, produces = { "application/json" })
    @ResponseBody
    public HybridRespone update(@RequestBody HybridRequest request) {
        checkRequiredArg("hybrid", request.getHybrid());
        checkRequiredArg("project", request.getProject());
        checkRequiredArg("model", request.getModel());
        checkRequiredArg("cubes", request.getCubes());
        HybridInstance hybridInstance = hybridService.updateHybridInstance(request.getHybrid(), request.getProject(), request.getModel(),
                request.getCubes());
        return hybridInstance2response(hybridInstance);
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE, produces = { "application/json" })
    @ResponseBody
    public void delete(String hybrid, String project) {
        checkRequiredArg("hybrid", hybrid);
        checkRequiredArg("project", project);
        hybridService.deleteHybridInstance(hybrid, project);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public Collection<HybridRespone> list(@RequestParam(required = false) String project, @RequestParam(required = false) String model) {
        List<HybridInstance> hybridInstances = hybridService.listHybrids(project, model);
        List<HybridRespone> hybridRespones = Lists.newArrayListWithCapacity(hybridInstances.size());

        for (HybridInstance hybridInstance : hybridInstances) {
            hybridRespones.add(hybridInstance2response(hybridInstance));
        }

        return hybridRespones;
    }

    @RequestMapping(value = "{hybrid}", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public HybridRespone get(@PathVariable String hybrid) {
        HybridInstance hybridInstance = hybridService.getHybridInstance(hybrid);
        return hybridInstance2response(hybridInstance);
    }

    private HybridRespone hybridInstance2response(HybridInstance hybridInstance){
        DataModelDesc modelDesc = hybridInstance.getModel();
        return new HybridRespone(modelDesc == null ? HybridRespone.NO_PROJECT : modelDesc.getProject(), modelDesc == null ? HybridRespone.NO_MODEL : modelDesc.getName(), hybridInstance);
    }
}
