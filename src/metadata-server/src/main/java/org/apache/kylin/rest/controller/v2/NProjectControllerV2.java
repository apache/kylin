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
package org.apache.kylin.rest.controller.v2;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.ProjectService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping("/api/projects")
public class NProjectControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @ApiOperation(value = "getProjects", tags = { "SM" })
    @GetMapping(value = "", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    public EnvelopeResponse getProjects(@RequestParam(value = "projectName", required = false) String project,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer size,
            @RequestParam(value = "exact", required = false, defaultValue = "true") boolean exactMatch) {

        List<ProjectInstance> readableProjects = projectService.getReadableProjects(project, exactMatch);
        Map<String, Object> projects = getDataResponse("projects", readableProjects, offset, size);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, projects, "");
    }

}
