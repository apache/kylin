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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.response.NCubeDescResponse;
import org.apache.kylin.rest.response.NCubeResponse;
import org.apache.kylin.rest.service.ModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;

@RestController
@RequestMapping(value = "/api/cube_desc", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
public class NCubeDescControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    @ApiOperation(value = "getCube", tags = { "AI" })
    @GetMapping(value = "/{projectName}/{cubeName}")
    @ResponseBody
    public EnvelopeResponse<NCubeResponse> getCube(@PathVariable("projectName") String project,
            @PathVariable("cubeName") String modelAlias) {
        checkProjectName(project);
        NCubeDescResponse cubeDesc = modelService.getCubeWithExactModelName(modelAlias, project);

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, new NCubeResponse(cubeDesc), "");
    }

}
