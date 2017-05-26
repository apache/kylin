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

package org.apache.kylin.rest.controller2;

import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.HybridRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.HybridService;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/hybrids")
public class HybridControllerV2 extends BasicController {

    @Autowired
    private HybridService hybridService;

    @RequestMapping(value = "", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse createV2(@RequestHeader("Accept-Language") String lang,
            @RequestBody HybridRequest request) {
        MsgPicker.setMsg(lang);

        checkRequiredArg("hybrid", request.getHybrid());
        checkRequiredArg("project", request.getProject());
        checkRequiredArg("model", request.getModel());
        checkRequiredArg("cubes", request.getCubes());
        HybridInstance instance = hybridService.createHybridCube(request.getHybrid(), request.getProject(),
                request.getModel(), request.getCubes());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, instance, "");
    }

    @RequestMapping(value = "", method = RequestMethod.PUT, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateV2(@RequestHeader("Accept-Language") String lang,
            @RequestBody HybridRequest request) {
        MsgPicker.setMsg(lang);

        checkRequiredArg("hybrid", request.getHybrid());
        checkRequiredArg("project", request.getProject());
        checkRequiredArg("model", request.getModel());
        checkRequiredArg("cubes", request.getCubes());
        HybridInstance instance = hybridService.updateHybridCube(request.getHybrid(), request.getProject(),
                request.getModel(), request.getCubes());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, instance, "");
    }

    @RequestMapping(value = "", method = RequestMethod.DELETE, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteV2(@RequestHeader("Accept-Language") String lang, @RequestBody HybridRequest request) {
        MsgPicker.setMsg(lang);

        checkRequiredArg("hybrid", request.getHybrid());
        checkRequiredArg("project", request.getProject());
        checkRequiredArg("model", request.getModel());
        hybridService.deleteHybridCube(request.getHybrid(), request.getProject(), request.getModel());
    }

    @RequestMapping(value = "", method = RequestMethod.GET, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse listV2(@RequestHeader("Accept-Language") String lang,
            @RequestParam(required = false) String project, @RequestParam(required = false) String model) {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, hybridService.listHybrids(project, model), "");
    }

    @RequestMapping(value = "{hybrid}", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getV2(@RequestHeader("Accept-Language") String lang, @PathVariable String hybrid) {
        MsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, hybridService.getHybridInstance(hybrid), "");
    }
}
