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

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.ExternalFilterRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.ExtFilterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

/**
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/extFilter")
public class ExternalFilterControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ExternalFilterControllerV2.class);

    @Autowired
    @Qualifier("extFilterService")
    private ExtFilterService extFilterService;

    @RequestMapping(value = "/saveExtFilter", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void saveExternalFilterV2(@RequestHeader("Accept-Language") String lang,
            @RequestBody ExternalFilterRequest request) throws IOException {
        MsgPicker.setMsg(lang);

        String filterProject = request.getProject();
        ExternalFilterDesc desc = JsonUtil.readValue(request.getExtFilter(), ExternalFilterDesc.class);
        desc.setUuid(UUID.randomUUID().toString());
        extFilterService.saveExternalFilter(desc);
        extFilterService.syncExtFilterToProject(new String[] { desc.getName() }, filterProject);
    }

    @RequestMapping(value = "/updateExtFilter", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void updateExternalFilterV2(@RequestHeader("Accept-Language") String lang,
            @RequestBody ExternalFilterRequest request) throws IOException {
        MsgPicker.setMsg(lang);

        ExternalFilterDesc desc = JsonUtil.readValue(request.getExtFilter(), ExternalFilterDesc.class);
        extFilterService.updateExternalFilter(desc);
        extFilterService.syncExtFilterToProject(new String[] { desc.getName() }, request.getProject());
    }

    @RequestMapping(value = "/{filter}/{project}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void removeFilterV2(@RequestHeader("Accept-Language") String lang, @PathVariable String filter,
            @PathVariable String project) throws IOException {
        MsgPicker.setMsg(lang);

        extFilterService.removeExtFilterFromProject(filter, project);
        extFilterService.removeExternalFilter(filter);
    }

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getExternalFiltersV2(@RequestHeader("Accept-Language") String lang,
            @RequestParam(value = "project", required = true) String project) throws IOException {
        MsgPicker.setMsg(lang);

        List<ExternalFilterDesc> filterDescs = Lists.newArrayList();
        filterDescs.addAll(extFilterService.getProjectManager().listExternalFilterDescs(project).values());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, filterDescs, "");
    }

}
