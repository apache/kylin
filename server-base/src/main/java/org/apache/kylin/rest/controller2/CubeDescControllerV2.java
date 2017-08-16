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
import java.util.HashMap;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CubeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Preconditions;

/**
 */
@Controller
@RequestMapping(value = "/cube_desc")
public class CubeDescControllerV2 extends BasicController {

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @RequestMapping(value = "/{projectName}/{cubeName}", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse getDescV2(@PathVariable String projectName, @PathVariable String cubeName) throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        Draft draft = cubeService.getCubeDraft(cubeName, projectName);

        if (cube == null && draft == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        HashMap<String, CubeDesc> result = new HashMap<>();
        if (cube != null) {
            Preconditions.checkState(!cube.getDescriptor().isDraft());
            result.put("cube", cube.getDescriptor());
        }
        if (draft != null) {
            CubeDesc dc = (CubeDesc) draft.getEntity();
            Preconditions.checkState(dc.isDraft());
            result.put("draft", dc);
        }
        
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, result, "");
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

}
