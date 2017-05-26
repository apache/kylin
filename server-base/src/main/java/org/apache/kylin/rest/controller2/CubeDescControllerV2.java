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
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author xduo
 * 
 */
@Controller
@RequestMapping(value = "/cube_desc")
public class CubeDescControllerV2 extends BasicController {

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    /**
     * Get detail information of the "Cube ID"
     * 
     * @param cubeName
     *            Cube Name
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCubeV2(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) {
        MsgPicker.setMsg(lang);
        Message msg = MsgPicker.getMsg();

        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        CubeDesc cSchema = cubeInstance.getDescriptor();
        if (cSchema != null) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, new CubeDesc[] { cSchema }, "");
        } else {
            throw new BadRequestException(String.format(msg.getCUBE_DESC_NOT_FOUND(), cubeName));
        }
    }

    /**
     * Get detail information of the "Cube ID"
     * return CubeDesc instead of CubeDesc[]
     *
     * @param cubeName
     *            Cube Name
     * @return
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/desc", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getDescV2(@RequestHeader("Accept-Language") String lang, @PathVariable String cubeName) {
        MsgPicker.setMsg(lang);
        Message msg = MsgPicker.getMsg();

        HashMap<String, CubeDesc> data = new HashMap<String, CubeDesc>();

        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        CubeDesc desc = cubeInstance.getDescriptor();
        if (desc == null)
            throw new BadRequestException(String.format(msg.getCUBE_DESC_NOT_FOUND(), cubeName));

        if (!desc.isDraft()) {
            data.put("cube", desc);

            String draftName = cubeName + "_draft";
            CubeInstance draftCubeInstance = cubeService.getCubeManager().getCube(draftName);
            if (draftCubeInstance != null) {
                CubeDesc draftCubeDesc = draftCubeInstance.getDescriptor();
                if (draftCubeDesc != null && draftCubeDesc.isDraft()) {
                    data.put("draft", draftCubeDesc);
                }
            }
        } else {
            data.put("draft", desc);

            String parentName = cubeName.substring(0, cubeName.lastIndexOf("_draft"));
            CubeInstance parentCubeInstance = cubeService.getCubeManager().getCube(parentName);
            if (parentCubeInstance != null) {
                CubeDesc parentDesc = parentCubeInstance.getDescriptor();
                if (parentDesc != null && !parentDesc.isDraft()) {
                    data.put("cube", parentDesc);
                }
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

}
