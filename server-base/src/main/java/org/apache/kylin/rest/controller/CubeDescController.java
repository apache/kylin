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

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author xduo
 * 
 */
@Controller
@RequestMapping(value = "/cube_desc")
public class CubeDescController extends BasicController {

    @Autowired
    private CubeService cubeService;

    /**
     * Get detail information of the "Cube ID"
     * 
     * @param cubeDescName
     *            Cube ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET })
    @ResponseBody
    public CubeDesc[] getCube(@PathVariable String cubeName) {
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance == null) {
            return null;
        }
        CubeDesc cSchema = cubeInstance.getDescriptor();
        if (cSchema != null) {
            return new CubeDesc[] { cSchema };
        } else {
            return null;
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
    @RequestMapping(value = "/{cubeName}/desc", method = { RequestMethod.GET })
    @ResponseBody
    public CubeDesc getDesc(@PathVariable String cubeName) {
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance == null) {
            return null;
        }
        CubeDesc cSchema = cubeInstance.getDescriptor();
        if (cSchema != null) {
            return cSchema;
        } else {
            return null;
        }
    }

    /**
     * Initiate the very beginning of a streaming cube. Will seek the latest offests of each partition from streaming
     * source (kafka) and record in the cube descriptor; In the first build job, it will use these offests as the start point.
     * @param cubeName
     * @return
     */
    @RequestMapping(value = "/{cubeName}/initStartOffsets", method = { RequestMethod.PUT })
    @ResponseBody
    public GeneralResponse initStartOffsets(@PathVariable String cubeName) {
       CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);

        String msg = "";
        if (cubeInstance == null) {
            msg = "Cube '" + cubeName + "' not found.";
            throw new IllegalArgumentException(msg);
        }
        if (cubeInstance.getSourceType() != ISourceAware.ID_STREAMING) {
            msg = "Cube '" + cubeName + "' is not a Streaming Cube.";
            throw new IllegalArgumentException(msg);
        }

        final GeneralResponse response = new GeneralResponse();
        try {
            final Map<Integer, Long> startOffsets = KafkaClient.getCurrentOffsets(cubeInstance);
            CubeDesc desc = cubeInstance.getDescriptor();
            desc.setPartitionOffsetStart(startOffsets);
            cubeService.getCubeDescManager().updateCubeDesc(desc);
            response.setProperty("result", "success");
            response.setProperty("offsets", startOffsets.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return response;
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

}
