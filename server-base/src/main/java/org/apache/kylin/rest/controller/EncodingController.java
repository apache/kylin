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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.EncodingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Controller
@RequestMapping(value = "/encodings")
public class EncodingController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(EncodingController.class);

    @Autowired
    private EncodingService encodingService;

    /**
     * Get valid encodings for the datatype, if no datatype parameter, return all encodings.
     *
     * @return suggestion map
     */
    @RequestMapping(value = "valid_encodings", method = { RequestMethod.GET })
    @ResponseBody
    public EnvelopeResponse getValidEncodings() {

        Set<String> allDatatypes = Sets.newHashSet();
        allDatatypes.addAll(DataType.DATETIME_FAMILY);
        allDatatypes.addAll(DataType.INTEGER_FAMILY);
        allDatatypes.addAll(DataType.NUMBER_FAMILY);
        allDatatypes.addAll(DataType.STRING_FAMILY);

        Map<String, List<String>> datatypeValidEncodings = Maps.newHashMap();
        for (String dataTypeStr : allDatatypes) {
            datatypeValidEncodings.put(dataTypeStr, encodingService.getValidEncodings(DataType.getType(dataTypeStr)));
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, datatypeValidEncodings, "");
    }
}
