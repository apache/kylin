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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.rest.util.SparkHistoryUIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@ConditionalOnProperty(name = "kylin.history-server.enable", havingValue = "true")
@RequestMapping(value = SparkHistoryUIUtil.HISTORY_UI_BASE)
public class SparkHistoryUIController {

    @Autowired
    @Qualifier("SparkHistoryUIUtil")
    private SparkHistoryUIUtil sparkHistoryUIUtil;

    @ApiOperation(value = "proxy", tags = { "DW" })
    @RequestMapping(value = "/**")
    @ResponseBody
    public void proxy(HttpServletRequest request, HttpServletResponse response) throws IOException {
        sparkHistoryUIUtil.proxy(request, response);
    }
}
