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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.IndexGlutenCacheRequest;
import org.apache.kylin.rest.request.InternalTableGlutenCacheRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GlutenCacheResponse;
import org.apache.kylin.rest.service.GlutenCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping(value = "/api", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class GlutenCacheController extends NBasicController {

    @Autowired
    private GlutenCacheService glutenCacheService;

    /**
     * RPC Call: for build job update gluten cache
     */
    @PostMapping(value = "/cache/gluten_cache")
    @ApiOperation(value = "gluten cache", tags = { "Gluten" })
    @ResponseBody
    public EnvelopeResponse<GlutenCacheResponse> glutenCache(@RequestBody List<String> cacheCommands) {
        checkCollectionRequiredArg("cacheCommands", cacheCommands);
        val result = glutenCacheService.glutenCache(cacheCommands);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    /**
     * RPC Call: for API update gluten cache
     */
    @PostMapping(value = "/cache/gluten_cache_async")
    @ApiOperation(value = "gluten cache", tags = { "Gluten" })
    @ResponseBody
    public EnvelopeResponse<String> glutenCacheAsync(@RequestBody List<String> cacheCommands) {
        checkCollectionRequiredArg("cacheCommands", cacheCommands);
        glutenCacheService.glutenCacheAsync(cacheCommands);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/internal_table/gluten_cache")
    @ApiOperation(value = "index gluten cache", tags = { "Gluten" })
    @ResponseBody
    public EnvelopeResponse<String> internalTableGlutenCache(@RequestBody InternalTableGlutenCacheRequest request,
            HttpServletRequest servletRequest) throws Exception {
        log.info("Internal table gluten cache request is [{}]", request);
        val projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        checkRequiredArg("database", request.getDatabase());
        checkRequiredArg("table", request.getTable());
        glutenCacheService.internalTableGlutenCache(request, servletRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    @PostMapping(value = "/index/gluten_cache")
    @ApiOperation(value = "index gluten cache", tags = { "Gluten" })
    @ResponseBody
    public EnvelopeResponse<String> indexGlutenCache(@RequestBody IndexGlutenCacheRequest request,
            HttpServletRequest servletRequest) throws Exception {
        log.info("Index gluten cache request is [{}]", request);
        val projectName = checkProjectName(request.getProject());
        request.setProject(projectName);
        checkRequiredArg("model", request.getModel());
        glutenCacheService.indexGlutenCache(request, servletRequest);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }
}
