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

import java.util.Set;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.CustomFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;

@Controller
@RequestMapping(value = "/api/custom", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class CustomFileController extends NBasicController {

    @Autowired
    private CustomFileService customFileService;

    @PostMapping(value = "jar")
    @ResponseBody
    public EnvelopeResponse<Set<String>> upload(@RequestParam("file") MultipartFile file,
            @RequestParam("project") String project, @RequestParam("jar_type") String jarType) {
        checkStreamingEnabled();
        if (file.isEmpty()) {
            return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, Sets.newHashSet(), "");
        }
        checkRequiredArg("jar_type", jarType);
        String projectName = checkProjectName(project);
        Set<String> classList = customFileService.uploadJar(file, projectName, jarType);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, classList, "");
    }

    @DeleteMapping(value = "jar")
    @ResponseBody
    public EnvelopeResponse<String> removeJar(@RequestParam("project") String project,
            @RequestParam("jar_name") String jarName, @RequestParam("jar_type") String jarType) {
        checkStreamingEnabled();
        checkRequiredArg("jar_type", jarType);
        String projectName = checkProjectName(project);
        String removedJar = customFileService.removeJar(projectName, jarName, jarType);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, removedJar, "");
    }
}
