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

package org.apache.kylin.rest.delegate;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.rest.controller.BaseController;
import org.apache.kylin.rest.service.JobInfoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/api/job_delegate", produces = { HTTP_VND_APACHE_KYLIN_JSON})
public class JobMetadataDelegateController extends BaseController {

    @Autowired
    private JobInfoService jobInfoService;

    @PostMapping(value = "/discard_job")
    @ResponseBody
    public void discardJob(@RequestParam("project") String project, @RequestParam("jobId") String jobId,
            @RequestHeader HttpHeaders headers) throws IOException {
        List<String> jobIdList = Lists.newArrayList(jobId.split(","));
        jobIdList.stream().forEach(eachJobId -> ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .discardJob(eachJobId));
    }
}
