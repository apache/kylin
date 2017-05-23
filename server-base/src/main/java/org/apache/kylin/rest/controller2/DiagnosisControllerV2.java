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
 *
 */

package org.apache.kylin.rest.controller2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.DiagnosisService;
import org.apache.kylin.rest.service.ProjectServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

@Controller
@RequestMapping(value = "/diag")
public class DiagnosisControllerV2 extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosisControllerV2.class);

    @Autowired
    @Qualifier("diagnosisService")
    private DiagnosisService dgService;

    @Autowired
    @Qualifier("projectServiceV2")
    private ProjectServiceV2 projectServiceV2;

    /**
     * Get bad query history
     */

    @RequestMapping(value = "/sql", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getBadQuerySqlV2(@RequestHeader("Accept-Language") String lang, @RequestParam(value = "project", required = false) String project, @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset, @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) throws IOException {
        MsgPicker.setMsg(lang);

        HashMap<String, Object> data = new HashMap<String, Object>();
        List<BadQueryEntry> badEntry = Lists.newArrayList();
        if (project != null) {
            BadQueryHistory badQueryHistory = dgService.getProjectBadQueryHistory(project);
            badEntry.addAll(badQueryHistory.getEntries());
        } else {
            for (ProjectInstance projectInstance : projectServiceV2.getReadableProjects()) {
                BadQueryHistory badQueryHistory = dgService.getProjectBadQueryHistory(projectInstance.getName());
                badEntry.addAll(badQueryHistory.getEntries());
            }
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (badEntry.size() <= offset) {
            offset = badEntry.size();
            limit = 0;
        }

        if ((badEntry.size() - offset) < limit) {
            limit = badEntry.size() - offset;
        }

        data.put("badQueries", badEntry.subList(offset, offset + limit));
        data.put("size", badEntry.size());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    /**
     * Get diagnosis information for project
     */

    @RequestMapping(value = "/project/{project}/download", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void dumpProjectDiagnosisInfoV2(@RequestHeader("Accept-Language") String lang, @PathVariable String project, final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        MsgPicker.setMsg(lang);

        String filePath;
        filePath = dgService.dumpProjectDiagnosisInfo(project);

        setDownloadResponse(filePath, response);
    }

    /**
     * Get diagnosis information for job
     */

    @RequestMapping(value = "/job/{jobId}/download", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void dumpJobDiagnosisInfoV2(@RequestHeader("Accept-Language") String lang, @PathVariable String jobId, final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        MsgPicker.setMsg(lang);

        String filePath;
        filePath = dgService.dumpJobDiagnosisInfo(jobId);

        setDownloadResponse(filePath, response);
    }
}
