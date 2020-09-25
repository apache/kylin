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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.persistence.AutoDeleteDirectory;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.service.DiagnosisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

@Controller
@RequestMapping(value = "/diag")
public class DiagnosisController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosisController.class);

    @Autowired
    @Qualifier("diagnosisService")
    private DiagnosisService dgService;

    /**
     * Get bad query history
     */
    @RequestMapping(value = "/{project}/sql", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<BadQueryEntry> getBadQuerySql(@PathVariable String project) {

        List<BadQueryEntry> badEntry = Lists.newArrayList();
        try {
            BadQueryHistory badQueryHistory = dgService.getProjectBadQueryHistory(project);
            badEntry.addAll(badQueryHistory.getEntries());
        } catch (IOException e) {
            throw new InternalErrorException("Failed to get bad queries.", e);
        }

        return badEntry;
    }

    /**
     * Get diagnosis information for project
     */
    @RequestMapping(value = "/project/{project}/download", method = { RequestMethod.GET }, produces = {
            "application/json" })
    @ResponseBody
    public void dumpProjectDiagnosisInfo(@PathVariable String project, final HttpServletRequest request,
            final HttpServletResponse response) {
        try (AutoDeleteDirectory diagDir = new AutoDeleteDirectory("diag_project", "")) {
            String filePath = dgService.dumpProjectDiagnosisInfo(project, diagDir.getFile());
            setDownloadResponse(filePath, response);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to dump project diagnosis info. " + e.getMessage(), e);
        }

    }

    /**
     * Get diagnosis information for job
     */
    @RequestMapping(value = "/job/{jobId}/download", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public void dumpJobDiagnosisInfo(@PathVariable String jobId, final HttpServletRequest request,
            final HttpServletResponse response) {
        try (AutoDeleteDirectory diagDir = new AutoDeleteDirectory("diag_job", "")) {
            String filePath = dgService.dumpJobDiagnosisInfo(jobId, diagDir.getFile());
            setDownloadResponse(filePath, response);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to dump job diagnosis info. " + e.getMessage(), e);
        }

    }

}
