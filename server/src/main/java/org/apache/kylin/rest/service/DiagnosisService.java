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

package org.apache.kylin.rest.service;

import java.io.File;
import java.io.IOException;

import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.tool.DiagnosisInfoCLI;
import org.apache.kylin.tool.JobDiagnosisInfoCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.io.Files;

@Component("diagnosisService")
public class DiagnosisService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosisService.class);

    private File getDumpDir() {
        return Files.createTempDir();
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public BadQueryHistory getProjectBadQueryHistory(String project) throws IOException {
        return getBadQueryHistoryManager().getBadQueriesForProject(project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpProjectDiagnosisInfo(String project) throws IOException {
        String[] args = { "-project", project, "-destDir", getDumpDir().getAbsolutePath() };
        logger.info("DiagnosisInfoCLI args: " + args);
        DiagnosisInfoCLI diagnosisInfoCli = new DiagnosisInfoCLI();
        diagnosisInfoCli.execute(args);
        return diagnosisInfoCli.getExportDest();
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpJobDiagnosisInfo(String jobId) throws IOException {
        String[] args = { "-jobId", jobId, "-destDir", getDumpDir().getAbsolutePath() };
        logger.info("JobDiagnosisInfoCLI args: " + args);
        JobDiagnosisInfoCLI jobInfoExtractor = new JobDiagnosisInfoCLI();
        jobInfoExtractor.execute(args);
        return jobInfoExtractor.getExportDest();
    }
}
