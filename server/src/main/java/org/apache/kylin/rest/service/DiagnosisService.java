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

import java.io.IOException;

import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.tool.DiagnosisInfoCLI;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

@Component("diagnosisService")
public class DiagnosisService extends BasicService {

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public BadQueryHistory getProjectBadQueryHistory(String project) throws IOException {
        return getBadQueryHistoryManager().getBadQueriesForProject(project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpDiagnosisInfo(String project) throws IOException {
        String tempLocation = System.getProperty("java.io.tmpdir");
        String[] args = { "-project", project, "-destDir", tempLocation, "-compress", "true" };
        DiagnosisInfoCLI diagnosisInfoCli = new DiagnosisInfoCLI();
        diagnosisInfoCli.execute(args);
        return diagnosisInfoCli.getExportDest();
    }
}
