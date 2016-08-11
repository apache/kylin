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
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.rest.constant.Constant;
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

    private String getDiagnosisPackageName(File destDir) {
        for (File subDir : destDir.listFiles()) {
            if (subDir.isDirectory()) {
                for (File file : subDir.listFiles()) {
                    if (file.getName().endsWith(".zip")) {
                        return file.getAbsolutePath();
                    }
                }
            }
        }
        throw new RuntimeException("Diagnosis package not found in directory: " + destDir.getAbsolutePath());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public BadQueryHistory getProjectBadQueryHistory(String project) throws IOException {
        return getBadQueryHistoryManager().getBadQueriesForProject(project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpProjectDiagnosisInfo(String project) throws IOException {
        File exportPath = getDumpDir();
        String[] args = { project, exportPath.getAbsolutePath() };
        runDiagnosisCLI(args);
        return getDiagnosisPackageName(exportPath);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpJobDiagnosisInfo(String jobId) throws IOException {
        File exportPath = getDumpDir();
        String[] args = { jobId, exportPath.getAbsolutePath() };
        runDiagnosisCLI(args);
        return getDiagnosisPackageName(exportPath);
    }

    private void runDiagnosisCLI(String[] args) throws IOException {
        File cwd = new File("");
        logger.info("Current path: " + cwd.getAbsolutePath());

        logger.info("DiagnosisInfoCLI args: " + Arrays.toString(args));
        File script = new File(KylinConfig.getKylinHome() + File.separator + "bin", "diag.sh");
        if (!script.exists()) {
            throw new RuntimeException("diag.sh not found at " + script.getAbsolutePath());
        }

        String diagCmd = script.getAbsolutePath() + " " + StringUtils.join(args, " ");
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        Pair<Integer, String> cmdOutput = executor.execute(diagCmd, new org.apache.kylin.common.util.Logger() {
            @Override
            public void log(String message) {
                logger.info(message);
            }
        });

        if (cmdOutput.getKey() != 0) {
            throw new RuntimeException("Failed to generate diagnosis package.");
        }
    }
}
