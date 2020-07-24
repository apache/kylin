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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.metadata.badquery.BadQueryEntry;
import org.apache.kylin.metadata.badquery.BadQueryHistory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.ValidateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

@Component("diagnosisService")
public class DiagnosisService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosisService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private JobService jobService;

    private String getDiagnosisPackageName(File destDir) {
        Message msg = MsgPicker.getMsg();

        File[] files = destDir.listFiles();
        if (files == null) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getDIAG_PACKAGE_NOT_AVAILABLE(), destDir.getAbsolutePath()));
        }
        for (File subDir : files) {
            if (subDir.isDirectory()) {
                for (File file : subDir.listFiles()) {
                    if (file.getName().endsWith(".zip")) {
                        return file.getAbsolutePath();
                    }
                }
            }
        }
        throw new BadRequestException(
                String.format(Locale.ROOT, msg.getDIAG_PACKAGE_NOT_FOUND(), destDir.getAbsolutePath()));
    }

    public BadQueryHistory getProjectBadQueryHistory(String project) throws IOException {
        aclEvaluate.checkProjectOperationPermission(project);
        return getBadQueryHistoryManager().getBadQueriesForProject(project);
    }

    public String dumpProjectDiagnosisInfo(String project, File exportPath) throws IOException {
        Message msg = MsgPicker.getMsg();
        ProjectInstance projectInstance =
                ProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                        .getProject(ValidateUtil.convertStringToBeAlphanumericUnderscore(project));
        if (null == projectInstance) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getDIAG_PROJECT_NOT_FOUND(), project));
        }
        aclEvaluate.checkProjectOperationPermission(projectInstance);
        String[] args = { project, exportPath.getAbsolutePath() };
        runDiagnosisCLI(args);
        return getDiagnosisPackageName(exportPath);
    }

    public String dumpJobDiagnosisInfo(String jobId, File exportPath) throws IOException {
        Message msg = MsgPicker.getMsg();
        JobInstance jobInstance = jobService.getJobInstance(jobId);
        if (null == jobInstance) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getDIAG_JOBID_NOT_FOUND(), jobId));
        }
        aclEvaluate.checkProjectOperationPermission(jobInstance);
        String[] args = { jobId, exportPath.getAbsolutePath() };
        runDiagnosisCLI(args);
        return getDiagnosisPackageName(exportPath);
    }

    private void runDiagnosisCLI(String[] args) throws IOException {
        Message msg = MsgPicker.getMsg();

        File cwd = new File("");
        logger.debug("Current path: {}", cwd.getAbsolutePath());
        logger.debug("DiagnosisInfoCLI args: {}", Arrays.toString(args));
        File script = new File(KylinConfig.getKylinHome() + File.separator + "bin", "diag.sh");
        if (!script.exists()) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getDIAG_NOT_FOUND(), script.getAbsolutePath()));
        }

        String diagCmd = script.getAbsolutePath() + " " + StringUtils.join(args, " ");
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        Pair<Integer, String> cmdOutput = executor.execute(diagCmd);

        if (cmdOutput.getFirst() != 0) {
            throw new BadRequestException(msg.getGENERATE_DIAG_PACKAGE_FAIL());
        }
    }

    public List<BadQueryEntry> getQueriesByType(List<BadQueryEntry> allBadEntries, String queryType)
            throws IOException {

        List<BadQueryEntry> filteredEntries = Lists.newArrayList();
        for (BadQueryEntry entry : allBadEntries) {
            if (null != entry && entry.getAdj().equals(queryType)) {
                filteredEntries.add(entry);
            }
        }
        return filteredEntries;
    }

    public Map<String, Object> getQueries(Integer pageOffset, Integer pageSize, String queryType,
            List<BadQueryEntry> allBadEntries) throws IOException {
        HashMap<String, Object> data = new HashMap<>();
        List<BadQueryEntry> filteredEntries = getQueriesByType(allBadEntries, queryType);

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        if (filteredEntries.size() <= offset) {
            offset = filteredEntries.size();
            limit = 0;
        }

        if ((filteredEntries.size() - offset) < limit) {
            limit = filteredEntries.size() - offset;
        }

        data.put("badQueries", filteredEntries.subList(offset, offset + limit));
        data.put("size", filteredEntries.size());
        return data;
    }

}
