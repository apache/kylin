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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_SAMPLING_RANGE_INVALID;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.engine.spark.job.NTableSamplingJob;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;

@Component("tableSamplingService")
public class TableSamplingService extends BasicService implements TableSamplingSupporter {

    private static final int MAX_SAMPLING_ROWS = 20_000_000;
    private static final int MIN_SAMPLING_ROWS = 10_000;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Override
    @Transaction(project = 1)
    public List<String> sampling(Set<String> tables, String project, int rows, int priority, String yarnQueue,
            Object tag) {
        aclEvaluate.checkProjectWritePermission(project);
        NExecutableManager execMgr = NExecutableManager.getInstance(getConfig(), project);
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getConfig(), project);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getConfig(), project);

        val existingJobs = collectRunningSamplingJobs(tables, project);
        List<String> jobIds = Lists.newArrayList();
        tables.forEach(table -> {
            // if existing a related job, discard it
            if (existingJobs.containsKey(table)) {
                execMgr.discardJob(existingJobs.get(table).getId());
            }

            JobManager.checkStorageQuota(project);
            val tableDesc = tableMgr.getTableDesc(table);
            val samplingJob = NTableSamplingJob.create(tableDesc, project, getUsername(), rows, priority, yarnQueue,
                    tag);
            jobIds.add(samplingJob.getId());
            execMgr.addJob(NExecutableManager.toPO(samplingJob, project));
            long startOfDay = TimeUtil.getDayStart(System.currentTimeMillis());
            jobStatisticsManager.updateStatistics(startOfDay, 0, 0, 1);
        });
        return jobIds;
    }

    public boolean hasSamplingJob(String project, String table) {
        aclEvaluate.checkProjectWritePermission(project);
        return !collectRunningSamplingJobs(Sets.newHashSet(table), project).isEmpty();
    }

    private Map<String, AbstractExecutable> collectRunningSamplingJobs(Set<String> tables, String project) {
        final List<AbstractExecutable> jobs = NExecutableManager
                .getInstance(KylinConfig.readSystemKylinConfig(), project).getAllJobs(0, Long.MAX_VALUE).stream()
                .filter(job -> !ExecutableState.valueOf(job.getOutput().getStatus()).isFinalState())
                .map(job -> getManager(NExecutableManager.class, job.getProject()).fromPO(job)) //
                .filter(NTableSamplingJob.class::isInstance) //
                .filter(job -> tables.contains(job.getTargetSubject())) //
                .collect(Collectors.toList());

        Map<String, AbstractExecutable> map = Maps.newHashMap();
        jobs.forEach(job -> map.put(job.getTargetSubject(), job));
        return map;
    }

    public static void checkSamplingRows(int rows) {
        if (rows > MAX_SAMPLING_ROWS || rows < MIN_SAMPLING_ROWS) {
            throw new KylinException(JOB_SAMPLING_RANGE_INVALID, MIN_SAMPLING_ROWS, MAX_SAMPLING_ROWS);
        }
    }

    public static void checkSamplingTable(String tableName) {
        Message msg = MsgPicker.getMsg();
        if (tableName == null || StringUtils.isEmpty(tableName.trim())) {
            throw new KylinException(INVALID_TABLE_NAME, msg.getFailedForNoSamplingTable());
        }

        if (tableName.contains(" ") || !tableName.contains(".") || tableName.split("\\.").length != 2) {
            throw new KylinException(INVALID_TABLE_NAME, msg.getSamplingFailedForIllegalTableName());
        }
    }
}
