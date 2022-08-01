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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.job.NResourceDetectStep;
import org.apache.kylin.engine.spark.job.NTableSamplingJob;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;

public class TableSamplingServiceTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";
    private static final int SAMPLING_ROWS = 20000;

    @InjectMocks
    private TableSamplingService tableSamplingService = Mockito.spy(new TableSamplingService());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        createTestMetadata();
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "11");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(tableSamplingService, "aclEvaluate", aclEvaluate);
    }

    @Test
    public void testSkipResourceDetectWithGlobalSettings() {
        overwriteSystemProp("kylin.engine.steps.skip", NResourceDetectStep.class.getCanonicalName());
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        Set<String> tables = Sets.newHashSet(table1);
        tableSamplingService.sampling(tables, PROJECT, SAMPLING_ROWS, 0, null, null);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        final List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(1, allExecutables.size());
        final NTableSamplingJob samplingJob = (NTableSamplingJob) allExecutables.get(0);
        final List<AbstractExecutable> tasks = samplingJob.getTasks();
        Assert.assertEquals(1, tasks.size());
        Assert.assertTrue(tasks.get(0) instanceof NTableSamplingJob.SamplingStep);
    }

    @Test
    public void testSkipResourceDetectWithProjectSettings() {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(PROJECT, copyForWrite -> {
            LinkedHashMap<String, String> properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.engine.steps.skip", NResourceDetectStep.class.getCanonicalName());
            copyForWrite.setOverrideKylinProps(properties);
        });
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        Set<String> tables = Sets.newHashSet(table1);
        tableSamplingService.sampling(tables, PROJECT, SAMPLING_ROWS, 0, null, null);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        final List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(1, allExecutables.size());
        final NTableSamplingJob samplingJob = (NTableSamplingJob) allExecutables.get(0);
        final List<AbstractExecutable> tasks = samplingJob.getTasks();
        Assert.assertEquals(1, tasks.size());
        Assert.assertTrue(tasks.get(0) instanceof NTableSamplingJob.SamplingStep);
    }

    @Test
    public void testSampling() {
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        final String table2 = "DEFAULT.TEST_ACCOUNT";
        Set<String> tables = Sets.newHashSet(table1, table2);
        tableSamplingService.sampling(tables, PROJECT, SAMPLING_ROWS, 0, null, null);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);

        final List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(2, allExecutables.size());

        final AbstractExecutable job1 = allExecutables.get(0);
        Assert.assertEquals(0, job1.getPriority());
        Assert.assertTrue(job1 instanceof NTableSamplingJob);
        NTableSamplingJob samplingJob1 = (NTableSamplingJob) job1;
        Assert.assertEquals("TABLE_SAMPLING", samplingJob1.getName());
        Assert.assertEquals(PROJECT, samplingJob1.getProject());
        final String tableNameOfSamplingJob1 = samplingJob1.getParam(NBatchConstants.P_TABLE_NAME);
        Assert.assertTrue(tables.contains(tableNameOfSamplingJob1));
        Assert.assertEquals(PROJECT, samplingJob1.getParam(NBatchConstants.P_PROJECT_NAME));
        Assert.assertEquals("ADMIN", samplingJob1.getSubmitter());
        Assert.assertEquals(2, ((NTableSamplingJob) job1).getTasks().size());

        final AbstractExecutable job2 = allExecutables.get(1);
        Assert.assertEquals(0, job2.getPriority());
        Assert.assertTrue(job2 instanceof NTableSamplingJob);
        NTableSamplingJob samplingJob2 = (NTableSamplingJob) job2;
        Assert.assertEquals("TABLE_SAMPLING", samplingJob2.getName());
        final String tableNameOfSamplingJob2 = samplingJob2.getParam(NBatchConstants.P_TABLE_NAME);
        Assert.assertEquals(PROJECT, samplingJob2.getProject());
        Assert.assertTrue(tables.contains(tableNameOfSamplingJob2));
        Assert.assertEquals(PROJECT, samplingJob2.getParam(NBatchConstants.P_PROJECT_NAME));
        Assert.assertEquals("ADMIN", samplingJob2.getSubmitter());

        Assert.assertEquals(tables, Sets.newHashSet(tableNameOfSamplingJob1, tableNameOfSamplingJob2));
    }

    @Test
    public void testSamplingKillAnExistingNonFinalJob() {
        // initialize a sampling job and assert the status of it
        String table = "DEFAULT.TEST_KYLIN_FACT";
        tableSamplingService.sampling(Sets.newHashSet(table), PROJECT, SAMPLING_ROWS, ExecutablePO.DEFAULT_PRIORITY,
                null, null);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(1, allExecutables.size());
        val initialJob = allExecutables.get(0);
        Assert.assertEquals(ExecutableState.READY, initialJob.getStatus());

        // launch another job on the same table will discard the already existing job and create a new job(secondJob)
        Assert.assertTrue(tableSamplingService.hasSamplingJob(PROJECT, table));
        UnitOfWork.doInTransactionWithRetry(() -> {
            tableSamplingService.sampling(Sets.newHashSet(table), PROJECT, SAMPLING_ROWS, ExecutablePO.DEFAULT_PRIORITY,
                    null, null);
            return null;
        }, PROJECT);
        Assert.assertEquals(ExecutableState.DISCARDED, initialJob.getStatus());
        allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(2, allExecutables.size());
        List<AbstractExecutable> nonFinalStateJob = allExecutables.stream() //
                .filter(job -> !job.getStatus().isFinalState()) //
                .collect(Collectors.toList());
        Assert.assertEquals(1, nonFinalStateJob.size());
        val secondJob = nonFinalStateJob.get(0);
        Assert.assertEquals(ExecutableState.READY, secondJob.getStatus());

        // modify the status of the second sampling job to Running
        // launch another job on the same table will discard the second job and create a new job(thirdJob)
        Assert.assertTrue(tableSamplingService.hasSamplingJob(PROJECT, table));
        UnitOfWork.doInTransactionWithRetry(() -> {
            executableManager.updateJobOutput(secondJob.getId(), ExecutableState.RUNNING);
            tableSamplingService.sampling(Sets.newHashSet(table), PROJECT, SAMPLING_ROWS, ExecutablePO.DEFAULT_PRIORITY,
                    null, null);
            return null;
        }, PROJECT);
        Assert.assertEquals(ExecutableState.DISCARDED, secondJob.getStatus());
        allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(3, allExecutables.size());
        nonFinalStateJob = allExecutables.stream() //
                .filter(job -> !job.getStatus().isFinalState()) //
                .collect(Collectors.toList());
        Assert.assertEquals(1, nonFinalStateJob.size());
        val thirdJob = nonFinalStateJob.get(0);
        Assert.assertEquals(ExecutableState.READY, thirdJob.getStatus());

        // modify the status of the third sampling job to Error
        // launch another job on the same table will discard the second job and create a new job(fourthJob)
        Assert.assertTrue(tableSamplingService.hasSamplingJob(PROJECT, table));
        UnitOfWork.doInTransactionWithRetry(() -> {
            executableManager.updateJobOutput(thirdJob.getId(), ExecutableState.ERROR);
            tableSamplingService.sampling(Sets.newHashSet(table), PROJECT, SAMPLING_ROWS, ExecutablePO.DEFAULT_PRIORITY,
                    null, null);
            return null;
        }, PROJECT);
        Assert.assertEquals(ExecutableState.DISCARDED, thirdJob.getStatus());
        allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(4, allExecutables.size());
        nonFinalStateJob = allExecutables.stream() //
                .filter(job -> !job.getStatus().isFinalState()) //
                .collect(Collectors.toList());
        Assert.assertEquals(1, nonFinalStateJob.size());
        val fourthJob = nonFinalStateJob.get(0);
        Assert.assertEquals(ExecutableState.READY, fourthJob.getStatus());
    }
}
