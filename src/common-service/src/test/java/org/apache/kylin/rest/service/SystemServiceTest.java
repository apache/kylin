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

import static org.junit.Assert.fail;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.request.DiagProgressRequest;
import org.apache.kylin.rest.response.DiagStatusResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.tool.constant.DiagTypeEnum;
import org.apache.kylin.tool.constant.StageEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import lombok.val;

public class SystemServiceTest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @InjectMocks
    private SystemService systemService = Mockito.spy(new SystemService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetDiagPackagePath() throws Exception {
        Cache<String, SystemService.DiagInfo> exportPathMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        File uuid = new File(mainDir, "uuid");
        File date = new File(uuid, "date");
        date.mkdirs();
        File zipFile = new File(date, "diag.zip");
        zipFile.createNewFile();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        diagInfo.setExportFile(uuid);
        diagInfo.setStage(StageEnum.DONE.toString());
        exportPathMap.put("test2", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", exportPathMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        val result = systemService.getDiagPackagePath("test2");
        Assert.assertTrue(result.endsWith("diag.zip"));
    }

    @Test
    public void testGetExtractorStatus() throws Exception {
        Cache<String, SystemService.DiagInfo> extractorMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        extractorMap.put("test1", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", extractorMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        val result = systemService.getExtractorStatus("test1");
        Assert.assertEquals("PREPARE", ((DiagStatusResponse) result.getData()).getStage());
    }

    @Test
    public void testStopDiagTask() throws Exception {
        String uuid = "test3";
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future task = executorService.submit(() -> {
        });
        task.get();
        Cache<String, SystemService.DiagInfo> futureMap = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
                .build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        diagInfo.setTask(task);
        futureMap.put(uuid, diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", futureMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        systemService.stopDiagTask(uuid);

    }

    @Test
    public void testDumpLocalQueryDiagPackage() {
        overwriteSystemProp("kylin.security.allow-non-admin-generate-query-diag-package", "false");
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        systemService.dumpLocalQueryDiagPackage(null, null);
    }

    @Test
    public void testGetQueryDiagPackagePath() throws Exception {
        Cache<String, SystemService.DiagInfo> exportPathMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        File uuid = new File(mainDir, "uuid");
        File date = new File(uuid, "date");
        date.mkdirs();
        File zipFile = new File(date, "diag.zip");
        zipFile.createNewFile();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo(mainDir, null, DiagTypeEnum.QUERY);
        diagInfo.setExportFile(uuid);
        diagInfo.setStage(StageEnum.DONE.toString());
        exportPathMap.put("test2", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", exportPathMap);
        val result = systemService.getDiagPackagePath("test2");
        Assert.assertTrue(result.endsWith("diag.zip"));
    }

    @Test
    public void testGetQueryExtractorStatus() {
        overwriteSystemProp("kylin.security.allow-non-admin-generate-query-diag-package", "false");
        Cache<String, SystemService.DiagInfo> extractorMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo(null, null, DiagTypeEnum.QUERY);
        extractorMap.put("test1", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", extractorMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        val result = systemService.getExtractorStatus("test1");
        Assert.assertEquals("PREPARE", ((DiagStatusResponse) result.getData()).getStage());
    }

    @Test
    public void testStopQueryDiagTask() throws Exception {
        String uuid = "testQuery";
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future task = executorService.submit(() -> {
        });
        task.get();
        Cache<String, SystemService.DiagInfo> futureMap = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
                .build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo(null, task, DiagTypeEnum.FULL);
        diagInfo.setTask(task);
        futureMap.put(uuid, diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", futureMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        systemService.stopDiagTask(uuid);
    }

    @Test
    public void testDumpLocalDiagPackage() {
        systemService.dumpLocalDiagPackage(null, null, "dd5a6451-0743-4b32-b84d-2ddc80524276", null, "test");
        systemService.dumpLocalDiagPackage(null, null, null, "5bc63cbe-a2fe-fa4e-3142-1bb4ebab8f98", "test");
    }

    @Test
    public void testUpdateDiagProgress() {
        Cache<String, SystemService.DiagInfo> extractorMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        extractorMap.put("testUpdate", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", extractorMap);

        DiagProgressRequest diagProgressRequest = new DiagProgressRequest();
        diagProgressRequest.setDiagId("testUpdate");
        diagProgressRequest.setStage(StageEnum.DONE.toString());
        diagProgressRequest.setProgress(100);

        systemService.updateDiagProgress(diagProgressRequest);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);

        val result = systemService.getExtractorStatus("testUpdate");
        Assert.assertEquals(StageEnum.DONE.toString(), result.getData().getStage());
        Assert.assertEquals(new Float(100), result.getData().getProgress());
    }

    @Test
    public void testRecoverMetadata() {
        try {
            systemService.reloadMetadata();
        } catch (Exception e) {
            fail("reload should be successful but not");
        }
    }

    @Test
    public void testGetReadOnlyConfig() {
        final String project = "default", modelName = "nmodel_basic";
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject("default", update_project -> {
            LinkedHashMap<String, String> overrideKylinProps = update_project.getOverrideKylinProps();
            overrideKylinProps.put("kylin.engine.spark-conf.spark.sql.shuffle.partitions", "10");
        });
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), project);
        NDataModel model = NDataModelManager.getInstance(getTestConfig(), project)
                .getDataModelDescByAlias(modelName);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(model.getId());
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            LinkedHashMap<String, String> overrideProps = indexPlan.getOverrideProps();
            overrideProps.put("kylin.engine.spark-conf.spark.sql.shuffle.partitions", "100");
            overrideProps.put("kylin.index.rule-scheduler-data", "11");
            copyForWrite.setOverrideProps(overrideProps);
        });

        Map<String, String> systemConfig = systemService.getReadOnlyConfig("", "");
        Map<String, String> projectConfig = systemService.getReadOnlyConfig(project, "");
        Map<String, String> modelConfig = systemService.getReadOnlyConfig(project, modelName);
        Assert.assertEquals("UT", systemConfig.get("kylin.env"));
        Assert.assertEquals("10", projectConfig.get("kylin.engine.spark-conf.spark.sql.shuffle.partitions"));
        Assert.assertEquals("100", modelConfig.get("kylin.engine.spark-conf.spark.sql.shuffle.partitions"));
        Assert.assertEquals(null, modelConfig.get("kylin.index.rule-scheduler-data"));
        assertKylinExeption(() -> systemService.getReadOnlyConfig("", modelName),
                "Please fill in the project parameters.");
        assertKylinExeption(() -> systemService.getReadOnlyConfig("noexistProject", modelName),
                "Please confirm and try again later.");
    }

}
