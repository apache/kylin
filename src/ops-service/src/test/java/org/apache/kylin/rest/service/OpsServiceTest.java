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

import static org.apache.kylin.metadata.favorite.QueryHistoryIdOffset.OffsetType.ACCELERATE;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.asynctask.MetadataRestoreTask.MetadataRestoreStatus;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.MockClusterManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.reponse.MetadataBackupResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.tool.JobInfoTool;
import org.apache.kylin.tool.MetadataTool;
import org.apache.kylin.tool.QueryHistoryOffsetTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ClusterManager.class, JobInfoTool.class, MetadataTool.class, QueryHistoryOffsetTool.class })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
public class OpsServiceTest extends NLocalFileMetadataTestCase {
    public OpsService opsService;

    @Before
    public void init() {
        PowerMockito.mockStatic(ClusterManager.class);
        ClusterManager clusterManager = new MockClusterManager();
        PowerMockito.when(ClusterManager.getInstance()).thenReturn(clusterManager);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        opsService = new OpsService();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
        OpsService.MetadataBackup.getRunningTask().clear();
    }

    @Test
    public void testBackupAndRestoreMetadata() throws IOException {
        String project = "default";
        QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(project);
        QueryHistoryIdOffset originOffset = offsetManager.get(ACCELERATE);
        originOffset.setOffset(999L);
        offsetManager.saveOrUpdate(originOffset);

        FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(project);
        FavoriteRule.Condition cond = new FavoriteRule.Condition(null, "10");
        FavoriteRule originRule = new FavoriteRule(Collections.singletonList(cond), "count", true);
        favoriteRuleManager.createRule(originRule);

        JobInfoMapper jobInfoMapper = JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobInfoMapper();
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId("mock_job_id");
        jobInfo.setProject(project);
        jobInfo.setUpdateTime(System.currentTimeMillis());
        ExecutablePO originExecutable = new ExecutablePO();
        originExecutable.setName("origin_executable");
        byte[] originJobContent = JobInfoUtil.serializeExecutablePO(originExecutable);
        jobInfo.setJobContent(originJobContent);
        jobInfoMapper.insert(jobInfo);

        String outPutPath = opsService.backupMetadata(project);
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = OpsService.getMetadataBackupList(project);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (outPutPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatus.SUCCEED) {
                    return true;
                }
            }
            return false;
        });

        FavoriteRule newRule = new FavoriteRule(Collections.singletonList(cond), "count", true);
        FavoriteRule.Condition cond2 = new FavoriteRule.Condition(null, "20");
        newRule.setConds(Collections.singletonList(cond2));
        favoriteRuleManager.updateRule(newRule);
        FavoriteRule currentRule = favoriteRuleManager.getByName("count");
        Assert.assertNotEquals(((FavoriteRule.Condition) originRule.getConds().get(0)).getRightThreshold(),
                ((FavoriteRule.Condition) currentRule.getConds().get(0)).getRightThreshold());

        QueryHistoryIdOffset offset = offsetManager.get(ACCELERATE);
        offset.setOffset(111L);
        offsetManager.saveOrUpdate(offset);
        QueryHistoryIdOffset currentOffset = offsetManager.get(ACCELERATE);
        Assert.assertNotEquals(originOffset.getOffset(), currentOffset.getOffset());

        ExecutablePO modifiedExecutable = new ExecutablePO();
        modifiedExecutable.setName("modified_job_executable");
        byte[] modifiedJobContent = JobInfoUtil.serializeExecutablePO(modifiedExecutable);
        jobInfo.setJobContent(modifiedJobContent);
        jobInfoMapper.updateByJobIdSelective(jobInfo);
        JobInfo currentJobInfo = jobInfoMapper.selectByJobId(jobInfo.getJobId());
        Assert.assertNotEquals(originExecutable.getName(),
                Objects.requireNonNull(JobInfoUtil.deserializeExecutablePO(currentJobInfo)).getName());

        opsService.restoreMetadata(outPutPath, project, true);
        await().atMost(1, TimeUnit.MINUTES).until(() -> !OpsService.MetadataRestore.hasMetadataRestoreRunning());

        currentRule = favoriteRuleManager.getByName("count");
        Assert.assertEquals(((FavoriteRule.Condition) originRule.getConds().get(0)).getRightThreshold(),
                ((FavoriteRule.Condition) currentRule.getConds().get(0)).getRightThreshold());
        currentOffset = offsetManager.get(ACCELERATE);
        Assert.assertEquals(originOffset.getOffset(), currentOffset.getOffset());
        currentJobInfo = jobInfoMapper.selectByJobId(jobInfo.getJobId());
        Assert.assertEquals(originExecutable.getName(),
                Objects.requireNonNull(JobInfoUtil.deserializeExecutablePO(currentJobInfo)).getName());
    }

    @Test
    public void deleteBackup() throws IOException {
        String backupPath = opsService.backupMetadata(null);
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = OpsService
                    .getMetadataBackupList(UnitOfWork.GLOBAL_UNIT);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (backupPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatus.SUCCEED) {
                    return true;
                }
            }
            return false;
        });
        Assert.assertTrue(
                OpsService.deleteMetadataBackup(Lists.newArrayList(backupPath), null).contains("delete succeed"));
        Assert.assertTrue(OpsService
                .deleteMetadataBackup(Lists.newArrayList(getTestConfig().getHdfsWorkingDirectory() + "sss",
                        getTestConfig().getHdfsWorkingDirectory()), null)
                .contains("path not exist" + " : " + "can not delete path not in metadata backup dir"));
    }

    @Test
    public void testMetadataRestoreStatus() throws IOException {
        String backupPath = opsService.backupMetadata(UnitOfWork.GLOBAL_UNIT);
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> backupList = OpsService.getMetadataBackupList(UnitOfWork.GLOBAL_UNIT);
            for (MetadataBackupResponse response : backupList) {
                if (backupPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatus.SUCCEED) {
                    return true;
                }
            }
            return false;
        });
        String uuid = opsService.restoreMetadata(backupPath, null, false);
        await().atMost(10, TimeUnit.MINUTES).until(() -> {
            MetadataRestoreStatus status = opsService.getMetadataRestoreStatus(uuid, UnitOfWork.GLOBAL_UNIT);
            return status != null && !status.equals(MetadataRestoreStatus.IN_PROGRESS);
        });
        await().atMost(1, TimeUnit.MINUTES).until(() -> !OpsService.MetadataRestore.hasMetadataRestoreRunning());

        Assert.assertEquals(MetadataRestoreStatus.SUCCEED,
                opsService.getMetadataRestoreStatus(uuid, UnitOfWork.GLOBAL_UNIT));

        JobInfoTool jobInfoTool = Mockito.mock(JobInfoTool.class);
        Mockito.doThrow(new IOException()).when(jobInfoTool).restoreProject(anyString(), anyString(), anyBoolean());
        OpsService.MetadataRestore operator = new OpsService.MetadataRestore(backupPath, UnitOfWork.GLOBAL_UNIT, false);
        operator.setJobInfoTool(jobInfoTool);
        String uuid2 = OpsService.MetadataRestore.submitMetadataRestore(operator);

        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            MetadataRestoreStatus status = opsService.getMetadataRestoreStatus(uuid2, UnitOfWork.GLOBAL_UNIT);
            return status != null && !status.equals(MetadataRestoreStatus.IN_PROGRESS);
        });
        Assert.assertEquals(MetadataRestoreStatus.FAILED,
                opsService.getMetadataRestoreStatus(uuid2, UnitOfWork.GLOBAL_UNIT));
    }

    @Test
    public void testCancelAndDeleteMetadataBackup() throws IOException {
        String backupPath = opsService.backupMetadata(UnitOfWork.GLOBAL_UNIT);
        JobContextUtil.getJobContext(getTestConfig());
        JobInfoMapper jobInfoMapper = JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobInfoMapper();
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId("mock_job_id3");
        jobInfo.setProject(UnitOfWork.GLOBAL_UNIT);
        jobInfo.setUpdateTime(System.currentTimeMillis());
        ExecutablePO originExecutable = new ExecutablePO();
        originExecutable.setName("origin_executable");
        byte[] originJobContent = JobInfoUtil.serializeExecutablePO(originExecutable);
        jobInfo.setJobContent(originJobContent);
        jobInfoMapper.insert(jobInfo);

        Assert.assertTrue(HadoopUtil.getWorkingFileSystem().exists(new Path(backupPath)));
        opsService.cancelAndDeleteMetadataBackup(backupPath, UnitOfWork.GLOBAL_UNIT);
        Assert.assertEquals(OpsService.MetadataBackupStatus.CANCELED,
                OpsService.getMetadataBackupList(UnitOfWork.GLOBAL_UNIT).stream()
                        .filter(backup -> backup.getPath().equals(backupPath)).findFirst().get().getStatus());
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> !HadoopUtil.getWorkingFileSystem().exists(new Path(backupPath)));
    }

    @Test
    public void testBackupMetadataFailed() throws Exception {
        QueryHistoryOffsetTool queryHistoryOffsetTool = Mockito.mock(QueryHistoryOffsetTool.class);
        Mockito.doThrow(new IOException()).when(queryHistoryOffsetTool).backup(anyString(), anyString());
        OpsService.MetadataBackup metadataBackup = new OpsService.MetadataBackup(
                OpsService.getMetaBackupStoreDir(UnitOfWork.GLOBAL_UNIT), HadoopUtil.getWorkingFileSystem(),
                "test_failed", UnitOfWork.GLOBAL_UNIT);
        metadataBackup.setQueryHistoryOffsetTool(queryHistoryOffsetTool);
        String backupPath = metadataBackup.startBackup();
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = OpsService
                    .getMetadataBackupList(UnitOfWork.GLOBAL_UNIT);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (backupPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatus.FAILED) {
                    return true;
                }
            }
            return false;
        });
        Assert.assertEquals(OpsService.MetadataBackupStatus.FAILED,
                OpsService.getMetadataBackupList(UnitOfWork.GLOBAL_UNIT).get(0).getStatus());
    }

    @Test
    public void testMetadataBackupStatusDirtyRead() throws IOException {
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        String username = AclPermissionUtil.getCurrentUsername();
        // to avoid JsonUtil class initialize take too long time
        new JsonUtil();
        MockedMetadataRestore operator1 = new MockedMetadataRestore(OpsService.GLOBAL_METADATA_BACKUP_PATH, fileSystem,
                username, UnitOfWork.GLOBAL_UNIT);
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        String path1 = operator1.startBackup();

        await().atMost(5, TimeUnit.SECONDS).until(() -> OpsService.getMetadataBackupList(UnitOfWork.GLOBAL_UNIT)
                .stream().anyMatch(response -> response.getPath().equals(path1)));
        List<MetadataBackupResponse> projectMetadataBackupList1 = OpsService
                .getMetadataBackupList(UnitOfWork.GLOBAL_UNIT).stream()
                .filter(response -> response.getPath().equals(path1)).collect(Collectors.toList());
        // default retry get in progress status
        Assert.assertEquals(1, projectMetadataBackupList1.size());
        Assert.assertEquals(OpsService.MetadataBackupStatus.IN_PROGRESS, projectMetadataBackupList1.get(0).getStatus());
        opsService.cancelAndDeleteMetadataBackup(path1, UnitOfWork.GLOBAL_UNIT);

        // for retry time is zero, status write time takes too long, will get an unknown status.
        OpsService.resetStatusReadRetryTime(0);
        MockedMetadataRestore operator2 = new MockedMetadataRestore(OpsService.GLOBAL_METADATA_BACKUP_PATH, fileSystem,
                username, UnitOfWork.GLOBAL_UNIT);
        String path2 = operator2.startBackup();
        await().atMost(5, TimeUnit.SECONDS).until(() -> OpsService.getMetadataBackupList(UnitOfWork.GLOBAL_UNIT)
                .stream().anyMatch(response -> response.getPath().equals(path2)));
        List<MetadataBackupResponse> projectMetadataBackupList2 = OpsService
                .getMetadataBackupList(UnitOfWork.GLOBAL_UNIT).stream()
                .filter(response -> response.getPath().equals(path2)).collect(Collectors.toList());

        // default retry get in progress status
        Assert.assertEquals(1, projectMetadataBackupList2.size());
        Assert.assertEquals(OpsService.MetadataBackupStatus.UNKNOWN, projectMetadataBackupList2.get(0).getStatus());
    }

    @Test
    public void testMetadataRestoreFailed() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String project = "metadata_restore_failed_test";
        NProjectManager projectManager = NProjectManager.getInstance(config);
        projectManager.createProject(project, AclPermissionUtil.getCurrentUsername(), null, null);
        JobInfoMapper jobInfoMapper = JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobInfoMapper();
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId("mock_job_id2");
        jobInfo.setProject(project);
        jobInfo.setUpdateTime(System.currentTimeMillis());
        ExecutablePO originExecutable = new ExecutablePO();
        originExecutable.setName("origin_executable");
        byte[] originJobContent = JobInfoUtil.serializeExecutablePO(originExecutable);
        jobInfo.setJobContent(originJobContent);
        jobInfoMapper.insert(jobInfo);

        String outPutPath = opsService.backupMetadata(project);
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = OpsService.getMetadataBackupList(project);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (outPutPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatus.SUCCEED) {
                    return true;
                }
            }
            return false;
        });
        projectManager.dropProject(project);
        jobInfoMapper.deleteByJobId(jobInfo.getJobId());
        Assert.assertNull(projectManager.getProject(project));
        Assert.assertNull(jobInfoMapper.selectByJobId(jobInfo.getJobId()));

        opsService.restoreMetadata(outPutPath, project, false);
        await().atMost(1, TimeUnit.MINUTES).until(() -> !OpsService.MetadataRestore.hasMetadataRestoreRunning());
        projectManager.reloadAll();
        Assert.assertNotEquals(projectManager.getProject(project), null);
        Assert.assertNotEquals(jobInfoMapper.selectByJobId(jobInfo.getJobId()), null);
        Thread.sleep(1000);

        projectManager.dropProject(project);
        jobInfoMapper.deleteByJobId(jobInfo.getJobId());
        JobInfoTool jobInfoTool = Mockito.mock(JobInfoTool.class);
        Mockito.doThrow(new IOException()).when(jobInfoTool).restoreProject(anyString(), anyString(), anyBoolean());
        OpsService.MetadataRestore operator = new OpsService.MetadataRestore(outPutPath, project, false);
        operator.setJobInfoTool(jobInfoTool);
        OpsService.MetadataRestore.submitMetadataRestore(operator);
        await().atMost(1, TimeUnit.MINUTES).until(() -> !OpsService.MetadataRestore.hasMetadataRestoreRunning());
        projectManager.reloadAll();
        Assert.assertNull(jobInfoMapper.selectByJobId(jobInfo.getJobId()));
        Assert.assertNull(projectManager.getProject(project));
    }

    private static class MockedMetadataRestore extends OpsService.MetadataBackup {

        public MockedMetadataRestore(String dir, FileSystem fs, String user, String project) {
            super(dir, fs, user, project);
        }

        @Override
        public void updateStatus() throws IOException {
            try (FSDataOutputStream fos = getFs().create(getStatusPath(), true)) {
                String value = JsonUtil.writeValueAsString(getStatusInfo());
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                fos.writeUTF(value);
            }
        }
    }
}
