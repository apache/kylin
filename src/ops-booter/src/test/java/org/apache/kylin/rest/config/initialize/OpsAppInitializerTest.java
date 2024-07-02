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

package org.apache.kylin.rest.config.initialize;

import static org.apache.kylin.metadata.asynctask.MetadataRestoreTask.MetadataRestoreStatus.IN_PROGRESS;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.asynctask.MetadataRestoreTask;
import org.apache.kylin.rest.MockClusterManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.reponse.MetadataBackupResponse;
import org.apache.kylin.rest.service.OpsService;
import org.apache.kylin.tool.JobInfoTool;
import org.apache.kylin.tool.MetadataTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ClusterManager.class, JobInfoTool.class, MetadataTool.class })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.profiler.AsyncProfiler" })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OpsAppInitializerTest extends NLocalFileMetadataTestCase {
    OpsService opsService;

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
    public void testMetadataBackupInterruptedA() throws IOException {
        String path = opsService.backupMetadata(null);
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        OpsService.MetadataBackup.getRunningTask()
                .get(OpsService.MetadataBackup.getProjectMetadataKeyFromRootPath(path)).getFirst().cancel(true);
        OpsAppInitializer opsAppInitializer = new OpsAppInitializer();
        opsAppInitializer.checkMetadataBackupTaskStatus();
        MetadataBackupResponse pathResponse = OpsService.getMetadataBackupList(null).stream()
                .filter(x -> x.getPath().equals(path)).collect(Collectors.toList()).get(0);
        Assert.assertEquals(OpsService.MetadataBackupStatus.FAILED, pathResponse.getStatus());
    }

    @Test
    public void testMetadataRestoreInterruptedB() throws IOException {
        String path = opsService.backupMetadata(null);
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        await().atMost(2, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = OpsService.getMetadataBackupList(null);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (path.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatus.SUCCEED) {
                    return true;
                }
            }
            return false;
        });
        String restoreTask = opsService.restoreMetadata(path, null, false);
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> IN_PROGRESS == opsService.getMetadataRestoreStatus(restoreTask, UnitOfWork.GLOBAL_UNIT));
        OpsService.MetadataRestore.getRunningTask().cancel(true);
        Assert.assertEquals(IN_PROGRESS, opsService.getMetadataRestoreStatus(restoreTask, UnitOfWork.GLOBAL_UNIT));
        OpsAppInitializer opsAppInitializer = new OpsAppInitializer();
        opsAppInitializer.beforeStarted();
        Assert.assertEquals(MetadataRestoreTask.MetadataRestoreStatus.FAILED,
                opsService.getMetadataRestoreStatus(restoreTask, UnitOfWork.GLOBAL_UNIT));
    }

}
