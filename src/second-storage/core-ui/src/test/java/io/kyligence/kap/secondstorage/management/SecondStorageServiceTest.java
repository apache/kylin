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

package io.kyligence.kap.secondstorage.management;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.project.NProjectManager;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import lombok.val;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ NProjectManager.class, SecondStorageUtil.class, NExecutableManager.class, UserGroupInformation.class,
        AbstractExecutable.class })
public class SecondStorageServiceTest extends NLocalFileMetadataTestCase {
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    private NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
    private SecondStorageService secondStorageService = new SecondStorageService();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenAnswer(invocation -> userGroupInformation);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        Unsafe.setProperty("kylin.external-storage.cluster.config", "src/test/resources/test.yaml");
        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        Unsafe.clearProperty("kylin.external-storage.cluster.config");
        cleanupTestMetadata();
    }

    @Test
    public void listAvailableNodes() {
        PowerMockito.mockStatic(NProjectManager.class);
        val projectManager = Mockito.mock(NProjectManager.class);
        PowerMockito.when(NProjectManager.getInstance(Mockito.any(KylinConfig.class)))
                .thenAnswer(invocation -> projectManager);
        Mockito.when(projectManager.listAllProjects()).thenReturn(Collections.emptyList());
        ClusterInfo cluster = new ClusterInfo();
        Map<String, List<Node>> clusterNodes = new HashMap<>();
        cluster.setCluster(clusterNodes);
        clusterNodes.put("pair1", Collections.singletonList(new Node().setName("node01").setIp("127.0.0.1").setPort(9000)));
        clusterNodes.put("pair2", Collections.singletonList(new Node().setName("node02").setIp("127.0.0.2").setPort(9000)));
        clusterNodes.put("pair3", Collections.singletonList(new Node().setName("node03").setIp("127.0.0.3").setPort(9000)));
        SecondStorageNodeHelper.initFromCluster(cluster, null, null);
        val result = secondStorageService.listAvailableNodes();
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(3, result.values().stream().mapToLong(List::size).sum());
    }

    private void prepareManger() {
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "getProjectLocks", String.class))
                .toReturn(new ArrayList<>());
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isGlobalEnable")).toReturn(true);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isProjectEnable", String.class)).toReturn(true);
        PowerMockito.stub(PowerMockito.method(SecondStorageUtil.class, "isModelEnable", String.class, String.class))
                .toReturn(true);
        PowerMockito.stub(PowerMockito.method(NExecutableManager.class, "getInstance", KylinConfig.class, String.class))
                .toReturn(executableManager);
    }

    @Test
    public void validateProjectDisable() {
        ProjectEnableRequest projectEnableRequest = new ProjectEnableRequest();
        projectEnableRequest.setProject("project");
        projectEnableRequest.setEnabled(false);
        prepareManger();
        List<String> jobs = Arrays.asList("job1", "job2");
        AbstractExecutable job1 = PowerMockito.mock(AbstractExecutable.class);
        AbstractExecutable job2 = PowerMockito.mock(AbstractExecutable.class);
        PowerMockito.when(job1.getStatus()).thenReturn(ExecutableState.RUNNING);
        PowerMockito.when(job2.getStatus()).thenReturn(ExecutableState.SUCCEED);
        PowerMockito.when(job1.getJobType()).thenReturn(JobTypeEnum.INDEX_BUILD);
        PowerMockito.when(job2.getJobType()).thenReturn(JobTypeEnum.EXPORT_TO_SECOND_STORAGE);

        Mockito.when(job1.getProject()).thenReturn("project");
        Mockito.when(job2.getProject()).thenReturn("project");

        Mockito.when(job1.getTargetSubject()).thenReturn("model1");
        Mockito.when(job2.getTargetSubject()).thenReturn("model2");

        Mockito.when(executableManager.getJobs()).thenReturn(jobs);
        Mockito.when(executableManager.getJob("job1")).thenReturn(job1);
        Mockito.when(executableManager.getJob("job2")).thenReturn(job2);
        Assert.assertEquals(1, secondStorageService.validateProjectDisable(projectEnableRequest.getProject()).size());
    }
}
