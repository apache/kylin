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

package org.apache.kylin.metadata.epoch;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.awaitility.Awaitility.await;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@MetadataInfo(onlyProps = true)
class EpochManagerTest {

    @Test
    void testUpdateGlobalEpoch() {
        EpochManager epochManager = EpochManager.getInstance();
        Assertions.assertNull(epochManager.getGlobalEpoch());
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        val globalEpoch = epochManager.getGlobalEpoch();
        long time1 = globalEpoch.getLastEpochRenewTime();
        Assertions.assertNotNull(globalEpoch);
        await().atLeast(10, TimeUnit.MILLISECONDS).until(() -> epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false));
        Assertions.assertNotEquals(time1, epochManager.getGlobalEpoch().getLastEpochRenewTime());
    }

    @Test
    @OverwriteProp(key = "kylin.server.leader-race.enabled", value = "false")
    void testKeepGlobalEpoch() {
        EpochManager epochManager = EpochManager.getInstance();
        Assertions.assertNull(epochManager.getGlobalEpoch());
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        val globalEpoch = epochManager.getGlobalEpoch();
        long time1 = globalEpoch.getLastEpochRenewTime();
        Assertions.assertNotNull(globalEpoch);
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        Assertions.assertEquals(time1, epochManager.getGlobalEpoch().getLastEpochRenewTime());
    }

    @Test
    @OverwriteProp(key = "kylin.server.leader-race.enabled", value = "false")
    void testKeepProjectEpochWhenOwnerChanged() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        val prjMgr = NProjectManager.getInstance(config);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertNull(epochManager.getEpoch(prj.getName()));
        }
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertEquals(epochManager.getEpoch(prj.getName()).getCurrentEpochOwner(),
                    EpochOrchestrator.getOwnerIdentity());
            Assertions.assertEquals(Long.MAX_VALUE, epochManager.getEpoch(prj.getName()).getLastEpochRenewTime());

        }
        epochManager.setIdentity("newOwner");
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertEquals("newOwner", epochManager.getEpoch(prj.getName()).getCurrentEpochOwner());
            Assertions.assertEquals(Long.MAX_VALUE, epochManager.getEpoch(prj.getName()).getLastEpochRenewTime());
            Assertions.assertEquals(2, epochManager.getEpoch(prj.getName()).getMvcc());
        }
    }

    @Test
    void testUpdateProjectEpoch() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        val prjMgr = NProjectManager.getInstance(config);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertNull(epochManager.getEpoch(prj.getName()));
        }
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertNotNull(epochManager.getEpoch(prj.getName()));
        }
    }

    @Test
    @OverwriteProp(key = "kylin.server.leader-race.heart-beat-timeout", value = "1")
    void testEpochExpired() {
        long timeout = 1;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance();
        val prjMgr = NProjectManager.getInstance(config);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertNull(epochManager.getEpoch(prj.getName()));
        }
        epochManager.updateAllEpochs();
        await().atLeast(timeout * 2, TimeUnit.SECONDS);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertFalse(epochManager.checkEpochOwner(prj.getName()));
        }
    }

    @Test
    void testUpdateEpochAtOneTime() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        val prjMgr = NProjectManager.getInstance(config);
        val epochMgr = EpochManager.getInstance();
        val epochMgrCopy = EpochManager.getInstance();
        val cdl = new CountDownLatch(2);
        new Thread(() -> {
            try {
                epochMgr.updateAllEpochs();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                cdl.countDown();
            }
        }).start();
        new Thread(() -> {
            try {
                epochMgrCopy.updateAllEpochs();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                cdl.countDown();
            }
        }).start();
        cdl.await(10, TimeUnit.SECONDS);
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertTrue(epochMgr.checkEpochOwner(prj.getName()));
        }
    }

    @Test
    void testSetAndUnSetMaintenanceMode_Single() {
        EpochManager epochManager = EpochManager.getInstance();
        Assertions.assertNull(epochManager.getGlobalEpoch());
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);
        Assertions.assertFalse(epochManager.isMaintenanceMode());
        epochManager.setMaintenanceMode("MODE1");
        Assertions.assertTrue(epochManager.isMaintenanceMode());
        epochManager.unsetMaintenanceMode("MODE1");
        Assertions.assertFalse(epochManager.isMaintenanceMode());
    }

    @Test
    void testSetAndUnSetMaintenanceMode_Batch() {

        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());

        Epoch e2 = new Epoch();
        e2.setEpochTarget("test2");
        e2.setCurrentEpochOwner("owner2");
        e2.setEpochId(1);
        e2.setLastEpochRenewTime(System.currentTimeMillis());

        getEpochStore().insertBatch(Arrays.asList(e1, e2));

        EpochManager epochManager = EpochManager.getInstance();
        epochManager.tryUpdateEpoch(EpochManager.GLOBAL, false);

        epochManager.setMaintenanceMode("mode1");
        Assertions.assertTrue(epochManager.isMaintenanceMode());
    }

    @Test
    void testReleaseOwnedEpochs() {

        String testIdentity = "testIdentity";

        EpochManager epochManager = EpochManager.getInstance();

        epochManager.setIdentity(testIdentity);
        epochManager.tryUpdateEpoch("test1", false);
        epochManager.tryUpdateEpoch("test2", false);

        //check owner
        Assertions.assertTrue(getEpochStore().list().stream().allMatch(epochManager::checkEpochOwnerOnly));

        epochManager.releaseOwnedEpochs();

    }

    @Test
    void testForceUpdateEpoch() {
        EpochManager epochManager = EpochManager.getInstance();
        Assertions.assertNull(epochManager.getGlobalEpoch());
        epochManager.updateEpochWithNotifier(EpochManager.GLOBAL, true);
        Assertions.assertNotNull(epochManager.getGlobalEpoch());
    }

    @Test
    @MetadataInfo(onlyProps = false)
    void testUpdateProjectEpochWithResourceGroupEnabled() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.getResourceGroup();
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(true);
        });
        EpochManager epochManager = EpochManager.getInstance();
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertNull(epochManager.getEpoch(prj.getName()));
        }
        epochManager.updateAllEpochs();
        for (ProjectInstance prj : prjMgr.listAllProjects()) {
            Assertions.assertNull(epochManager.getEpoch(prj.getName()));
        }
    }

    @Test
    void testGetEpochOwnerWithException() {
        EpochManager epochManager = EpochManager.getInstance();
        Assertions.assertThrows(IllegalStateException.class, () -> {
            epochManager.getEpochOwner(null);
        });
    }

    @Test
    void testGetEpochOwnerWithEpochIsNull() {
        EpochManager epochManager = EpochManager.getInstance();
        String epoch = epochManager.getEpochOwner("notexist");
        Assertions.assertNull(epoch);
    }

    @Test
    void testUpdateEpoch() {
        EpochManager epochManager = EpochManager.getInstance();
        Assertions.assertNull(epochManager.getGlobalEpoch());
        epochManager.updateEpochWithNotifier("_global", false);
        Assertions.assertNotNull(epochManager.getGlobalEpoch());
    }

    @Test
    void testTryForceInsertOrUpdateEpochBatchTransaction() {
        List<String> projects = Lists.newArrayList("test_add");
        EpochManager epochManager = EpochManager.getInstance();

        Assertions.assertTrue(getEpochStore().list().isEmpty());
        boolean result = epochManager.tryForceInsertOrUpdateEpochBatchTransaction(projects, false, "test", false);
        Assertions.assertTrue(result);
        Assertions.assertFalse(getEpochStore().list().isEmpty());

        Epoch e1 = new Epoch();
        e1.setEpochTarget("test1");
        e1.setCurrentEpochOwner("owner1");
        e1.setEpochId(1);
        e1.setLastEpochRenewTime(System.currentTimeMillis());
        getEpochStore().insertBatch(Lists.newArrayList(e1));

        result = epochManager.tryForceInsertOrUpdateEpochBatchTransaction(projects, false, "test", false);
        Assertions.assertTrue(result);

        result = epochManager.tryForceInsertOrUpdateEpochBatchTransaction(Lists.newArrayList(), false, "test", false);
        Assertions.assertFalse(result);
    }

    @Test
    void testCheckEpochOwnerInsensitive() {
        String testIdentity = "testIdentity";

        EpochManager epochManager = EpochManager.getInstance();

        epochManager.setIdentity(testIdentity);

        List<String> projectLists = Arrays.asList("test1", "test2");

        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        projectLists.forEach(projectTemp -> {
            projectManager.createProject(projectTemp, "abcd", "", null);
            epochManager.tryUpdateEpoch(projectTemp, false);
        });

        Assertions.assertEquals("testIdentity", epochManager.getEpochOwner("TesT1"));
        Assertions.assertEquals("testIdentity", epochManager.getEpochOwner("TEST2"));

        Assertions.assertTrue(epochManager.checkEpochOwner("TesT1"));
        Assertions.assertTrue(epochManager.checkEpochOwner("TEST2"));
    }

    @Test
    void testListProjectWithPermission() {
        String testIdentity = "testIdentity";

        EpochManager epochManager = EpochManager.getInstance();

        epochManager.setIdentity(testIdentity);

        List<String> projectLists = Arrays.asList("test1", "test2");

        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        projectLists.forEach(projectTemp -> projectManager.createProject(projectTemp, "abcd", "", null));
        //only update one target
        epochManager.tryUpdateEpoch(projectLists.get(0), false);

        List<String> projectListWithPermission = ReflectionTestUtils.invokeMethod(epochManager,
                "listProjectWithPermission");

        //project + global = epoch with permission
        assert projectListWithPermission != null;
        Assertions.assertEquals(projectManager.listAllProjects().size(), projectListWithPermission.size() - 1);

    }

    @Test
    void testBatchRenewWithRetry() {
        String testIdentity = "testIdentity";
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.setIdentity(testIdentity);

        List<String> projectLists = Arrays.asList("test1", "test2");
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        projectLists.forEach(projectTemp -> {
            projectManager.createProject(projectTemp, "abcd", "", null);
            epochManager.tryUpdateEpoch(projectTemp, false);
        });

        Assertions.assertEquals("testIdentity", epochManager.getEpochOwner("TesT1"));
        Assertions.assertEquals("testIdentity", epochManager.getEpochOwner("TEST2"));

        long curTime = System.currentTimeMillis();
        List<Epoch> epochList = getEpochStore().list();
        Set<String> successRenew = ReflectionTestUtils.invokeMethod(epochManager.getEpochUpdateManager(),
                "innerRenewEpochWithRetry", new HashSet<>(epochList));
        assert successRenew != null;
        Assertions.assertEquals(epochList.size(), successRenew.size());
        Assertions.assertTrue(
                getEpochStore().list().stream().allMatch(epoch -> epoch.getLastEpochRenewTime() >= curTime));
    }

    @Test
    void testInnerRenewEpoch() {
        String testIdentity = "testIdentity";
        EpochManager epochManager = EpochManager.getInstance();
        epochManager.setIdentity(testIdentity);

        List<String> projectLists = Arrays.asList("test1", "test2");
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        projectLists.forEach(projectTemp -> {
            projectManager.createProject(projectTemp, "abcd", "", null);
            epochManager.tryUpdateEpoch(projectTemp, false);
        });

        Assertions.assertEquals("testIdentity", epochManager.getEpochOwner("TesT1"));
        Assertions.assertEquals("testIdentity", epochManager.getEpochOwner("TEST2"));

        val curTime = System.currentTimeMillis();
        val epoches = getEpochStore().list();
        Set<String> successRenew = ReflectionTestUtils.invokeMethod(epochManager.getEpochUpdateManager(),
                "innerRenewEpoch", epoches);
        assert successRenew != null;
        Assertions.assertEquals(epoches.size(), successRenew.size());
        Assertions.assertTrue(
                getEpochStore().list().stream().allMatch(epoch -> epoch.getLastEpochRenewTime() >= curTime));

    }

    EpochStore getEpochStore() {
        try {
            return EpochStore.getEpochStore(getTestConfig());
        } catch (Exception e) {
            throw new RuntimeException("cannnot init epoch store!");
        }
    }

}
