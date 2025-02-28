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

package org.apache.kylin.metadata.resourcegroup;

import static org.apache.kylin.metadata.resourcegroup.RequestTypeEnum.BUILD;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class ResourceGroupManagerTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetResourceGroup() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        ResourceGroup resourceGroup = manager.getResourceGroup();
        Assert.assertFalse(resourceGroup.isResourceGroupEnabled());
        Assert.assertEquals("RESOURCE_GROUP/relation", resourceGroup.getResourcePath());
        Assert.assertTrue(resourceGroup.getResourceGroupEntities().isEmpty());
        Assert.assertTrue(resourceGroup.getResourceGroupMappingInfoList().isEmpty());
        Assert.assertTrue(resourceGroup.getKylinInstances().isEmpty());
    }

    @Test
    public void testUpdateResourceGrop() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(true);
        });
        Assert.assertTrue(manager.getResourceGroup().isResourceGroupEnabled());
    }

    @Test
    public void testResourceGroupInitialized() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        Assert.assertTrue(manager.resourceGroupInitialized());
    }

    @Test
    public void testIsResourceGroupEnabled() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        Assert.assertFalse(manager.isResourceGroupEnabled());
    }

    @Test
    public void testIsProjectBindToResourceGroup() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        Assert.assertTrue(manager.isProjectBindToResourceGroup("_global"));
        Assert.assertFalse(manager.isProjectBindToResourceGroup("default"));
    }

    @Test
    public void testInitResourceGroup() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.initResourceGroup();
    }

    @Test
    public void testGetInstancesForProject() {
        String host = "127.0.0.2:7070";
        String project = "test_project";
        mockResourceGroup(host, project);

        ResourceGroupManager manager = ResourceGroupManager.getInstance(getTestConfig());
        List<String> instances = manager.getInstancesForProject(project);
        Assert.assertEquals(1, instances.size());
        Assert.assertEquals(host, instances.get(0));
    }

    @Test
    public void testListProjectWithPermission() {
        KylinConfig conf = getTestConfig();
        NProjectManager prjManager = NProjectManager.getInstance(conf);
        ResourceGroupManager rgManager = ResourceGroupManager.getInstance(conf);
        Assert.assertEquals(prjManager.listAllProjects().size(), rgManager.listProjectWithPermission().size());

        String project = "default";
        mockResourceGroup(AddressUtil.getLocalInstance(), project);
        Assert.assertEquals(1, rgManager.listProjectWithPermission().size());
        Assert.assertEquals(project, rgManager.listProjectWithPermission().get(0));
    }

    private void mockResourceGroup(String host, String project) {
        ResourceGroupManager manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(true);
            String rgId = RandomUtil.randomUUIDStr();
            byte[] instanceBytes = String
                    .format(Locale.ROOT, "{\"instance\":\"%s\", \"resource_group_id\":\"%s\"}", host, rgId).getBytes();
            KylinInstance instance = JsonUtil.readValueQuietly(instanceBytes, KylinInstance.class);
            copyForWrite.setKylinInstances(Collections.singletonList(instance));

            byte[] infoString = String.format(Locale.ROOT,
                    "{\"project\":\"%s\", \"resource_group_id\":\"%s\", \"request_type\":\"%s\"}", project, rgId, BUILD)
                    .getBytes();
            ResourceGroupMappingInfo mappingInfo = JsonUtil.readValueQuietly(infoString,
                    ResourceGroupMappingInfo.class);
            copyForWrite.setResourceGroupMappingInfoList(Collections.singletonList(mappingInfo));
        });
    }
}
