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

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
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
        Assert.assertEquals("/_global/resource_group/relation.json", resourceGroup.getResourcePath());
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
}
