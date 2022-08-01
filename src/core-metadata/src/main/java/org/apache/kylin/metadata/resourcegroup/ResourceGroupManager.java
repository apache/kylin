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

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceGroupManager {
    private KylinConfig config;
    private CachedCrudAssist<ResourceGroup> crud;

    public static ResourceGroupManager getInstance(KylinConfig config) {
        return config.getManager(ResourceGroupManager.class);
    }

    // called by reflection
    static ResourceGroupManager newInstance(KylinConfig config) {
        return new ResourceGroupManager(config);
    }

    private ResourceGroupManager(KylinConfig cfg) {
        this.config = cfg;
        crud = new CachedCrudAssist<ResourceGroup>(getStore(), ResourceStore.RESOURCE_GROUP, ResourceGroup.class) {
            @Override
            protected ResourceGroup initEntityAfterReload(ResourceGroup entity, String resourceName) {
                return entity;
            }
        };
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    /**
     * @return There is only one resource group metadata.
     */
    public ResourceGroup getResourceGroup() {
        List<ResourceGroup> resourceGroup = crud.listAll();
        if (CollectionUtils.isEmpty(resourceGroup)) {
            return null;
        }
        return resourceGroup.get(0);
    }

    public boolean resourceGroupInitialized() {
        return CollectionUtils.isNotEmpty(crud.listAll());
    }

    public boolean isProjectBindToResourceGroup(String project) {
        if (UnitOfWork.GLOBAL_UNIT.equals(project)) {
            return true;
        }
        return getResourceGroup().getResourceGroupMappingInfoList().stream()
                .anyMatch(mapping -> project.equals(mapping.getProject()));
    }

    public boolean isResourceGroupEnabled() {
        // resource group metadata not exist
        if (!resourceGroupInitialized()) {
            return false;
        }
        return getResourceGroup().isResourceGroupEnabled();
    }

    public void initResourceGroup() {
        if (!resourceGroupInitialized()) {
            save(new ResourceGroup());
        }
    }

    public interface ResourceGroupUpdater {
        void modify(ResourceGroup copyForWrite);
    }

    public ResourceGroup updateResourceGroup(ResourceGroupUpdater updater) {
        ResourceGroup cached = getResourceGroup();
        ResourceGroup copy = copyForWrite(cached);
        updater.modify(copy);
        return updateResourceGroup(copy);
    }

    public boolean instanceHasPermissionToOwnEpochTarget(String epochTarget, String server) {
        if (UnitOfWork.GLOBAL_UNIT.equals(epochTarget) || !isResourceGroupEnabled()) {
            return true;
        }
        // when resource group enabled, project owner must be in the build resource group
        ResourceGroup resourceGroup = getResourceGroup();
        String epochServerResourceGroupId = resourceGroup.getKylinInstances().stream()
                .filter(instance -> instance.getInstance().equals(server)).map(KylinInstance::getResourceGroupId)
                .findFirst().orElse(null);
        return resourceGroup.getResourceGroupMappingInfoList().stream()
                .filter(mappingInfo -> mappingInfo.getProject().equals(epochTarget))
                .filter(mappingInfo -> mappingInfo.getRequestType() == RequestTypeEnum.BUILD)
                .anyMatch(mappingInfo -> mappingInfo.getResourceGroupId().equals(epochServerResourceGroupId));
    }

    private ResourceGroup copyForWrite(ResourceGroup resourceGroup) {
        Preconditions.checkNotNull(resourceGroup);
        return crud.copyForWrite(resourceGroup);
    }

    private ResourceGroup updateResourceGroup(ResourceGroup resourceGroup) {
        if (!crud.contains(ResourceGroup.RESOURCE_GROUP)) {
            throw new IllegalArgumentException("Resource Group metadata does not exist!");
        }
        return save(resourceGroup);
    }

    private ResourceGroup save(ResourceGroup resourceGroup) {
        crud.save(resourceGroup);
        return resourceGroup;
    }
}
