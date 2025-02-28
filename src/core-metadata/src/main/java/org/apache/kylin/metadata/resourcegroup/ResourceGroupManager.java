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
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;

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
        crud = new CachedCrudAssist<ResourceGroup>(getStore(), MetadataType.RESOURCE_GROUP, null, ResourceGroup.class) {
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
        return crud.get(ResourceGroup.RESOURCE_GROUP);
    }

    public boolean resourceGroupInitialized() {
        return getResourceGroup() != null;
    }

    public boolean isProjectBindToResourceGroup(String project) {
        if (UnitOfWork.GLOBAL_UNIT.equals(project)) {
            return true;
        }
        return getResourceGroup().getResourceGroupMappingInfoList().stream()
                .anyMatch(mapping -> project.equalsIgnoreCase(mapping.getProject()));
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
            save(copyForWrite(new ResourceGroup()));
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
    
    public List<String> getInstancesForProject(String project) {
        ResourceGroup resourceGroup = getResourceGroup();
        List<String> ids = resourceGroup.getResourceGroupMappingInfoList().stream()
                .filter(mapping -> project.equalsIgnoreCase(mapping.getProject())
                        && mapping.getRequestType() == RequestTypeEnum.BUILD)
                .map(ResourceGroupMappingInfo::getResourceGroupId).distinct().collect(Collectors.toList());
        return resourceGroup.getKylinInstances().stream()
                .filter(instance -> ids.contains(instance.getResourceGroupId())).map(KylinInstance::getInstance)
                .collect(Collectors.toList());
    }

    public List<String> listProjectWithPermission() {
        if (!isResourceGroupEnabled()) {
            return NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                    .collect(Collectors.toList());
        }
        // when resource group enabled, project owner must be in the build resource group
        ResourceGroup resourceGroup = getResourceGroup();
        String server = AddressUtil.getLocalInstance();
        String currentServerResourceGroupId = resourceGroup.getKylinInstances().stream()
                .filter(instance -> instance.getInstance().equals(server)).map(KylinInstance::getResourceGroupId)
                .findFirst().orElse(null);
        return resourceGroup.getResourceGroupMappingInfoList().stream()
                .filter(mappingInfo -> mappingInfo.getRequestType() == RequestTypeEnum.BUILD
                        && mappingInfo.getResourceGroupId().equals(currentServerResourceGroupId))
                .map(ResourceGroupMappingInfo::getProject).distinct().collect(Collectors.toList());
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
