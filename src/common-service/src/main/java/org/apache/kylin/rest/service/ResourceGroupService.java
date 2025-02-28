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

import org.apache.kylin.metadata.resourcegroup.ResourceGroup;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.resourecegroup.ResourceGroupRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import lombok.val;

@Service("resourceGroupService")
public class ResourceGroupService extends BasicService {

    @Autowired
    public AclEvaluate aclEvaluate;

    @Transaction
    public void updateResourceGroup(ResourceGroupRequest request) {
        aclEvaluate.checkIsGlobalAdmin();
        val manager = getManager(ResourceGroupManager.class);
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(request.isResourceGroupEnabled());
            copyForWrite.setResourceGroupEntities(request.getResourceGroupEntities());
            copyForWrite.setKylinInstances(request.getKylinInstances());
            copyForWrite.setResourceGroupMappingInfoList(request.getResourceGroupMappingInfoList());
        });
    }

    public ResourceGroup getResourceGroup() {
        val manager = getManager(ResourceGroupManager.class);
        return manager.getResourceGroup();
    }
}
