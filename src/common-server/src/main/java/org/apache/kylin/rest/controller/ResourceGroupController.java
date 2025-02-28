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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.EpochCheckBroadcastNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.metadata.resourcegroup.ResourceGroup;
import org.apache.kylin.rest.handler.resourcegroup.IResourceGroupRequestValidator;
import org.apache.kylin.rest.request.resourecegroup.ResourceGroupRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.ResourceGroupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/resource_groups", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class ResourceGroupController extends NBasicController {
    @Autowired
    private ResourceGroupService resourceGroupService;

    @Autowired
    private List<IResourceGroupRequestValidator> requestValidatorList;

    @ApiOperation(value = "resourceGroup", tags = { "SM" })
    @PutMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> updateResourceGroup(@RequestBody ResourceGroupRequest request) {
        checkResourceGroupRequest(request);

        if (!resourceGroupChanged(request)) {
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        }

        resourceGroupService.updateResourceGroup(request);
        EventBusFactory.getInstance().postAsync(new EpochCheckBroadcastNotifier());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private void checkResourceGroupRequest(ResourceGroupRequest request) {
        requestValidatorList.forEach(validator -> validator.validate(request));
    }

    private boolean resourceGroupChanged(ResourceGroupRequest request) {
        ResourceGroup originResourceGroup = resourceGroupService.getResourceGroup();
        if (!request.isResourceGroupEnabled() && !originResourceGroup.isResourceGroupEnabled()) {
            return false;
        }
        if (request.isResourceGroupEnabled() != originResourceGroup.isResourceGroupEnabled()) {
            return true;
        }
        if (checkListChanged(request.getResourceGroupEntities(), originResourceGroup.getResourceGroupEntities())) {
            return true;
        }
        if (checkListChanged(request.getKylinInstances(), originResourceGroup.getKylinInstances())) {
            return true;
        }
        return checkListChanged(request.getResourceGroupMappingInfoList(),
                originResourceGroup.getResourceGroupMappingInfoList());
    }

    private boolean checkListChanged(Collection<?> list1, Collection<?> list2) {
        return !CollectionUtils.isEqualCollection(list1, list2);
    }
}
