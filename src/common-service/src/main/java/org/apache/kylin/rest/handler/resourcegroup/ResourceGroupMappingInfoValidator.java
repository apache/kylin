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

package org.apache.kylin.rest.handler.resourcegroup;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.resourcegroup.RequestTypeEnum;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupEntity;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupMappingInfo;
import org.apache.kylin.rest.request.resourecegroup.ResourceGroupRequest;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Order(500)
@Component
public class ResourceGroupMappingInfoValidator implements IResourceGroupRequestValidator {
    @Override
    public void validate(ResourceGroupRequest request) {
        if (!request.isResourceGroupEnabled()) {
            return;
        }
        // check project exist and not empty
        List<String> resourceGroups = request.getResourceGroupEntities().stream().map(ResourceGroupEntity::getId)
                .collect(Collectors.toList());
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<ResourceGroupMappingInfo> mappingInfo = request.getResourceGroupMappingInfoList();
        for (ResourceGroupMappingInfo info : mappingInfo) {
            if (StringUtils.isBlank(info.getProject())) {
                throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getEmptyProjectInMappingInfo());
            }
            ProjectInstance prjInstance = projectManager.getProject(info.getProject());
            if (prjInstance == null) {
                throw new KylinException(PROJECT_NOT_EXIST, info.getProject());
            }
            if (StringUtils.isBlank(info.getResourceGroupId())) {
                throw new KylinException(INVALID_PARAMETER,
                        MsgPicker.getMsg().getEmptyResourceGroupIdInMappingInfo());
            }
            if (!resourceGroups.contains(info.getResourceGroupId())) {
                throw new KylinException(INVALID_PARAMETER,
                        MsgPicker.getMsg().getResourceGroupIdNotExistInMappingInfo(info.getResourceGroupId()));
            }
        }

        // check the relationship between project and resource group
        Map<String, List<ResourceGroupMappingInfo>> projectMappingInfo = mappingInfo.stream()
                .collect(Collectors.groupingBy(ResourceGroupMappingInfo::getProject));
        for (Map.Entry<String, List<ResourceGroupMappingInfo>> entry : projectMappingInfo.entrySet()) {
            String project = entry.getKey();
            List<ResourceGroupMappingInfo> projectMapping = entry.getValue();

            boolean bindInvalidTotalNum = projectMapping.size() > 2;
            boolean bindInvalidNumInOneType = projectMapping.stream()
                    .filter(info -> info.getRequestType() == RequestTypeEnum.BUILD).count() > 1
                    || projectMapping.stream().filter(info -> info.getRequestType() == RequestTypeEnum.QUERY)
                    .count() > 1;

            if (bindInvalidTotalNum || bindInvalidNumInOneType) {
                throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getProjectBindingResourceGroupInvalid(), project));
            }
        }
    }
}
