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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_ID_ALREADY_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_ID_EMPTY;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupEntity;
import org.apache.kylin.rest.request.resourecegroup.ResourceGroupRequest;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Order(300)
@Component
public class ResourceGroupEntityValidator implements IResourceGroupRequestValidator {
    @Override
    public void validate(ResourceGroupRequest request) {
        if (!request.isResourceGroupEnabled()) {
            return;
        }
        List<ResourceGroupEntity> entities = request.getResourceGroupEntities();
        HashMap<String, Integer> entityIds = new HashMap<>();
        for (ResourceGroupEntity entity : entities) {
            String entityId = entity.getId();
            if (StringUtils.isBlank(entityId)) {
                throw new KylinException(RESOURCE_GROUP_ID_EMPTY);
            } else {
                if (!entityIds.containsKey(entityId)) {
                    entityIds.put(entityId, 1);
                } else {
                    entityIds.put(entityId, entityIds.get(entityId) + 1);
                }
                if (entityIds.get(entityId) > 1) {
                    throw new KylinException(RESOURCE_GROUP_ID_ALREADY_EXIST, entityId);
                }
            }
        }
    }
}
