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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_ENABLE_FAILED;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.request.resourecegroup.ResourceGroupRequest;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Order(200)
@Component
public class ResourceGroupEnabledValidator implements IResourceGroupRequestValidator {
    @Override
    public void validate(ResourceGroupRequest request) {
        if (!request.isResourceGroupEnabled()) {
            return;
        }
        if (CollectionUtils.isNotEmpty(request.getResourceGroupEntities())) {
            return;
        }
        throw new KylinException(RESOURCE_GROUP_ENABLE_FAILED);
    }
}
