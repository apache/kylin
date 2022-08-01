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

package org.apache.kylin.rest.request.resourecegroup;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.resourcegroup.KylinInstance;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupEntity;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupMappingInfo;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class ResourceGroupRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("resource_group_enabled")
    private boolean resourceGroupEnabled;

    @JsonProperty("resource_groups")
    private List<ResourceGroupEntity> resourceGroupEntities;

    @JsonProperty("instances")
    private List<KylinInstance> kylinInstances;

    @JsonProperty("mapping_info")
    private List<ResourceGroupMappingInfo> resourceGroupMappingInfoList;
}
