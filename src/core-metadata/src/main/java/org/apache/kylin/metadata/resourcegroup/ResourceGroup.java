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

import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.EqualsAndHashCode;
import lombok.Setter;

@Setter
@EqualsAndHashCode(callSuper = false)
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class ResourceGroup extends RootPersistentEntity {
    @JsonProperty("resource_group_enabled")
    private boolean resourceGroupEnabled = false;

    @JsonProperty("resource_groups")
    private List<ResourceGroupEntity> resourceGroupEntities = Lists.newArrayList();

    @JsonProperty("instances")
    private List<KylinInstance> kylinInstances = Lists.newArrayList();

    @JsonProperty("mapping_info")
    private List<ResourceGroupMappingInfo> resourceGroupMappingInfoList = Lists.newArrayList();

    public static final String RESOURCE_GROUP = "relation";

    @Override
    public String resourceName() {
        return RESOURCE_GROUP;
    }


    @Override
    public MetadataType resourceType() {
        return MetadataType.RESOURCE_GROUP;
    }

    public boolean isResourceGroupEnabled() {
        return resourceGroupEnabled;
    }

    public List<ResourceGroupEntity> getResourceGroupEntities() {
        return Collections.unmodifiableList(resourceGroupEntities);
    }

    public List<KylinInstance> getKylinInstances() {
        return Collections.unmodifiableList(kylinInstances);
    }

    public List<ResourceGroupMappingInfo> getResourceGroupMappingInfoList() {
        return Collections.unmodifiableList(resourceGroupMappingInfoList);
    }
}
