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

package org.apache.kylin.rest.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.springframework.security.acls.model.Permission;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class UserAcl extends RootPersistentEntity {

    @JsonProperty
    private String username;
    @JsonProperty("permissions")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Set<Integer> permissionMasks = Collections.emptySet();
    @JsonProperty("data_query_projects")
    private List<String> dataQueryProjects;

    public UserAcl() {
    }

    public UserAcl(String username) {
        this(username, Collections.emptySet());
    }

    public UserAcl(String username, Set<Permission> permissions) {
        this.username = username;
        if (!CollectionUtils.isEmpty(permissions)) {
            this.permissionMasks = permissions.stream().map(Permission::getMask).collect(Collectors.toSet());
        }
    }

    public void addPermission(Integer permissionMask) {
        if (CollectionUtils.isEmpty(permissionMasks)) {
            permissionMasks = new HashSet<>();
        }
        permissionMasks.add(permissionMask);
    }

    public void addPermission(Permission permission) {
        addPermission(permission.getMask());
    }

    public void addDataQueryProject(String project) {
        if (StringUtils.isEmpty(project)) {
            return;
        }
        if (CollectionUtils.isEmpty(dataQueryProjects)) {
            dataQueryProjects = new ArrayList<>();
        }
        if (!dataQueryProjects.contains(project)) {
            dataQueryProjects.add(project);
        }
    }

    public void deletePermission(Integer permissionMask) {
        if (CollectionUtils.isNotEmpty(permissionMasks)) {
            permissionMasks.remove(permissionMask);
        }
    }

    public void deleteDataQueryProject(String project) {
        if (StringUtils.isEmpty(project) || CollectionUtils.isEmpty(dataQueryProjects)) {
            return;
        }
        dataQueryProjects.remove(project);
    }

    public void deletePermission(Permission permission) {
        deletePermission(permission.getMask());
    }

    public Set<Integer> getPermissionMasks() {
        return permissionMasks;
    }

    public boolean hasPermission(Permission permission) {
        return hasPermission(permission.getMask());
    }

    public boolean hasPermission(Integer permissionMask) {
        return CollectionUtils.isNotEmpty(permissionMasks) && permissionMasks.contains(permissionMask);
    }

    @Override
    public String resourceName() {
        return username;
    }

    @Override
    public MetadataType resourceType() {
        return MetadataType.USER_GLOBAL_ACL;
    }

    @Override
    public int hashCode() {
        return 31 + (Objects.isNull(username) ? 0 : username.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        UserAcl other = (UserAcl) obj;
        return Objects.equals(username, other.username);
    }

}
