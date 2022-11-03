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

package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

public class AccessEntryResponse {

    private Permission permission;
    private Serializable id;
    private Sid sid;
    private boolean granting;
    @Getter
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("ext_permissions")
    private List<Permission> extPermissions;

    public AccessEntryResponse() {
    }

    public AccessEntryResponse(Serializable id, Sid sid, Permission permission, boolean granting) {
        Assert.notNull(sid, "Sid required");
        Assert.notNull(permission, "Permission required");
        this.id = id;
        this.sid = sid;
        this.permission = AclPermissionUtil.convertToBasePermission(permission);
        this.extPermissions = AclPermissionUtil.convertToCompositePermission(permission).getExtPermissions();
        this.granting = granting;
    }

    public Permission getPermission() {
        return permission;
    }

    public void setPermission(Permission permission) {
        this.permission = AclPermissionUtil.convertToBasePermission(permission);
        this.extPermissions = AclPermissionUtil.convertToCompositePermission(permission).getExtPermissions();
    }

    public Serializable getId() {
        return id;
    }

    public Sid getSid() {
        return sid;
    }

    public boolean isGranting() {
        return granting;
    }

}
