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

import java.util.List;

import org.apache.kylin.metadata.user.ManagedUser;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import lombok.Getter;

@Getter
public class UserInfoResponse {
    @JsonProperty("username")
    private String username;
    @JsonProperty("authorities")
    @JsonSerialize(using = ManagedUser.SimpleGrantedAuthoritySerializer.class)
    @JsonDeserialize(using = ManagedUser.SimpleGrantedAuthorityDeserializer.class)
    private List<SimpleGrantedAuthority> authorities;
    @JsonProperty("disabled")
    private boolean disabled;
    @JsonProperty("default_password")
    private boolean defaultPassword;
    @JsonProperty("locked")
    private boolean locked;
    @JsonProperty("uuid")
    protected String uuid;
    @JsonProperty("last_modified")
    protected long lastModified;
    @JsonProperty("create_time")
    protected long createTime;
    @JsonProperty("locked_time")
    private long lockedTime;
    @JsonProperty("wrong_time")
    private int wrongTime;
    @JsonProperty("first_login_failed_time")
    private long firstLoginFailedTime;

    public UserInfoResponse(ManagedUser managedUser) {
        this.username = managedUser.getUsername();
        this.authorities = managedUser.getAuthorities();
        this.disabled = managedUser.isDisabled();
        this.defaultPassword = managedUser.isDefaultPassword();
        this.locked = managedUser.isLocked();
        this.uuid = managedUser.getUuid();
        this.lastModified = managedUser.getLastModified();
        this.createTime = managedUser.getCreateTime();
        this.lockedTime = managedUser.getLockedTime();
        this.wrongTime = managedUser.getWrongTime();
        this.firstLoginFailedTime = managedUser.getFirstLoginFailedTime();
    }
}
