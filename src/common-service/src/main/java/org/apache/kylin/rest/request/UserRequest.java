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

package org.apache.kylin.rest.request;

import static org.apache.kylin.metadata.user.ManagedUser.DEFAULT_GROUP;
import static org.apache.kylin.metadata.user.ManagedUser.DISABLED_ROLE;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.insensitive.UserInsensitiveRequest;
import org.apache.kylin.metadata.user.ManagedUser;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import lombok.Data;

@SuppressWarnings("serial")
@Data
public class UserRequest implements UserInsensitiveRequest {

    @JsonProperty
    private String username;
    @JsonProperty
    private String password;
    @JsonProperty
    private List<String> authorities = Lists.newArrayList();
    @JsonProperty
    private Boolean disabled;
    @JsonProperty
    private Boolean defaultPassword;

    public UserRequest() {
    }

    public ManagedUser updateManager(ManagedUser managedUser) {
        if (disabled != null) {
            managedUser.setDisabled(disabled);
        }
        if (defaultPassword != null) {
            managedUser.setDefaultPassword(defaultPassword);
        }
        if (!StringUtils.isEmpty(password))
            managedUser.setPassword(password);
        if (authorities != null && !authorities.isEmpty()) {
            if (authorities.stream().anyMatch(authority -> DISABLED_ROLE.equals(authority))) {
                managedUser.setDisabled(true);
                authorities.remove(DISABLED_ROLE);
            }
            List<SimpleGrantedAuthority> authorities = this.authorities.stream().map(SimpleGrantedAuthority::new)
                    .collect(Collectors.toList());
            if (!authorities.contains(DEFAULT_GROUP)) {
                authorities.add(DEFAULT_GROUP);
            }
            managedUser.setAuthorities(authorities);
        }

        return managedUser;
    }

}
