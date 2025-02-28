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
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.ArgsTypeJsonDeserializer;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.insensitive.UserInsensitiveRequest;
import org.apache.kylin.metadata.user.ManagedUser;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

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
    @JsonDeserialize(using = ArgsTypeJsonDeserializer.BooleanJsonDeserializer.class)
    private Boolean disabled;
    @JsonProperty
    private Boolean defaultPassword;

    public UserRequest() {
    }

    public List<SimpleGrantedAuthority> transformSimpleGrantedAuthorities() {
        return this.authorities.stream().map(SimpleGrantedAuthority::new).collect(Collectors.toList());
    }

    public ManagedUser updateManager(ManagedUser managedUser) {
        if (Objects.nonNull(disabled)) {
            managedUser.setDisabled(disabled);
        }
        if (Objects.nonNull(defaultPassword)) {
            managedUser.setDefaultPassword(defaultPassword);
        }
        if (StringUtils.isNotEmpty(password)) {
            managedUser.setPassword(password);
        }
        if (StringUtils.isNotEmpty(username) && StringUtils.isEmpty(managedUser.getUsername())) {
            managedUser.setUsername(username);
        }
        if (CollectionUtils.isNotEmpty(authorities)) {
            if (authorities.stream().anyMatch(DISABLED_ROLE::equals)) {
                managedUser.setDisabled(true);
                authorities.remove(DISABLED_ROLE);
            }
            List<SimpleGrantedAuthority> simpleGrantedAuthorities = transformSimpleGrantedAuthorities();
            if (!simpleGrantedAuthorities.contains(DEFAULT_GROUP)) {
                simpleGrantedAuthorities.add(DEFAULT_GROUP);
            }
            managedUser.setAuthorities(simpleGrantedAuthorities);
        }
        return managedUser;
    }

}
