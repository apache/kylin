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

package org.apache.kylin.rest.service;

import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class CaseInsensitiveUserGroupService extends NUserGroupService {
    public static final Logger logger = LoggerFactory.getLogger(CaseInsensitiveUserGroupService.class);

    @Override
    public Set<String> listUserGroups(String username) {
        try {
            UserDetails user = userService.loadUserByUsername(username);
            return user.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(Collectors.toSet());
        } catch (Exception e) {
            logger.warn("Cat not load user by username {}", username, e);
        }

        return Sets.newHashSet();
    }
}
