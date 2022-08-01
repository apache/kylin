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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.metadata.user.ManagedUser;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class StaticUserGroupService extends OpenUserGroupService {
    @Override
    public Map<String, List<String>> getUserAndUserGroup() throws IOException {
        Map<String, List<String>> result = Maps.newHashMap();
        List<ManagedUser> users = userService.listUsers();
        for (ManagedUser user : users) {
            for (SimpleGrantedAuthority authority : user.getAuthorities()) {
                String role = authority.getAuthority();
                List<String> usersInGroup = result.get(role);
                if (usersInGroup == null) {
                    result.put(role, Lists.newArrayList(user.getUsername()));
                } else {
                    usersInGroup.add(user.getUsername());
                }
            }
        }
        return result;
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name) {
        try {
            List<ManagedUser> ret = Lists.newArrayList();
            List<ManagedUser> managedUsers = userService.listUsers();
            for (ManagedUser user : managedUsers) {
                if (user.getAuthorities().contains(new SimpleGrantedAuthority(name))) {
                    ret.add(user);
                }
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException("");
        }
    }

    @Override
    public List<String> getAllUserGroups() {
        List<String> groups = Lists.newArrayList();
        groups.add(Constant.ROLE_ADMIN);
        groups.add(Constant.ROLE_ANALYST);
        return groups;
    }
}
