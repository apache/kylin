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

import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class UserGroupService extends BasicService implements IUserGroupService {

    @Autowired
    AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    // add param project to check user's permission
    public List<String> listAllAuthorities(String project) throws IOException {
        if (project == null) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectAdminPermission(project);
        }
        return getAllUserGroups();
    }

    public boolean exists(String name) throws IOException {
        return getAllUserGroups().contains(name);
    }

    public abstract Map<String, List<String>> getGroupMembersMap() throws IOException;

    public abstract List<ManagedUser> getGroupMembersByName(String name) throws IOException;

    protected abstract List<String> getAllUserGroups() throws IOException;

    public abstract void addGroup(String name) throws IOException;

    public abstract void deleteGroup(String name) throws IOException;

    //user's group information is stored by user its own.Object user group does not hold user's ref.
    public abstract void modifyGroupUsers(String groupName, List<String> users) throws IOException;
}
