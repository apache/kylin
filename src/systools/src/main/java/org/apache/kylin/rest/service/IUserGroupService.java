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

package org.apache.kylin.rest.service;

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.usergroup.UserGroup;
import org.apache.kylin.rest.response.UserGroupResponseKI;

public interface IUserGroupService {
    //need project to indicate user's permission.only global admin and project admin can get.
    List<String> listAllAuthorities() throws IOException;

    List<String> getAuthoritiesFilterByGroupName(String userGroupName) throws IOException;

    List<UserGroup> listUserGroups() throws IOException;

    List<UserGroup> getUserGroupsFilterByGroupName(String userGroupName) throws IOException;

    String getGroupNameByUuid(String uuid);

    String getUuidByGroupName(String groupName);

    boolean exists(String name) throws IOException;

    List<ManagedUser> getGroupMembersByName(String name) throws IOException;

    List<String> getAllUserGroups() throws IOException;

    Set<String> listUserGroups(String username);

    void addGroup(String name) throws IOException;

    void deleteGroup(String name) throws IOException;

    void modifyGroupUsers(String groupName, List<String> users) throws IOException;

    Map<String, List<String>> getUserAndUserGroup() throws IOException;

    List<UserGroupResponseKI> getUserGroupResponse(List<UserGroup> userGroups) throws IOException;

    void addGroups(List<String> groups);

    default boolean isAdminGroup(String group) {
        return ROLE_ADMIN.equalsIgnoreCase(group)
                || KylinConfig.getInstanceFromEnv().getLDAPAdminRole().equalsIgnoreCase(group);
    }
}
