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

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.annotation.ThirdPartyDependencies;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.usergroup.UserGroup;

@ThirdPartyDependencies({
        @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager",
                classes = {"StaticUserGroupService", "StaticUserService"})
})
public abstract class OpenUserGroupService extends NUserGroupService {

    public abstract List<ManagedUser> getGroupMembersByName(String name);

    public abstract List<String> getAllUserGroups();

    @Override
    public List<UserGroup> listUserGroups() {
        return getUserGroupSpecialUuid();
    }

    @Override
    public void addGroup(String name) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getGroupEditNotAllowedForCustom(), "addGroup"));
    }

    @Override
    public void deleteGroup(String name) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getGroupEditNotAllowedForCustom(), "deleteGroup"));
    }

    @Override
    public void modifyGroupUsers(String groupName, List<String> users) {
        throw new UnsupportedOperationException(String.format(Locale.ROOT,
                MsgPicker.getMsg().getGroupEditNotAllowedForCustom(), "modifyGroupUsers"));
    }

    @Override
    public String getGroupNameByUuid(String uuid) {
        return uuid;
    }

    @Override
    public String getUuidByGroupName(String groupName) {
        return groupName;
    }

    @Override
    public List<UserGroup> getUserGroupsFilterByGroupName(String userGroupName) {
        aclEvaluate.checkIsGlobalAdmin();
        return StringUtils.isEmpty(userGroupName) ? getUserGroupSpecialUuid()
                : getUserGroupSpecialUuid().stream().filter(userGroup -> userGroup.getGroupName()
                        .toUpperCase(Locale.ROOT).contains(userGroupName.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toList());
    }

}
