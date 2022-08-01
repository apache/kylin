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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_USERGROUP_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_USERGROUP_NAME;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.common.persistence.transaction.AclTCRRevokeEventNotifier;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.usergroup.UserGroup;
import org.apache.kylin.rest.request.UpdateGroupRequest;
import org.apache.kylin.rest.request.UserGroupRequest;
import org.apache.kylin.rest.response.UserGroupResponseKI;
import org.apache.kylin.rest.service.AclTCRService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/user_group", produces = { HTTP_VND_APACHE_KYLIN_JSON })
public class NUserGroupController extends NBasicController {

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @ApiOperation(value = "getUsersByGroup", tags = {
            "MID" }, notes = "Update URL: group_members, group_name; Update Param: group_name, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/group_members/{uuid:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<ManagedUser>>> getUsersByGroupName(
            @PathVariable(value = "uuid") String groupUuid, //
            @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        String groupName = userGroupService.getGroupNameByUuid(groupUuid);
        List<ManagedUser> members = userGroupService.getGroupMembersByName(groupName);

        if (StringUtils.isNotBlank(name)) {
            members = members.stream().filter(user -> StringUtils.containsIgnoreCase(user.getUsername(), name))
                    .collect(Collectors.toList());
        }

        val subList = PagingUtil.cutPage(members, pageOffset, pageSize);
        //LDAP users dose not have authorities
        for (ManagedUser user : subList) {
            userService.completeUserInfo(user);
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.get(subList, members, pageOffset, pageSize), "get groups members");
    }

    @ApiOperation(value = "groups", tags = { "MID" })
    @GetMapping(value = "/groups")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<List<String>> listUserAuthorities() throws IOException {
        List<String> groups = userGroupService.listAllAuthorities();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, groups, "get groups");
    }

    @ApiOperation(value = "getUsersByGroup", tags = {
            "MID" }, notes = "Update URL: users_with_group; Update Param: page_offset, page_size, user_group_name; Update Response: total_size")
    @GetMapping(value = "/users_with_group", produces = { HTTP_VND_APACHE_KYLIN_JSON,
            HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<UserGroupResponseKI>>> getUsersWithGroup(
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "user_group_name", required = false, defaultValue = "") String userGroupName)
            throws IOException {
        List<UserGroup> groups = userGroupService.getUserGroupsFilterByGroupName(userGroupName);
        List<UserGroup> subList = PagingUtil.cutPage(groups, pageOffset, pageSize);
        List<UserGroupResponseKI> result = userGroupService.getUserGroupResponse(subList);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(result, groups.size()),
                "get users with group and id");
    }

    @ApiOperation(value = "userAndGroup", tags = { "MID" })
    @GetMapping(value = "/users_and_groups")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<Map<String, List<String>>> getUsersAndGroups() throws IOException {
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, userGroupService.getUserAndUserGroup(), "");
    }

    @ApiOperation(value = "getUsersByGroup", tags = {
            "MID" }, notes = "Update URL: group_name; Update Param: group_name")
    @PostMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addUserGroup(@RequestBody UserGroupRequest addUserGroupRequest) throws IOException {
        checkGroupName(addUserGroupRequest.getGroupName());
        userGroupService.addGroup(addUserGroupRequest.getGroupName());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "add user group");
    }

    @ApiOperation(value = "getUsersByGroup", tags = {
            "MID" }, notes = "Update URL: group_name; Update Param: group_name")
    @DeleteMapping(value = "/{uuid:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delUserGroup(@PathVariable(value = "uuid") String groupUuid) throws IOException {
        String groupName = userGroupService.getGroupNameByUuid(groupUuid);
        userGroupService.deleteGroup(groupName);
        aclTCRService.revokeAclTCR(groupName, false);
        EventBusFactory.getInstance().postAsync(new AclTCRRevokeEventNotifier(groupName, true));
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "del user group");
    }

    //move users in/out from groups
    @ApiOperation(value = "updateUserAndGroup", tags = { "MID" })
    @PutMapping(value = "/users")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addOrDelUsers(@RequestBody UpdateGroupRequest updateGroupRequest)
            throws IOException {
        checkGroupName(updateGroupRequest.getGroup());
        userGroupService.modifyGroupUsers(updateGroupRequest.getGroup(), updateGroupRequest.getUsers());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "modify users in user group");
    }

    @ApiOperation(value = "getUsersByGroup", tags = {
            "MID" }, notes = "Update URL: group_name; Update Param: group_name")
    @PostMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchAddUserGroups(@RequestBody List<String> groups) {
        groups.forEach(this::checkGroupName);
        userGroupService.addGroups(groups);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "batch add user groups");
    }

    @ApiOperation(value = "getUsersByGroup", tags = {
            "MID" }, notes = "Update URL: group_name; Update Param: group_name")
    @DeleteMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchDelUserGroup(@RequestBody List<String> groups) throws IOException {
        for (String group : groups) {
            userGroupService.deleteGroup(group);
            aclTCRService.revokeAclTCR(group, false);
            EventBusFactory.getInstance().postAsync(new AclTCRRevokeEventNotifier(group, true));
        }
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "batch del user group");
    }

    public void checkGroupName(String groupName) {
        val msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(groupName)) {
            throw new KylinException(EMPTY_USERGROUP_NAME, msg.getEmptyGroupName());
        }
        if (groupName.startsWith(".")) {
            throw new KylinException(INVALID_USERGROUP_NAME, msg.getInvalidNameStartWithDot());
        }
        if (!groupName.equals(groupName.trim())) {
            throw new KylinException(INVALID_USERGROUP_NAME, msg.getInvalidNameStartOrEndWithBlank());
        }
        if (Pattern.compile("[^\\x00-\\xff]").matcher(groupName).find()) {
            throw new KylinException(INVALID_USERGROUP_NAME, msg.getInvalidNameContainsOtherCharacter());
        }
        if (Pattern.compile("[\\\\/:*?\"<>|]").matcher(groupName).find()) {
            throw new KylinException(INVALID_USERGROUP_NAME, msg.getInvalidNameContainsInlegalCharacter());
        }
    }
}
