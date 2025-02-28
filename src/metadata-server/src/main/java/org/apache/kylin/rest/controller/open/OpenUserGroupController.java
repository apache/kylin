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

package org.apache.kylin.rest.controller.open;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NUserGroupController;
import org.apache.kylin.rest.request.UpdateGroupRequest;
import org.apache.kylin.rest.request.UserGroupRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.UserInfoResponse;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
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

@Controller
@RequestMapping(value = "/api/user_group", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenUserGroupController extends NBasicController {
    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    private NUserGroupController userGroupController;

    @Autowired
    private UserService userService;

    @ApiOperation(value = "listGroups", tags = { "MID" })
    @GetMapping(value = "/groups")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<String>>> listGroups(
            @RequestParam(value = "group_name", required = false) String groupName,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<String> groups = userGroupService.listAllAuthorities();
        if (StringUtils.isNotBlank(groupName)) {
            groups = groups.stream().filter(group -> isCaseSensitive ? group.contains(groupName)
                    : StringUtils.containsIgnoreCase(group, groupName)).collect(Collectors.toList());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(groups, pageOffset, pageSize), "");
    }

    @ApiOperation(value = "getUsersByGroup", tags = { "MID" })
    @GetMapping(value = "/group_members/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<UserInfoResponse>>> getUsersByGroup(
            @PathVariable(value = "group_name") String groupName,
            @RequestParam(value = "username", required = false) String username,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<ManagedUser> members = userGroupService.getGroupMembersByName(groupName);
        if (StringUtils.isNotBlank(username)) {
            members = members.stream().filter(user -> StringUtils.containsIgnoreCase(user.getUsername(), username))
                    .collect(Collectors.toList());
        }
        //LDAP users dose not have authorities
        for (ManagedUser user : members) {
            userService.completeUserInfo(user);
        }
        List<UserInfoResponse> userInfoResponses = members.stream().map(UserInfoResponse::new)
                .collect(Collectors.toList());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.get(userInfoResponses, pageOffset, pageSize), "");
    }

    @ApiOperation(value = "addUserGroup", tags = { "MID" })
    @PostMapping(value = "/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addUserGroup(@PathVariable(value = "group_name") String groupName)
            throws IOException {
        UserGroupRequest request = new UserGroupRequest();
        request.setGroupName(groupName);
        return userGroupController.addUserGroup(request);
    }

    @ApiOperation(value = "addUserGroupWithBody", tags = { "MID" })
    @PostMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addUserGroupWithBody(@RequestBody UserGroupRequest request) throws IOException {
        return userGroupController.addUserGroup(request);
    }

    @ApiOperation(value = "delUserGroup", tags = { "MID" })
    @DeleteMapping(value = "/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delUserGroup(@PathVariable(value = "group_name") String groupName)
            throws IOException {
        String groupUuid = userGroupService.getUuidByGroupName(groupName);
        return userGroupController.delUserGroup(groupUuid);
    }

    @ApiOperation(value = "delUserGroupWithBody", tags = { "MID" })
    @DeleteMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delUserGroupWithBody(@RequestBody UserGroupRequest request) throws IOException {
        userGroupController.checkGroupName(request.getGroupName());
        String groupUuid = userGroupService.getUuidByGroupName(request.getGroupName());
        return userGroupController.delUserGroup(groupUuid);
    }

    @ApiOperation(value = "addOrDelUsersInGroup", tags = { "MID" })
    @PutMapping(value = "/users")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addOrDelUsersInGroup(@RequestBody UpdateGroupRequest updateGroupRequest)
            throws IOException {
        return userGroupController.addOrDelUsers(updateGroupRequest);
    }

    @ApiOperation(value = "getUsersByGroup", tags = {
            "MID" }, notes = "Update URL: group_name; Update Param: group_name")
    @PostMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchAddUserGroups(@RequestBody List<String> groups) {
        return userGroupController.batchAddUserGroups(groups);
    }

    @ApiOperation(value = "getUsersByGroup", tags = {
            "MID" }, notes = "Update URL: group_name; Update Param: group_name")
    @DeleteMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchDelUserGroup(@RequestBody List<String> groups) throws IOException {
        return userGroupController.batchDelUserGroup(groups);
    }
}
