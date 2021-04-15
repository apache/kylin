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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.UserGroupService;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

@Controller
@RequestMapping(value = "/user_group")
public class KylinUserGroupController extends BasicController {

    @Autowired
    @Qualifier("userGroupService")
    private UserGroupService userGroupService;

    @RequestMapping(value = "/groups", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse<String> listUserAuthorities(
            @RequestParam(value = "project", required = false) String project,
            @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "isFuzzMatch", required = false) boolean isFuzzMatch,
            @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) throws IOException {
        HashMap<String, Object> data = new HashMap<>();
        Map<String, List<String>> userGroupMap = userGroupService.getGroupMembersMap();
        Map<String, Set<String>> usersWithGroup = new HashMap<>();
        List<String> groupsByFuzzyMatching = getManagedGroupsByFuzzyMatching(name, isFuzzMatch, getAllGroups(project));
        List<String> subList = PagingUtil.cutPage(groupsByFuzzyMatching, offset, limit);
        for (String g : subList) {
            List<String> userNames = userGroupMap.get(g);
            usersWithGroup.put(g, (userNames == null ? Sets.newHashSet() : Sets.newHashSet(userNames)));
        }
        data.put("groups", usersWithGroup);
        data.put("size", groupsByFuzzyMatching.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/{name:.+}", method = { RequestMethod.POST }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse<String> addUserGroup(@PathVariable String name) throws IOException {
        userGroupService.addGroup(name);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    @RequestMapping(value = "/{name:.+}", method = { RequestMethod.DELETE }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse<String> delUserGroup(@PathVariable String name) throws IOException {
        if (StringUtils.equalsIgnoreCase(name, Constant.GROUP_ALL_USERS)
                || StringUtils.equalsIgnoreCase(name, Constant.ROLE_ADMIN)) {
            throw new InternalErrorException("Can not delete group " + name);
        }
        userGroupService.deleteGroup(name);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    //move users in/out from groups
    @RequestMapping(value = "/users/{name:.+}", method = { RequestMethod.POST, RequestMethod.PUT }, produces = {
            "application/json" })
    @ResponseBody
    public EnvelopeResponse<String> addOrDelUsers(@PathVariable String name, @RequestBody List<String> users)
            throws IOException {
        if (StringUtil.equals(name, Constant.ROLE_ADMIN) && users.size() == 0) {
            throw new InternalErrorException("role_admin must have at least one user");
        }
        userGroupService.modifyGroupUsers(name, users);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "");
    }

    //move users in/out from groups
    @RequestMapping(value = "/users/{name:.+}", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse<String> getUsersByGroup(@PathVariable String name) throws IOException {
        HashMap<String, Object> data = new HashMap<>();
        List<ManagedUser> users = userGroupService.getGroupMembersByName(name);
        data.put("users", users);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private List<String> getAllGroups(String project) throws IOException {
        return userGroupService.listAllAuthorities(project);
    }

    private List<String> getManagedGroupsByFuzzyMatching(String nameSeg, boolean isFuzzyMatch, List<String> groups) {
        //for name fuzzy matching
        if (StringUtils.isBlank(nameSeg)) {
            return groups;
        }

        List<String> groupsByFuzzyMatching = new ArrayList<>();
        for (String u : groups) {
            if (!isFuzzyMatch && StringUtils.equals(u, nameSeg)) {
                groupsByFuzzyMatching.add(u);
            }
            if (isFuzzyMatch && StringUtils.containsIgnoreCase(u, nameSeg)) {
                groupsByFuzzyMatching.add(u);
            }
        }
        return groupsByFuzzyMatching;
    }
}
