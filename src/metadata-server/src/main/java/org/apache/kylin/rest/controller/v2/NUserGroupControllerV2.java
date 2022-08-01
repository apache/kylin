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
package org.apache.kylin.rest.controller.v2;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.controller.NBasicController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/user_group")
public class NUserGroupControllerV2 extends NBasicController {

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @ApiOperation(value = "getUsersWithGroup", tags = { "MID" })
    @GetMapping(value = "/usersWithGroup", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<Map<String, Object>> getUsersWithGroup(
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "userGroupName", required = false, defaultValue = "") String userGroupName)
            throws IOException {
        List<Pair<String, Set<String>>> usersWithGroup = new ArrayList<>();
        List<String> groups = userGroupService.getAuthoritiesFilterByGroupName(userGroupName);

        List<String> subList = PagingUtil.cutPage(groups, pageOffset, pageSize);
        for (String group : subList) {
            Set<String> groupMembers = new TreeSet<>();
            for (ManagedUser user : userGroupService.getGroupMembersByName(group)) {
                groupMembers.add(user.getUsername());
            }
            usersWithGroup.add(Pair.newPair(group, groupMembers));
        }
        Map<String, Object> result = new HashMap<>();
        result.put("usersWithGroup", usersWithGroup);
        result.put("size", groups.size());
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "get users with group");
    }

}
