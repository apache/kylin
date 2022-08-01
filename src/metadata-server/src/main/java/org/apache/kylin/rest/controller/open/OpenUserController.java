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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.UserInfoResponse;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.controller.NUserController;
import org.apache.kylin.rest.request.PasswordChangeRequest;
import org.apache.kylin.rest.request.UserRequest;
import org.springframework.beans.factory.annotation.Autowired;
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
@RequestMapping(value = "/api/user", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenUserController extends NBasicController {
    @Autowired
    private NUserController userController;

    @Autowired
    private UserService userService;

    @ApiOperation(value = "listAllUsers", tags = { "MID" })
    @GetMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<UserInfoResponse>>> listAllUsers(
            @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<ManagedUser> usersByFuzzyMatching = userService.getManagedUsersByFuzzMatching(name, isCaseSensitive);
        //LDAP users dose not have authorities
        for (ManagedUser u : usersByFuzzyMatching) {
            userService.completeUserInfo(u);
        }
        List<UserInfoResponse> userInfoResponses = usersByFuzzyMatching.stream().map(UserInfoResponse::new)
                .collect(Collectors.toList());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.get(userInfoResponses, pageOffset, pageSize), "");
    }

    @ApiOperation(value = "createUser", tags = { "MID" })
    @PostMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> createUser(@RequestBody ManagedUser user) throws IOException {
        return userController.createUser(user);
    }

    @ApiOperation(value = "updateUser", tags = { "MID" })
    @PutMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> updateUser(@RequestBody UserRequest user) throws IOException {
        return userController.updateUser(user);
    }

    @ApiOperation(value = "delete", tags = { "MID" })
    @DeleteMapping(value = "/{username:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delete(@PathVariable("username") String username) {
        return userController.delete(username);
    }

    @ApiOperation(value = "updateUserPassword", tags = { "MID" })
    @PutMapping(value = "/password")
    @ResponseBody
    public EnvelopeResponse<String> updateUserPassword(@RequestBody PasswordChangeRequest user) {
        return userController.updateUserPassword(user);
    }

    @ApiOperation(value = "deleteUser", tags = { "MID" })
    @DeleteMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> deleteUser(@RequestBody ManagedUser request) {
        return userController.delete(request.getUsername());
    }

    @ApiOperation(value = "deleteUser", tags = { "MID" })
    @DeleteMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchDelete(@RequestBody List<String> usernames) throws IOException {
        return userController.batchDelete(usernames);
    }

    @ApiOperation(value = "createUser", tags = {
            "MID" }, notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PostMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchCreate(@RequestBody List<ManagedUser> users) throws IOException {
        return userController.batchCreate(users);
    }
}
