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

import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.PagingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Handle user authentication request to protected kylin rest resources by
 * spring security.
 *
 * @author xduo
 *
 */
@Controller
@RequestMapping(value = "/user")
public class UserController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(UserController.class);
    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @RequestMapping(value = "/authentication", method = RequestMethod.POST, produces = { "application/json" })
    public UserDetails authenticate() {
        UserDetails userDetails = authenticatedUser();
        logger.debug("User login: {}", userDetails);
        return userDetails;
    }

    @RequestMapping(value = "/authentication", method = RequestMethod.GET, produces = { "application/json" })
    public UserDetails authenticatedUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication == null) {
            logger.debug("authentication is null.");
            return null;
        }

        if (authentication.getPrincipal() instanceof UserDetails) {
            return (UserDetails) authentication.getPrincipal();
        }

        if (authentication.getDetails() instanceof UserDetails) {
            return (UserDetails) authentication.getDetails();
        }

        return null;
    }

    @RequestMapping(value = "/users", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse listAllUsers(@RequestParam(value = "project", required = false) String project,
                                         @RequestParam(value = "name", required = false) String name,
                                         @RequestParam(value = "groupName", required = false) String groupName,
                                         @RequestParam(value = "isFuzzMatch", required = false) boolean isFuzzMatch,
                                         @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
                                         @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit) throws IOException {
        if (project == null) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectAdminPermission(project);
        }
        HashMap<String, Object> data = new HashMap<>();
        List<ManagedUser> usersByFuzzMatching = userService.listUsers();
        List<ManagedUser> subList = PagingUtil.cutPage(usersByFuzzMatching, offset, limit);
        //LDAP users dose not have authorities
        for (ManagedUser u : subList) {
            userService.completeUserInfo(u);
        }
        data.put("users", subList);
        data.put("size", usersByFuzzMatching.size());
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }
}
