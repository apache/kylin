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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

/**
 * Handle user authentication request to protected kylin rest resources by
 * spring security.
 * 
 * @author xduo
 * 
 */
@Controller
@RequestMapping(value = "/user")
public class UserController {

    private static final Logger logger = LoggerFactory.getLogger(UserController.class);
    @Autowired
    UserService userService;

    @RequestMapping(value = "/authentication", method = RequestMethod.POST, produces = "application/json")
    public UserDetails authenticate() {
        return authenticatedUser();
    }

    @RequestMapping(value = "/authentication", method = RequestMethod.GET, produces = "application/json")
    public UserDetails authenticatedUser() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication == null) {
            logger.debug("authentication is null.");
            return null;
        }
        
        if (authentication.getPrincipal() instanceof UserDetails) {
            logger.debug("authentication.getPrincipal() is " + authentication.getPrincipal());
            return (UserDetails) authentication.getPrincipal();
        }


        if (authentication.getDetails() instanceof UserDetails) {
            logger.debug("authentication.getDetails() is " + authentication.getDetails());
            return (UserDetails) authentication.getDetails();
        }
        
        return null;
    }

    @RequestMapping(value = "/authentication/authorities", method = RequestMethod.GET, produces = "application/json")
    public List<String> getAuthorities() {
//        return userService.getUserAuthorities();
        ArrayList<String> lists = new ArrayList<>();
        lists.add("ROLE_ADMIN");
        lists.add("ROLE_MODELER");
        lists.add("ROLE_ANALYST");
        return lists;
    }

}
