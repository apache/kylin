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

package org.apache.kylin.rest.security;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_LOGIN_FAILED;

import org.apache.kylin.common.annotation.ThirdPartyDependencies;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.NUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;

@ThirdPartyDependencies({ @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager", classes = {
        "StaticAuthenticationProvider" }) })
public abstract class OpenAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(OpenAuthenticationProvider.class);

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired
    @Qualifier("userGroupService")
    NUserGroupService userGroupService;

    public UserService getUserService() {
        return userService;
    }

    public NUserGroupService getUserGroupService() {
        return userGroupService;
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        if (!authenticateImpl(authentication)) {
            logger.error("Failed to auth user: {}", authentication.getPrincipal());
            throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg());
        }
        ManagedUser user;
        try {
            user = (ManagedUser) getUserService().loadUserByUsername((String) authentication.getPrincipal());
        } catch (Exception e) {
            String userName = (String) authentication.getPrincipal();
            String password = (String) authentication.getCredentials();
            user = new ManagedUser(userName, password, true, Constant.GROUP_ALL_USERS);
        }
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(user,
                authentication.getCredentials(), user.getAuthorities());
        return token;
    }

    /**
     * The method verifys that whether the user being logged in is legal or not.
     * @param authentication : this object contains two key attributes
     *               principal, it is a String type and it represents the username uploaded from the page.
     *               credentials, it is a String type and it represents the unencrypted password uploaded from the page.
     * @return Whether you allow this user to log in
     */
    public abstract boolean authenticateImpl(Authentication authentication);

    @Override
    public final boolean supports(Class<?> aClass) {
        return true;
    }
}
