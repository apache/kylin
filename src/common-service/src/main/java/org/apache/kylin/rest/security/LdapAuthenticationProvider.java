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

import org.apache.kylin.rest.service.LdapUserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.Assert;

import org.apache.kylin.guava30.shaded.common.hash.HashFunction;
import org.apache.kylin.guava30.shaded.common.hash.Hashing;

/**
 * A wrapper class for the authentication provider; Will do something more for Kylin.
 */
public class LdapAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(LdapAuthenticationProvider.class);

    @Autowired
    @Qualifier("userService")
    LdapUserService ldapUserService;

    private final AuthenticationProvider authenticationProvider;

    private final HashFunction hf;

    public LdapAuthenticationProvider(AuthenticationProvider authenticationProvider) {
        Assert.notNull(authenticationProvider, "The embedded authenticationProvider should not be null.");
        this.authenticationProvider = authenticationProvider;
        this.hf = Hashing.murmur3_128();
    }

    @Override
    public Authentication authenticate(Authentication authentication) {
        Authentication auth = null;
        try {
            auth = authenticationProvider.authenticate(authentication);
        } catch (BadCredentialsException e) {
            throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg(), e);
        } catch (AuthenticationException ae) {
            logger.error("Failed to auth user: {}", authentication.getName(), ae);
            throw ae;
        } catch (IncorrectResultSizeDataAccessException e) {
            logger.error("Ldap username {} is not unique", authentication.getName());
            throw new BadCredentialsException(USER_LOGIN_FAILED.getMsg(), e);
        }

        if (auth.getDetails() == null) {
            throw new UsernameNotFoundException(
                    "User not found in LDAP, check whether he/she has been added to the groups.");
        }

        String userName = auth.getDetails() instanceof UserDetails ? ((UserDetails) auth.getDetails()).getUsername()
                : authentication.getName();

        ldapUserService.onUserAuthenticated(userName);
        logger.debug("Authenticated userName: {}", userName);
        return auth;
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authenticationProvider.supports(authentication);
    }
}
