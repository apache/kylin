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

import static org.apache.kylin.cache.cachemanager.CacheConstants.USER_CACHE;

import java.nio.charset.Charset;
import java.util.Arrays;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.UserService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.Assert;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.hash.HashFunction;
import org.apache.kylin.shaded.com.google.common.hash.Hashing;

/**
 * A wrapper class for the authentication provider; Will do something more for Kylin.
 */
public class KylinAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(KylinAuthenticationProvider.class);

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired
    private CacheManager cacheManager;

    //Embedded authentication provider
    private AuthenticationProvider authenticationProvider;

    private HashFunction hf = null;

    public KylinAuthenticationProvider(AuthenticationProvider authenticationProvider) {
        super();
        Assert.notNull(authenticationProvider, "The embedded authenticationProvider should not be null.");
        this.authenticationProvider = authenticationProvider;
        hf = Hashing.murmur3_128();
    }

    @PostConstruct
    public void init() {
        Preconditions.checkNotNull(cacheManager, "cacheManager is not injected yet");
    }
    
    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {

        byte[] hashKey = hf.hashString(authentication.getName() + authentication.getCredentials(), Charset.defaultCharset()).asBytes();
        String userKey = Arrays.toString(hashKey);

        Authentication authed;
        Cache.ValueWrapper authedUser = cacheManager.getCache(USER_CACHE).get(userKey);
        if (authedUser != null) {
            authed = (Authentication) authedUser.get();
            SecurityContextHolder.getContext().setAuthentication(authed);
        } else {
            try {
                authed = authenticationProvider.authenticate(authentication);

                ManagedUser user;

                if (authed.getDetails() == null) {
                    //authed.setAuthenticated(false);
                    throw new UsernameNotFoundException(
                            "User not found in LDAP, check whether he/she has been added to the groups.");
                }

                if (authed.getDetails() instanceof UserDetails) {
                    UserDetails details = (UserDetails) authed.getDetails();
                    user = new ManagedUser(details.getUsername(), details.getPassword(), false,
                            details.getAuthorities());
                } else {
                    user = new ManagedUser(authentication.getName(), "skippped-ldap", false, authed.getAuthorities());
                }
                Assert.notNull(user, "The UserDetail is null.");

                String username = user.getUsername();
                logger.debug("User {} authorities : {}", username, user.getAuthorities());
                if (!userService.userExists(username)) {
                    userService.createUser(user);
                } else if (needUpdateUser(user, username)) {
                    userService.updateUser(user);
                }

                cacheManager.getCache(USER_CACHE).put(userKey, authed);
            } catch (AuthenticationException e) {
                logger.error("Failed to auth user: " + authentication.getName(), e);
                throw e;
            }

            logger.debug("Authenticated user " + authed.toString());
        }

        return authed;
    }

    // in case ldap users changing.
    private boolean needUpdateUser(ManagedUser user, String username) {
        return KylinConfig.getInstanceFromEnv().getSecurityProfile().equals("ldap")
                && !userService.loadUserByUsername(username).equals(user);
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authenticationProvider.supports(authentication);
    }

    public AuthenticationProvider getAuthenticationProvider() {
        return authenticationProvider;
    }

    public void setAuthenticationProvider(AuthenticationProvider authenticationProvider) {
        this.authenticationProvider = authenticationProvider;
    }

}
