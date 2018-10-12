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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.util.Assert;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * A wrapper class for the authentication provider; Will do something more for Kylin.
 */
public class KylinAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(KylinAuthenticationProvider.class);

    private final static com.google.common.cache.Cache<String, Authentication> userCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .removalListener(new RemovalListener<String, Authentication>() {
                @Override
                public void onRemoval(RemovalNotification<String, Authentication> notification) {
                    KylinAuthenticationProvider.logger.debug("User cache {} is removed due to {}",
                            notification.getKey(), notification.getCause());
                }
            }).build();

    @Autowired
    @Qualifier("userService")
    UserService userService;

    //Embedded authentication provider
    private AuthenticationProvider authenticationProvider;

    private HashFunction hf = null;

    public KylinAuthenticationProvider(AuthenticationProvider authenticationProvider) {
        super();
        Assert.notNull(authenticationProvider, "The embedded authenticationProvider should not be null.");
        this.authenticationProvider = authenticationProvider;
        hf = Hashing.murmur3_128();
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {

        byte[] hashKey = hf.hashString(authentication.getName() + authentication.getCredentials()).asBytes();
        String userKey = Arrays.toString(hashKey);

        if (userService.isEvictCacheFlag()) {
            userCache.invalidateAll();
            userService.setEvictCacheFlag(false);
        }
        Authentication authed = userCache.getIfPresent(userKey);

        if (null != authed) {
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
                } else if (!userService.loadUserByUsername(username).equals(user)) {
                    // in case ldap users changing.
                    userService.updateUser(user);
                }

                userCache.put(userKey, authed);
            } catch (AuthenticationException e) {
                logger.error("Failed to auth user: " + authentication.getName(), e);
                throw e;
            }

            logger.debug("Authenticated user " + authed.toString());
        }

        return authed;
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
