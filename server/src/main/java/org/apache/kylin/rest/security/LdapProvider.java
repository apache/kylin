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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.authentication.LdapAuthenticator;
import org.springframework.security.ldap.userdetails.LdapAuthoritiesPopulator;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * @author xduo
 * @deprecated replaced by KylinAuthenticationProvider
 * 
 */
public class LdapProvider extends LdapAuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(LdapProvider.class);

    @Autowired
    UserService userService;

    @Autowired
    private CacheManager cacheManager;

    MessageDigest md = null;

    /**
     * @param authenticator
     * @param authoritiesPopulator
     */
    public LdapProvider(LdapAuthenticator authenticator, LdapAuthoritiesPopulator authoritiesPopulator) {
        super(authenticator, authoritiesPopulator);

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to init Message Digest ", e);
        }
    }

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        Authentication authed = null;
        Cache userCache = cacheManager.getCache("UserCache");
        md.reset();
        byte[] hashKey = md.digest((authentication.getName() + authentication.getCredentials()).getBytes());
        String userKey = Arrays.toString(hashKey);

        Element authedUser = userCache.get(userKey);
        if (null != authedUser) {
            authed = (Authentication) authedUser.getObjectValue();
            SecurityContextHolder.getContext().setAuthentication(authed);
        } else {
            try {
                authed = super.authenticate(authentication);
                userCache.put(new Element(userKey, authed));
            } catch (AuthenticationException e) {
                logger.error("Failed to auth user: " + authentication.getName(), e);
                throw e;
            }

            UserDetails user = new User(authentication.getName(), "skippped-ldap", authed.getAuthorities());

            if (!userService.userExists(authentication.getName())) {
                userService.createUser(user);
            } else {
                userService.updateUser(user);
            }
        }

        return authed;
    }
}
