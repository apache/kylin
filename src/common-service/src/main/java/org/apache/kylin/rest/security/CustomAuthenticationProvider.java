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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.util.Assert;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CustomAuthenticationProvider implements AuthenticationProvider {

    private static final Logger logger = LoggerFactory.getLogger(CustomAuthenticationProvider.class);

    private static Map<String, String> cache = new HashMap<>();//temporary, need to replace by memcache

    private CustomAuthenticator authenticator;

    private CustomAuthoritiesPopulator authoritiesPopulator;

    private HashFunction hf;

    public CustomAuthenticationProvider(CustomAuthenticator authenticator, CustomAuthoritiesPopulator authoritiesPopulator){
        Assert.notNull(authenticator, "The authenticator should not be null.");
        this.hf = Hashing.murmur3_128();
        this.authenticator = authenticator;
        this.authoritiesPopulator = authoritiesPopulator;
    }


    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        String userName = authentication.getName();
        byte[] hashKey = hf.hashString(userName + authentication.getCredentials(),
                Charset.defaultCharset()).asBytes();

        String userKey = Arrays.toString(hashKey);
        String authedUser = cache.get(userKey);
        if(authedUser == null){
            authenticator.authentication(authentication);
            cache.put(userKey, "success");
        }

        UserDetails userDetails = authoritiesPopulator.loadUserByUserName(userName);
        Authentication authed = new UsernamePasswordAuthenticationToken(userDetails, authentication.getCredentials(),
                new ArrayList<>(userDetails.getAuthorities()));
        logger.debug("User {} authorities : {}", authed.getName(), authed.getAuthorities());//getAuthorities: permission list
        SecurityContextHolder.getContext().setAuthentication(authed);

        return authed;
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return true;
    }
}
