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

package org.apache.kylin.rest.security.cas;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.security.KylinUserManager;
import org.apache.kylin.rest.security.ManagedUser;
import org.jasig.cas.client.authentication.AttributePrincipal;
import org.jasig.cas.client.validation.Assertion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.CredentialsExpiredException;
import org.springframework.security.cas.userdetails.AbstractCasAssertionUserDetailsService;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An implementation of AbstractCasAssertionUserDetailsService
 * 
 * It is used to load/create Kylin user when authenticated by CAS server. Users are created when they are not exists
 * in Kylin system. Default authorities configured by {@link CasUserDetailsService#defaultAuthorities} are
 * applied to user during creation.
 */
public class CasUserDetailsService extends AbstractCasAssertionUserDetailsService {
    private static final Logger logger = LoggerFactory.getLogger(CasUserDetailsService.class);

    // a default password that should not exists in user stores.
    // since encryption is applied to the plain text, a simple value like 'NO_PASSWORD' is okay.
    private static final String NON_EXISTENT_PASSWORD_VALUE = "NO_PASSWORD";

    private String[] defaultAuthorities = {"ALL_USERS"};

    public void setDefaultAuthorities(String[] defaultAuthorities) {
        this.defaultAuthorities = defaultAuthorities;
    }

    @Override
    protected UserDetails loadUserDetails(Assertion assertion) {
        if (assertion == null) {
            throw new CredentialsExpiredException("bad assertion");
        }
        ManagedUser user = parseUserDetails(assertion);
        // create user if not exists
        KylinUserManager kylinUserManager = KylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
        ManagedUser existUser = kylinUserManager.get(user.getUsername());
        if (existUser == null) {
            kylinUserManager.update(user);
        }
        return kylinUserManager.get(user.getUsername());
    }

    protected ManagedUser parseUserDetails(Assertion assertion) {
        AttributePrincipal principal = assertion.getPrincipal();
        List<GrantedAuthority> grantedAuthorities = Stream.of(defaultAuthorities)
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toList());
        return new ManagedUser(principal.getName(), NON_EXISTENT_PASSWORD_VALUE, true, grantedAuthorities);
    }
}
