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

package org.apache.kylin.rest.security.saml;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.security.KylinUserManager;
import org.apache.kylin.rest.security.ManagedUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.saml.SAMLCredential;

/**
 * An simple implementation of SAMLUserDetailsService.
 * 
 * It is used to load/create Kylin user when authenticated by SAML IdP. Users are created when they are not exists
 * in Kylin system. Default authorities configured by {@link SAMLSimpleUserDetailsService#defaultAuthorities} are
 * applied to user during creation. It differs from the implementation
 * {@link org.apache.kylin.rest.security.SAMLUserDetailsService} which loads user information
 * from LDAP.
 */
public class SAMLSimpleUserDetailsService implements org.springframework.security.saml.userdetails.SAMLUserDetailsService {
    private static final Logger logger = LoggerFactory.getLogger(SAMLSimpleUserDetailsService.class);

    private static final String NO_EXISTENCE_PASSWORD = "NO_PASSWORD";

    private String[] defaultAuthorities = {"ALL_USERS"};

    public void setDefaultAuthorities(String[] defaultAuthorities) {
        this.defaultAuthorities = defaultAuthorities;
    }

    @Override
    public Object loadUserBySAML(SAMLCredential samlCredential) throws UsernameNotFoundException {
        final String userEmail = samlCredential.getAttributeAsString("email");
        logger.debug("samlCredential.email:" + userEmail);
        KylinUserManager userManager = KylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
        ManagedUser existUser = userManager.get(userEmail);
        // create if not exists
        if (existUser == null) {
            ManagedUser user = new ManagedUser(userEmail, NO_EXISTENCE_PASSWORD, true, defaultAuthorities);
            userManager.update(user);
        }
        return userManager.get(userEmail);
    }
}
