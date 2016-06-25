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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.ldap.userdetails.LdapUserDetailsService;
import org.springframework.security.saml.SAMLCredential;

/**
 * An implementation of SAMLUserDetailsService by delegating the query to LdapUserDetailsService.
 */
public class SAMLUserDetailsService implements org.springframework.security.saml.userdetails.SAMLUserDetailsService {

    private static final Logger logger = LoggerFactory.getLogger(SAMLUserDetailsService.class);
    private LdapUserDetailsService ldapUserDetailsService;

    public SAMLUserDetailsService(LdapUserDetailsService ldapUserDetailsService) {
        this.ldapUserDetailsService = ldapUserDetailsService;
    }

    @Override
    public Object loadUserBySAML(SAMLCredential samlCredential) throws UsernameNotFoundException {
        final String userEmail = samlCredential.getAttributeAsString("email");
        logger.debug("samlCredential.email:" + userEmail);
        final String userName = userEmail.substring(0, userEmail.indexOf("@"));

        UserDetails userDetails = null;
        try {
            userDetails = ldapUserDetailsService.loadUserByUsername(userName);
        } catch (org.springframework.security.core.userdetails.UsernameNotFoundException e) {
            logger.error("User not found in LDAP, check whether he/she has been added to the groups.", e);
        }
        logger.debug("userDeail by search ldap with '" + userName + "' is: " + userDetails);
        return userDetails;
    }
}
