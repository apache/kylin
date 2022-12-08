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

import java.util.Collection;
import java.util.Map;

import org.apache.kylin.rest.service.LdapUserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.ldap.userdetails.LdapUserDetailsMapper;

public class LdapCaseIgnoreUserDetailsContextMapper extends LdapUserDetailsMapper {

    private static final Logger logger = LoggerFactory.getLogger(LdapCaseIgnoreUserDetailsContextMapper.class);

    @Autowired
    @Qualifier("userService")
    private LdapUserService ldapUserService;

    @Override
    public UserDetails mapUserFromContext(DirContextOperations ctx, String username,
            Collection<? extends GrantedAuthority> authorities) {
        String dn = ctx.getNameInNamespace();
        logger.debug("Mapping user details from context with DN {}", dn);
        Map<String, String> dnMap = ldapUserService.getDnMapperMap();
        String realName = dnMap.get(dn);
        logger.debug("ldap real name is {}", realName);
        return super.mapUserFromContext(ctx, realName, authorities);
    }

}
