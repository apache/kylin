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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.constant.Constant;
import org.springframework.ldap.core.ContextSource;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;

/**
 * @author xduo
 * 
 */
public class AuthoritiesPopulator extends DefaultLdapAuthoritiesPopulator {

    SimpleGrantedAuthority adminRoleAsAuthority;

    SimpleGrantedAuthority adminAuthority = new SimpleGrantedAuthority(Constant.ROLE_ADMIN);
    SimpleGrantedAuthority modelerAuthority = new SimpleGrantedAuthority(Constant.ROLE_MODELER);
    SimpleGrantedAuthority analystAuthority = new SimpleGrantedAuthority(Constant.ROLE_ANALYST);

    Set<GrantedAuthority> defaultAuthorities = new HashSet<GrantedAuthority>();

    /**
     * @param contextSource
     * @param groupSearchBase
     */
    public AuthoritiesPopulator(ContextSource contextSource, String groupSearchBase, String adminRole, String defaultRole) {
        super(contextSource, groupSearchBase);
        this.adminRoleAsAuthority = new SimpleGrantedAuthority(adminRole);

        String[] defaultRoles = StringUtils.split(defaultRole, ",");
        if (ArrayUtils.contains(defaultRoles, Constant.ROLE_MODELER))
            this.defaultAuthorities.add(modelerAuthority);
        if (ArrayUtils.contains(defaultRoles, Constant.ROLE_ANALYST))
            this.defaultAuthorities.add(analystAuthority);
    }

    @Override
    public Set<GrantedAuthority> getGroupMembershipRoles(String userDn, String username) {
        Set<GrantedAuthority> authorities = super.getGroupMembershipRoles(userDn, username);

        authorities.addAll(defaultAuthorities);

        if (authorities.contains(adminRoleAsAuthority)) {
            authorities.add(adminAuthority);
            authorities.add(modelerAuthority);
            authorities.add(analystAuthority);
        }

        if (authorities.contains(modelerAuthority)) {
            authorities.add(analystAuthority);
        }

        return authorities;
    }

}
