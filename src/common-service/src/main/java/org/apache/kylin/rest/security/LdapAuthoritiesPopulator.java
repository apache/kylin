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

import java.util.Set;

import javax.naming.directory.SearchControls;

import org.apache.kylin.rest.constant.Constant;
import org.springframework.ldap.core.ContextSource;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class LdapAuthoritiesPopulator extends DefaultLdapAuthoritiesPopulator {

    private SimpleGrantedAuthority adminRoleAsAuthority;

    public LdapAuthoritiesPopulator(ContextSource contextSource, String groupSearchBase, String adminRole,
            SearchControls searchControls) {
        super(contextSource, groupSearchBase);
        setConvertToUpperCase(false);
        setRolePrefix("");
        this.adminRoleAsAuthority = new SimpleGrantedAuthority(adminRole);
        this.getLdapTemplate().setSearchControls(searchControls);
    }

    @Override
    public Set<GrantedAuthority> getGroupMembershipRoles(String userDn, String username) {
        Set<GrantedAuthority> authorities = super.getGroupMembershipRoles(userDn, username);
        Set<GrantedAuthority> userAuthorities = Sets.newHashSet(authorities);
        if (authorities.contains(adminRoleAsAuthority)) {
            userAuthorities.add(new SimpleGrantedAuthority(Constant.ROLE_ADMIN));
        }
        return userAuthorities;
    }
}
