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

package org.apache.kylin.rest.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

public class AclPermissionUtil {

    private AclPermissionUtil() {
        throw new IllegalStateException("Class AclPermissionUtil is an utility class !");
    }

    public static List<String> transformAuthorities(Collection<? extends GrantedAuthority> authorities) {
        Set<String> groups = new HashSet<>();
        for (GrantedAuthority authority : authorities) {
            groups.add(authority.getAuthority());
        }
        List<String> ret = new ArrayList<>(groups);
        return ret;
    }

    public static Set<String> getCurrentUserGroups() {
        Set<String> groups = new HashSet<>();
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null) {
            return groups;
        }
        Collection<? extends GrantedAuthority> authorities = auth.getAuthorities();
        for (GrantedAuthority authority : authorities) {
            groups.add(authority.getAuthority());
        }
        return groups;
    }
}
