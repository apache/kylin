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

package org.apache.kylin.rest.service;

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.metadata.user.ManagedUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.userdetails.UserDetails;

public class CaseInsensitiveKylinUserService extends KylinUserService {

    private final Logger logger = LoggerFactory.getLogger(CaseInsensitiveKylinUserService.class);

    @Override
    public boolean userExists(String userName) {
        logger.trace("judge user exist: {}", userName);
        return getKylinUserManager().exists(userName);
    }

    @Override
    public List<String> listAdminUsers() throws IOException {
        return listUsers().parallelStream()
                .filter(managedUser -> managedUser.getAuthorities().parallelStream().anyMatch(
                        simpleGrantedAuthority -> Constant.ROLE_ADMIN.equals(simpleGrantedAuthority.getAuthority())))
                .map(ManagedUser::getUsername).collect(Collectors.toList());
    }

    @Override
    public List<String> listNormalUsers() throws IOException {
        return listUsers().parallelStream()
                .filter(managedUser -> managedUser.getAuthorities().parallelStream().noneMatch(
                        simpleGrantedAuthority -> Constant.ROLE_ADMIN.equals(simpleGrantedAuthority.getAuthority())))
                .map(ManagedUser::getUsername).collect(Collectors.toList());
    }

    @Override
    public boolean isGlobalAdmin(String username) throws IOException {
        try {
            UserDetails userDetails = loadUserByUsername(username);
            return isGlobalAdmin(userDetails);
        } catch (Exception e) {
            logger.warn("Cat not load user by username {}", username, e);
        }

        return false;
    }

    @Override
    public boolean isGlobalAdmin(UserDetails userDetails) throws IOException {
        return userDetails != null && userDetails.getAuthorities().stream()
                .anyMatch(grantedAuthority -> grantedAuthority.getAuthority().equals(ROLE_ADMIN));
    }

    @Override
    public Set<String> retainsNormalUser(Set<String> usernames) throws IOException {
        Set<String> results = new HashSet<>();
        for (String username : usernames) {
            if (!isGlobalAdmin(username)) {
                results.add(username);
            }
        }
        return results;
    }

    @Override
    public boolean containsGlobalAdmin(Set<String> usernames) throws IOException {
        for (String username : usernames) {
            if (isGlobalAdmin(username)) {
                return true;
            }
        }
        return false;
    }
}
