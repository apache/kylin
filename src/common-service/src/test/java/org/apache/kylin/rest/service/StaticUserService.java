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

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

public class StaticUserService extends OpenUserService {
    private List<ManagedUser> users = Lists.newArrayList();

    @PostConstruct
    public void init() {
        ManagedUser admin = new ManagedUser();
        admin.addAuthorities(Constant.ROLE_ADMIN);
        admin.setPassword("123456");
        admin.setUsername("admin");
        admin.setDisabled(false);
        admin.setLocked(false);
        users.add(admin);
        ManagedUser test = new ManagedUser();
        test.addAuthorities(Constant.ROLE_ANALYST);
        test.setPassword("123456");
        test.setUsername("test");
        test.setDisabled(false);
        test.setLocked(false);
        users.add(test);
    }

    @Override
    public List<ManagedUser> listUsers() {
        return users;
    }

    @Override
    public List<String> listAdminUsers() {
        List<String> admins = Lists.newArrayList();
        for (ManagedUser user : users) {
            if (user.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))) {
                admins.add(user.getUsername());
            }
        }
        return admins;
    }

    @Override
    public boolean userExists(String s) {
        for (ManagedUser user : users) {
            if (s.equals(user.getUsername())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public UserDetails loadUserByUsername(String s) {
        for (ManagedUser user : users) {
            if (s.equals(user.getUsername())) {
                return user;
            }
        }
        throw new UsernameNotFoundException(s);
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
        // do nothing
    }
}
