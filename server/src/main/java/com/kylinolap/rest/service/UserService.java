/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;

import com.kylinolap.rest.security.UserManager;

/**
 * @author xduo
 * 
 */
@Component("userService")
public class UserService implements UserManager{

    @Autowired
    protected UserManager userManager;

    public List<String> getUserAuthorities() {
       return userManager.getUserAuthorities();
    }

    @Override
    public void createUser(UserDetails user) {
        userManager.createUser(user);
    }

    @Override
    public void updateUser(UserDetails user) {
        userManager.updateUser(user);
    }

    @Override
    public void deleteUser(String username) {
        userManager.deleteUser(username);
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        userManager.changePassword(oldPassword, newPassword);
    }

    @Override
    public boolean userExists(String username) {
        return userManager.userExists(username);
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        return userManager.loadUserByUsername(username);
    }

}
