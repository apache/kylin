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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

public class KylinUserGroupService implements IUserGroupService {

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Override
    public List<String> listAllAuthorities(String project) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        return getAllUserAuthorities();
    }

    List<String> getAllUserAuthorities() throws IOException {
        List<String> all = new ArrayList<>();
        for (UserDetails user : userService.listUsers()) {
            for (GrantedAuthority auth : user.getAuthorities()) {
                if (!all.contains(auth.getAuthority())) {
                    all.add(auth.getAuthority());
                }
            }
        }
        return all;
    }

    @Override
    public boolean exists(String name) throws IOException {
        return getAllUserAuthorities().contains(name);
    }
}
