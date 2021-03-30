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

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.security.ManagedUser;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

public class UserServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Test
    public void testBasics() throws IOException {
        userService.deleteUser("MODELER");

        Assert.assertTrue(!userService.userExists("MODELER"));

        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        authorities.add(new SimpleGrantedAuthority(Constant.ROLE_ADMIN));
        ManagedUser user = new ManagedUser("MODELER", "PWD", false, authorities);
        userService.createUser(user);

        Assert.assertTrue(userService.userExists("MODELER"));

        UserDetails ud = userService.loadUserByUsername("MODELER");
        Assert.assertEquals("MODELER", ud.getUsername());
        Assert.assertEquals("PWD", ud.getPassword());
        Assert.assertEquals(Constant.ROLE_ADMIN, ud.getAuthorities().iterator().next().getAuthority());
        Assert.assertEquals(2, ud.getAuthorities().size());

    }

    @Test
    public void testDeleteAdmin() throws IOException {
        try {
            userService.deleteUser("ADMIN");
            throw new InternalErrorException();
        } catch (InternalErrorException e) {
            Assert.assertEquals(e.getMessage(), "User ADMIN is not allowed to be deleted.");
        }

    }
}
