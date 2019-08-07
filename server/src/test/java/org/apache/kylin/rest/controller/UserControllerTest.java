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

package org.apache.kylin.rest.controller;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;

import java.io.IOException;

/**
 * @author xduo
 */
public class UserControllerTest extends ServiceTestBase {

    @Autowired
    private UserController userController;

    private ManagedUser userAdmin = new ManagedUser("ADMIN", "KYLIN", false, Constant.ROLE_ADMIN);

    private ManagedUser userModeler = new ManagedUser("MODELER", "MODELER", false, Constant.ROLE_MODELER);

    @Test
    public void testLogin() throws IOException {
        logInWithUser(userAdmin);
        UserDetails userDetail = userController.authenticatedUser();
        ManagedUser user = (ManagedUser) userDetail;
        Assert.assertTrue(user.equals(userAdmin));
    }

    @Test
    public void testLoginAsAnotherUser() throws IOException {
        logInWithUser(userModeler);
        UserDetails userDetail = userController.authenticate();
        ManagedUser user = (ManagedUser) userDetail;
        Assert.assertTrue(user.equals(userModeler));
    }

    private void logInWithUser(ManagedUser user) {
        UsernamePasswordAuthenticationToken token = new UsernamePasswordAuthenticationToken(user, user.getPassword(),
                user.getAuthorities());
        token.setDetails(SecurityContextHolder.getContext().getAuthentication().getDetails());
        SecurityContextHolder.getContext().setAuthentication(token);
    }
}
