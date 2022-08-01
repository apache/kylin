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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinUserServiceTest extends NLocalFileMetadataTestCase {

    private KylinUserService kylinUserService;

    @Before
    public void setup() {
        createTestMetadata();
        kylinUserService = Mockito.spy(new KylinUserService());
        NKylinUserManager userManager = NKylinUserManager.getInstance(getTestConfig());
        userManager.update(new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void loadUserByUsername() {
    }

    @Test
    public void testLoadUser() {
        UserDetails user = kylinUserService.loadUserByUsername("ADMIN");
        Assert.assertEquals("ADMIN", user.getUsername());
        user = kylinUserService.loadUserByUsername("AdMIn");
        Assert.assertEquals("ADMIN", user.getUsername());
    }

    @Test(expected = UsernameNotFoundException.class)
    public void testLoadUserWithWhiteSpace() {
        String username = "ADMI N";
        kylinUserService.loadUserByUsername(username);
    }

    @Test
    public void testUpdateUser() {
        String username = "ADMIN";
        ManagedUser user = (ManagedUser) kylinUserService.loadUserByUsername(username);
        Assert.assertFalse(user.isLocked());
        user.setLocked(true);
        kylinUserService.updateUser(user);
        user = (ManagedUser) kylinUserService.loadUserByUsername(username);
        Assert.assertTrue(user.isLocked());
        user.setLocked(false);
        kylinUserService.updateUser(user);
    }

    @Test
    public void testUserExists() {
        ManagedUser user = new ManagedUser();
        user.setUsername("tTtUser");
        List<SimpleGrantedAuthority> roles = new ArrayList<>();
        roles.add(new SimpleGrantedAuthority("ALL_USERS"));
        user.setGrantedAuthorities(roles);
        kylinUserService.createUser(user);
        Assert.assertTrue(kylinUserService.userExists("tTtUser"));
        Assert.assertTrue(kylinUserService.userExists("tttuser"));
        Assert.assertTrue(kylinUserService.userExists("TTTUSER"));
        Assert.assertFalse(kylinUserService.userExists("NOTEXIST"));
    }

}
