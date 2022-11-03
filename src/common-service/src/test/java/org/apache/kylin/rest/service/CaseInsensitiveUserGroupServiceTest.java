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

import java.util.Arrays;
import java.util.Set;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.util.ReflectionTestUtils;

public class CaseInsensitiveUserGroupServiceTest extends NLocalFileMetadataTestCase {

    private CaseInsensitiveUserGroupService userGroupService;

    private CaseInsensitiveKylinUserService kylinUserService;

    @Before
    public void setup() {
        createTestMetadata();
        overwriteSystemProp("kylin.metadata.key-case-insensitive", "true");
        kylinUserService = Mockito.spy(new CaseInsensitiveKylinUserService());
        userGroupService = Mockito.spy(new CaseInsensitiveUserGroupService());
        ReflectionTestUtils.setField(userGroupService, "userService", kylinUserService);
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
    public void testListUserGroups() {
        Set<String> userGroups = userGroupService.listUserGroups("ADMIN");
        Assert.assertEquals(4, userGroups.size());
        Assert.assertTrue(userGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(userGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(userGroups.contains(Constant.ROLE_MODELER));
        Assert.assertTrue(userGroups.contains(Constant.GROUP_ALL_USERS));

        userGroups = userGroupService.listUserGroups("AdMiN");
        Assert.assertEquals(4, userGroups.size());
        Assert.assertTrue(userGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(userGroups.contains(Constant.ROLE_ADMIN));
        Assert.assertTrue(userGroups.contains(Constant.ROLE_MODELER));
        Assert.assertTrue(userGroups.contains(Constant.GROUP_ALL_USERS));

        userGroups = userGroupService.listUserGroups("NOTEXISTS");
        Assert.assertEquals(0, userGroups.size());
    }
}
