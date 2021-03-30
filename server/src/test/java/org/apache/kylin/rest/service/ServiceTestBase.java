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
import java.util.Arrays;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.ManagedUser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author xduo
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:applicationContext.xml", "classpath:kylinSecurity.xml",
        "classpath:kylinMetrics.xml" })
@ActiveProfiles("testing")
public class ServiceTestBase extends LocalFileMetadataTestCase {

    private static TestingServer server;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired
    @Qualifier("userGroupService")
    UserGroupService userGroupService;

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        server = new TestingServer(12181, true);
        server.start();
    }

    @AfterClass
    public static void tearDownResource() throws IOException {
        server.stop();
    }

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Broadcaster.getInstance(config).notifyClearAll();
        List<String> userGroups = userGroupService.getAllUserGroups();
        if (!userGroups.contains(Constant.GROUP_ALL_USERS)) {
            userGroupService.addGroup(Constant.GROUP_ALL_USERS);
        }
        if (!userGroups.contains(Constant.ROLE_ADMIN)) {
            userGroupService.addGroup(Constant.ROLE_ADMIN);
        }
        if (!userGroups.contains(Constant.ROLE_MODELER)) {
            userGroupService.addGroup(Constant.ROLE_MODELER);
        }
        if (!userGroups.contains(Constant.ROLE_ANALYST)) {
            userGroupService.addGroup(Constant.ROLE_ANALYST);
        }

        if (!userService.userExists("ADMIN")) {
            userService.createUser(new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                    new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
        }

        if (!userService.userExists("MODELER")) {
            userService.createUser(new ManagedUser("MODELER", "MODELER", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                    new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
        }

        if (!userService.userExists("ANALYST")) {
            userService.createUser(new ManagedUser("ANALYST", "ANALYST", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ANALYST))));
        }

    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    /**
     * better keep this method, otherwise cause error
     * org.apache.kylin.rest.service.TestBase.initializationError
     */
    @Test
    public void test() throws Exception {
    }
}
