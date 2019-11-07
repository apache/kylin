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

package org.apache.kylin.rest.security;

import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CustomUserDetailServiceTest extends LocalFileMetadataTestCase {
    CustomUserDetailService userService;

    @Before
    public void setUp() throws Exception {
        System.setProperty(KylinConfig.KYLIN_CONF, LOCALMETA_TEST_DATA);
        this.createTestMetadata();
        this.userService = new CustomUserDetailService();
        this.userService.init();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testLoadUserByUsername() {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("test_meta/kylin-user.yaml");
        String configFile = resource.getFile();
        SecurityUsers users = userService.loadYamlInternal(configFile, SecurityUsers.class);
        Assert.assertNotNull("loadYamlInternal config user empty", users);
    }

    @Test(expected = InternalErrorException.class)
    public void testLoadUserByUsernameWithException() {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("test_meta/kylin-user-empty.yaml");
        String configFile = resource.getFile();
        SecurityUsers users = userService.loadYamlInternal(configFile, SecurityUsers.class);
    }

    @Test
    public void loadUserFromProperties() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.security.auth.config.filename", "test_meta/kylin-user.properties");
        userService.loadUser();
        ConcurrentHashMap<String, CustomUser> userMap = userService.getUserMap();
        Assert.assertNotNull("load user config with kylin.properties error, result is null", userMap);
    }
}
