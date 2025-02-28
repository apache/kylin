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

package org.apache.kylin.metadata.user;

import java.util.Arrays;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.rest.constant.Constant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

class NKylinUserManagerTest extends NLocalFileMetadataTestCase {

    @BeforeEach
    void setUp() {
        this.createTestMetadata();
    }

    @AfterEach
    void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    void testGetAndExist() {
        NKylinUserManager manager = NKylinUserManager.getInstance(getTestConfig());

        // no user
        Assertions.assertFalse(manager.exists("noexist"));
        Assertions.assertFalse(manager.exists(null));

        // has ADMIN
        ManagedUser adminUser = new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                new SimpleGrantedAuthority(Constant.ROLE_MODELER)));
        manager.update(adminUser);
        Assertions.assertFalse(manager.exists("noexist"));

        // admin exists
        Assertions.assertTrue(manager.exists("ADMIN"));
        Assertions.assertTrue(manager.exists("admIN"));

        // get
        Assertions.assertNotNull(manager.get("ADMIN"));
        Assertions.assertNotNull(manager.get("admIN"));
        Assertions.assertNull(manager.get("notexist"));
        Assertions.assertNull(manager.get(null));
    }

    @Test
    void testNameSuffix() {
        NKylinUserManager manager = NKylinUserManager.getInstance(getTestConfig());
        ManagedUser normalUser = new ManagedUser("test_ut", "KYLIN", false, Arrays.asList(
                new SimpleGrantedAuthority(Constant.ROLE_ANALYST), new SimpleGrantedAuthority(Constant.ROLE_MODELER)));
        manager.update(normalUser);
        Assertions.assertTrue(manager.exists("test_ut"));
        Assertions.assertFalse(manager.exists("ut"));
        Assertions.assertNotNull(manager.get("test_ut"));
        Assertions.assertNull(manager.get("ut"));
    }
}
