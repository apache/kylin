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

import static org.apache.kylin.common.constant.Constants.HIDDEN_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.constant.Constant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

@MetadataInfo(onlyProps = true)
class ConfigServiceTest {

    private static final String PASS_KEY = "kylin.source.jdbc.pass";

    private final ConfigService configService = new ConfigService();

    @AfterEach
    void tearDown() {
        SecurityContextHolder.clearContext();
    }

    private void login(String role) {
        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken("U", "U", role));
    }

    @Test
    void testIsCloud() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        assertFalse(configService.isCloud());

        config.setProperty("kylin.env.channel", "cloud");
        assertTrue(configService.isCloud());
    }

    @Test
    void testFetchAllKeepsEncryptedValueWhenRandomKeyDisabled() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String encrypted = EncryptUtil.encryptWithPrefix("kylin");
        config.setProperty(PASS_KEY, encrypted);
        login(Constant.ROLE_ANALYST);

        assertEquals(encrypted, configService.fetchAll().getProperty(PASS_KEY));
    }

    @Test
    void testFetchAllHidesEncryptedValueFromNonAdmin() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty(PASS_KEY, EncryptUtil.encryptWithPrefix("kylin"));
        config.setProperty("kylin.random-encrypt-key.enabled", "true");
        login(Constant.ROLE_ANALYST);

        Properties properties = configService.fetchAll();
        assertEquals(HIDDEN_VALUE, properties.getProperty(PASS_KEY));
        // plain values are untouched
        assertEquals("cloud", properties.getProperty("kylin.env.channel", "cloud"));
    }

    @Test
    void testFetchAllExposesEncryptedValueToAdmin() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String encrypted = EncryptUtil.encryptWithPrefix("kylin");
        config.setProperty(PASS_KEY, encrypted);
        config.setProperty("kylin.random-encrypt-key.enabled", "true");
        login(Constant.ROLE_ADMIN);

        assertEquals(encrypted, configService.fetchAll().getProperty(PASS_KEY));
    }
}
