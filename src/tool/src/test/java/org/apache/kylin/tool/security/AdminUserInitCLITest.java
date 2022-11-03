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

package org.apache.kylin.tool.security;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import org.apache.kylin.tool.garbage.StorageCleaner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import lombok.val;

public class AdminUserInitCLITest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testInitAdminUser() throws Exception {
        overwriteSystemProp("kylin.security.user-password-encoder", BCryptPasswordEncoder.class.getName());
        // before create admin user
        val config = KylinConfig.getInstanceFromEnv();
        NKylinUserManager beforeCreateAdminManager = NKylinUserManager.getInstance(config);
        Assert.assertEquals(0, beforeCreateAdminManager.list().size());

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output, false, Charset.defaultCharset().name()));

        // metadata without user, create admin user
        AdminUserInitCLI.initAdminUser(true);
        // clear cache, reload metadata
        ResourceStore.clearCache(config);
        config.clearManagers();
        NKylinUserManager afterCreateAdminManager = NKylinUserManager.getInstance(config);
        Assert.assertTrue(afterCreateAdminManager.exists("ADMIN"));

        // assert output on console
        Assert.assertTrue(output.toString(Charset.defaultCharset().name())
                .startsWith(StorageCleaner.ANSI_RED
                        + "Create default user finished. The username of initialized user is ["
                        + StorageCleaner.ANSI_RESET + "ADMIN" + StorageCleaner.ANSI_RED + "], which password is "));
        Assert.assertTrue(output.toString(Charset.defaultCharset().name())
                .endsWith("Please keep the password properly. "
                        + "And if you forget the password, you can reset it according to user manual."
                        + StorageCleaner.ANSI_RESET + "\n"));

        System.setOut(System.out);

        // already have admin user
        AdminUserInitCLI.initAdminUser(true);
        // clear cache, reload metadata
        ResourceStore.clearCache(config);
        config.clearManagers();
        NKylinUserManager afterCreateAdminManager2 = NKylinUserManager.getInstance(config);
        Assert.assertEquals(1, afterCreateAdminManager2.list().size());
    }

    @Test
    public void testGenerateRandomPassword() {
        String password = AdminUserInitCLI.generateRandomPassword();
        Assert.assertTrue(AdminUserInitCLI.PASSWORD_PATTERN.matcher(password).matches());
    }

    @Test
    public void testSkipCreateAdminUserInLdapProfile() throws Exception {
        overwriteSystemProp("kylin.security.profile", "ldap");
        AdminUserInitCLI.initAdminUser(true);
        NKylinUserManager userManager = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
        Assert.assertTrue(userManager.list().isEmpty());
    }

    @Test
    public void testOpenLdapCustomSecurityLimit() throws Exception {
        overwriteSystemProp("kylin.security.remove-ldap-custom-security-limit-enabled", "true");
        overwriteSystemProp("kylin.security.user-password-encoder", BCryptPasswordEncoder.class.getName());
        AdminUserInitCLI.initAdminUser(true);
        val config = KylinConfig.getInstanceFromEnv();
        ResourceStore.clearCache(config);
        config.clearManagers();
        NKylinUserManager userManager = NKylinUserManager.getInstance(config);
        Assert.assertTrue(userManager.exists("ADMIN"));
    }
}
