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

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.util.PasswordEncodeFactory;
import org.apache.kylin.common.persistence.metadata.PersistException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.tool.garbage.StorageCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;

public class AdminUserInitCLI {
    protected static final Logger logger = LoggerFactory.getLogger(AdminUserInitCLI.class);

    public static final String ADMIN_USER_NAME = "ADMIN";
    public static final String ADMIN_USER_RES_PATH = "/_global/user/ADMIN";

    public static final Pattern PASSWORD_PATTERN = Pattern
            .compile("^(?=.*\\d)(?=.*[a-zA-Z])(?=.*[~!@#$%^&*(){}|:\"<>?\\[\\];',./`]).{8,}$");

    public static final String PASSWORD_VALID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
            + "~!@#$%^&*(){}|:\"<>?[];',./`";

    public static final int DEFAULT_PASSWORD_LENGTH = 8;

    public static void main(String[] args) {
        try {
            boolean randomPasswordEnabled = KylinConfig.getInstanceFromEnv().getRandomAdminPasswordEnabled();
            initAdminUser(randomPasswordEnabled);
        } catch (Exception e) {
            logger.error("Create Admin user failed.", e);
            Unsafe.systemExit(1);
        }
        Unsafe.systemExit(0);
    }

    public static void initAdminUser(boolean randomPasswordEnabled) throws Exception {
        val config = KylinConfig.getInstanceFromEnv();

        if ("ldap".equalsIgnoreCase(config.getSecurityProfile()) && !config.isRemoveLdapCustomSecurityLimitEnabled()) {
            return;
        }

        NKylinUserManager userManager = NKylinUserManager.getInstance(config);
        if (!randomPasswordEnabled) {
            return;
        }

        if (CollectionUtils.isNotEmpty(userManager.list())) {
            logger.info("The user has been initialized and does not need to be initialized again");
            return;
        }

        String password = generateRandomPassword();

        ManagedUser managedUser = new ManagedUser(ADMIN_USER_NAME,
                PasswordEncodeFactory.newUserPasswordEncoder().encode(password), true, ROLE_ADMIN,
                Constant.GROUP_ALL_USERS);
        managedUser.setUuid(RandomUtil.randomUUIDStr());

        val metaStore = ResourceStore.getKylinMetaStore(config).getMetadataStore();
        try {
            logger.info("Start init default user.");
            RawResource rawResource = new RawResource(ADMIN_USER_RES_PATH,
                    ByteSource.wrap(JsonUtil.writeValueAsBytes(managedUser)), System.currentTimeMillis(), 0L);
            metaStore.putResource(rawResource, null, UnitOfWork.DEFAULT_EPOCH_ID);

            String blackColorUsernameForPrint = StorageCleaner.ANSI_RESET + ADMIN_USER_NAME + StorageCleaner.ANSI_RED;
            String blackColorPasswordForPrint = StorageCleaner.ANSI_RESET + password + StorageCleaner.ANSI_RED;
            String info = String.format(Locale.ROOT,
                    "Create default user finished. The username of initialized user is [%s], which password is [%s].\n"
                            + "Please keep the password properly. And if you forget the password, you can reset it according to user manual.",
                    blackColorUsernameForPrint, blackColorPasswordForPrint);
            System.out.println(StorageCleaner.ANSI_RED + info + StorageCleaner.ANSI_RESET);
        } catch (PersistException e) {
            logger.warn("{} user has been created on another node.", ADMIN_USER_NAME);
        }
    }

    public static String generateRandomPassword() {
        String password;
        do {
            password = RandomStringUtils.random(DEFAULT_PASSWORD_LENGTH, PASSWORD_VALID_CHARS.toCharArray());
        } while (!PASSWORD_PATTERN.matcher(password).matches());
        return password;
    }
}
