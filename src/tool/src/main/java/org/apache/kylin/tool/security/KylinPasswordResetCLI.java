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

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.tool.MetadataTool;
import org.apache.kylin.tool.constant.StringConstant;
import org.apache.kylin.util.PasswordEncodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.password.PasswordEncoder;

import lombok.val;

public class KylinPasswordResetCLI {
    protected static final Logger logger = LoggerFactory.getLogger(KylinPasswordResetCLI.class);

    public static void main(String[] args) {
        int exit;
        try {
            exit = reset() ? 0 : 1;
        } catch (Exception e) {
            exit = 1;
            logger.warn("Fail to reset admin password.", e);
        }
        Unsafe.systemExit(exit);
    }

    public static boolean reset() throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (config.isQueryNodeOnly()) {
            logger.error("Only job/all node can update metadata.");
            return false;
        }

        PasswordEncoder pwdEncoder = PasswordEncodeFactory.newUserPasswordEncoder();
        String id = "USER_INFO/ADMIN";

        ResourceStore aclStore = ResourceStore.getKylinMetaStore(config);
        NKylinUserManager userManager = NKylinUserManager.getInstance(config);

        val user = userManager.get("ADMIN");
        if (user == null) {
            logger.warn("The password cannot be reset because there is no ADMIN user.");
            return false;
        }
        val res = aclStore.getResource(id);
        if (res == null) {
            logger.warn("The password cannot be reset because there is no ADMIN user.");
            return false;
        }
        user.clearAuthenticateFailedRecord();

        boolean randomPasswordEnabled = KylinConfig.getInstanceFromEnv().getRandomAdminPasswordEnabled();
        String password = randomPasswordEnabled ? AdminUserInitCLI.generateRandomPassword() : "KYLIN";

        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig conf = KylinConfig.getInstanceFromEnv();
            NKylinUserManager.getInstance(conf).updateUser("ADMIN", copyForWrite -> {
                copyForWrite.setPassword(pwdEncoder.encode(password));
                copyForWrite.setDefaultPassword(true);
            });
            return true;
        }, UnitOfWork.GLOBAL_UNIT);

        logger.trace("update user : {}", user.getUsername());
        logger.info("User {}'s password is set to default password.", user.getUsername());

        try {
            MetadataTool.backup(config);
        } catch (Exception e) {
            logger.error("metadata backup failed", e);
        }

        if (randomPasswordEnabled) {
            String blackColorUsernameForPrint = StringConstant.ANSI_RESET + AdminUserInitCLI.ADMIN_USER_NAME
                    + StringConstant.ANSI_RED;
            String blackColorPasswordForPrint = StringConstant.ANSI_RESET + password + StringConstant.ANSI_RED;
            String info = String.format(Locale.ROOT,
                    "Reset password of [%s] succeed. The password is [%s].\n" + "Please keep the password properly.",
                    blackColorUsernameForPrint, blackColorPasswordForPrint);
            System.out.println(StringConstant.ANSI_RED + info + StringConstant.ANSI_RESET);
        } else {
            System.out.println(
                    StringConstant.ANSI_YELLOW + "Reset the ADMIN password successfully." + StringConstant.ANSI_RESET);
        }

        return true;
    }
}
