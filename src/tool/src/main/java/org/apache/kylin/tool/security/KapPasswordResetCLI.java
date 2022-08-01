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
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.util.PasswordEncodeFactory;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.user.NKylinUserManager;
import org.apache.kylin.tool.MaintainModeTool;
import org.apache.kylin.tool.MetadataTool;
import org.apache.kylin.tool.garbage.StorageCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.password.PasswordEncoder;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;

public class KapPasswordResetCLI {
    protected static final Logger logger = LoggerFactory.getLogger(KapPasswordResetCLI.class);

    public static void main(String[] args) {
        int exit;
        MaintainModeTool maintainModeTool = new MaintainModeTool("admin password resetting");
        maintainModeTool.init();
        try {
            maintainModeTool.markEpochs();
            if (EpochManager.getInstance().isMaintenanceMode()) {
                Runtime.getRuntime().addShutdownHook(new Thread(maintainModeTool::releaseEpochs));
            }
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
        String id = "/_global/user/ADMIN";

        ResourceStore aclStore = ResourceStore.getKylinMetaStore(config);
        NKylinUserManager userManager = NKylinUserManager.getInstance(config);

        val user = userManager.get("ADMIN");
        if (user == null) {
            logger.warn("The password cannot be reset because there is no ADMIN user.");
            return false;
        }
        boolean randomPasswordEnabled = KylinConfig.getInstanceFromEnv().getRandomAdminPasswordEnabled();
        String password = randomPasswordEnabled ? AdminUserInitCLI.generateRandomPassword() : "KYLIN";
        user.setPassword(pwdEncoder.encode(password));
        user.setDefaultPassword(true);

        val res = aclStore.getResource(id);

        if (res == null) {
            logger.warn("The password cannot be reset because there is no ADMIN user.");
            return false;
        }

        user.clearAuthenticateFailedRecord();

        UnitOfWork.doInTransactionWithRetry(
                () -> ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).checkAndPutResource(id,
                        ByteSource.wrap(JsonUtil.writeValueAsBytes(user)), aclStore.getResource(id).getMvcc()),
                UnitOfWork.GLOBAL_UNIT);

        logger.trace("update user : {}", user.getUsername());
        logger.info("User {}'s password is set to default password.", user.getUsername());

        try {
            MetadataTool.backup(config);
        } catch (Exception e) {
            logger.error("metadata backup failed", e);
        }

        if (randomPasswordEnabled) {
            String blackColorUsernameForPrint = StorageCleaner.ANSI_RESET + AdminUserInitCLI.ADMIN_USER_NAME
                    + StorageCleaner.ANSI_RED;
            String blackColorPasswordForPrint = StorageCleaner.ANSI_RESET + password + StorageCleaner.ANSI_RED;
            String info = String.format(Locale.ROOT,
                    "Reset password of [%s] succeed. The password is [%s].\n" + "Please keep the password properly.",
                    blackColorUsernameForPrint, blackColorPasswordForPrint);
            System.out.println(StorageCleaner.ANSI_RED + info + StorageCleaner.ANSI_RESET);
        } else {
            System.out.println(
                    StorageCleaner.ANSI_YELLOW + "Reset the ADMIN password successfully." + StorageCleaner.ANSI_RESET);
        }

        return true;
    }
}
