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

package org.apache.kylin.common;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFsSwitch {
    private static final Logger logger = LoggerFactory.getLogger(ReadFsSwitch.class);

    static volatile boolean fsOrFsBackup = false;
    static volatile long fsBackupCount = 0;
    static volatile long resetMillis = 0L;

    public static String select(String fileSystem, String fileSystemBackup) {
        if (resetMillis > 0 && System.currentTimeMillis() > resetMillis) {
            fsOrFsBackup = false;
            resetMillis = 0;
            logger.info("Backup read FS is back to off, switch=" + fsOrFsBackup);
        }

        if (fsOrFsBackup) {
            if (fsBackupCount++ % 100 == 0)
                logger.info("Returning backup read FS: " + fileSystemBackup + "  (empty means the origin working-dir)");

            return fileSystemBackup;
        } else {
            return fileSystem;
        }
    }

    public static void turnOnBackupFsWhile() {
        if (!fsOrFsBackup && resetMillis == 0) {
            fsOrFsBackup = true;
            fsBackupCount = 0;
            int sec = KapConfig.getInstanceFromEnv().getParquetReadFileSystemBackupResetSec();
            resetMillis = System.currentTimeMillis() + 1000L * sec; // 1 min later
            logger.info("Backup read FS is on for {} sec, switch={}", sec, fsOrFsBackup);
        }
    }

    public static boolean turnOnSwitcherIfBackupFsAllowed(Throwable ex, String backupFsAllowedStrings) {
        // prevents repeated entrance, this method MUST NOT return true for the same query more than once
        if (!fsOrFsBackup && resetMillis == 0) {
            while (ex != null) {
                String exceptionClassName = ex.getClass().getName().toLowerCase(Locale.ROOT);
                String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase(Locale.ROOT) : "";
                for (String backupFsAllowedString : backupFsAllowedStrings.split(",")) {
                    if (exceptionClassName.contains(backupFsAllowedString) || msg.contains(backupFsAllowedString)) {
                        turnOnBackupFsWhile();
                        return true;
                    }
                    ex = ex.getCause();
                }
            }
        }
        return false;
    }
}
